// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <errno.h>
#include <stdlib.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "internal/handle_garble.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::Field;
using ::testing::NotNull;

class MwTest : public BasicFixture {
 public:
  void SetUp() override {
    if (!Introspection().SupportsType2() && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Skipping due to lack of MW support.";
    }
  }

 protected:
  static constexpr int kBufferMemoryPages = 4;
  static constexpr uint32_t kRKey = 1024;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
    ibv_mr* mr;
    ibv_cq* cq;
    ibv_qp* qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
    if (!setup.context) {
      return absl::InternalError("Failed to register mr.");
    }
    memset(setup.buffer.data(), 0, setup.buffer.size());
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.qp = ibv_.CreateQp(setup.pd, setup.cq);
    if (!setup.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
    return setup;
  }
};

TEST_F(MwTest, Alloc) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  EXPECT_THAT(mw, NotNull());
}

TEST_F(MwTest, DeallocInvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(mw->handle);
  EXPECT_EQ(ibv_dealloc_mw(mw), ENOENT);
}

TEST_F(MwTest, Bind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  verbs_util::PostType1Bind(setup.qp, mw, bind_arg);
  EXPECT_THAT(verbs_util::WaitForCompletion(setup.cq),
              IsOkAndHolds(Field(&ibv_wc::status, IBV_WC_SUCCESS)));
}

TEST_F(MwTest, BindType1ReadWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), mr,
                                          IBV_ACCESS_REMOTE_READ),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_SUCCESS
                               : IBV_WC_MW_BIND_ERR));
}

TEST_F(MwTest, BindType1AtomicWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), mr,
                                          IBV_ACCESS_REMOTE_ATOMIC),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_SUCCESS
                               : IBV_WC_MW_BIND_ERR));
}

TEST_F(MwTest, Read) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.qp, read);
  EXPECT_THAT(verbs_util::WaitForCompletion(setup.cq),
              IsOkAndHolds(Field(&ibv_wc::status, IBV_WC_SUCCESS)));
}

TEST_F(MwTest, BindReadDiffQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* bind_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(bind_qp, NotNull());
  ibv_.SetUpSelfConnectedRcQp(bind_qp, setup.port_gid);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(
      verbs_util::BindType1MwSync(bind_qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.qp, read);
  EXPECT_THAT(verbs_util::WaitForCompletion(setup.cq),
              IsOkAndHolds(Field(&ibv_wc::status, IBV_WC_SUCCESS)));
}

TEST_F(MwTest, Unbind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  EXPECT_THAT(verbs_util::BindType1MwSync(
                  setup.qp, mw, setup.buffer.span().subspan(0, 0), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

// Binding with invalid resource (QP, MW, MR) handle can go three ways:
// 1. Interface returns immediate error when trying to post to the QP.
// 2. Posting to QP is successful but completes in error.
// 3  Posting to QP is successful and the bind succeeds. This is probably due to
//    some driver ignoring handles and only using the relevant info in the user
//    space struct.
TEST_F(MwTest, BindInvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(setup.mr->handle);
  ibv_mw_bind bind_args = verbs_util::CreateType1MwBind(
      /*wr_id=*/1, setup.buffer.span(), setup.mr);
  int result = ibv_bind_mw(setup.qp, mw, &bind_args);
  ASSERT_THAT(result, AnyOf(0, ENOENT));
  if (result == 0) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.qp->send_cq));
    EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_MW_BIND_ERR));
    EXPECT_EQ(completion.wr_id, 1);
    EXPECT_EQ(completion.qp_num, setup.qp->qp_num);
    if (completion.status == IBV_WC_SUCCESS) {
      EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                       setup.buffer.data(), mw->rkey),
                  IsOkAndHolds(IBV_WC_SUCCESS));
    }
  }
}

TEST_F(MwTest, BindInvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(mw->handle);
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  int result = ibv_bind_mw(setup.qp, mw, &bind_arg);
  ASSERT_THAT(result, AnyOf(0, ENOENT));
  if (result == 0) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.qp->send_cq));
    EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_MW_BIND_ERR));
    EXPECT_EQ(completion.wr_id, 1);
    EXPECT_EQ(completion.qp_num, setup.qp->qp_num);
    if (completion.status == IBV_WC_SUCCESS) {
      EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                       setup.buffer.data(), mw->rkey),
                  IsOkAndHolds(IBV_WC_SUCCESS));
    }
  }
}

TEST_F(MwTest, BindInvalidQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(setup.qp->handle);
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  int result = ibv_bind_mw(setup.qp, mw, &bind_arg);
  ASSERT_THAT(result, AnyOf(0, ENOENT));
  if (result == 0) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.qp->send_cq));
    EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_MW_BIND_ERR));
    EXPECT_EQ(completion.wr_id, 1);
    EXPECT_EQ(completion.qp_num, setup.qp->qp_num);
    if (completion.status == IBV_WC_SUCCESS) {
      EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                       setup.buffer.data(), mw->rkey),
                  IsOkAndHolds(IBV_WC_SUCCESS));
    }
  }
}

TEST_F(MwTest, DeregMrWhenBound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  int result = ibv_.DeregMr(setup.mr);
  if (result == 0) {
    EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                     setup.buffer.data(), mw->rkey),
                IsOkAndHolds(IBV_WC_REM_OP_ERR));
  } else {
    EXPECT_EQ(result, EBUSY);
    EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                     setup.buffer.data(), mw->rkey),
                IsOkAndHolds(IBV_WC_SUCCESS));
  }
}

TEST_F(MwTest, DestroyQpWithType1Bound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  EXPECT_EQ(ibv_.DestroyQp(setup.qp), 0);
}

TEST_F(MwTest, CrossQpManagement) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  // Create a second QP.
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq, NotNull());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, cq);
  ASSERT_THAT(qp, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp, setup.port_gid);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  // Rebind on second.
  EXPECT_THAT(
      verbs_util::BindType1MwSync(qp, mw, setup.buffer.span(), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwTest, DeallocPdWithOutstandingMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  EXPECT_EQ(ibv_.DeallocPd(setup.pd), EBUSY);
}

class MwType2Test : public MwTest {
 public:
  void SetUp() override {
    if (!Introspection().SupportsType2()) {
      GTEST_SKIP() << "Nic does not support Type2 MW";
    }
  }
};

TEST_F(MwType2Test, Type2Alloc) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_alloc_mw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  EXPECT_EQ(ibv_dealloc_mw(mw), 0);
}

TEST_F(MwType2Test, BindType2ReadWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  EXPECT_THAT(
      verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                  /*rkey=*/kRKey, mr, IBV_ACCESS_REMOTE_READ),
      IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, BindType2AtomicWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(
      verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                  /*rkey=*/kRKey, mr, IBV_ACCESS_REMOTE_READ),
      IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, Type2BindOnType1) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                          /*rkey=*/kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, Bind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  EXPECT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, UnsignaledBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, mw2, setup.buffer.span(), /*rkey=*/kRKey, setup.mr);
  bind.send_flags = bind.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, bind);

  // Second bind will expect its completion.
  ibv_send_wr bind2 = verbs_util::CreateType2BindWr(
      /*wr_id=*/2, type2_mw, setup.buffer.span(), /*rkey=*/1025, setup.mr);
  verbs_util::PostSend(setup.qp, bind2);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.wr_id, 2);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
}

TEST_F(MwType2Test, BindType2ReadDiffQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* bind_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(bind_qp, NotNull());
  ibv_.SetUpSelfConnectedRcQp(bind_qp, setup.port_gid);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(bind_qp, mw, setup.buffer.span(),
                                          /*rkey=*/1012, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), /*rkey=*/1012);
  verbs_util::PostSend(setup.qp, read);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

// Multiple uses of the same rkey.
TEST_F(MwType2Test, BindRKeyReuse) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  EXPECT_THAT(verbs_util::BindType2MwSync(setup.qp, mw2, setup.buffer.span(),
                                          /*rkey=*/kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

// Multiple uses of the same rkey.
TEST_F(MwType2Test, UnsignaledBindError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());

  ibv_send_wr bind2 = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, mw2, setup.buffer.span(), /*rkey=*/kRKey, setup.mr);
  bind2.send_flags = bind2.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, bind2);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
}

// Test using the same rkey in 2 different contexts.
TEST_F(MwType2Test, BindRKeyIsolation) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK_AND_ASSIGN(BasicSetup setup2, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_mw* mw2 = ibv_.AllocMw(setup2.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  EXPECT_THAT(verbs_util::BindType2MwSync(setup2.qp, mw2, setup2.buffer.span(),
                                          kRKey, setup2.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, Read) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), type2_mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, Unbind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ibv_send_wr invalidate = verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey);
  verbs_util::PostSend(setup.qp, invalidate);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(completion.opcode, IBV_WC_LOCAL_INV);

  EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), type2_mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(MwType2Test, UnsignaledInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, mw2, setup.buffer.span(),
                                          kRKey + 1, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, type2_mw->rkey);
  invalidate.send_flags = invalidate.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, invalidate);
  // Do another operation to ensure no completion.
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), mw2->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, UnsignaledInvalidateError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey + 1);
  invalidate.send_flags = invalidate.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, invalidate);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
}

TEST_F(MwType2Test, DoubleUnbind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // First invalidate.
  ibv_send_wr invalidate = verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey);
  verbs_util::PostSend(setup.qp, invalidate);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(completion.opcode, IBV_WC_LOCAL_INV);
  verbs_util::PostSend(setup.qp, invalidate);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
}

TEST_F(MwType2Test, Type1BindOnType2) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  EXPECT_EQ(ibv_bind_mw(setup.qp, type2_mw, &bind_arg), EINVAL);
}

TEST_F(MwType2Test, DestroyWithType2Bound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // Should fail due to outstanding MW.
  EXPECT_EQ(ibv_destroy_qp(setup.qp), EBUSY);
}

TEST_F(MwType2Test, InvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, type2_mw, setup.buffer.span(), /*rkey=*/10, setup.mr);
  // Invalidate MR.
  ibv_mr fake_mr = {};
  bind.bind_mw.bind_info.mr = &fake_mr;
  verbs_util::PostSend(setup.qp, bind);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  ASSERT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), type2_mw->rkey),
              IsOkAndHolds(IBV_WC_WR_FLUSH_ERR));
}

TEST_F(MwType2Test, InvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw fake_mw = {};
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, &fake_mw, setup.buffer.span(), /*rkey=*/10, setup.mr);
  verbs_util::PostSend(setup.qp, bind);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
}

TEST_F(MwType2Test, DeregMrWhenBound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  int result = ibv_dereg_mr(setup.mr);
  if (result == 0) {
    EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                     setup.buffer.data(), type2_mw->rkey),
                IsOkAndHolds(IBV_WC_REM_OP_ERR));
  } else {
    ASSERT_EQ(result, EBUSY);
    EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                     setup.buffer.data(), type2_mw->rkey),
                IsOkAndHolds(IBV_WC_SUCCESS));
  }
}

TEST_F(MwType2Test, InUseRkey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  EXPECT_THAT(verbs_util::BindType2MwSync(setup.qp, mw2, setup.buffer.span(),
                                          kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, CrossQpBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq2, NotNull());
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_THAT(qp2, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp2, setup.port_gid);
  ASSERT_THAT(verbs_util::BindType2MwSync(qp2, type2_mw, setup.buffer.span(),
                                          /*rkey=*/1028, setup.mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
  // Make sure old rkey still works.
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), kRKey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, CrossQpInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq2, NotNull());
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_THAT(qp2, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp2, setup.port_gid);
  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, /*rkey=*/kRKey);
  verbs_util::PostSend(qp2, invalidate);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(cq2));
  ASSERT_EQ(completion.status, IBV_WC_MW_BIND_ERR);

  // Make sure old rkey still works.
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), kRKey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, Rebind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ASSERT_THAT(
      verbs_util::BindType2MwSync(setup.qp, type2_mw, setup.buffer.span(),
                                  /*rkey=*/1028, setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  // Make sure the new rkey works.
  ASSERT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), /*rkey=*/1028),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // Make sure the old rkey does not work.
  ASSERT_THAT(verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                   setup.buffer.data(), kRKey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(MwType2Test, CrossQpRead) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq2, NotNull());
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_THAT(qp2, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp2, setup.port_gid);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), /*rkey=*/kRKey);
  verbs_util::PostSend(qp2, read);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(cq2));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(MwType2Test, ChangeQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ASSERT_THAT(verbs_util::BindType2MwSync(setup.qp, type2_mw,
                                          setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // Unbind.
  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, /*rkey=*/kRKey);
  verbs_util::PostSend(setup.qp, invalidate);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(completion.opcode, IBV_WC_LOCAL_INV);
  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq2, NotNull());
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_THAT(qp2, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp2, setup.port_gid);
  // Bind to qp2.
  ASSERT_THAT(verbs_util::BindType2MwSync(qp2, type2_mw, setup.buffer.span(),
                                          /*rkey=*/1028, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // Make sure old rkey still works.
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), /*rkey=*/kRKey);
  verbs_util::PostSend(qp2, read);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(cq2));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

class MwBindTest : public BasicFixture,
                   public ::testing::WithParamInterface<ibv_mw_type> {
 public:
  void SetUp() override {
    if (GetParam() == IBV_MW_TYPE_1 && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Nic does not support Type1 MW";
    }
    if (GetParam() == IBV_MW_TYPE_2 && !Introspection().SupportsType2()) {
      GTEST_SKIP() << "Nic does not support Type2 MW";
    }
  }

 protected:
  struct BasicSetup {
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
    RdmaMemBlock buffer;
    ibv_cq* cq;
    ibv_qp* qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.buffer = ibv_.AllocBuffer(/*pages=*/4);
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.qp = ibv_.CreateQp(setup.pd, setup.cq);
    ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
    return setup;
  }

  void AttemptBind(const BasicSetup& setup, int mr_access, int bind_access,
                   ibv_wc_status expected) {
    ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, mr_access);
    ASSERT_THAT(mr, NotNull());
    ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
    ASSERT_THAT(mw, NotNull());
    // Do Bind.
    switch (GetParam()) {
      case IBV_MW_TYPE_1: {
        ASSERT_THAT(verbs_util::BindType1MwSync(
                        setup.qp, mw, setup.buffer.span(), mr, bind_access),
                    IsOkAndHolds(expected));
      } break;
      case IBV_MW_TYPE_2: {
        static int rkey = 17;
        ASSERT_THAT(
            verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                        ++rkey, mr, bind_access),
            IsOkAndHolds(expected));
      } break;
      default:
        CHECK(false) << "Unknown param.";
    }
  }
};

TEST_P(MwBindTest, AllPermissions) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                          IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                          IBV_ACCESS_REMOTE_WRITE;
  AttemptBind(setup, kMrAccess, kBindAccess, IBV_WC_SUCCESS);
}

TEST_P(MwBindTest, MissingLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_MW_BIND;
  const int kBindAccess = IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, MissingBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
                        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_MW_BIND;
  const ibv_wc_status kExpected = Introspection().ShouldDeviateForCurrentTest()
                                      ? IBV_WC_SUCCESS
                                      : IBV_WC_MW_BIND_ERR;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, MissingRemoteAtomic) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_REMOTE_ATOMIC;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, MissingRemoteRead) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, MissingRemoteWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ;
  const int kBindAccess = IBV_ACCESS_REMOTE_WRITE;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, EmptyBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = 0;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, ReadOnly) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_READ;
  const int kBindAccess = IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, WriteOnly) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_WRITE;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, NoMrBindAccess) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_REMOTE_READ;
  const int kBindAccess = IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = Introspection().ShouldDeviateForCurrentTest()
                                      ? IBV_WC_SUCCESS
                                      : IBV_WC_MW_BIND_ERR;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MwBindTest, BindWhenQpError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_OK(ibv_.SetQpError(setup.qp));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_ERR);

  ibv_wc_status status;
  switch (GetParam()) {
    case IBV_MW_TYPE_1: {
      ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
      ASSERT_OK_AND_ASSIGN(status, verbs_util::BindType1MwSync(
                                       setup.qp, mw, setup.buffer.span(), mr));
      break;
    }
    case IBV_MW_TYPE_2: {
      ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
      ASSERT_OK_AND_ASSIGN(
          status, verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                              /*rkey=*/1025, mr));
      break;
    }
    default:
      CHECK(false) << "Unknown param.";
  }
  EXPECT_EQ(status, IBV_WC_WR_FLUSH_ERR);
}

INSTANTIATE_TEST_SUITE_P(MwBindTestCase, MwBindTest,
                         ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2));

// TODO(author1): Create on one QP and destroy on another with wrong PD
// (pending PD).
// TODO(author1): Check range checks (pending implementation).
// TODO(author1): Check window permissions (pending implementation).
// TODO(author1): Invalid pd (when support is added).
// TODO(author1): Bind MW, QP and MR from different pds (when pd support is
// added).

class MwType2AdvancedTest : public MwTest {
 public:
  void SetUp() override {
    if (!Introspection().SupportsType2()) {
      GTEST_SKIP() << "Nic does not support Type2 MW";
    }
  }

 protected:
  static constexpr int kSubmissionBatch = 100;
  // Outstanding below this value will trigger another batch submission.
  static constexpr int kTargetOutstanding = 300;
  static constexpr int kMaxOutstanding = kSubmissionBatch + kTargetOutstanding;
  // Number of ops which need to complete before taking action (unbind, rebind,
  // etc.).
  static constexpr int kWarmupCount = 100;

  struct QpInfo {
    ibv_cq* cq;
    ibv_qp* qp;
    int successful_count;
    ibv_wc failing_completion;
  };

  struct AdvancedSetup {
    BasicSetup basic;
    ibv_mw* mw;
    // Two connected QPs.
    QpInfo owner;
    // Extra work was done to use this QP so that QP where the MW is modified
    // has incoming ops.
    QpInfo reader;
    // Arbitrary number of self connected reader QPs in the same PD.
    std::vector<QpInfo> reader_only_qps;

    std::vector<std::thread> threads;
  };

  absl::StatusOr<AdvancedSetup> CreateAdvancedSetup() {
    AdvancedSetup setup;
    ASSIGN_OR_RETURN(setup.basic, CreateBasicSetup());

    setup.mw = ibv_.AllocMw(setup.basic.pd, IBV_MW_TYPE_2);
    if (!setup.mw) {
      return absl::InternalError("Failed to allocate mw.");
    }
    setup.owner.cq = ibv_.CreateCq(setup.basic.context);
    if (!setup.owner.cq) {
      return absl::InternalError("Failed to create owner's cq.");
    }
    setup.owner.qp = ibv_.CreateQp(setup.basic.pd, setup.owner.cq);
    if (!setup.owner.qp) {
      return absl::InternalError("Failed to create owner's qp.");
    }
    setup.reader.cq = ibv_.CreateCq(setup.basic.context, kMaxOutstanding);
    if (!setup.reader.cq) {
      return absl::InternalError("Failed to reader's cq.");
    }
    setup.reader.qp = ibv_.CreateQp(setup.basic.pd, setup.reader.cq,
                                    setup.reader.cq, nullptr, kMaxOutstanding,
                                    kMaxOutstanding, IBV_QPT_RC, /*sig_all=*/0);
    if (!setup.reader.qp) {
      return absl::InternalError("Failed to reader's qp.");
    }
    ibv_.SetUpLoopbackRcQps(setup.owner.qp, setup.reader.qp,
                            setup.basic.port_gid);

    // Only have 1 extra reader on forge due to CPU limitations...
    const int reader_threads = getenv("UNITTEST_ON_FORGE") ? 1 : 4;
    for (int i = 0; i < reader_threads; ++i) {
      QpInfo info;
      info.cq = ibv_.CreateCq(setup.basic.context, kMaxOutstanding);
      CHECK(info.cq);
      info.qp = ibv_.CreateQp(setup.basic.pd, info.cq, info.cq, nullptr,
                              kMaxOutstanding, kMaxOutstanding, IBV_QPT_RC,
                              /*sig_all=*/0);
      CHECK(info.qp);
      ibv_.SetUpSelfConnectedRcQp(info.qp, setup.basic.port_gid);
      setup.reader_only_qps.push_back(info);
    }
    ASSIGN_OR_RETURN(ibv_wc_status result,
                     verbs_util::BindType2MwSync(setup.owner.qp, setup.mw,
                                                 setup.basic.buffer.span(),
                                                 kRKey, setup.basic.mr));
    if (result != IBV_WC_SUCCESS) {
      return absl::InternalError(
          absl::StrCat("Failed to bind type 2 MW (", result, ")."));
    }
    return setup;
  }

  // Inner loop of a reader thread. It continually issues Read WQEs until
  // failure or |max_count| is reached.
  void ReaderLoop(const BasicSetup& basic, uint32_t rkey, QpInfo& qp_info,
                  std::atomic<size_t>& total_reads,
                  absl::optional<int> max_count,
                  absl::Notification& cancel_notification) {
    // Setup a single read.
    ibv_sge sg = verbs_util::CreateSge(basic.buffer.span(), basic.mr);
    ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sg, /*num_sge=*/1,
                                                basic.buffer.data(), rkey);

    // Submit |kSubmissionBatch| at a time.
    ibv_send_wr submissions[kSubmissionBatch];
    for (int i = 0; i < kSubmissionBatch; ++i) {
      memcpy(&submissions[i], &read, sizeof(ibv_send_wr));
      submissions[i].next = &submissions[i + 1];
    }
    submissions[kSubmissionBatch - 1].next = nullptr;

    ibv_wc completions[kSubmissionBatch];
    int outstanding = 0;
    qp_info.successful_count = 0;
    qp_info.failing_completion.status = IBV_WC_SUCCESS;
    while (!cancel_notification.HasBeenNotified() &&
           qp_info.failing_completion.status == IBV_WC_SUCCESS) {
      // Maybe issue work.
      if (outstanding <= kTargetOutstanding) {
        verbs_util::PostSend(qp_info.qp, submissions[0]);
        outstanding += kSubmissionBatch;
      }
      // Wait a little.
      absl::SleepFor(absl::Milliseconds(50));
      // Poll for completions.
      int count = ibv_poll_cq(qp_info.cq, kSubmissionBatch, completions);
      total_reads.fetch_add(count, std::memory_order_relaxed);
      for (int i = 0; i < count; ++i) {
        if (completions[i].status != IBV_WC_SUCCESS) {
          memcpy(&qp_info.failing_completion, &completions[i],
                 sizeof(completions[0]));
          break;
        }
        ++qp_info.successful_count;
      }
      outstanding -= count;
      if (max_count.has_value() &&
          qp_info.successful_count > max_count.value()) {
        break;
      }
    }
  }

  // The reader threads aim to be clients which keeps the QP pipeline full
  // with read WQEs until the first failure. If passed |max_count| is passed,
  // then the thread ends after |max_count| successful reads or the first
  // failure.
  void StartReaderThreads(AdvancedSetup& advanced, uint32_t rkey,
                          std::atomic<size_t>& total_reads,
                          absl::Notification& cancel_notification,
                          absl::optional<int> max_count = {}) {
    advanced.threads.push_back(std::thread([this, &advanced, rkey, &total_reads,
                                            max_count, &cancel_notification]() {
      ReaderLoop(advanced.basic, rkey, advanced.reader, total_reads, max_count,
                 cancel_notification);
    }));
    for (QpInfo& info : advanced.reader_only_qps) {
      advanced.threads.push_back(
          std::thread([this, &advanced, &info, rkey, max_count, &total_reads,
                       &cancel_notification]() {
            ReaderLoop(advanced.basic, rkey, info, total_reads, max_count,
                       cancel_notification);
          }));
    }
    // Block until at least a few ops complete.
    while (total_reads.load(std::memory_order_relaxed) < kWarmupCount) {
      absl::SleepFor(absl::Milliseconds(20));
    }
  }

  // Checks that all readers completed with the same failure.
  void VerifyFailure(AdvancedSetup& advanced) const {
    ibv_wc_status status = advanced.reader.failing_completion.status;
    for (const QpInfo& info : advanced.reader_only_qps) {
      EXPECT_EQ(status, info.failing_completion.status)
          << "Not all QPs failed with the same error.";
    }
    EXPECT_EQ(status, IBV_WC_REM_ACCESS_ERR);
  }

  // Joins all reader threads.
  void JoinAll(AdvancedSetup& advanced) {
    for (auto& thread : advanced.threads) {
      thread.join();
    }
  }
};

TEST_F(MwType2AdvancedTest, OnlyReads) {
  ASSERT_OK_AND_ASSIGN(AdvancedSetup advanced, CreateAdvancedSetup());
  absl::Notification cancel_notification;
  std::atomic<size_t> total_reads = 0;
  StartReaderThreads(advanced, advanced.mw->rkey, total_reads,
                     cancel_notification, kWarmupCount);
  JoinAll(advanced);
}

TEST_F(MwType2AdvancedTest, Rebind) {
  ASSERT_OK_AND_ASSIGN(AdvancedSetup advanced, CreateAdvancedSetup());
  absl::Notification cancel_notification;

  std::atomic<size_t> total_reads = 0;
  StartReaderThreads(advanced, advanced.mw->rkey, total_reads,
                     cancel_notification);

  LOG(INFO) << "Started reader";

  // Issue rebind.
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, advanced.mw, advanced.basic.buffer.span(), kRKey + 1,
      advanced.basic.mr);
  verbs_util::PostSend(advanced.owner.qp, bind);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(advanced.owner.cq));
  ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(completion.opcode, IBV_WC_BIND_MW);

  JoinAll(advanced);
  LOG(INFO) << "Reader end.";
  VerifyFailure(advanced);
}

TEST_F(MwType2AdvancedTest, Invalidate) {
  ASSERT_OK_AND_ASSIGN(AdvancedSetup advanced, CreateAdvancedSetup());
  absl::Notification cancel_notification;
  std::atomic<size_t> total_reads = 0;
  StartReaderThreads(advanced, advanced.mw->rkey, total_reads,
                     cancel_notification);

  // Invalidate.
  ibv_send_wr invalidate = verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey);
  ibv_send_wr* bad_wr;
  ASSERT_EQ(ibv_post_send(advanced.owner.qp, &invalidate, &bad_wr), 0);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(advanced.owner.cq));
  ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(completion.opcode, IBV_WC_LOCAL_INV);

  JoinAll(advanced);
  VerifyFailure(advanced);
}

TEST_F(MwType2AdvancedTest, Dealloc) {
  ASSERT_OK_AND_ASSIGN(AdvancedSetup advanced, CreateAdvancedSetup());
  absl::Notification cancel_notification;
  std::atomic<size_t> total_reads = 0;
  StartReaderThreads(advanced, advanced.mw->rkey, total_reads,
                     cancel_notification);

  // Delete.
  ASSERT_EQ(ibv_.DeallocMw(advanced.mw), 0);

  JoinAll(advanced);
  VerifyFailure(advanced);
}

// TODO(author1): Permissions change (when implemented).
// TODO(author1): Addr change (when implemented).
// TODO(author1): Length change (when implemented).

}  // namespace rdma_unit_test
