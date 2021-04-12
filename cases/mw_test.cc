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
#include <sched.h>
#include <stdlib.h>
#include <string.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/status_matchers.h"
#include "public/rdma-memblock.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

class MwTest : public BasicFixture {
 public:
  void SetUp() override {
    if (!Introspection().SupportsType2() && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Skipping due to lack of MW support.";
    }
  }

 protected:
  static constexpr int kBufferMemoryPages = 4;
  static constexpr absl::Duration kBindTimeout = absl::Seconds(10);
  static constexpr uint32_t kRKey = 1024;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    ibv_pd* pd;
    ibv_mr* mr;
    ibv_cq* cq;
    ibv_qp* qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
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
    ibv_.SetUpSelfConnectedRcQp(setup.qp,
                                ibv_.GetContextAddressInfo(setup.context));
    return setup;
  }
};

TEST_F(MwTest, Alloc) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  EXPECT_NE(nullptr, mw);
}

TEST_F(MwTest, DeallocUnknown) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw dummy_mw = {};
  dummy_mw.context = setup.context;
  dummy_mw.handle = -1;
  EXPECT_EQ(ENOENT, ibv_dealloc_mw(&dummy_mw));
}

TEST_F(MwTest, Bind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  verbs_util::PostType1Bind(setup.qp, mw, bind_arg);
  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
}

TEST_F(MwTest, BindType1ReadWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ASSERT_NE(nullptr, mr);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status bind_result = verbs_util::BindType1MwSync(
      setup.qp, mw, setup.buffer.span(), mr, IBV_ACCESS_REMOTE_READ);
  const ibv_wc_status kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? IBV_WC_MW_BIND_ERR
                                                           : IBV_WC_SUCCESS;
  EXPECT_EQ(kExpected, bind_result);
}

TEST_F(MwTest, BindType1AtomicWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ASSERT_NE(nullptr, mr);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status bind_result = verbs_util::BindType1MwSync(
      setup.qp, mw, setup.buffer.span(), mr, IBV_ACCESS_REMOTE_ATOMIC);
  const ibv_wc_status kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? IBV_WC_MW_BIND_ERR
                                                           : IBV_WC_SUCCESS;
  EXPECT_EQ(kExpected, bind_result);
}

TEST_F(MwTest, Read) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);

  ibv_wc_status result =
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, result);

  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.qp, read);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
}

TEST_F(MwTest, BindReadDiffQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* bind_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_NE(nullptr, bind_qp);
  ibv_.SetUpSelfConnectedRcQp(bind_qp,
                              ibv_.GetContextAddressInfo(setup.context));
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status bind_result =
      verbs_util::BindType1MwSync(bind_qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, bind_result);

  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.qp, read);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
}

TEST_F(MwTest, Unbind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);

  ibv_wc_status bind_result =
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, bind_result);

  bind_result = verbs_util::BindType1MwSync(
      setup.qp, mw, setup.buffer.span().subspan(0, 0), setup.mr);
  EXPECT_EQ(IBV_WC_SUCCESS, bind_result);
}

TEST_F(MwTest, InvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);

  ibv_mr dummy_mr_val{};
  ibv_mr* dummy_mr = &dummy_mr_val;
  dummy_mr->handle = -1;
  // Some clients do client side validation on type 1. First check
  // succcess/failure of the bind and if successful than check for completion.
  ibv_mw_bind bind_args = verbs_util::CreateType1MwBind(
      /*wr_id=*/1, setup.buffer.span(), dummy_mr,
      IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
          IBV_ACCESS_REMOTE_ATOMIC);
  int result = ibv_bind_mw(setup.qp, mw, &bind_args);
  EXPECT_THAT(result, testing::AnyOf(0, EPERM, EOPNOTSUPP));
  if (result == 0) {
    ibv_wc completion =
        verbs_util::WaitForCompletion(setup.qp->send_cq).value();
    if (completion.status == IBV_WC_SUCCESS) {
      EXPECT_EQ(IBV_WC_BIND_MW, completion.opcode);
    }
  }
}

TEST_F(MwTest, InvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);

  ibv_mw dummy_mw{};
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  ibv_mw* target = &dummy_mw;
  int result = ibv_bind_mw(setup.qp, target, &bind_arg);
  EXPECT_EQ(EINVAL, result);
}

TEST_F(MwTest, DeregMrWhenBound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);

  ibv_wc_status result =
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, result);

  const int kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? EBUSY : 0;
  EXPECT_EQ(kExpected, ibv_.DeregMr(setup.mr));
}

TEST_F(MwTest, DestroyQpWithType1Bound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status result =
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, result);
  // Destroy
  EXPECT_EQ(0, ibv_.DestroyQp(setup.qp));
}

TEST_F(MwTest, CrossQpManagement) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  // Create a second QP.
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq);
  ibv_qp* qp = ibv_.CreateQp(setup.pd, cq);
  ASSERT_NE(nullptr, qp);
  ibv_.SetUpSelfConnectedRcQp(qp, ibv_.GetContextAddressInfo(setup.context));
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status result =
      verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, result);
  // Rebind on second.
  result = verbs_util::BindType1MwSync(qp, mw, setup.buffer.span(), setup.mr);
  EXPECT_EQ(IBV_WC_SUCCESS, result);
}

TEST_F(MwTest, BindFullCommandQueue) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  // Create a QP which is not flowing to use for the bind.
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq);
  ibv_qp* qp = ibv_.CreateQp(setup.pd, cq);
  ASSERT_NE(nullptr, qp);
  // Fill the send queue.
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, nullptr, /*num_sge=*/0);
  ibv_send_wr* bad_wr = nullptr;
  int send_result = ibv_post_send(qp, &send, &bad_wr);
  ASSERT_EQ(0, send_result) << "Expected at least 1 success.";
  while (send_result == 0) {
    send_result = ibv_post_send(qp, &send, &bad_wr);
  }
  ASSERT_EQ(ENOMEM, send_result);

  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  int result = ibv_bind_mw(qp, mw, &bind_arg);
  EXPECT_EQ(ENOMEM, result);
}

TEST_F(MwTest, DeallocPdWithOutstandingMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);

  int result = ibv_.DeallocPd(setup.pd);
  EXPECT_EQ(EBUSY, result);
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
  ASSERT_NE(nullptr, mw);
  int result = ibv_dealloc_mw(mw);
  EXPECT_EQ(0, result);
}

TEST_F(MwType2Test, BindType2ReadWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ibv_wc_status bind_result =
      verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                  /*rkey=*/kRKey, mr, IBV_ACCESS_REMOTE_READ);
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, bind_result);
}

TEST_F(MwType2Test, BindType2AtomicWithNoLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kNoAccessPermissins = 0;
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, kNoAccessPermissins);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ibv_wc_status bind_result =
      verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                  /*rkey=*/kRKey, mr, IBV_ACCESS_REMOTE_READ);
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, bind_result);
}

TEST_F(MwType2Test, Type2BindOnType1) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, mw, setup.buffer.span(), /*rkey=*/kRKey, setup.mr);
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, status);
}

TEST_F(MwType2Test, Bind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  EXPECT_EQ(IBV_WC_SUCCESS, status);
}

TEST_F(MwType2Test, UnsignaledBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw2);
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, mw2, setup.buffer.span(), /*rkey=*/kRKey, setup.mr);
  bind.send_flags = bind.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, bind);

  // Second bind will expect its completion.
  ibv_send_wr bind2 = verbs_util::CreateType2BindWr(
      /*wr_id=*/2, type2_mw, setup.buffer.span(), /*rkey=*/1025, setup.mr);
  verbs_util::PostSend(setup.qp, bind2);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(2, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
}

TEST_F(MwType2Test, BindType2ReadDiffQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* bind_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_NE(nullptr, bind_qp);
  ibv_.SetUpSelfConnectedRcQp(bind_qp,
                              ibv_.GetContextAddressInfo(setup.context));
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw);
  ibv_wc_status bind_result = verbs_util::BindType2MwSync(
      bind_qp, mw, setup.buffer.span(), /*rkey=*/1012, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, bind_result);

  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), /*rkey=*/1012);
  verbs_util::PostSend(setup.qp, read);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

// Multiple uses of the same rkey.
TEST_F(MwType2Test, BindRKeyReuse) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw2);

  ibv_wc_status result = verbs_util::BindType2MwSync(
      setup.qp, mw2, setup.buffer.span(), /*rkey=*/kRKey, setup.mr);
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, result);
}

// Multiple uses of the same rkey.
TEST_F(MwType2Test, UnsignaledBindError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw2);

  ibv_send_wr bind2 = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, mw2, setup.buffer.span(), /*rkey=*/kRKey, setup.mr);
  bind2.send_flags = bind2.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, bind2);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, completion.status);
}

// Test using the same rkey in 2 different contexts.
TEST_F(MwType2Test, BindRKeyIsolation) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK_AND_ASSIGN(BasicSetup setup2, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_mw* mw2 = ibv_.AllocMw(setup2.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw2);
  ibv_wc_status result = verbs_util::BindType2MwSync(
      setup2.qp, mw2, setup2.buffer.span(), kRKey, setup2.mr);
  EXPECT_EQ(IBV_WC_SUCCESS, result);
}

TEST_F(MwType2Test, Read) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), type2_mw->rkey);
  EXPECT_EQ(IBV_WC_SUCCESS, status);
}

TEST_F(MwType2Test, Unbind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_send_wr invalidate = verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey);
  verbs_util::PostSend(setup.qp, invalidate);

  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_EQ(IBV_WC_LOCAL_INV, completion.opcode);

  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), type2_mw->rkey);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, status);
}

TEST_F(MwType2Test, UnsignaledInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw2);
  status = verbs_util::BindType2MwSync(setup.qp, mw2, setup.buffer.span(),
                                       kRKey + 1, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, type2_mw->rkey);
  invalidate.send_flags = invalidate.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, invalidate);

  // Do another operation to ensure no completion.
  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), mw2->rkey);
  EXPECT_EQ(IBV_WC_SUCCESS, status);
}

TEST_F(MwType2Test, UnsignaledInvalidateError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey + 1);
  invalidate.send_flags = invalidate.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.qp, invalidate);
  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, completion.status);
}

TEST_F(MwType2Test, DoubleUnbind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  // First invalidate.
  ibv_send_wr invalidate = verbs_util::CreateInvalidateWr(/*wr_id=*/1, kRKey);
  verbs_util::PostSend(setup.qp, invalidate);
  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_EQ(IBV_WC_LOCAL_INV, completion.opcode);

  verbs_util::PostSend(setup.qp, invalidate);
  completion = verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, completion.status);
}

TEST_F(MwType2Test, Type1BindOnType2) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_mw_bind bind_arg =
      verbs_util::CreateType1MwBind(/*wr_id=*/1, setup.buffer.span(), setup.mr);
  int result = ibv_bind_mw(setup.qp, type2_mw, &bind_arg);
  EXPECT_EQ(EINVAL, result);
}

TEST_F(MwType2Test, DestroyWithType2Bound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);
  // Should fail due to outstanding MW.
  EXPECT_EQ(EBUSY, ibv_destroy_qp(setup.qp));
}

TEST_F(MwType2Test, InvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, type2_mw, setup.buffer.span(), /*rkey=*/10, setup.mr);
  // Invalidate MR.
  ibv_mr fake_mr = {};
  bind.bind_mw.bind_info.mr = &fake_mr;
  verbs_util::PostSend(setup.qp, bind);

  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_MW_BIND_ERR, completion.status);

  ibv_wc_status status =
      verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                           setup.buffer.data(), type2_mw->rkey);
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, status);
}

TEST_F(MwType2Test, InvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw fake_mw = {};
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, &fake_mw, setup.buffer.span(), /*rkey=*/10, setup.mr);
  verbs_util::PostSend(setup.qp, bind);

  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, completion.status);
}

TEST_F(MwType2Test, DeregMrWhenBound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ASSERT_EQ(EBUSY, ibv_dereg_mr(setup.mr));

  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), type2_mw->rkey);
  EXPECT_EQ(IBV_WC_SUCCESS, status);
}

TEST_F(MwType2Test, InUseRkey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, mw2);
  status = verbs_util::BindType2MwSync(setup.qp, mw2, setup.buffer.span(),
                                       kRKey, setup.mr);
  EXPECT_EQ(IBV_WC_MW_BIND_ERR, status);
}

TEST_F(MwType2Test, CrossQpBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq2);
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_NE(nullptr, qp2);
  ibv_.SetUpSelfConnectedRcQp(qp2, ibv_.GetContextAddressInfo(setup.context));
  ibv_wc_status result = verbs_util::BindType2MwSync(
      qp2, type2_mw, setup.buffer.span(), /*rkey=*/1028, setup.mr);
  ASSERT_EQ(IBV_WC_MW_BIND_ERR, result);

  // Make sure old rkey still works.
  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), kRKey);
  EXPECT_EQ(IBV_WC_SUCCESS, status);
}

TEST_F(MwType2Test, CrossQpInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq2);
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_NE(nullptr, qp2);
  ibv_.SetUpSelfConnectedRcQp(qp2, ibv_.GetContextAddressInfo(setup.context));
  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, /*rkey=*/kRKey);
  verbs_util::PostSend(qp2, invalidate);
  ibv_wc completion = verbs_util::WaitForCompletion(cq2, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_MW_BIND_ERR, completion.status);

  // Make sure old rkey still works.
  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), kRKey);
  EXPECT_EQ(IBV_WC_SUCCESS, status);
}

TEST_F(MwType2Test, Rebind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  status = verbs_util::BindType2MwSync(setup.qp, type2_mw, setup.buffer.span(),
                                       /*rkey=*/1028, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  // Make sure the new rkey works.
  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), /*rkey=*/1028);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  // Make sure the old rkey does not work.
  status = verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                                setup.buffer.data(), kRKey);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, status);
}

TEST_F(MwType2Test, CrossQpRead) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq2);
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_NE(nullptr, qp2);
  ibv_.SetUpSelfConnectedRcQp(qp2, ibv_.GetContextAddressInfo(setup.context));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), /*rkey=*/kRKey);
  verbs_util::PostSend(qp2, read);
  ibv_wc completion = verbs_util::WaitForCompletion(cq2, kBindTimeout).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(MwType2Test, ChangeQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(nullptr, type2_mw);
  ibv_wc_status status = verbs_util::BindType2MwSync(
      setup.qp, type2_mw, setup.buffer.span(), kRKey, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  // Unbind.
  ibv_send_wr invalidate =
      verbs_util::CreateInvalidateWr(/*wr_id=*/1, /*rkey=*/kRKey);
  verbs_util::PostSend(setup.qp, invalidate);
  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.cq, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_EQ(IBV_WC_LOCAL_INV, completion.opcode);

  ibv_cq* cq2 = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq2);
  ibv_qp* qp2 = ibv_.CreateQp(setup.pd, cq2);
  ASSERT_NE(nullptr, qp2);
  ibv_.SetUpSelfConnectedRcQp(qp2, ibv_.GetContextAddressInfo(setup.context));

  // Bind to qp2.
  status = verbs_util::BindType2MwSync(qp2, type2_mw, setup.buffer.span(),
                                       /*rkey=*/1028, setup.mr);
  ASSERT_EQ(IBV_WC_SUCCESS, status);

  // Make sure old rkey still works.
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), /*rkey=*/kRKey);
  verbs_util::PostSend(qp2, read);
  completion = verbs_util::WaitForCompletion(cq2).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

class MWBindTest : public BasicFixture,
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
    ibv_pd* pd;
    RdmaMemBlock buffer;
    ibv_cq* cq;
    ibv_qp* qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
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
    ibv_.SetUpSelfConnectedRcQp(setup.qp,
                                ibv_.GetContextAddressInfo(setup.context));
    return setup;
  }

  void AttemptBind(const BasicSetup& setup, int mr_access, int bind_access,
                   ibv_wc_status expected) {
    ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, mr_access);
    ASSERT_NE(nullptr, mr);
    ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
    ASSERT_NE(nullptr, mw);

    // Do Bind.
    switch (GetParam()) {
      case IBV_MW_TYPE_1: {
        EXPECT_EQ(expected,
                  verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(),
                                              mr, bind_access));
      } break;
      case IBV_MW_TYPE_2: {
        static int rkey = 17;
        EXPECT_EQ(expected,
                  verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                              ++rkey, mr, bind_access));
      } break;
      default:
        CHECK(false) << "Unknown param.";
    }
  }
};

TEST_P(MWBindTest, AllPermissions) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                          IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                          IBV_ACCESS_REMOTE_WRITE;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, MissingLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_MW_BIND;
  const int kBindAccess = IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, MissingBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
                        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_MW_BIND;
  const ibv_wc_status kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? IBV_WC_MW_BIND_ERR
                                                           : IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, MissingRemoteAtomic) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_REMOTE_ATOMIC;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, MissingRemoteRead) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, MissingRemoteWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ;
  const int kBindAccess = IBV_ACCESS_REMOTE_WRITE;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, EmptyBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                        IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess = 0;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, ReadOnly) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_READ;
  const int kBindAccess = IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, WriteOnly) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_WRITE;
  const int kBindAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND | IBV_ACCESS_REMOTE_WRITE;
  const ibv_wc_status kExpected = IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

TEST_P(MWBindTest, NoMrBindAccess) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int kMrAccess = IBV_ACCESS_REMOTE_READ;
  const int kBindAccess = IBV_ACCESS_REMOTE_READ;
  const ibv_wc_status kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? IBV_WC_MW_BIND_ERR
                                                           : IBV_WC_SUCCESS;
  AttemptBind(setup, kMrAccess, kBindAccess, kExpected);
}

INSTANTIATE_TEST_SUITE_P(MWBindTestCase, MWBindTest,
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
                            ibv_.GetContextAddressInfo(setup.basic.context));

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
      ibv_.SetUpSelfConnectedRcQp(
          info.qp, ibv_.GetContextAddressInfo(setup.basic.context));
      setup.reader_only_qps.push_back(info);
    }

    // Setup MW.
    ibv_wc_status result = verbs_util::BindType2MwSync(
        setup.owner.qp, setup.mw, setup.basic.buffer.span(), kRKey,
        setup.basic.mr);
    CHECK_EQ(IBV_WC_SUCCESS, result);

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
  ibv_wc completion =
      verbs_util::WaitForCompletion(advanced.owner.cq, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_EQ(IBV_WC_BIND_MW, completion.opcode);

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
  ASSERT_EQ(0, ibv_post_send(advanced.owner.qp, &invalidate, &bad_wr));

  ibv_wc completion =
      verbs_util::WaitForCompletion(advanced.owner.cq, kBindTimeout).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_EQ(IBV_WC_LOCAL_INV, completion.opcode);

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
  int result = ibv_.DeallocMw(advanced.mw);
  ASSERT_EQ(0, result);

  JoinAll(advanced);
  VerifyFailure(advanced);
}

// TODO(author1): Permissions change (when implemented).
// TODO(author1): Addr change (when implemented).
// TODO(author1): Length change (when implemented).

}  // namespace rdma_unit_test
