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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <thread>  // NOLINT
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "internal/handle_garble.h"
#include "internal/verbs_attribute.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"

#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/loopback_fixture.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::Field;
using ::testing::Ne;
using ::testing::NotNull;

class MwFixture : public LoopbackFixture {
 protected:
  void SetUp() override {
    LoopbackFixture::SetUp();
    if (!Introspection().SupportsType2() && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Skipping due to lack of MW support.";
    }
  }

  // Post a type 1 or type 2 bind verbs to QP, depending on the type of `mw`.
  int PostBind(ibv_qp* qp, ibv_mw* mw, absl::Span<uint8_t> buffer, ibv_mr* mr) {
    constexpr int kAccess = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                            IBV_ACCESS_REMOTE_ATOMIC;
    switch (mw->type) {
      case IBV_MW_TYPE_1: {
        ibv_mw_bind bind =
            verbs_util::CreateType1MwBindWr(/*wr_id=*/1, buffer, mr, kAccess);
        return ibv_bind_mw(qp, mw, &bind);
      }
      case IBV_MW_TYPE_2: {
        static uint32_t rkey = 1024;
        ibv_send_wr bind = verbs_util::CreateType2BindWr(
            /*wr_id=*/1, mw, buffer, rkey, mr, kAccess);
        ibv_send_wr* bad_wr = nullptr;
        int status = ibv_post_send(qp, &bind, &bad_wr);
        return status;
      }
      default: {
        LOG(WARNING) << "Unknown mw type " << mw->type;
        return -1;
      }
    }
  }

  // Post a type 1 or type 2 bind verbs to QP, depending on the type of `mw`,
  // then wait and return its completion.
  absl::StatusOr<ibv_wc_status> ExecuteBind(
      ibv_qp* qp, ibv_mw* mw, absl::Span<uint8_t> buffer, ibv_mr* mr,
      int access = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                   IBV_ACCESS_REMOTE_ATOMIC) {
    switch (mw->type) {
      case IBV_MW_TYPE_1:
        return verbs_util::ExecuteType1MwBind(qp, mw, buffer, mr, access);
      case IBV_MW_TYPE_2:
        static uint32_t rkey = 1024;
        return verbs_util::ExecuteType2MwBind(qp, mw, buffer, rkey++, mr,
                                              access);
    }
  }
};

class MwGeneralTest : public MwFixture,
                      public testing::WithParamInterface<ibv_mw_type> {
 protected:
  void SetUp() override {
    MwFixture::SetUp();
    if (GetParam() == IBV_MW_TYPE_1 && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Requires Type 1 MW support.";
    }
    if (GetParam() == IBV_MW_TYPE_2 && !Introspection().SupportsType2()) {
      GTEST_SKIP() << "Requires Type 2 MW support.";
    }
  }
};

TEST_P(MwGeneralTest, Alloc) {
  const ibv_mw_type mw_type = GetParam();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_alloc_mw(setup.pd, mw_type);
  ASSERT_THAT(mw, NotNull());
  EXPECT_EQ(mw->pd, setup.pd);
  EXPECT_EQ(mw->type, mw_type);
  EXPECT_EQ(ibv_dealloc_mw(mw), 0);
}

TEST_P(MwGeneralTest, DeallocInvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(mw->handle);
  EXPECT_EQ(ibv_dealloc_mw(mw), ENOENT);
}

TEST_P(MwGeneralTest, Bind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(ExecuteBind(setup.local_qp, mw, setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(MwGeneralTest, Read) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(ExecuteBind(setup.remote_qp, mw, setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.local_qp, read);
  EXPECT_THAT(verbs_util::WaitForCompletion(setup.cq),
              IsOkAndHolds(Field(&ibv_wc::status, IBV_WC_SUCCESS)));
}

TEST_P(MwGeneralTest, ReadZeroBased) {
  if (GetParam() == IBV_MW_TYPE_1) {
    GTEST_SKIP()
        << "Zero based access is not supported for type 1 MW according "
           "to the verbs specification.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  int access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_ZERO_BASED;
  ASSERT_THAT(ExecuteBind(setup.remote_qp, mw, setup.buffer.span(), setup.mr,
                          access_flags),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               /*remote_buffer=*/nullptr, mw->rkey);
  verbs_util::PostSend(setup.local_qp, read);
  EXPECT_THAT(verbs_util::WaitForCompletion(setup.cq),
              IsOkAndHolds(Field(&ibv_wc::status, IBV_WC_SUCCESS)));
}

TEST_P(MwGeneralTest, BindReadDiffQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  // Not bound on remote qp.
  ASSERT_THAT(ExecuteBind(setup.local_qp, mw, setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.local_qp, read);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  enum ibv_wc_status type2_expected =
      Introspection().GeneratesRetryExcOnConnTimeout() ? IBV_WC_RETRY_EXC_ERR
                                                       : IBV_WC_REM_ACCESS_ERR;
  EXPECT_EQ(completion.status,
            GetParam() == IBV_MW_TYPE_1 ? IBV_WC_SUCCESS : type2_expected);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.qp_num, setup.local_qp->qp_num);
}

// Binding with invalid resource (QP, MW, MR) handle can go three ways:
// 1. Interface returns immediate error when trying to post to the QP with an
// invalid QP (not MR/MW) handle. This is
//    specified by the IBTA spec (See IBTA 11.4.1.1 Post Send Request).
// 2. Posting to QP is successful but the bind completes in error.
// 3  Posting to QP is successful and the bind succeeds. This is probably due to
//    some driver ignoring handles and only using the relevant info in the user
//    space struct.
TEST_P(MwGeneralTest, BindInvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(setup.mr->handle);
  int result = PostBind(setup.remote_qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(result, 0);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.remote_qp->send_cq));
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_MW_BIND_ERR));
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.qp_num, setup.remote_qp->qp_num);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_SUCCESS));
  }
}

TEST_P(MwGeneralTest, BindInvalidMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(mw->handle);
  int result = PostBind(setup.remote_qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_EQ(result, 0);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_qp->send_cq));
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_MW_BIND_ERR));
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.qp_num, setup.remote_qp->qp_num);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_SUCCESS));
  } else {
    EXPECT_EQ(verbs_util::GetQpState(setup.remote_qp), IBV_QPS_ERR);
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_RETRY_EXC_ERR));
  }
}

TEST_P(MwGeneralTest, BindInvalidQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  HandleGarble garble(setup.remote_qp->handle);
  int result = PostBind(setup.remote_qp, mw, setup.buffer.span(), setup.mr);
  ASSERT_THAT(result, AnyOf(0, ENOENT));
  if (result == 0) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                setup.remote_qp->send_cq));
    EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_MW_BIND_ERR));
    EXPECT_EQ(completion.wr_id, 1);
    EXPECT_EQ(completion.qp_num, setup.remote_qp->qp_num);
    if (completion.status == IBV_WC_SUCCESS) {
      EXPECT_THAT(
          verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                      setup.mr, setup.buffer.data(), mw->rkey),
          IsOkAndHolds(IBV_WC_SUCCESS));
    }
  }
}

TEST_P(MwGeneralTest, DeregMrWhenBound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(ExecuteBind(setup.remote_qp, mw, setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  int result = ibv_.DeregMr(setup.mr);
  // Some NICs allow deregistering MRs with MW bound.
  if (result == 0) {
    EXPECT_EQ(0, 1);
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_REM_OP_ERR));
  } else {
    EXPECT_EQ(result, EBUSY);
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_SUCCESS));
  }
}

INSTANTIATE_TEST_SUITE_P(
    MwGeneralTest, MwGeneralTest,
    ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2),
    [](const testing::TestParamInfo<MwGeneralTest::ParamType>& info) {
      switch (info.param) {
        case IBV_MW_TYPE_1:
          return "Type1";
        case IBV_MW_TYPE_2:
          return "Type2";
      }
    });

class MwType1Test : public LoopbackFixture {
 protected:
  void SetUp() override {
    LoopbackFixture::SetUp();
    if (!Introspection().SupportsType1()) {
      GTEST_SKIP() << "Nic does not support Type1 MW";
    }
  }
};

TEST_F(MwType1Test, BindZeroLengthMw) {
  // For type 1 MW can invalid the RKEY by binding a zero length MW.
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType1MwBind(setup.local_qp, mw,
                                             setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  uint32_t original_rkey = mw->rkey;
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_THAT(
      verbs_util::ExecuteType1MwBind(
          setup.local_qp, mw, setup.buffer.span().subspan(0, 0), setup.mr),
      IsOkAndHolds(IBV_WC_SUCCESS));
  // Verify the original RKey is not accessible.
  EXPECT_THAT(
      verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(), setup.mr,
                                  setup.buffer.data(), original_rkey),
      IsOkAndHolds(expected));
}

TEST_F(MwType1Test, UnsignaledBindError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock unregistered_memory = ibv_.AllocBuffer(/*pages = */ 1);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ibv_mw_bind bind = verbs_util::CreateType1MwBindWr(
      /*wr_id=*/1, unregistered_memory.span(), setup.mr);
  bind.send_flags = bind.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostType1Bind(setup.remote_qp, mw, bind);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
}

TEST_F(MwType1Test, DestroyQpWithType1Bound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType1MwBind(setup.remote_qp, mw,
                                             setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  EXPECT_EQ(ibv_.DestroyQp(setup.remote_qp), 0);
}

TEST_F(MwType1Test, CrossQpManagement) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType1MwBind(setup.local_qp, mw,
                                             setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // Rebind on second.
  EXPECT_THAT(verbs_util::ExecuteType1MwBind(setup.remote_qp, mw,
                                             setup.buffer.span(), setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType1Test, DeallocPdWithOutstandingMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  ASSERT_THAT(mw, NotNull());
  EXPECT_EQ(ibv_.DeallocPd(setup.pd), EBUSY);
}

TEST_F(MwType1Test, BindType1WhenQpError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_OK(ibv_.ModifyQpToError(setup.remote_qp));
  ASSERT_EQ(verbs_util::GetQpState(setup.remote_qp), IBV_QPS_ERR);

  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_1);
  EXPECT_THAT(verbs_util::ExecuteType1MwBind(setup.remote_qp, mw,
                                             setup.buffer.span(), mr),
              IsOkAndHolds(IBV_WC_WR_FLUSH_ERR));
}

class MwType2Test : public LoopbackFixture {
 protected:
  // Only the last 8 bit of the R_Key is owned by the user.
  static constexpr uint32_t kIndexMask = 0xffffff00;
  static constexpr uint32_t kKeyMask = 0xff;
  static constexpr uint32_t kRKey = 1024;

  void SetUp() override {
    LoopbackFixture::SetUp();
    if (!Introspection().SupportsType2()) {
      GTEST_SKIP() << "Nic does not support Type2 MW";
    }
  }
};

TEST_F(MwType2Test, Bind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.local_qp, mw, setup.buffer.span(), kRKey, mr,
                  IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                      IBV_ACCESS_REMOTE_ATOMIC),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, UnsignaledBind) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, mw2, setup.buffer.span(), kRKey, setup.mr);
  bind.send_flags = bind.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.remote_qp, bind);

  // Second bind will expect its completion.
  constexpr uint32_t kOtherRKey = kRKey + 1;
  ibv_send_wr bind2 = verbs_util::CreateType2BindWr(
      /*wr_id=*/2, type2_mw, setup.buffer.span(), kOtherRKey, setup.mr);
  verbs_util::PostSend(setup.remote_qp, bind2);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.wr_id, 2);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
}

TEST_F(MwType2Test, BindZeroLengthMw) {
  // Type 2 MW forbid zero length bind.
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.local_qp, mw, setup.buffer.subspan(0, 0), kRKey, mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, BindRKeyReuse) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw1 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw1, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw1, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_mw* mw2 = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw2, NotNull());
  // Although we are asking for the same R_KEY, the NIC will only extract the
  // final 8 bit from the user's request. Difference in index (higher 24 bits)
  // will still make the second MW's R_KEY valid.
  EXPECT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw2, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  EXPECT_NE(mw1->rkey, mw2->rkey);
}

// The following tests that rkeys assigned to memory windows are distinct.
// An rkey is 32 bit in length - 8 (least significant) bits is the
// user-supplied key and the rest 24 bits, called index, is assigned by the HCA.
// The user is free to pass the same 8 bits for each memory window bind, hence
// the index must be distinct.
TEST_F(MwType2Test, DistinctRKeys) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::flat_hash_set<uint32_t> used;
  // Create as many memory windows as possible, but less than 4096.
  // 4096 memory windows gives us a 40% chance of rkey collision,
  // assuming that a buggy implementation assigns rkeys randomly.
  const uint32_t kNumMws = std::min(4096, Introspection().device_attr().max_mw);
  for (uint32_t i = 0; i < kNumMws; ++i) {
    ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
    ASSERT_THAT(mw, NotNull()) << "mw is null. iteration number = " << i;
    EXPECT_THAT(verbs_util::ExecuteType2MwBind(
                    setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
                IsOkAndHolds(IBV_WC_SUCCESS));
    EXPECT_EQ(kRKey & 0xff, mw->rkey & 0xff);
    // The following call to insert will only return true if
    // the memory window key has not been assigned before.
    // Note that because the last 8 user-supplied bits are the same,
    // verifying this is equivalent to verifying that the index is distinct.
    EXPECT_TRUE(used.insert(mw->rkey).second) << "duplicate key: " << mw->rkey;
  }
}

TEST_F(MwType2Test, UnsignaledBindError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock uniregistered_memory = ibv_.AllocBuffer(/*pages = */ 1);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, mw, uniregistered_memory.span(), kRKey, setup.mr);
  bind.send_flags = bind.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.remote_qp, bind);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
}

TEST_F(MwType2Test, LocalInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ASSERT_THAT(verbs_util::ExecuteLocalInvalidate(setup.remote_qp, mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_THAT(
      verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(), setup.mr,
                                  setup.buffer.data(), mw->rkey),
      IsOkAndHolds(expected));
}

TEST_F(MwType2Test, UnsignaledLocalInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_send_wr invalidate =
      verbs_util::CreateLocalInvalidateWr(/*wr_id=*/1, mw->rkey);
  invalidate.send_flags = invalidate.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.remote_qp, invalidate);
  // Do another operation to ensure no completion.
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_THAT(
      verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(), setup.mr,
                                  setup.buffer.data(), mw->rkey),
      IsOkAndHolds(expected));
}

TEST_F(MwType2Test, UnsignaledLocalInvalidateError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_send_wr invalidate =
      verbs_util::CreateLocalInvalidateWr(/*wr_id=*/1, mw->rkey + 1);
  invalidate.send_flags = invalidate.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(setup.local_qp, invalidate);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  // General Memory Management Error (IB Spec 11.6.2) undefined in libibverbs.
  EXPECT_THAT(completion.status, Ne(IBV_WC_SUCCESS));
}

TEST_F(MwType2Test, Type1BindOnType2) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* type2_mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(type2_mw, NotNull());
  ibv_mw_bind bind_arg = verbs_util::CreateType1MwBindWr(
      /*wr_id=*/1, setup.buffer.span(), setup.mr);
  EXPECT_EQ(ibv_bind_mw(setup.remote_qp, type2_mw, &bind_arg), EINVAL);
}

TEST_F(MwType2Test, DestroyQpWithType2Bound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // IBTA o10-2.2.5: HCA must allow consumer to destroy the QP while Type 2B MW
  // are still bound to the QP.
  EXPECT_EQ(ibv_.DestroyQp(setup.remote_qp), 0);
}

TEST_F(MwType2Test, DeregMrWhenBound) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // See IBTA 10.6.7.2.6 DEREGISTERING REGIONS WITH BOUND WINDOWS, roughly
  // speaking, the HCA has options whether to allow the deregistration to be
  // carried out, potentially leaving "orphan" mw. If the HCA allow orphan MW,
  // it must guarantee to check whether the buffer is registered at the time of
  // remote access.
  int result = ibv_.DeregMr(setup.mr);
  if (result == 0) {
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));

  } else {
    EXPECT_EQ(result, EBUSY);
    EXPECT_THAT(
        verbs_util::ExecuteRdmaRead(setup.local_qp, setup.buffer.span(),
                                    setup.mr, setup.buffer.data(), mw->rkey),
        IsOkAndHolds(IBV_WC_SUCCESS));
  }
}

TEST_F(MwType2Test, RebindToDifferentQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  ibv_qp* new_local_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(new_local_qp, NotNull());
  ibv_qp* new_remote_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(new_remote_qp, NotNull());
  ASSERT_OK(
      ibv_.SetUpLoopbackRcQps(new_local_qp, new_remote_qp, setup.port_attr));
  // Type 2 MW does not allow rebinding an already bound MW. See IBTA table 78.
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  new_remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, RebindToDifferentRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // Type 2 MW does not allow rebinding an already bound MW. See IBTA table 78.
  constexpr uint32_t kNewRKey = kRKey + 1;
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kNewRKey, setup.mr),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, CrossQpInvalidate) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  // MW is bound to `setup.remote_qp`, so invalidating on `setup.local_qp`
  // should fail.
  ASSERT_THAT(verbs_util::ExecuteLocalInvalidate(setup.local_qp, mw->rkey),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_F(MwType2Test, CrossQpRead) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_THAT(mw, NotNull());
  ASSERT_THAT(verbs_util::ExecuteType2MwBind(
                  setup.remote_qp, mw, setup.buffer.span(), kRKey, setup.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));

  ibv_qp* new_local_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(new_local_qp, NotNull());
  ibv_qp* new_remote_qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(new_remote_qp, NotNull());
  ASSERT_OK(
      ibv_.SetUpLoopbackRcQps(new_local_qp, new_remote_qp, setup.port_attr));

  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_THAT(
      verbs_util::ExecuteRdmaRead(new_local_qp, setup.buffer.span(), setup.mr,
                                  setup.buffer.data(), mw->rkey),
      IsOkAndHolds(expected));
}

TEST_F(MwType2Test, BindType1WhenQpError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_OK(ibv_.ModifyQpToError(setup.remote_qp));
  ASSERT_EQ(verbs_util::GetQpState(setup.remote_qp), IBV_QPS_ERR);

  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  EXPECT_THAT(verbs_util::ExecuteType2MwBind(setup.remote_qp, mw,
                                             setup.buffer.span(), kRKey, mr),
              IsOkAndHolds(IBV_WC_WR_FLUSH_ERR));
}

struct MwBindAccessTestParameters {
  int mr_access;
  int mw_access;
  std::vector<ibv_wc_status> expected_bind_status;
  std::string name;
};

class MwBindAccessTest
    : public MwFixture,
      public ::testing::WithParamInterface<
          std::tuple<ibv_mw_type, MwBindAccessTestParameters>> {
 protected:
  void SetUp() override {
    MwFixture::SetUp();
    if (std::get<ibv_mw_type>(GetParam()) == IBV_MW_TYPE_1 &&
        !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Nic does not support Type1 MW";
    }
    if (std::get<ibv_mw_type>(GetParam()) == IBV_MW_TYPE_2 &&
        !Introspection().SupportsType2()) {
      GTEST_SKIP() << "Nic does not support Type2 MW";
    }
  }

  absl::StatusOr<ibv_wc_status> ExecuteBind(ibv_mw_type bind_type, ibv_qp* qp,
                                            ibv_mw* mw,
                                            absl::Span<uint8_t> buffer,
                                            ibv_mr* mr, int access) {
    static uint32_t rkey = 1024;
    switch (bind_type) {
      case IBV_MW_TYPE_1:
        return verbs_util::ExecuteType1MwBind(qp, mw, buffer, mr, access);
      case IBV_MW_TYPE_2:
        return verbs_util::ExecuteType2MwBind(qp, mw, buffer, rkey++, mr,
                                              access);
    }
  }
};

TEST_P(MwBindAccessTest, MwBindAccessTests) {
  const ibv_mw_type mw_type = std::get<ibv_mw_type>(GetParam());
  const MwBindAccessTestParameters params =
      std::get<MwBindAccessTestParameters>(GetParam());

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer, params.mr_access);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, mw_type);
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(ExecuteBind(mw_type, setup.remote_qp, mw, setup.buffer.span(), mr,
                          params.mw_access),
              IsOkAndHolds(testing::AnyOfArray(params.expected_bind_status)));
}

INSTANTIATE_TEST_SUITE_P(
    MwBindAccessTest, MwBindAccessTest,
    ::testing::Combine(
        ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2),
        ::testing::Values(
            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND |
                             IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_WRITE,
                .mw_access = IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_WRITE,
                .expected_bind_status = {IBV_WC_SUCCESS},
                .name = "AllAccessPermissions"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_MW_BIND,
                .mw_access = 0,
                .expected_bind_status = {IBV_WC_SUCCESS},
                .name = "MrBindAccessSufficientForMwBind"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
                             IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
                .mw_access = 0,
                .expected_bind_status = {IBV_WC_MW_BIND_ERR,
                                         IBV_WC_LOC_QP_OP_ERR,
                                         IBV_WC_WR_FLUSH_ERR},
                .name = "MrBindAccessNecessaryForMwBind"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_MW_BIND,
                .mw_access = IBV_ACCESS_REMOTE_READ,
                .expected_bind_status = {IBV_WC_SUCCESS},
                .name = "MwRemoteReadRequiresOnlyMrBindAccess"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND,
                .mw_access = IBV_ACCESS_REMOTE_WRITE,
                .expected_bind_status = {IBV_WC_SUCCESS},
                .name = "MrLocalWriteSufficientForMwRemoteWrite"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_MW_BIND,
                .mw_access = IBV_ACCESS_REMOTE_WRITE,
                .expected_bind_status = {IBV_WC_MW_BIND_ERR,
                                         IBV_WC_LOC_QP_OP_ERR,
                                         IBV_WC_WR_FLUSH_ERR},
                .name = "MrLocalWriteNecessaryForMwRemoteWrite"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_MW_BIND,
                .mw_access = IBV_ACCESS_REMOTE_ATOMIC,
                .expected_bind_status = {IBV_WC_SUCCESS},
                .name = "MrLocalAtomicSufficientForMwRemoteAtomic"},

            MwBindAccessTestParameters{
                .mr_access = IBV_ACCESS_MW_BIND,
                .mw_access = IBV_ACCESS_REMOTE_ATOMIC,
                .expected_bind_status = {IBV_WC_MW_BIND_ERR,
                                         IBV_WC_LOC_QP_OP_ERR,
                                         IBV_WC_WR_FLUSH_ERR},
                .name = "MrLocalAtomicNecessaryForMwRemoteAtomic"}

            )),
    [](const testing::TestParamInfo<MwBindAccessTest::ParamType>& info) {
      return absl::StrCat(std::get<MwBindAccessTestParameters>(info.param).name,
                          std::get<ibv_mw_type>(info.param) == IBV_MW_TYPE_1
                              ? "Type1"
                              : "Type2");
    });

// TODO(author1): Invalid pd (when support is added).
class MwType2AdvancedTest : public LoopbackFixture {
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
  static constexpr uint32_t kRKey = 1024;

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
    setup.owner.qp = ibv_.CreateQp(setup.basic.pd, setup.owner.cq, IBV_QPT_RC);
    if (!setup.owner.qp) {
      return absl::InternalError("Failed to create owner's qp.");
    }
    setup.reader.cq = ibv_.CreateCq(setup.basic.context, kMaxOutstanding);
    if (!setup.reader.cq) {
      return absl::InternalError("Failed to reader's cq.");
    }
    setup.reader.qp = ibv_.CreateQp(setup.basic.pd, setup.reader.cq, IBV_QPT_RC,
                                    QpInitAttribute()
                                        .set_max_send_wr(kMaxOutstanding)
                                        .set_max_recv_wr(kMaxOutstanding));
    if (!setup.reader.qp) {
      return absl::InternalError("Failed to reader's qp.");
    }
    RETURN_IF_ERROR(ibv_.SetUpLoopbackRcQps(setup.owner.qp, setup.reader.qp,
                                            setup.basic.port_attr));
    ASSIGN_OR_RETURN(ibv_wc_status result,
                     verbs_util::ExecuteType2MwBind(setup.owner.qp, setup.mw,
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
    // Block until at least a few ops complete.
    while (total_reads.load(std::memory_order_relaxed) < kWarmupCount) {
      absl::SleepFor(absl::Milliseconds(20));
    }
  }

  // Checks that all readers completed with the same failure.
  void VerifyFailure(AdvancedSetup& advanced,
                     std::optional<ibv_wc_status> status_override = {}) const {
    ibv_wc_status status = advanced.reader.failing_completion.status;
    for (const QpInfo& info : advanced.reader_only_qps) {
      EXPECT_EQ(status, info.failing_completion.status)
          << "Not all QPs failed with the same error.";
    }

    if (status_override.has_value()) {
      EXPECT_EQ(status, status_override.value());
      return;
    }

    enum ibv_wc_status expected =
        Introspection().GeneratesRetryExcOnConnTimeout()
            ? IBV_WC_RETRY_EXC_ERR
            : IBV_WC_REM_ACCESS_ERR;
    EXPECT_EQ(status, expected);
  }

  // Joins all reader threads.
  static void JoinAll(AdvancedSetup& advanced) {
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
  auto thread_cleanup = absl::MakeCleanup([&advanced] { JoinAll(advanced); });

  LOG(INFO) << "Started reader";

  // Issue rebind.
  ibv_send_wr bind = verbs_util::CreateType2BindWr(
      /*wr_id=*/1, advanced.mw, advanced.basic.buffer.span(),
      advanced.mw->rkey + 1, advanced.basic.mr);
  verbs_util::PostSend(advanced.owner.qp, bind);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(advanced.owner.cq));
  ASSERT_EQ(completion.status, IBV_WC_MW_BIND_ERR);
  ASSERT_EQ(completion.opcode, IBV_WC_BIND_MW);

  std::move(thread_cleanup).Invoke();
  LOG(INFO) << "Reader end.";

  VerifyFailure(advanced);
}

TEST_F(MwType2AdvancedTest, Invalidate) {
  ASSERT_OK_AND_ASSIGN(AdvancedSetup advanced, CreateAdvancedSetup());
  absl::Notification cancel_notification;
  std::atomic<size_t> total_reads = 0;
  StartReaderThreads(advanced, advanced.mw->rkey, total_reads,
                     cancel_notification);
  auto thread_cleanup = absl::MakeCleanup([&advanced] { JoinAll(advanced); });

  // Invalidate.
  ibv_send_wr invalidate =
      verbs_util::CreateLocalInvalidateWr(/*wr_id=*/1, advanced.mw->rkey);
  ibv_send_wr* bad_wr;
  ASSERT_EQ(ibv_post_send(advanced.owner.qp, &invalidate, &bad_wr), 0);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(advanced.owner.cq));
  ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(completion.opcode, IBV_WC_LOCAL_INV);

  std::move(thread_cleanup).Invoke();
  VerifyFailure(advanced);
}

TEST_F(MwType2AdvancedTest, Dealloc) {
  ASSERT_OK_AND_ASSIGN(AdvancedSetup advanced, CreateAdvancedSetup());
  absl::Notification cancel_notification;
  std::atomic<size_t> total_reads = 0;
  StartReaderThreads(advanced, advanced.mw->rkey, total_reads,
                     cancel_notification);
  auto thread_cleanup = absl::MakeCleanup([&advanced] { JoinAll(advanced); });

  // Delete.
  ASSERT_EQ(ibv_.DeallocMw(advanced.mw), 0);

  std::move(thread_cleanup).Invoke();
  VerifyFailure(advanced);
}

// TODO(author1): Permissions change (when implemented).
// TODO(author1): Addr change (when implemented).
// TODO(author1): Length change (when implemented).

}  // namespace rdma_unit_test
