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
#include <string.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/status_matchers.h"
#include "public/rdma-memblock.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

class PdTest : public BasicFixture {
 protected:
  static constexpr size_t kBufferMemoryPages = 1;
};

TEST_F(PdTest, OpenPd) {
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_pd* pd = ibv_.AllocPd(context);
  EXPECT_NE(nullptr, pd);
}

TEST_F(PdTest, OpenManyPd) {
  ibv_context* context = ibv_.OpenDevice().value();
  for (int i = 0; i < 500; ++i) {
    auto* pd = ibv_.AllocPd(context);
    EXPECT_NE(nullptr, pd);
  }
}

TEST_F(PdTest, DeleteUnknownPd) {
  if (!Introspection().CorrectlyReportsInvalidObjects()) GTEST_SKIP();
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_pd dummy;
  dummy.context = context;
  EXPECT_EQ(ENOENT, ibv_dealloc_pd(&dummy));
}

// Check with a pointer in the correct range.
TEST_F(PdTest, DeleteInvalidPd) {
  if (!Introspection().CorrectlyReportsInvalidObjects()) GTEST_SKIP();
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_NE(nullptr, cq);
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  fake_pd->handle = original.handle;
  EXPECT_EQ(EINVAL, ibv_dealloc_pd(fake_pd));
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

TEST_F(PdTest, AllocQpWithFakePd) {
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_NE(nullptr, cq);
  ibv_cq* cq2 = ibv_.CreateCq(context);
  ASSERT_NE(nullptr, cq2);
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  ibv_qp* qp = ibv_.CreateQp(fake_pd, cq2);
  EXPECT_EQ(nullptr, qp);
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

TEST_F(PdTest, AllocMrWithPd) {
  ibv_context* context = ibv_.OpenDevice().value();
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_NE(nullptr, pd);
  ibv_mr* mr = ibv_.RegMr(pd, buffer);
  EXPECT_NE(nullptr, mr);
}

TEST_F(PdTest, AllocMrWithInvalidPd) {
  ibv_context* context = ibv_.OpenDevice().value();
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_NE(nullptr, cq);
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  ibv_mr* mr = ibv_.RegMr(fake_pd, buffer);
  EXPECT_EQ(nullptr, mr);
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

TEST_F(PdTest, AllocMwWithInvalidPd) {
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_NE(nullptr, cq);
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  ibv_mw* mw = ibv_.AllocMw(fake_pd, IBV_MW_TYPE_1);
  EXPECT_EQ(nullptr, mw);
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

class PdBindTest : public BasicFixture,
                   public ::testing::WithParamInterface<ibv_mw_type> {
 public:
  void SetUp() override {
    if (GetParam() == IBV_MW_TYPE_1 && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "NIC does not support type 1.";
    }
    if (GetParam() == IBV_MW_TYPE_2 && !Introspection().SupportsType2()) {
      GTEST_SKIP() << "NIC does not support type 2.";
    }
  }

 protected:
  static constexpr uint32_t kType2RKey = 1024;
  static constexpr uint32_t kClientMemoryPages = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    ibv_cq* cq;
    ibv_pd* qp_pd;
    ibv_pd* other_pd;
    ibv_qp* qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.qp_pd = ibv_.AllocPd(setup.context);
    if (!setup.qp_pd) {
      return absl::InternalError("Failed to create the qp's pd.");
    }
    setup.other_pd = ibv_.AllocPd(setup.context);
    if (!setup.other_pd) {
      return absl::InternalError("Failed to create the other pd.");
    }
    setup.qp = ibv_.CreateQp(setup.qp_pd, setup.cq);
    if (!setup.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    ibv_.SetUpSelfConnectedRcQp(setup.qp,
                                ibv_.GetContextAddressInfo(setup.context));
    return setup;
  }
};

TEST_P(PdBindTest, MwOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_mw* mw = ibv_.AllocMw(setup.other_pd, GetParam());
  if (GetParam() == IBV_MW_TYPE_1) {
    // Some clients do client side validation on type 1. First check
    // succcess/failure of the bind and if successful than check for completion.
    ibv_mw_bind bind_args = verbs_util::CreateType1MwBind(
        /*wr_id=*/1, setup.buffer.span(), mr,
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);
    int result = ibv_bind_mw(setup.qp, mw, &bind_args);
    EXPECT_THAT(result, testing::AnyOf(0, EPERM));
    if (result == 0) {
      ibv_wc completion =
          verbs_util::WaitForCompletion(setup.qp->send_cq).value();
      if (completion.status == IBV_WC_SUCCESS) {
        EXPECT_EQ(IBV_WC_BIND_MW, completion.opcode);
      }
    }

  } else {
    EXPECT_EQ(IBV_WC_MW_BIND_ERR,
              verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                          kType2RKey, mr));
  }
}

TEST_P(PdBindTest, MrOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mw* mw = ibv_.AllocMw(setup.qp_pd, GetParam());
  if (GetParam() == IBV_MW_TYPE_1) {
    // Some clients do client side validation on type 1. First check
    // succcess/failure of the bind and if successful than check for completion.
    ibv_mw_bind bind_args = verbs_util::CreateType1MwBind(
        /*wr_id=*/1, setup.buffer.span(), mr,
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);
    int result = ibv_bind_mw(setup.qp, mw, &bind_args);
    EXPECT_THAT(result, testing::AnyOf(0, EPERM));
    if (result == 0) {
      ibv_wc completion =
          verbs_util::WaitForCompletion(setup.qp->send_cq).value();
      if (completion.status == IBV_WC_SUCCESS) {
        EXPECT_EQ(IBV_WC_BIND_MW, completion.opcode);
      }
    }
  } else {
    EXPECT_EQ(IBV_WC_MW_BIND_ERR,
              verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                          kType2RKey, mr));
  }
}

TEST_P(PdBindTest, MrMwOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mw* mw = ibv_.AllocMw(setup.other_pd, GetParam());
  if (GetParam() == IBV_MW_TYPE_1) {
    EXPECT_EQ(IBV_WC_MW_BIND_ERR, verbs_util::BindType1MwSync(
                                      setup.qp, mw, setup.buffer.span(), mr));
  } else {
    EXPECT_EQ(IBV_WC_MW_BIND_ERR,
              verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                          kType2RKey, mr));
  }
}

INSTANTIATE_TEST_SUITE_P(PdBindTestCases, PdBindTest,
                         ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2));

class PdRcLoopbackMrTest : public BasicFixture {
 protected:
  static constexpr uint32_t kClientMemoryPages = 1;
  static constexpr uint64_t kCompareAdd = 1;
  static constexpr uint64_t kSwap = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    ibv_cq* local_cq;
    ibv_cq* remote_cq;
    ibv_pd* qp_pd;
    ibv_pd* other_pd;
    ibv_qp* local_qp;
    ibv_qp* remote_qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.local_cq = ibv_.CreateCq(setup.context);
    if (!setup.local_cq) {
      return absl::InternalError("Failed to create local cq.");
    }
    setup.remote_cq = ibv_.CreateCq(setup.context);
    if (!setup.remote_cq) {
      return absl::InternalError("Failed to create remote cq.");
    }
    setup.qp_pd = ibv_.AllocPd(setup.context);
    if (!setup.qp_pd) {
      return absl::InternalError("Failed to the qp's pd.");
    }
    setup.other_pd = ibv_.AllocPd(setup.context);
    if (!setup.other_pd) {
      return absl::InternalError("Failed to another pd.");
    }
    setup.local_qp = ibv_.CreateQp(setup.qp_pd, setup.local_cq);
    if (!setup.local_qp) {
      return absl::InternalError("Failed to create local qp.");
    }
    setup.remote_qp = ibv_.CreateQp(setup.qp_pd, setup.remote_cq);
    if (!setup.remote_qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    ibv_.SetUpLoopbackRcQps(setup.local_qp, setup.remote_qp,
                            ibv_.GetContextAddressInfo(setup.context));
    return setup;
  }
};

TEST_F(PdRcLoopbackMrTest, SendMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), remote_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);

  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=1*/ 1);
  verbs_util::PostSend(setup.local_qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, SendMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), remote_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);

  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=1*/ 1);
  verbs_util::PostSend(setup.local_qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.remote_cq).value();
  EXPECT_THAT(completion.status,
              testing::AnyOf(IBV_WC_LOC_PROT_ERR, IBV_WC_LOC_QP_OP_ERR));
  completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_REM_OP_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicReadMrOtherPdLocal) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicReadMrOtherPdRemote) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicWriteMrOtherPdLocal) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicWriteMrOtherPdRemote) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicFetchAddMrOtherPdLocal) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicFetchAddMrOtherPdRemote) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicCompSwapMrOtherPdLocal) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

TEST_F(PdRcLoopbackMrTest, BasicCompSwapMrOtherPdRemote) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);

  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

class PdUdLoopbackTest : public BasicFixture {
 protected:
  static constexpr uint32_t kClientMemoryPages = 1;
  static constexpr uint32_t kMaxQpWr = 200;
  static constexpr int kQKey = 200;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    ibv_cq* local_cq;
    ibv_cq* remote_cq;
    ibv_pd* qp_pd;
    ibv_pd* other_pd;
    ibv_qp* local_qp;
    ibv_qp* remote_qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.local_cq = ibv_.CreateCq(setup.context);
    if (!setup.local_cq) {
      return absl::InternalError("Failed to create local cq.");
    }
    setup.remote_cq = ibv_.CreateCq(setup.context);
    if (!setup.remote_cq) {
      return absl::InternalError("Failed to create remote cq.");
    }
    setup.qp_pd = ibv_.AllocPd(setup.context);
    if (!setup.qp_pd) {
      return absl::InternalError("Failed to allocate the qp's pd.");
    }
    setup.other_pd = ibv_.AllocPd(setup.context);
    if (!setup.other_pd) {
      return absl::InternalError("Failed to allocate another pd.");
    }
    setup.local_qp =
        ibv_.CreateQp(setup.qp_pd, setup.local_cq, setup.local_cq, nullptr,
                      kMaxQpWr, kMaxQpWr, IBV_QPT_UD, /*sig_all=*/0);
    if (!setup.local_qp) {
      return absl::InternalError("Failed to create local qp.");
    }
    absl::Status status = ibv_.SetUpUdQp(
        setup.local_qp, ibv_.GetContextAddressInfo(setup.context), kQKey);
    if (!status.ok()) {
      return absl::InternalError("Failed to set up local ud qp.");
    }
    setup.remote_qp =
        ibv_.CreateQp(setup.qp_pd, setup.remote_cq, setup.remote_cq, nullptr,
                      kMaxQpWr, kMaxQpWr, IBV_QPT_UD, /*sig_all=*/0);
    if (!setup.remote_qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    status = ibv_.SetUpUdQp(setup.remote_qp,
                            ibv_.GetContextAddressInfo(setup.context), kQKey);
    if (!status.ok()) {
      return absl::InternalError("Failed to set up remote ud qp.");
    }
    return setup;
  }
};

TEST_F(PdUdLoopbackTest, SendAhOnOtherPd) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateAh(setup.other_pd);
  ibv_mr* mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = setup.remote_qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(setup.local_qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.local_cq).value();
  // It should return IBV_WC_LOC_QP_OP_ERR, see InfiniBand™ Architecture
  // Specification, Volume 1, P466, C10-10.
  EXPECT_THAT(completion.status,
              ::testing::AnyOf(IBV_WC_LOC_QP_OP_ERR, IBV_WC_SUCCESS));
}

class Type1MwPdTest : public BasicFixture {
 public:
  void SetUp() override {
    if (!Introspection().SupportsType1()) {
      GTEST_SKIP() << "Nic does not support MW";
    }
  }

 protected:
  static constexpr size_t kClientMemoryPages = 1;
  static constexpr uint64_t kCompareAdd = 1;
  static constexpr uint64_t kSwap = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    ibv_cq* cq;
    ibv_pd* mw_pd;
    ibv_mw* mw;
    ibv_pd* qp_pd;
    ibv_qp* qp;
    ibv_mr* mr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    // Create and Bind mw on pd mw_pd.
    setup.mw_pd = ibv_.AllocPd(setup.context);
    if (!setup.mw_pd) {
      return absl::InternalError("Failed to allocate the mw's pd");
    }
    setup.mw = ibv_.AllocMw(setup.mw_pd, IBV_MW_TYPE_1);
    if (!setup.mw) {
      return absl::InternalError("Failed to allocate mw.");
    }
    ibv_mr* mr = ibv_.RegMr(setup.mw_pd, setup.buffer);
    if (!mr) {
      return absl::InternalError("Failed to register bind mr.");
    }
    ibv_qp* bind_qp = ibv_.CreateQp(setup.mw_pd, setup.cq);
    if (!bind_qp) {
      return absl::InternalError("Failed to create qp for bind.");
    }
    ibv_.SetUpSelfConnectedRcQp(bind_qp,
                                ibv_.GetContextAddressInfo(setup.context));
    ibv_wc_status status =
        verbs_util::BindType1MwSync(bind_qp, setup.mw, setup.buffer.span(), mr);
    if (status != IBV_WC_SUCCESS) {
      return absl::InternalError("Failed to bind qp.");
    }
    // Create a QP on qp_pd and bring it up. Also create an MR on the same Pd.
    setup.qp_pd = ibv_.AllocPd(setup.context);
    if (!setup.qp_pd) {
      return absl::InternalError("Failed to allocate qp's pd.");
    }
    setup.qp = ibv_.CreateQp(setup.qp_pd, setup.cq);
    if (!setup.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    ibv_.SetUpSelfConnectedRcQp(setup.qp,
                                ibv_.GetContextAddressInfo(setup.context));
    setup.mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to create mr.");
    }
    return setup;
  }
};

TEST_F(Type1MwPdTest, ReadMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey);
  verbs_util::PostSend(setup.qp, read);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(Type1MwPdTest, WriteMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey);
  verbs_util::PostSend(setup.qp, write);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(Type1MwPdTest, FetchAddMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(Type1MwPdTest, CompSwapMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  sge.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.qp, comp_swap);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

class SrqPdTest : public BasicFixture {
 protected:
  static constexpr size_t kBufferMemoryPages = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context = nullptr;
    ibv_cq* cq = nullptr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    return setup;
  }
};

TEST_F(SrqPdTest, CreateSrq) {
  // Fun fact: SRQ can be of different QP with its associated QP(s).
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_NE(nullptr, pd1);
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_NE(nullptr, pd1);
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_NE(nullptr, srq);
  ibv_qp* qp = ibv_.CreateQp(pd2, setup.cq, srq);
  EXPECT_NE(nullptr, qp);
}

// When Pd of receive MR matches SRQ but not the receive QP.
TEST_F(SrqPdTest, SrqRecvMrSrqMatch) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_NE(nullptr, pd1);
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_NE(nullptr, pd1);
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_NE(nullptr, srq);
  ibv_qp* qp = ibv_.CreateQp(pd2, setup.cq, srq);
  ASSERT_NE(nullptr, qp);
  ibv_.SetUpSelfConnectedRcQp(qp, ibv_.GetContextAddressInfo(setup.context));
  ibv_mr* mr_recv = ibv_.RegMr(pd1, setup.buffer);
  ibv_mr* mr_send = ibv_.RegMr(pd2, setup.buffer);

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), mr_recv);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr_send);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(qp, send);

  for (int i = 0; i < 2; ++i) {
    ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
    EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  }
}

// When Pd of receive MR matches the QP but not the SRQ.
TEST_F(SrqPdTest, SrqRecvMrSrqMismatch) {
  if (!Introspection().CorrectlyReportsPdErrors()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_NE(nullptr, pd1);
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_NE(nullptr, pd1);
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_NE(nullptr, srq);
  ibv_qp* recv_qp = ibv_.CreateQp(pd2, recv_cq, srq);
  ASSERT_NE(nullptr, recv_qp);
  ibv_qp* send_qp = ibv_.CreateQp(pd2, send_cq);
  ASSERT_NE(nullptr, send_qp);
  ibv_.SetUpLoopbackRcQps(send_qp, recv_qp,
                          ibv_.GetContextAddressInfo(setup.context));
  ibv_mr* mr = ibv_.RegMr(pd2, setup.buffer);

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(send_qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(send_cq).value();
  EXPECT_EQ(IBV_WC_REM_OP_ERR, completion.status);
  completion = verbs_util::WaitForCompletion(recv_cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

// TODO(author1): Create Max
// TODO(author1): Threaded Pd creation/closure

}  // namespace rdma_unit_test
