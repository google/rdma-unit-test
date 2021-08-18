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
#include <thread>  // NOLINT
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
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::IsNull;
using ::testing::NotNull;

class PdTest : public BasicFixture {
 protected:
  static constexpr size_t kBufferMemoryPages = 1;
};

TEST_F(PdTest, OpenPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_pd* pd = ibv_.AllocPd(context);
  EXPECT_THAT(pd, NotNull());
}

TEST_F(PdTest, OpenManyPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  for (int i = 0; i < 500; ++i) {
    auto* pd = ibv_.AllocPd(context);
    EXPECT_THAT(pd, NotNull());
  }
}

TEST_F(PdTest, ThreadedAlloc) {
  static constexpr int kThreadCount = 5;
  static constexpr int kPdsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());

  std::array<std::array<ibv_pd*, kPdsPerThread>, kThreadCount> pds;
  pds = {{{nullptr}}};
  auto alloc_pds = [this, &context, &pds](int thread_id) {
    // No PDs can share the same position in the array, so no need for thread
    // synchronization.
    for (int i = 0; i < kPdsPerThread; ++i) {
      pds[thread_id][i] = ibv_.AllocPd(context);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(alloc_pds, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_pds : pds) {
    for (const auto& pd : thread_pds) {
      EXPECT_THAT(pd, NotNull());
    }
  }
}

TEST_F(PdTest, ThreadedAllocAndDealloc) {
  static constexpr int kThreadCount = 5;
  static constexpr int kPdsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());

  // Initialize to 1 since we are expecting the values to be 0 after
  // deallocating PDs.
  std::array<std::array<int, kPdsPerThread>, kThreadCount> dealloc_results;
  dealloc_results = {{{1}}};
  auto alloc_dealloc_pds = [this, &context, &dealloc_results](int thread_id) {
    for (int i = 0; i < kPdsPerThread; ++i) {
      ibv_pd* pd = ibv_.AllocPd(context);
      ASSERT_THAT(pd, NotNull());
      dealloc_results[thread_id][i] = ibv_.DeallocPd(pd);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(alloc_dealloc_pds, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_results : dealloc_results) {
    for (const auto& dealloc_result : thread_results) {
      EXPECT_EQ(dealloc_result, 0);
    }
  }
}

TEST_F(PdTest, DeleteUnknownPd) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_pd dummy{.context = context, .handle = 0};
  EXPECT_EQ(ENOENT, ibv_dealloc_pd(&dummy));
}

// Check with a pointer in the correct range.
TEST_F(PdTest, DeleteInvalidPd) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_THAT(cq, NotNull());
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  fake_pd->handle = original.handle;
  EXPECT_THAT(ibv_dealloc_pd(fake_pd), AnyOf(EINVAL, ENOENT));
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

TEST_F(PdTest, AllocQpWithFakePd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_THAT(cq, NotNull());
  ibv_cq* cq2 = ibv_.CreateCq(context);
  ASSERT_THAT(cq2, NotNull());
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  EXPECT_THAT(ibv_.CreateQp(fake_pd, cq2), IsNull());
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

TEST_F(PdTest, AllocMrWithPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  ibv_mr* mr = ibv_.RegMr(pd, buffer);
  EXPECT_THAT(mr, NotNull());
}

TEST_F(PdTest, AllocMrWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_THAT(cq, NotNull());
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  EXPECT_THAT(ibv_.RegMr(fake_pd, buffer), IsNull());
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

TEST_F(PdTest, AllocMwWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_THAT(cq, NotNull());
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_pd) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_pd* fake_pd = reinterpret_cast<ibv_pd*>(cq);
  fake_pd->context = context;
  EXPECT_EQ(ibv_.AllocMw(fake_pd, IBV_MW_TYPE_1), nullptr);
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
    verbs_util::PortGid port_gid;
    ibv_context* context;
    ibv_cq* cq;
    ibv_pd* qp_pd;
    ibv_pd* other_pd;
    ibv_qp* qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
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
    ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
    return setup;
  }
};

TEST_P(PdBindTest, MwOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.other_pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  if (GetParam() == IBV_MW_TYPE_1) {
    // Some clients do client side validation on type 1. First check
    // succcess/failure of the bind and if successful than check for completion.
    ibv_mw_bind bind_args = verbs_util::CreateType1MwBind(
        /*wr_id=*/1, setup.buffer.span(), mr,
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);
    int result = ibv_bind_mw(setup.qp, mw, &bind_args);
    EXPECT_THAT(result, AnyOf(0, EPERM));
    if (result == 0) {
      ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                           verbs_util::WaitForCompletion(setup.qp->send_cq));
      if (completion.status == IBV_WC_SUCCESS) {
        EXPECT_EQ(completion.opcode, IBV_WC_BIND_MW);
      }
    }

  } else {
    EXPECT_THAT(verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                            kType2RKey, mr),
                IsOkAndHolds(IBV_WC_MW_BIND_ERR));
  }
}

TEST_P(PdBindTest, MrOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.qp_pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  if (GetParam() == IBV_MW_TYPE_1) {
    // Some clients do client side validation on type 1. First check
    // succcess/failure of the bind and if successful than check for completion.
    ibv_mw_bind bind_args = verbs_util::CreateType1MwBind(
        /*wr_id=*/1, setup.buffer.span(), mr,
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);
    int result = ibv_bind_mw(setup.qp, mw, &bind_args);
    EXPECT_THAT(result, AnyOf(0, EPERM));
    if (result == 0) {
      ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                           verbs_util::WaitForCompletion(setup.qp->send_cq));
      if (completion.status == IBV_WC_SUCCESS) {
        EXPECT_EQ(completion.opcode, IBV_WC_BIND_MW);
      }
    }
  } else {
    ASSERT_OK_AND_ASSIGN(
        ibv_wc_status status,
        verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                    kType2RKey, mr));
    EXPECT_EQ(status, IBV_WC_MW_BIND_ERR);
  }
}

TEST_P(PdBindTest, MrMwOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ibv_mw* mw = ibv_.AllocMw(setup.other_pd, GetParam());
  if (GetParam() == IBV_MW_TYPE_1) {
    EXPECT_THAT(
        verbs_util::BindType1MwSync(setup.qp, mw, setup.buffer.span(), mr),
        IsOkAndHolds(IBV_WC_MW_BIND_ERR));
  } else {
    EXPECT_THAT(verbs_util::BindType2MwSync(setup.qp, mw, setup.buffer.span(),
                                            kType2RKey, mr),
                IsOkAndHolds(IBV_WC_MW_BIND_ERR));
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
    verbs_util::PortGid port_gid;
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
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
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
    ibv_.SetUpLoopbackRcQps(setup.local_qp, setup.remote_qp, setup.port_gid);
    return setup;
  }
};

TEST_F(PdRcLoopbackMrTest, SendMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), remote_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);

  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=1*/ 1);
  verbs_util::PostSend(setup.local_qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, SendMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), remote_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);

  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=1*/ 1);
  verbs_util::PostSend(setup.local_qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.remote_cq));
  EXPECT_THAT(completion.status,
              AnyOf(IBV_WC_LOC_PROT_ERR, IBV_WC_LOC_QP_OP_ERR));
  ASSERT_OK_AND_ASSIGN(completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicReadMrOtherPdLocal) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicReadMrOtherPdRemote) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicWriteMrOtherPdLocal) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicWriteMrOtherPdRemote) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicFetchAddMrOtherPdLocal) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicFetchAddMrOtherPdRemote) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicCompSwapMrOtherPdLocal) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicCompSwapMrOtherPdRemote) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

class PdUdLoopbackTest : public BasicFixture {
 public:
  void SetUp() override {
    if (!Introspection().SupportsUdQp()) {
      GTEST_SKIP() << "Nic does not support UD QP";
    }
  }

 protected:
  static constexpr uint32_t kClientMemoryPages = 1;
  static constexpr uint32_t kMaxQpWr = 200;
  static constexpr int kQKey = 200;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    verbs_util::PortGid port_gid;
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
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
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
    absl::Status status = ibv_.SetUpUdQp(setup.local_qp, setup.port_gid, kQKey);
    if (!status.ok()) {
      return absl::InternalError("Failed to set up local ud qp.");
    }
    setup.remote_qp =
        ibv_.CreateQp(setup.qp_pd, setup.remote_cq, setup.remote_cq, nullptr,
                      kMaxQpWr, kMaxQpWr, IBV_QPT_UD, /*sig_all=*/0);
    if (!setup.remote_qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    status = ibv_.SetUpUdQp(setup.remote_qp, setup.port_gid, kQKey);
    if (!status.ok()) {
      return absl::InternalError("Failed to set up remote ud qp.");
    }
    return setup;
  }
};

TEST_F(PdUdLoopbackTest, SendAhOnOtherPd) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateAh(setup.other_pd, setup.port_gid.gid);
  ASSERT_THAT(ah, NotNull());
  ibv_mr* mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = setup.remote_qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(setup.local_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  // It should return IBV_WC_LOC_QP_OP_ERR, see InfiniBand™ Architecture
  // Specification, Volume 1, P466, C10-10.
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_LOC_QP_OP_ERR, IBV_WC_SUCCESS));
}

class PdType1MwTest : public BasicFixture {
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
    verbs_util::PortGid port_gid;
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
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
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
    ibv_.SetUpSelfConnectedRcQp(bind_qp, setup.port_gid);
    ASSIGN_OR_RETURN(ibv_wc_status status,
                     verbs_util::BindType1MwSync(bind_qp, setup.mw,
                                                 setup.buffer.span(), mr));
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
    ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
    setup.mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to create mr.");
    }
    return setup;
  }
};

TEST_F(PdType1MwTest, ReadMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey);
  verbs_util::PostSend(setup.qp, read);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdType1MwTest, WriteMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey);
  verbs_util::PostSend(setup.qp, write);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdType1MwTest, FetchAddMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.qp, fetch_add);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdType1MwTest, CompSwapMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  sge.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mw->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.qp, comp_swap);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

class PdSrqTest : public BasicFixture {
 protected:
  static constexpr size_t kBufferMemoryPages = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context = nullptr;
    verbs_util::PortGid port_gid;
    ibv_cq* cq = nullptr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    return setup;
  }
};

TEST_F(PdSrqTest, CreateSrq) {
  // Fun fact: SRQ can be of different QP with its associated QP(s).
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_THAT(srq, NotNull());
  ibv_qp* qp = ibv_.CreateQp(pd2, setup.cq, srq);
  EXPECT_THAT(qp, NotNull());
}

// When Pd of receive MR matches SRQ but not the receive QP.
TEST_F(PdSrqTest, SrqRecvMrSrqMatch) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_THAT(srq, NotNull());
  ibv_qp* qp = ibv_.CreateQp(pd2, setup.cq, srq);
  ASSERT_THAT(qp, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp, setup.port_gid);
  ibv_mr* mr_recv = ibv_.RegMr(pd1, setup.buffer);
  ASSERT_THAT(mr_recv, NotNull());
  ibv_mr* mr_send = ibv_.RegMr(pd2, setup.buffer);
  ASSERT_THAT(mr_send, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), mr_recv);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr_send);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(qp, send);

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  }
}

// When Pd of receive MR matches the QP but not the SRQ.
TEST_F(PdSrqTest, SrqRecvMrSrqMismatch) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "NIC does not handle PD errors.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_THAT(srq, NotNull());
  ibv_qp* recv_qp = ibv_.CreateQp(pd2, recv_cq, srq);
  ASSERT_THAT(recv_qp, NotNull());
  ibv_qp* send_qp = ibv_.CreateQp(pd2, send_cq);
  ASSERT_THAT(send_qp, NotNull());
  ibv_.SetUpLoopbackRcQps(send_qp, recv_qp, setup.port_gid);
  ibv_mr* mr = ibv_.RegMr(pd2, setup.buffer);
  ASSERT_THAT(mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(send_qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(send_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(recv_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

// TODO(author1): Create Max

}  // namespace rdma_unit_test
