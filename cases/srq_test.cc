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

#include <string.h>

#include <cerrno>
#include <cstdint>
#include <optional>
#include <thread>  // NOLINT

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::Each;
using ::testing::IsNull;
using ::testing::NotNull;

class SrqTest : public BasicFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr char kSendContent = 'a';
  static constexpr char kRecvContent = 'b';

  struct BasicSetup {
    RdmaMemBlock send_buffer;
    RdmaMemBlock recv_buffer;
    ibv_context* context = nullptr;
    ibv_pd* pd = nullptr;
    ibv_mr* send_mr = nullptr;
    ibv_mr* recv_mr = nullptr;
    ibv_cq* send_cq = nullptr;
    ibv_cq* recv_cq = nullptr;
    ibv_srq* srq = nullptr;
    ibv_qp* send_qp = nullptr;
    ibv_qp* recv_qp = nullptr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.send_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.send_buffer.data(),
              setup.send_buffer.data() + setup.send_buffer.size(),
              kSendContent);
    setup.recv_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.recv_buffer.data(),
              setup.recv_buffer.data() + setup.send_buffer.size(),
              kRecvContent);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.send_mr = ibv_.RegMr(setup.pd, setup.send_buffer);
    if (!setup.send_mr) {
      return absl::InternalError("Failed to register send mr.");
    }
    setup.recv_mr = ibv_.RegMr(setup.pd, setup.recv_buffer);
    if (!setup.recv_mr) {
      return absl::InternalError("Failed to register recv mr.");
    }
    setup.send_cq = ibv_.CreateCq(setup.context);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    setup.srq = ibv_.CreateSrq(setup.pd);
    if (!setup.srq) {
      return absl::InternalError("Failed to create srq.");
    }
    setup.send_qp = ibv_.CreateQp(setup.pd, setup.send_cq);
    if (!setup.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    setup.recv_qp = ibv_.CreateQp(setup.pd, setup.recv_cq, setup.srq);
    if (!setup.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    ibv_.SetUpLoopbackRcQps(setup.send_qp, setup.recv_qp,
                            ibv_.GetLocalPortGid(setup.context));
    return setup;
  }
};

TEST_F(SrqTest, Create) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ASSERT_THAT(context, NotNull());
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  ibv_srq_init_attr attr;
  attr.attr = verbs_util::DefaultSrqAttr();
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  ASSERT_THAT(srq, NotNull());
  EXPECT_EQ(ibv_destroy_srq(srq), 0);
}

TEST_F(SrqTest, CreateWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ASSERT_THAT(context, NotNull());
  ibv_pd pd = {};
  pd.context = context;
  pd.handle = -1;
  ibv_srq_init_attr attr;
  attr.attr = verbs_util::DefaultSrqAttr();
  ASSERT_THAT(ibv_create_srq(&pd, &attr), IsNull());
}

TEST_F(SrqTest, Loopback) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge rsge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(setup.srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.send_buffer.span(), setup.send_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(setup.send_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.send_cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion,
                       verbs_util::WaitForCompletion(setup.recv_cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(setup.recv_buffer.span(), Each(kSendContent));
}

TEST_F(SrqTest, PostRecvToSrqQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_recv(setup.recv_qp, &recv, &bad_wr),
              AnyOf(EINVAL, ENOMEM));
}

TEST_F(SrqTest, MaxWr) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_attr attr;
  ASSERT_EQ(ibv_query_srq(setup.srq, &attr), 0);
  uint32_t max_wr = attr.max_wr;

  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(max_wr, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, recv_wrs.data(), &bad_wr), 0);
}

TEST_F(SrqTest, ExceedsMaxWr) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_attr attr;
  ASSERT_EQ(ibv_query_srq(setup.srq, &attr), 0);
  uint32_t max_wr = attr.max_wr;

  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(max_wr, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, recv_wrs.data(), &bad_wr), 0);
}

TEST_F(SrqTest, ModifyMaxWr) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  if (!Introspection().CheckCapability(IBV_DEVICE_SRQ_RESIZE)) {
    GTEST_SKIP() << "Device does not support SRQ resizing.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_attr attr;
  ASSERT_EQ(ibv_query_srq(setup.srq, &attr), 0);
  uint32_t max_wr = attr.max_wr;

  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(max_wr, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, recv_wrs.data(), &bad_wr), 0);
  // Post another RR to SRQ.
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv, &bad_wr),
              AnyOf(-1, ENOMEM, -ENOMEM));
  // Increment SRQ capacity and post again.
  attr.max_wr = attr.max_wr + 10;
  ASSERT_EQ(ibv_modify_srq(setup.srq, &attr, IBV_SRQ_MAX_WR), 0);
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, &recv, &bad_wr), 0);
}

TEST_F(SrqTest, ExceedsMaxWrInfinitChain) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  recv.next = &recv;
  ibv_recv_wr* bad_wr = nullptr;
  // mlx4 uses -1, mlx5 uses ENOMEM
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv, &bad_wr),
              AnyOf(-1, ENOMEM, -ENOMEM));
  EXPECT_EQ(bad_wr, &recv);
}

TEST_F(SrqTest, ExceedsDeviceCap) {
  const ibv_device_attr& device_attr = Introspection().device_attr();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_init_attr init_attr_bad_wr{.attr = verbs_util::DefaultSrqAttr()};
  init_attr_bad_wr.attr.max_wr =
      static_cast<uint32_t>(device_attr.max_srq_wr) + 1;
  EXPECT_EQ(ibv_.CreateSrq(setup.pd, init_attr_bad_wr), nullptr) << "max_wr";
  ibv_srq_init_attr init_attr_bad_sge = {
      .srq_context = nullptr,
      .attr = {.max_wr = verbs_util::kDefaultMaxWr,
               .max_sge = static_cast<uint32_t>(device_attr.max_srq_sge) + 1}};
  EXPECT_EQ(ibv_.CreateSrq(setup.pd, init_attr_bad_sge), nullptr) << "max_sge";
}

TEST_F(SrqTest, MaxSge) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_attr attr;
  ASSERT_EQ(ibv_query_srq(setup.srq, &attr), 0);
  uint32_t max_sge = attr.max_sge;

  ASSERT_LT(max_sge, setup.recv_buffer.size());
  std::vector<ibv_sge> sges(max_sge);
  for (size_t i = 0; i < sges.size(); ++i) {
    sges[i] =
        verbs_util::CreateSge(setup.recv_buffer.subspan(i, 1), setup.recv_mr);
  }
  ibv_recv_wr recv_valid = verbs_util::CreateRecvWr(
      /*wr_id=*/0, sges.data(), /*num_sge=*/sges.size() - 1);
  ibv_recv_wr recv_invalid = verbs_util::CreateRecvWr(/*wr_id=*/1, sges.data(),
                                                      /*num_sge=*/sges.size());
  recv_valid.next = &recv_valid;
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv_invalid, &bad_wr), 0);
}

TEST_F(SrqTest, ExceedsMaxSge) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_attr attr;
  ASSERT_EQ(ibv_query_srq(setup.srq, &attr), 0);
  uint32_t max_sge = attr.max_sge;

  ASSERT_LT(max_sge, setup.recv_buffer.size());
  std::vector<ibv_sge> sges(max_sge + 1);
  for (size_t i = 0; i < sges.size(); ++i) {
    sges[i] =
        verbs_util::CreateSge(setup.recv_buffer.subspan(i, 1), setup.recv_mr);
  }
  ibv_recv_wr recv_valid = verbs_util::CreateRecvWr(
      /*wr_id=*/0, sges.data(), /*num_sge=*/sges.size() - 1);
  ibv_recv_wr recv_invalid = verbs_util::CreateRecvWr(/*wr_id=*/1, sges.data(),
                                                      /*num_sge=*/sges.size());
  recv_valid.next = &recv_valid;
  ibv_recv_wr* bad_wr = nullptr;
  // mlx4 uses -1, mlx5 uses EINVAL
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv_invalid, &bad_wr),
              AnyOf(-1, EINVAL, -EINVAL));
  EXPECT_EQ(bad_wr, &recv_invalid);
}

TEST_F(SrqTest, RnR) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.send_buffer.span(), setup.send_mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  verbs_util::PostSend(setup.send_qp, send);
  ASSERT_OK_AND_ASSIGN(
      ibv_wc completion,
      verbs_util::WaitForCompletion(
          setup.send_cq, verbs_util::kDefaultErrorCompletionTimeout));
  EXPECT_THAT(completion.status,
              AnyOf(IBV_WC_RNR_RETRY_EXC_ERR, IBV_WC_RETRY_EXC_ERR));
  EXPECT_THAT(setup.recv_buffer.span(), Each(kRecvContent));
}

class SrqMultiThreadTest : public SrqTest {
 public:
  // Multithreaded tests uses [queue_capacity] to reserve enough space in all
  // queues, i.e. CQ, QP and SRQ.
  absl::StatusOr<BasicSetup> CreateBasicSetup(uint32_t queue_capacity) {
    BasicSetup setup;
    setup.send_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.send_buffer.data(),
              setup.send_buffer.data() + setup.send_buffer.size(),
              kSendContent);
    setup.recv_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.recv_buffer.data(),
              setup.recv_buffer.data() + setup.send_buffer.size(),
              kRecvContent);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.send_mr = ibv_.RegMr(setup.pd, setup.send_buffer);
    if (!setup.send_mr) {
      return absl::InternalError("Failed to register send mr.");
    }
    setup.recv_mr = ibv_.RegMr(setup.pd, setup.recv_buffer);
    if (!setup.recv_mr) {
      return absl::InternalError("Failed to register recv mr.");
    }
    setup.send_cq = ibv_.CreateCq(setup.context, queue_capacity);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context, queue_capacity);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    setup.srq = ibv_.CreateSrq(setup.pd, queue_capacity);
    if (!setup.srq) {
      return absl::InternalError("Failed to create srq.");
    }
    setup.send_qp = ibv_.CreateQp(setup.pd, setup.send_cq, setup.send_cq,
                                  /*srq=*/nullptr, queue_capacity,
                                  queue_capacity, IBV_QPT_RC, /*sig_all=*/0);
    if (!setup.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    setup.recv_qp = ibv_.CreateQp(setup.pd, setup.recv_cq, setup.recv_cq,
                                  setup.srq, queue_capacity, queue_capacity,
                                  IBV_QPT_RC, /*sig_all=*/0);
    if (!setup.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    ibv_.SetUpLoopbackRcQps(setup.send_qp, setup.recv_qp,
                            ibv_.GetLocalPortGid(setup.context));
    return setup;
  }
};

TEST_F(SrqMultiThreadTest, MultiThreadedSrqLoopback) {
  // concurrecy parameters
  static constexpr int kThreadCount = 50;
  static constexpr int kWrPerThread = 20;
  static constexpr int kTotalWr = kThreadCount * kWrPerThread;
  static constexpr uint32_t kRequestMaxWr = kTotalWr + 10;
  static_assert(kTotalWr <= kBufferMemoryPages * verbs_util::kPageSize,
                "Buffer too small for one byte send. Reduce total WRs or "
                "increase buffer size.");
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kRequestMaxWr));

  // Concurrently post buffers into the SRQ using multiple threads
  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; thread_id++) {
    threads.push_back(std::thread([thread_id, &setup]() {
      for (int i = 0; i < kWrPerThread; i++) {
        uint32_t wr_id = thread_id * kWrPerThread + i;
        ibv_sge sge = verbs_util::CreateSge(
            setup.recv_buffer.subspan(/*offset=*/wr_id, 1), setup.recv_mr);
        ibv_recv_wr recv = verbs_util::CreateRecvWr(wr_id, &sge, /*num_sge=*/1);
        verbs_util::PostSrqRecv(setup.srq, recv);
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // Post sends (1 more than expected).
  for (int i = 0; i <= kTotalWr; i++) {
    ibv_sge sge =
        verbs_util::CreateSge(setup.send_buffer.subspan(i, 1), setup.send_mr);
    ibv_send_wr send =
        verbs_util::CreateSendWr(/*wr_id=*/kTotalWr + i, &sge, /*num_sge=*/1);
    verbs_util::PostSend(setup.send_qp, send);
  }
  // Poll send completions.
  for (int i = 0; i < kTotalWr; i++) {
    ASSERT_OK_AND_ASSIGN(ibv_wc wc,
                         verbs_util::WaitForCompletion(setup.send_cq));
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS);
  }
  // The last send_wr should get an RnR.
  ASSERT_OK_AND_ASSIGN(ibv_wc wc, verbs_util::WaitForCompletion(setup.send_cq));
  EXPECT_EQ(wc.status, IBV_WC_RNR_RETRY_EXC_ERR) << "too many rr in srq";

  std::vector<bool> succeeded(kTotalWr, false);
  for (int i = 0; i < kTotalWr; i++) {
    ASSERT_OK_AND_ASSIGN(ibv_wc wc,
                         verbs_util::WaitForCompletion(setup.recv_cq));
    EXPECT_FALSE(succeeded[wc.wr_id]);
    succeeded[wc.wr_id] = wc.status == IBV_WC_SUCCESS;
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS)
        << "WR # " << wc.wr_id << " failed with status " << wc.status << ".";
  }
  for (int i = 0; i < kTotalWr; ++i) {
    EXPECT_TRUE(succeeded[i]) << "WR # " << i << " not succeeded.";
  }
  EXPECT_THAT(setup.recv_buffer.subspan(0, kTotalWr), Each(kSendContent));
}

}  // namespace rdma_unit_test
