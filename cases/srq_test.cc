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

#include <stdint.h>
#include <string.h>

#include <cerrno>
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
#include "cases/status_matchers.h"
#include "public/rdma-memblock.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

class SrqTest : public BasicFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr int kMaxWr = verbs_util::kDefaultMaxWr;
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

  absl::StatusOr<BasicSetup> CreateBasicSetup(uint32_t max_wr = kMaxWr) {
    BasicSetup setup;
    setup.send_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.send_buffer.data(),
              setup.send_buffer.data() + setup.send_buffer.size(),
              kSendContent);
    setup.recv_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.recv_buffer.data(),
              setup.recv_buffer.data() + setup.send_buffer.size(),
              kRecvContent);
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
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
    setup.send_cq = ibv_.CreateCq(setup.context, max_wr);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context, max_wr);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    setup.srq = ibv_.CreateSrq(setup.pd, max_wr);
    if (!setup.srq) {
      return absl::InternalError("Failed to create srq.");
    }
    setup.send_qp =
        ibv_.CreateQp(setup.pd, setup.send_cq, setup.send_cq, nullptr, max_wr,
                      max_wr, IBV_QPT_RC, /*sig_all=*/0);
    if (!setup.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    setup.recv_qp =
        ibv_.CreateQp(setup.pd, setup.recv_cq, setup.recv_cq, setup.srq, max_wr,
                      max_wr, IBV_QPT_RC, /*sig_all=*/0);
    if (!setup.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    ibv_.SetUpLoopbackRcQps(setup.send_qp, setup.recv_qp,
                            ibv_.GetContextAddressInfo(setup.context));
    return setup;
  }
};

TEST_F(SrqTest, Create) {
  ibv_context* context = ibv_.OpenDevice().value();
  ASSERT_NE(nullptr, context);
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_NE(nullptr, pd);
  ibv_srq_init_attr attr;
  attr.srq_context = context;
  attr.attr.max_sge = verbs_util::kDefaultMaxSge;
  attr.attr.max_wr = verbs_util::kDefaultMaxWr;
  attr.attr.srq_limit = 0;
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  ASSERT_NE(nullptr, srq);
  EXPECT_EQ(0, ibv_destroy_srq(srq));
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
  ibv_wc completion = verbs_util::WaitForCompletion(setup.send_cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(setup.recv_cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_THAT(setup.recv_buffer.span(), testing::Each(kSendContent));
}

TEST_F(SrqTest, MultiThreadedSrqLoopback) {
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
    ibv_wc wc = verbs_util::WaitForCompletion(setup.send_cq).value();
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS);
  }
  // The last send_wr should get an RnR.
  ibv_wc wc = verbs_util::WaitForCompletion(setup.send_cq).value();
  EXPECT_EQ(wc.status, IBV_WC_RNR_RETRY_EXC_ERR) << "too many rr in srq";

  std::vector<bool> succeeded(kTotalWr, false);
  for (int i = 0; i < kTotalWr; i++) {
    ibv_wc wc = verbs_util::WaitForCompletion(setup.recv_cq).value();
    EXPECT_FALSE(succeeded[wc.wr_id]);
    succeeded[wc.wr_id] = wc.status == IBV_WC_SUCCESS;
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS)
        << "WR # " << wc.wr_id << " failed with status " << wc.status << ".";
  }
  for (int i = 0; i < kTotalWr; ++i) {
    EXPECT_TRUE(succeeded[i]) << "WR # " << i << " not succeeded.";
  }
  EXPECT_THAT(setup.recv_buffer.subspan(0, kTotalWr),
              ::testing::Each(kSendContent));
}

TEST_F(SrqTest, PostRecvToSrqQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr = nullptr;
  int result = ibv_post_recv(setup.recv_qp, &recv, &bad_wr);
  EXPECT_EQ(ENOMEM, result);
}

TEST_F(SrqTest, OverflowSrq) {
  static constexpr uint32_t kProposedMaxWr = verbs_util::kDefaultMaxWr;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_init_attr init_attr;
  init_attr.attr.max_sge = verbs_util::kDefaultMaxSge;
  init_attr.attr.max_wr = kProposedMaxWr;
  init_attr.attr.srq_limit = 0;  // Not used by infiniband.
  ibv_srq* srq = ibv_create_srq(setup.pd, &init_attr);
  ASSERT_NE(nullptr, srq);
  uint32_t queue_size = init_attr.attr.max_wr;
  ASSERT_GE(queue_size, kProposedMaxWr);

  // Issue one more wrs to overflow the queue.
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(queue_size + 1, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  // mlx4 uses -1, mlx5 uses ENOMEM
  EXPECT_THAT(ibv_post_srq_recv(srq, recv_wrs.data(), &bad_wr),
              testing::AnyOf(-1, ENOMEM));
  ASSERT_EQ(&recv_wrs[queue_size], bad_wr);
  EXPECT_EQ(0, ibv_destroy_srq(srq));
}

TEST_F(SrqTest, ExceedsMaxWrInfinitChain) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);

  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  recv.next = &recv;

  ibv_recv_wr* bad_wr = nullptr;
  int result = ibv_post_srq_recv(setup.srq, &recv, &bad_wr);
  EXPECT_EQ(bad_wr, &recv);
  EXPECT_EQ(result, -1);  // mlx4 uses -1
}

TEST_F(SrqTest, ExceedsMaxSge) {
  static constexpr uint32_t kProposedMaxSge = verbs_util::kDefaultMaxSge;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_init_attr init_attr;
  init_attr.attr.max_sge = kProposedMaxSge;
  init_attr.attr.max_wr = verbs_util::kDefaultMaxWr;
  init_attr.attr.srq_limit = 0;  // Not used by infiniband.
  ibv_srq* srq = ibv_.CreateSrq(setup.pd, init_attr);
  ASSERT_NE(nullptr, srq);
  uint32_t sge_cap = init_attr.attr.max_sge;
  ASSERT_GE(sge_cap, kProposedMaxSge);

  ASSERT_LT(sge_cap, setup.recv_buffer.size());
  std::vector<ibv_sge> sges(sge_cap + 1);
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
  EXPECT_THAT(ibv_post_srq_recv(srq, &recv_invalid, &bad_wr),
              testing::AnyOf(-1, EINVAL));
  EXPECT_EQ(&recv_invalid, bad_wr);
}

TEST_F(SrqTest, RnR) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.send_buffer.span(), setup.send_mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  verbs_util::PostSend(setup.send_qp, send);
  ibv_wc completion =
      verbs_util::WaitForCompletion(setup.send_cq,
                                    verbs_util::kDefaultErrorCompletionTimeout)
          .value();
  EXPECT_THAT(completion.status,
              testing::AnyOf(IBV_WC_RNR_RETRY_EXC_ERR, IBV_WC_RETRY_EXC_ERR));
  EXPECT_THAT(setup.recv_buffer.span(), ::testing::Each(kRecvContent));
}

}  // namespace rdma_unit_test
