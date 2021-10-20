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

#include <stddef.h>
#include <string.h>

#include <cerrno>
#include <cstdint>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/batch_op_fixture.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::IsNull;
using ::testing::NotNull;

class CqExTest : public BasicFixture {
 protected:
  struct BasicSetup {
    ibv_context* context;
    ibv_pd* pd;
    ibv_comp_channel* channel;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.channel = ibv_.CreateChannel(setup.context);
    if (!setup.channel) {
      return absl::InternalError("Failed to create channel.");
    }
    return setup;
  }
};

TEST_F(CqExTest, Basic) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, ZeroCqe) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 0};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, IsNull());
}

TEST_F(CqExTest, MaxCqe) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe)};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, AboveMaxCqe) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe) + 1};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  EXPECT_THAT(cq, IsNull());
}

TEST_F(CqExTest, WithChannel) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10, .channel = setup.channel};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, ShareChannel) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10, .channel = setup.channel};
  ibv_cq_ex* cq1 = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq1, NotNull());
  ibv_cq_ex* cq2 = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq2, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq1)), 0);
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq2)), 0);
}

TEST_F(CqExTest, ZeroCompVector) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, LargeCompVector) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10,
                              .comp_vector = static_cast<uint32_t>(
                                  setup.context->num_comp_vectors - 1)};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, AboveMaxCompVector) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{
      .cqe = 10,
      .comp_vector = static_cast<uint32_t>(setup.context->num_comp_vectors)};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, IsNull());
}

class CqExOpTest : public BatchOpFixture {
 protected:
  struct WcInfo {
    uint64_t wr_id;
    ibv_wc_status status;
    uint32_t qp_num;
    uint64_t timestamp_hcaclock = 0;
    uint64_t timestamp_wallclock = 0;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    ASSIGN_OR_RETURN(BasicSetup setup, BatchOpFixture::CreateBasicSetup());
    // Set the supported timestamp types.
    support_completion_timestamp_hcaclock_ =
        verbs_util::CheckExtendedCompletionHasCapability(
            setup.context, IBV_WC_EX_WITH_COMPLETION_TIMESTAMP);
    support_completion_timestamp_wallclock_ =
        verbs_util::CheckExtendedCompletionHasCapability(
            setup.context, IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK);
    return setup;
  }

  uint64_t GetWcFlags() {
    uint64_t wc_flags = IBV_WC_EX_WITH_QP_NUM;
    if (support_completion_timestamp_hcaclock_) {
      wc_flags |= IBV_WC_EX_WITH_COMPLETION_TIMESTAMP;
    }
    if (support_completion_timestamp_wallclock_) {
      wc_flags |= IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK;
    }
    return wc_flags;
  }

  WcInfo GetWcInfo(ibv_cq_ex* cq) {
    WcInfo wc{.wr_id = cq->wr_id,
              .status = cq->status,
              .qp_num = ibv_wc_read_qp_num(cq)};
    if (support_completion_timestamp_hcaclock_) {
      wc.timestamp_hcaclock = ibv_wc_read_completion_ts(cq);
    }
    if (support_completion_timestamp_wallclock_) {
      wc.timestamp_wallclock = ibv_wc_read_completion_wallclock_ns(cq);
    }
    return wc;
  }

  void WaitForAndVerifyCompletions(ibv_cq_ex* cq, int count) {
    ASSERT_OK(verbs_util::WaitForPollingExtendedCompletion(cq));
    WcInfo wc = GetWcInfo(cq);
    ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
    if (support_completion_timestamp_hcaclock_) {
      EXPECT_GT(wc.timestamp_hcaclock, 0);
    }
    if (support_completion_timestamp_wallclock_) {
      EXPECT_GT(wc.timestamp_wallclock, 0);
    }

    // Maps qp_num to the next wr_id.
    absl::flat_hash_map<uint32_t, uint64_t> expected_wr_id;
    expected_wr_id.insert({wc.qp_num, wc.wr_id + 1});
    uint64_t last_timestamp_hcaclock = 0;
    uint64_t last_timestamp_wallclock = 0;
    for (int i = 1; i < count; ++i) {
      ASSERT_OK(verbs_util::WaitForNextExtendedCompletion(cq));
      WcInfo wc = GetWcInfo(cq);
      ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
      auto iter = expected_wr_id.find(wc.qp_num);
      if (iter == expected_wr_id.end()) {
        expected_wr_id.insert({wc.qp_num, wc.wr_id + 1});
      } else {
        ASSERT_EQ(wc.wr_id, iter->second++);
      }
      EXPECT_GE(wc.timestamp_hcaclock, last_timestamp_hcaclock);
      EXPECT_GE(wc.timestamp_wallclock, last_timestamp_wallclock);
      last_timestamp_hcaclock = wc.timestamp_hcaclock;
      last_timestamp_wallclock = wc.timestamp_wallclock;
    }
    ibv_end_poll(cq);
  }

  // Whether WCs support HCA clock timestamps or not.
  bool support_completion_timestamp_hcaclock_ = false;
  // Whether WCs support wallclock timestamps or not.
  bool support_completion_timestamp_wallclock_ = false;
};

TEST_F(CqExOpTest, BasicPollSendCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kMaxQpWr = 1;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  QpPair qp_pair;
  qp_pair.send_qp = ibv_.CreateQp(setup.pd, ibv_cq_ex_to_cq(send_cq),
                                  ibv_cq_ex_to_cq(recv_cq), nullptr, kMaxQpWr,
                                  kMaxQpWr, IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(qp_pair.send_qp, NotNull())
      << "Failed to create send qp - " << errno;
  qp_pair.recv_qp = ibv_.CreateQp(setup.pd, ibv_cq_ex_to_cq(send_cq),
                                  ibv_cq_ex_to_cq(recv_cq), nullptr, kMaxQpWr,
                                  kMaxQpWr, IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(qp_pair.recv_qp, NotNull())
      << "Failed to create recv qp - " << errno;
  qp_pair.dst_buffer = setup.dst_memblock.subspan(kMaxQpWr, kMaxQpWr);
  ibv_.SetUpLoopbackRcQps(qp_pair.send_qp, qp_pair.recv_qp, setup.port_gid);
  QueueWrite(setup, qp_pair);

  // Wait for completion and verify timestamp.
  ASSERT_OK(verbs_util::WaitForPollingExtendedCompletion(send_cq));
  ASSERT_EQ(send_cq->status, IBV_WC_SUCCESS);
  if (support_completion_timestamp_hcaclock_) {
    EXPECT_GT(ibv_wc_read_completion_ts(send_cq), 0);
  }
  if (support_completion_timestamp_wallclock_) {
    EXPECT_GT(ibv_wc_read_completion_wallclock_ns(send_cq), 0);
  }
  ibv_end_poll(send_cq);
}

TEST_F(CqExOpTest, BasicPollRecvCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kMaxQpWr = 1;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  QpPair qp_pair;
  qp_pair.send_qp = ibv_.CreateQp(setup.pd, ibv_cq_ex_to_cq(send_cq),
                                  ibv_cq_ex_to_cq(recv_cq), nullptr, kMaxQpWr,
                                  kMaxQpWr, IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(qp_pair.send_qp, NotNull())
      << "Failed to create send qp - " << errno;
  qp_pair.recv_qp = ibv_.CreateQp(setup.pd, ibv_cq_ex_to_cq(send_cq),
                                  ibv_cq_ex_to_cq(recv_cq), nullptr, kMaxQpWr,
                                  kMaxQpWr, IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(qp_pair.recv_qp, NotNull())
      << "Failed to create recv qp - " << errno;
  qp_pair.dst_buffer = setup.dst_memblock.subspan(kMaxQpWr, kMaxQpWr);
  ibv_.SetUpLoopbackRcQps(qp_pair.send_qp, qp_pair.recv_qp, setup.port_gid);
  QueueRecv(setup, qp_pair);
  QueueSend(setup, qp_pair);

  // Wait for completion and verify timestamp.
  ASSERT_OK(verbs_util::WaitForPollingExtendedCompletion(recv_cq));
  ASSERT_EQ(recv_cq->status, IBV_WC_SUCCESS);
  if (support_completion_timestamp_hcaclock_) {
    EXPECT_GT(ibv_wc_read_completion_ts(recv_cq), 0);
  }
  if (support_completion_timestamp_wallclock_) {
    EXPECT_GT(ibv_wc_read_completion_wallclock_ns(recv_cq), 0);
  }
  ibv_end_poll(recv_cq);
}

TEST_F(CqExOpTest, BatchPollSendCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kCqSize = 60;
  static constexpr int kQpPairCount = 20;
  static constexpr int kWritesPerQpPair = kCqSize / kQpPairCount;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  std::vector<QpPair> qp_pairs = CreateTestQpPairs(
      setup, ibv_cq_ex_to_cq(send_cq), ibv_cq_ex_to_cq(recv_cq),
      kWritesPerQpPair, kQpPairCount);

  for (auto& qp_pair : qp_pairs) {
    for (int i = 0; i < kWritesPerQpPair; ++i) {
      QueueWrite(setup, qp_pair);
    }
  }
  WaitForAndVerifyCompletions(send_cq, kWritesPerQpPair * kQpPairCount);
}

TEST_F(CqExOpTest, BatchPollRecvCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kCqSize = 60;
  static constexpr int kQpPairCount = 20;
  static constexpr int kSendsPerQpPair = kCqSize / kQpPairCount;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  std::vector<QpPair> qp_pairs = CreateTestQpPairs(
      setup, ibv_cq_ex_to_cq(send_cq), ibv_cq_ex_to_cq(recv_cq),
      kSendsPerQpPair, kQpPairCount);

  for (auto& qp_pair : qp_pairs) {
    for (int i = 0; i < kSendsPerQpPair; ++i) {
      QueueRecv(setup, qp_pair);
    }
  }
  for (auto& qp_pair : qp_pairs) {
    for (int i = 0; i < kSendsPerQpPair; ++i) {
      QueueSend(setup, qp_pair);
    }
  }
  WaitForAndVerifyCompletions(recv_cq, kSendsPerQpPair);
}

}  // namespace rdma_unit_test
