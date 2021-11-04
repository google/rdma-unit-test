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
#include <limits>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/barrier.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/batch_op_fixture.h"
#include "internal/handle_garble.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::IsNull;
using ::testing::NotNull;

class CqTest : public BasicFixture {
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

TEST_F(CqTest, Basic) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr, 0);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(cq), 0);
}

TEST_F(CqTest, DestroyInvalidCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq, NotNull());
  HandleGarble garble(cq->handle);
  ASSERT_EQ(ibv_destroy_cq(cq), ENOENT);
}

TEST_F(CqTest, NegativeCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_create_cq(setup.context, -1, nullptr, nullptr, 0), IsNull());
}

TEST_F(CqTest, ZeroCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_create_cq(setup.context, 0, nullptr, nullptr, 0), IsNull());
}

TEST_F(CqTest, MaxCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq =
      ibv_create_cq(setup.context, Introspection().device_attr().max_cqe,
                    nullptr, nullptr, 0);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(cq), 0);
}

TEST_F(CqTest, AboveMaxCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq =
      ibv_create_cq(setup.context, Introspection().device_attr().max_cqe + 1,
                    nullptr, nullptr, 0);
  EXPECT_THAT(cq, IsNull());
}

TEST_F(CqTest, WithChannel) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, setup.channel, 0);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(cq), 0);
}

TEST_F(CqTest, ShareChannel) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq1 = ibv_create_cq(setup.context, 10, nullptr, setup.channel, 0);
  ASSERT_THAT(cq1, NotNull());
  ibv_cq* cq2 = ibv_create_cq(setup.context, 10, nullptr, setup.channel, 0);
  ASSERT_THAT(cq2, NotNull());
  ASSERT_EQ(ibv_destroy_cq(cq1), 0);
  ASSERT_EQ(ibv_destroy_cq(cq2), 0);
}

TEST_F(CqTest, NegativeCompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_THAT(ibv_create_cq(setup.context, 10, nullptr, nullptr, -1), IsNull());
}

TEST_F(CqTest, ZeroCompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr, 0);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(cq), 0);
}

TEST_F(CqTest, LargeCompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr,
                             setup.context->num_comp_vectors - 1);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(cq), 0);
}

TEST_F(CqTest, AboveMaxCompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_THAT(ibv_create_cq(setup.context, 10, nullptr, nullptr,
                            setup.context->num_comp_vectors),
              IsNull());
}

// TODO(author1): Many/Max channels
// TODO(author1): Test lookup/delete with a different kind of object.
// TODO(author1): (likely in a different test) messing with comp vectors.

class CqBatchOpTest : public BatchOpFixture {
 protected:
  // Wait for |count| completions, and verify that:
  // (1) Completions come back with status IBV_WC_SUCCESS.
  // (2) Completions come back with the right qp_num.
  // (3) Completions come back in order.
  void WaitForAndVerifyCompletions(ibv_cq* cq, int count) {
    // Maps qp_num to the next wr_id.
    absl::flat_hash_map<uint32_t, uint64_t> expected_wr_id;
    for (int i = 0; i < count; ++i) {
      ASSERT_OK_AND_ASSIGN(ibv_wc wc, verbs_util::WaitForCompletion(cq));
      ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
      auto iter = expected_wr_id.find(wc.qp_num);
      if (iter == expected_wr_id.end()) {
        expected_wr_id.insert({wc.qp_num, wc.wr_id + 1});
      } else {
        ASSERT_EQ(wc.wr_id, iter->second++);
      }
    }
  }
};

TEST_F(CqBatchOpTest, SendSharedCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  static constexpr int kQueuePairCount = 2;
  const int writes_per_queue_pair = (cq->cqe - 10) / kQueuePairCount;
  const int total_completions = writes_per_queue_pair * kQueuePairCount;
  std::vector<QpPair> qp_pairs =
      CreateTestQpPairs(setup, cq, cq, writes_per_queue_pair, kQueuePairCount);
  ThreadedSubmission(
      qp_pairs, writes_per_queue_pair,
      [&setup, this](QpPair& qp_pair) { QueueWrite(setup, qp_pair); });
  WaitForAndVerifyCompletions(cq, total_completions);
}

// 2 CQs posting recv completions to a single completion queue.
TEST_F(CqBatchOpTest, RecvSharedCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  static constexpr int kQueuePairCount = 2;
  const int sends_per_queue_pair = (recv_cq->cqe - 10) / kQueuePairCount;
  const int total_completions = sends_per_queue_pair * kQueuePairCount;
  std::vector<QpPair> qp_pairs = CreateTestQpPairs(
      setup, send_cq, recv_cq, sends_per_queue_pair, kQueuePairCount);
  for (auto& qp_pair : qp_pairs) {
    for (int i = 0; i < sends_per_queue_pair; ++i) {
      QueueRecv(setup, qp_pair);
    }
  }
  ThreadedSubmission(qp_pairs, sends_per_queue_pair,
                     [&setup, this](QpPair& qp) { QueueSend(setup, qp); });
  WaitForAndVerifyCompletions(recv_cq, total_completions);
}

class CqOverflowTest : public CqBatchOpTest {
 protected:
  // Maximum amount of time for waiting for data to land in destination buffer.
  static constexpr absl::Duration kPollTime = absl::Seconds(10);
  // There is a time discrepancy between the data landing on the remote buffer
  // and the completion being generated in the local queue pair. Waiting a
  // small amount of time after WaitForData() returns before polling the
  // completion queue helps to reduce the race.
  static constexpr absl::Duration kCompletionWaitTime = absl::Seconds(1);

  // Wait for send/write data to land on the remote buffer. The function will
  // block until all bytes on |dst_buffer| is of |expected_value|. Notice there
  // is still a time discrepancy between the data landing on the desination
  // buffer and the completion entry being generated (but not pushed to
  // completion queue). Use to detect the completion of an op whose completion
  // entries is dropped due to CQ overflow.
  void WaitForData(absl::Span<uint8_t> dst_buffer, uint8_t expected_value,
                   absl::Duration poll_timeout = kPollTime) {
    auto read_value = [](uint8_t* target) {
      return reinterpret_cast<volatile std::atomic<uint8_t>*>(target)->load();
    };
    absl::Time stop = absl::Now() + poll_timeout;
    bool completed = false;
    for (absl::Time now = absl::Now(); now < stop; now = absl::Now()) {
      completed = true;
      for (size_t i = 0; i < dst_buffer.size(); ++i) {
        if (read_value(dst_buffer.data() + i) != expected_value) {
          completed = false;
        }
      }
      if (completed == false) {
        absl::SleepFor(absl::Milliseconds(10));
      } else {
        return;
      }
    }
    LOG(FATAL) << "Data failed to land.";  // Crash ok;
  }
};

TEST_F(CqOverflowTest, SendCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  static constexpr int kQueuePairCount = 1;
  const int total_writes = cq->cqe + 10;
  std::vector<QpPair> qp_pairs =
      CreateTestQpPairs(setup, cq, cq, total_writes, kQueuePairCount);
  for (int i = 0; i < total_writes; ++i) {
    QueueWrite(setup, qp_pairs[0]);
  }
  WaitForData(setup.dst_memblock.subspan(0, total_writes), kSrcContent);
  absl::SleepFor(kCompletionWaitTime);
  WaitForAndVerifyCompletions(cq, cq->cqe);
  verbs_util::ExpectNoCompletion(cq, absl::Seconds(1));
}

TEST_F(CqOverflowTest, SendSharedCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  const int cq_size = cq->cqe;
  static constexpr int kQueuePairCount = 5;
  const int writes_per_queue_pair = cq_size;
  std::vector<QpPair> qp_pairs =
      CreateTestQpPairs(setup, cq, cq, writes_per_queue_pair, kQueuePairCount);
  ThreadedSubmission(qp_pairs, writes_per_queue_pair,
                     [&setup, this](QpPair& qp) { QueueWrite(setup, qp); });
  WaitForData(
      setup.dst_memblock.subspan(0, writes_per_queue_pair * kQueuePairCount),
      kSrcContent);
  absl::SleepFor(kCompletionWaitTime);
  WaitForAndVerifyCompletions(cq, cq_size);
  verbs_util::ExpectNoCompletion(cq, absl::Seconds(1));
}

TEST_F(CqOverflowTest, RecvCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  const int total_sends = recv_cq->cqe + 10;
  std::vector<QpPair> qp_pairs =
      CreateTestQpPairs(setup, send_cq, recv_cq, total_sends, /*count=*/1);
  for (int i = 0; i < total_sends; ++i) {
    QueueRecv(setup, qp_pairs[0]);
  }
  for (int i = 0; i < total_sends; ++i) {
    QueueSend(setup, qp_pairs[0]);
  }
  WaitForData(setup.dst_memblock.subspan(0, total_sends), kSrcContent);
  absl::SleepFor(kCompletionWaitTime);
  WaitForAndVerifyCompletions(recv_cq, recv_cq->cqe);
  verbs_util::ExpectNoCompletion(recv_cq, absl::Seconds(1));
}

TEST_F(CqOverflowTest, RecvSharedCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  const int sends_per_queue_pair = recv_cq->cqe;
  static constexpr int kQueuePairCount = 5;
  const int total_sends = sends_per_queue_pair * kQueuePairCount;
  std::vector<QpPair> qp_pairs = CreateTestQpPairs(
      setup, send_cq, recv_cq, sends_per_queue_pair, kQueuePairCount);
  for (auto& qp_pair : qp_pairs) {
    for (int i = 0; i < sends_per_queue_pair; ++i) {
      QueueRecv(setup, qp_pair);
    }
  }
  ThreadedSubmission(qp_pairs, sends_per_queue_pair,
                     [&setup, this](QpPair& qp) { QueueSend(setup, qp); });
  WaitForData(setup.dst_memblock.subspan(0, total_sends), kSrcContent);
  absl::SleepFor(kCompletionWaitTime);
  WaitForAndVerifyCompletions(recv_cq, recv_cq->cqe);
  verbs_util::ExpectNoCompletion(recv_cq, absl::Seconds(1));
}

}  // namespace rdma_unit_test
