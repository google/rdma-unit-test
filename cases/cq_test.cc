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
#include <stddef.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <thread>  // NOLINT
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/barrier.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

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
  ASSERT_NE(nullptr, cq);
  ASSERT_EQ(0, ibv_destroy_cq(cq));
}

TEST_F(CqTest, NegativeCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, -1, nullptr, nullptr, 0);
  ASSERT_EQ(nullptr, cq);
}

TEST_F(CqTest, ZeroCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 0, nullptr, nullptr, 0);
  ASSERT_EQ(nullptr, cq);
}

TEST_F(CqTest, MaxCqe) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, std::numeric_limits<int>::max(),
                             nullptr, nullptr, 0);
  ASSERT_EQ(nullptr, cq);
}

TEST_F(CqTest, WithChannel) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, setup.channel, 0);
  ASSERT_NE(nullptr, cq);

  ASSERT_EQ(0, ibv_destroy_cq(cq));
}

TEST_F(CqTest, ShareChannel) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq1 = ibv_create_cq(setup.context, 10, nullptr, setup.channel, 0);
  ASSERT_NE(nullptr, cq1);
  ibv_cq* cq2 = ibv_create_cq(setup.context, 10, nullptr, setup.channel, 0);
  ASSERT_NE(nullptr, cq2);

  ASSERT_EQ(0, ibv_destroy_cq(cq1));
  ASSERT_EQ(0, ibv_destroy_cq(cq2));
}

TEST_F(CqTest, SmallCompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr, -1);
  ASSERT_EQ(nullptr, cq);
}

TEST_F(CqTest, CompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr, 0);
  ASSERT_NE(nullptr, cq);

  ASSERT_EQ(0, ibv_destroy_cq(cq));
}

TEST_F(CqTest, LargeCompVector) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr,
                             setup.context->num_comp_vectors);
  ASSERT_EQ(nullptr, cq);
}

TEST_F(CqTest, ThreadedCreate) {
  static constexpr int kThreadCount = 5;
  static constexpr int kCqsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());

  std::array<std::array<ibv_cq*, kCqsPerThread>, kThreadCount> cqs;
  cqs = {{{nullptr}}};
  auto create_qps = [this, &setup, &cqs](int thread_id) {
    // No CQs can share the same position in the array, so no need for thread
    // synchronization.
    for (int i = 0; i < kCqsPerThread; ++i) {
      cqs[thread_id][i] = ibv_.CreateCq(setup.context, 10);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(create_qps, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_cqs : cqs) {
    for (const auto& cq : thread_cqs) {
      EXPECT_NE(nullptr, cq);
    }
  }
}

TEST_F(CqTest, ThreadedCreateAndDestroy) {
  static constexpr int kThreadCount = 5;
  static constexpr int kCqsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());

  // Initialize to 1 since we are expecting the values to be 0 after destroying
  // CQs.
  std::array<std::array<int, kCqsPerThread>, kThreadCount> destroy_results;
  destroy_results = {{{1}}};
  auto create_destroy_cqs = [this, &setup, &destroy_results](int thread_id) {
    for (int i = 0; i < kCqsPerThread; ++i) {
      ibv_cq* cq = ibv_.CreateCq(setup.context, 10);
      ASSERT_NE(nullptr, cq);
      destroy_results[thread_id][i] = ibv_.DestroyCq(cq);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(create_destroy_cqs, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_results : destroy_results) {
    for (const auto& destroy_result : thread_results) {
      EXPECT_EQ(0, destroy_result);
    }
  }
}

// TODO(author1): Many/Max channels
// TODO(author1): Test lookup/delete with a different kind of object.
// TODO(author1): (likely in a different test) messing with comp vectors.

class CqAdvancedTest : public BasicFixture {
 protected:
  // Reserve the first byte (MSB(yte)) of the wr_id to represent the queue id.
  // X0000000 Queue Id
  // 0XXXXXXX Operation Id
  static constexpr uint64_t kQueueIdBits = 8;
  static constexpr uint64_t kQueueIdShift = sizeof(uint64_t) * 8 - kQueueIdBits;
  static constexpr uint64_t kQueueIdMask = 0xFFUL << kQueueIdShift;
  static constexpr uint64_t kOperationIdMask = ~kQueueIdMask;
  static constexpr absl::Duration kPollTimeout = absl::Seconds(5);

  struct QpInfo {
    ibv_qp* qp;
    uint64_t next_send_wr_id;
    uint64_t next_recv_wr_id;
  };

  struct BasicSetup {
    // Each index of src_memblock contains the index value.
    RdmaMemBlock src_memblock;
    // Initially initialized to 0. Each QP targets its wrs to write to the index
    // of dst_memblock which corresponds to the QP.
    RdmaMemBlock dst_memblock;
    ibv_context* context;
    ibv_pd* pd;
    ibv_mr* src_mr;
    ibv_mr* dst_mr;
    ibv_cq* send_cq;
    ibv_cq* recv_cq;
    std::vector<QpInfo> qps;
    // Holds the number of command queue entries each QP will have.
    int command_queue_slots;
  };

  enum class WorkType : uint8_t { kSend, kWrite };

  // Adds |count| QPs to setup.qps.
  absl::Status CreateTestQps(BasicSetup& setup, int count) {
    for (int i = 0; i < count; ++i) {
      size_t qp_id = setup.qps.size();
      CHECK_LT(qp_id, 1UL << kQueueIdBits) << "Maximum of 256 queues";
      QpInfo qp;
      qp.qp = ibv_.CreateQp(setup.pd, setup.send_cq, setup.recv_cq, nullptr,
                            setup.command_queue_slots,
                            setup.command_queue_slots, IBV_QPT_RC,
                            /*sig_all=*/0);
      CHECK(qp.qp) << "failed to create qp - " << errno;
      qp.next_send_wr_id = qp_id << kQueueIdShift;
      qp.next_recv_wr_id = qp_id << kQueueIdShift;
      ibv_.SetUpSelfConnectedRcQp(qp.qp,
                                  ibv_.GetLocalEndpointAttr(setup.context));
      setup.qps.push_back(qp);
    }
    return absl::OkStatus();
  }

  static uint64_t NextId(uint64_t wr_id) {
    uint64_t starting_mask = wr_id & kQueueIdMask;
    ++wr_id;
    // ID overflow is not supported.
    CHECK_EQ((wr_id & kQueueIdMask), starting_mask) << "ID overflow";
    return wr_id;
  }

  // Posts send WQE.
  void QueueSend(BasicSetup& setup, QpInfo* qp) {
    QueueWork(setup, qp, WorkType::kSend);
  }

  // Posts write WQE.
  void QueueWrite(BasicSetup& setup, QpInfo* qp) {
    QueueWork(setup, qp, WorkType::kWrite);
  }

  // Posts recv WQE.
  void QueueRecv(BasicSetup& setup, QpInfo* qp) {
    uint64_t compound_id = qp->next_recv_wr_id;
    int target_index = OwningQueueIndex(compound_id);
    ibv_sge sge;
    auto dst_buffer = setup.dst_memblock.span<uint16_t>();
    sge.addr = reinterpret_cast<uint64_t>(&dst_buffer[target_index]);
    sge.length = sizeof(typename decltype(dst_buffer)::value_type);
    sge.lkey = setup.dst_mr->lkey;
    ibv_recv_wr wqe =
        verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
    wqe.wr_id = qp->next_recv_wr_id;
    ibv_recv_wr* bad_wr;
    EXPECT_EQ(0, ibv_post_recv(qp->qp, &wqe, &bad_wr));
    qp->next_recv_wr_id = NextId(qp->next_recv_wr_id);
  }

  // QP requires a src_buffer slot per item of work queued.
  // For example slot 1 in src_buffer is shared by the first work queued on each
  // QP. Queueing more work than src_buffer slots is undefined.
  void QueueWork(BasicSetup& setup, QpInfo* qp, WorkType work_type) {
    uint64_t compound_id = qp->next_send_wr_id;
    uint64_t id = compound_id & kOperationIdMask;
    ibv_sge sge;
    auto src_buffer = setup.src_memblock.span<uint16_t>();
    auto dst_buffer = setup.dst_memblock.span<uint16_t>();
    DCHECK_LT(id, src_buffer.size());
    sge.addr = reinterpret_cast<uint64_t>(&src_buffer[id]);
    sge.length = sizeof(typename decltype(src_buffer)::value_type);
    sge.lkey = setup.src_mr->lkey;

    ibv_send_wr wqe;
    switch (work_type) {
      case WorkType::kSend:
        wqe = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
        break;
      case WorkType::kWrite:
        int target_index = OwningQueueIndex(qp->next_send_wr_id);
        wqe = verbs_util::CreateWriteWr(
            /*wr_id=*/1, &sge, /*num_sge=*/1,
            reinterpret_cast<uint8_t*>(&dst_buffer[target_index]),
            setup.dst_mr->rkey);
        break;
    }
    wqe.wr_id = qp->next_send_wr_id;
    ibv_send_wr* bad_wr;
    EXPECT_EQ(0, ibv_post_send(qp->qp, &wqe, &bad_wr));
    qp->next_send_wr_id = NextId(qp->next_send_wr_id);
  }

  // Polls a single time for |count| completions.
  std::vector<uint64_t> PollCompletions(int count, ibv_cq* cq) {
    std::vector<ibv_wc> completions(count);
    int returned = ibv_poll_cq(cq, count, completions.data());
    std::vector<uint64_t> result;
    for (int i = 0; i < returned; ++i) {
      ibv_wc& completion = completions[i];
      EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
      result.push_back(completion.wr_id);
    }
    return result;
  }

  static int OwningQueueIndex(uint64_t wr_id) { return wr_id >> kQueueIdShift; }

  bool WaitForChange(uint16_t* target, uint16_t expected_value) {
    auto read_value = [target]() {
      return reinterpret_cast<volatile std::atomic<uint16_t>*>(target)->load();
    };
    absl::Time stop = absl::Now() + kPollTimeout;
    while (read_value() != expected_value && absl::Now() < stop) {
      absl::SleepFor(absl::Milliseconds(10));
    }
    return read_value() == expected_value;
  }

  // This method does a course validation of wr_ids which came back. It ensures
  // each queue has an uninterrupted string of ids. Missing ids at the front and
  // end do not cause failures. Leading ids can be dropped due to overwrite.
  // Trailing ids could not yet have arrived due to a race between data landing
  // and completion becoming visible. This race means a given run may or may
  // not sucesscfully interleave or overflow a Completion Queue.
  void ValidateCompletions(BasicSetup& setup, std::vector<uint64_t> ids) {
    // Sort to remove interleaving.
    std::sort(ids.begin(), ids.end());

    int last_queue_index = 256;
    uint64_t last_wr_id = 0;
    for (uint64_t wr_id : ids) {
      if (OwningQueueIndex(wr_id) != last_queue_index) {
        last_queue_index = OwningQueueIndex(wr_id);
        EXPECT_LE(last_queue_index, setup.qps.size());
        last_wr_id = wr_id;
      } else {
        uint64_t expected = ++last_wr_id;
        EXPECT_EQ(expected, wr_id);
      }
    }
  }

  // Calls |work| |times_per_queue| on each QP in a separate thread per QP.
  // |Func| should have the signature: void foo(QpInfo* qp)
  template <typename Func>
  void ThreadedSubmission(BasicSetup& setup, int times_per_queue, Func work) {
    std::vector<std::thread> threads;
    absl::Barrier wait_barrier(setup.qps.size());
    for (auto& qp : setup.qps) {
      threads.push_back(
          std::thread([&qp, times_per_queue, &wait_barrier, work]() {
            wait_barrier.Block();
            for (int i = 0; i < times_per_queue; ++i) {
              work(&qp);
            }
          }));
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    // The maximumber number of operations which can be issued on a given QP.
    const size_t kMaxOpsPerQp = Introspection().device_attr().max_qp_wr;
    setup.src_memblock =
        ibv_.AllocBufferByBytes(kMaxOpsPerQp * sizeof(uint16_t));
    setup.dst_memblock = ibv_.AllocBufferByBytes(256 * sizeof(uint16_t));
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    auto src_buffer = setup.src_memblock.span<uint16_t>();
    for (size_t i = 0; i < src_buffer.size(); ++i) {
      src_buffer[i] = i;
    }
    setup.src_mr = ibv_.RegMr(setup.pd, setup.src_memblock);
    if (!setup.src_mr) {
      return absl::InternalError("Failed to register source mr.");
    }
    memset(setup.dst_memblock.data(), 0, setup.dst_memblock.size());
    setup.dst_mr = ibv_.RegMr(setup.pd, setup.dst_memblock);
    if (!setup.dst_mr) {
      return absl::InternalError("Failed to register destination mr.");
    }
    // Use large command queues to make overlap more likely.
    ibv_device_attr dev_attr = {};
    int query_result = ibv_query_device(setup.context, &dev_attr);
    if (query_result) {
      return absl::InternalError(
          absl::StrCat("Failed to query device (errno = ", query_result, ")."));
    }
    setup.command_queue_slots = kMaxOpsPerQp;
    // Make sure there is room in the wr queue to overflow cq.
    int cq_slots =
        std::min<int>(dev_attr.max_cqe, setup.command_queue_slots / 2);
    setup.send_cq = ibv_.CreateCq(setup.context, cq_slots);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context, cq_slots);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    return setup;
  }
};

TEST_F(CqAdvancedTest, SendCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int cq_size = setup.send_cq->cqe;
  const int overflow_amount = 10;
  const int total_writes = cq_size + overflow_amount;
  ASSERT_OK(CreateTestQps(setup, /*count=*/1));

  for (int i = 0; i < total_writes; ++i) {
    QueueWrite(setup, &setup.qps[0]);
  }
  auto dst_buffer = setup.dst_memblock.span<uint16_t>();
  EXPECT_TRUE(WaitForChange(&dst_buffer[0], total_writes - 1));
  std::vector<uint64_t> completions = PollCompletions(cq_size, setup.send_cq);
  ValidateCompletions(setup, completions);
}

TEST_F(CqAdvancedTest, SendSharedCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kQueueCount = 2;
  ASSERT_OK(CreateTestQps(setup, kQueueCount));
  const int cq_size = setup.send_cq->cqe;
  const int writes_per_queue = (cq_size - 10) / kQueueCount;

  auto dst_buffer = setup.dst_memblock.span<uint16_t>();
  ThreadedSubmission(setup, writes_per_queue,
                     [&setup, this](QpInfo* qp) { QueueWrite(setup, qp); });
  for (int i = 0; i < kQueueCount; ++i) {
    EXPECT_TRUE(WaitForChange(&dst_buffer[i], writes_per_queue - 1));
  }

  std::vector<uint64_t> completions = PollCompletions(cq_size, setup.send_cq);
  ValidateCompletions(setup, completions);
}

// 5 threads writing to the same shared completion queue.
TEST_F(CqAdvancedTest, SendSharedCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kQueueCount = 5;
  ASSERT_OK(CreateTestQps(setup, kQueueCount));
  const int cq_size = setup.send_cq->cqe;
  const int writes_per_queue = cq_size;

  ThreadedSubmission(setup, writes_per_queue,
                     [&setup, this](QpInfo* qp) { QueueWrite(setup, qp); });
  auto dst_buffer = setup.dst_memblock.span<uint16_t>();
  for (int i = 0; i < kQueueCount; ++i) {
    EXPECT_TRUE(WaitForChange(&dst_buffer[i], writes_per_queue - 1));
  }

  std::vector<uint64_t> completions = PollCompletions(cq_size, setup.send_cq);
  ValidateCompletions(setup, completions);
}

TEST_F(CqAdvancedTest, RecvCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const int cq_size = setup.recv_cq->cqe;
  const int overflow_amount = 10;
  const int total_sends = cq_size + overflow_amount;
  ASSERT_OK(CreateTestQps(setup, /*count=*/1));

  for (int i = 0; i < total_sends; ++i) {
    QueueRecv(setup, &setup.qps[0]);
  }
  for (int i = 0; i < total_sends; ++i) {
    QueueSend(setup, &setup.qps[0]);
  }
  auto dst_buffer = setup.dst_memblock.span<uint16_t>();
  EXPECT_TRUE(WaitForChange(&dst_buffer[0], total_sends - 1));
  std::vector<uint64_t> completions = PollCompletions(cq_size, setup.recv_cq);
  ValidateCompletions(setup, completions);
}

// 2 CQs posting recv completions to a single completion queue.
TEST_F(CqAdvancedTest, RecvSharedCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kQueueCount = 2;
  ASSERT_OK(CreateTestQps(setup, kQueueCount));
  const int cq_size = setup.recv_cq->cqe;
  const int sends_per_queue = (cq_size - 10) / kQueueCount;

  for (auto& qp : setup.qps) {
    for (int i = 0; i < sends_per_queue; ++i) {
      QueueRecv(setup, &qp);
    }
  }

  ThreadedSubmission(setup, sends_per_queue,
                     [&setup, this](QpInfo* qp) { QueueSend(setup, qp); });
  auto dst_buffer = setup.dst_memblock.span<uint16_t>();
  for (int i = 0; i < kQueueCount; ++i) {
    EXPECT_TRUE(WaitForChange(&dst_buffer[i], sends_per_queue - 1));
  }

  std::vector<uint64_t> completions = PollCompletions(cq_size, setup.recv_cq);
  ValidateCompletions(setup, completions);
}

TEST_F(CqAdvancedTest, RecvSharedCqOverflow) {
  if (Introspection().FullCqIdlesQp()) {
    GTEST_SKIP() << "This test assumes CQ overflow overwrites completions.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kQueueCount = 5;
  ASSERT_OK(CreateTestQps(setup, kQueueCount));
  const int cq_size = setup.recv_cq->cqe;
  const int sends_per_queue = cq_size;

  for (auto& qp : setup.qps) {
    for (int i = 0; i < sends_per_queue; ++i) {
      QueueRecv(setup, &qp);
    }
  }

  ThreadedSubmission(setup, sends_per_queue,
                     [&setup, this](QpInfo* qp) { QueueSend(setup, qp); });
  auto dst_buffer = setup.dst_memblock.span<uint16_t>();
  for (int i = 0; i < kQueueCount; ++i) {
    EXPECT_TRUE(WaitForChange(&dst_buffer[i], sends_per_queue - 1));
  }

  std::vector<uint64_t> completions = PollCompletions(cq_size, setup.recv_cq);
  ValidateCompletions(setup, completions);
}

}  // namespace rdma_unit_test
