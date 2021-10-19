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
#include <thread>  // NOLINT
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

class CqBatchOpTest : public BasicFixture {
 protected:
  static constexpr int kBufferSize = 16 * 1024 * 1024;  // 16 MB
  static constexpr uint8_t kSrcContent = 1;
  static constexpr uint8_t kDstContent = 0;

  struct QpInfo {
    ibv_qp* qp;
    uint64_t next_send_wr_id = 0;
    uint64_t next_recv_wr_id = 0;
    // Each Qp write/recv to a separate remote buffer and each op should write
    // to one distinct byte of memory. Therefore the size of the buffer
    // should be at least as large as the number of ops.
    absl::Span<uint8_t> dst_buffer;
  };

  struct BasicSetup {
    // Each index of src_memblock contains the index value.
    RdmaMemBlock src_memblock;
    // Initially initialized to 0. Each QP targets its wrs to write to the index
    // of dst_memblock which corresponds to the QP.
    RdmaMemBlock dst_memblock;
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
    ibv_mr* src_mr;
    ibv_mr* dst_mr;
  };

  enum class WorkType : uint8_t { kSend, kWrite };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.src_memblock = ibv_.AllocAlignedBufferByBytes(kBufferSize);
    std::fill_n(setup.src_memblock.data(), setup.src_memblock.size(),
                kSrcContent);
    setup.dst_memblock = ibv_.AllocAlignedBufferByBytes(kBufferSize);
    std::fill_n(setup.dst_memblock.data(), setup.dst_memblock.size(),
                kDstContent);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
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
    return setup;
  }

  // Create |count| qps.
  std::vector<QpInfo> CreateTestQps(BasicSetup& setup, ibv_cq* send_cq,
                                    ibv_cq* recv_cq, size_t max_qp_wr,
                                    int count) {
    CHECK_LE(max_qp_wr * count, setup.dst_memblock.size())
        << "Not enough space on destination buffer for all QPs.";
    std::vector<QpInfo> qps;
    for (int i = 0; i < count; ++i) {
      QpInfo qp;
      qp.qp = ibv_.CreateQp(setup.pd, send_cq, recv_cq, nullptr, max_qp_wr,
                            max_qp_wr, IBV_QPT_RC,
                            /*sig_all=*/0);
      CHECK(qp.qp) << "Failed to create qp - " << errno;
      ibv_.SetUpSelfConnectedRcQp(qp.qp, setup.port_gid);
      qp.dst_buffer = setup.dst_memblock.subspan(i * max_qp_wr, max_qp_wr);
      qps.push_back(qp);
    }
    return qps;
  }

  // Posts send WQE.
  int QueueSend(BasicSetup& setup, QpInfo& qp) {
    return QueueWork(setup, qp, WorkType::kSend);
  }

  // Posts write WQE.
  int QueueWrite(BasicSetup& setup, QpInfo& qp) {
    return QueueWork(setup, qp, WorkType::kWrite);
  }

  // Posts recv WQE.
  int QueueRecv(BasicSetup& setup, QpInfo& qp) {
    uint32_t wr_id = qp.next_recv_wr_id++;
    DCHECK_LT(wr_id, setup.dst_memblock.size());
    auto dst_buffer =
        qp.dst_buffer.subspan(wr_id, 1);  // use wr_id as index to the buffer.
    ibv_sge sge = verbs_util::CreateSge(dst_buffer, setup.dst_mr);
    ibv_recv_wr wqe = verbs_util::CreateRecvWr(wr_id, &sge, /*num_sge=*/1);
    ibv_recv_wr* bad_wr;
    return ibv_post_recv(qp.qp, &wqe, &bad_wr);
  }

  // Post a Send or Write WR to a QP. The WR uses a 1-byte buffer at byte 1,
  // then byte 2, and so on.
  int QueueWork(BasicSetup& setup, QpInfo& qp, WorkType work_type) {
    uint64_t wr_id = qp.next_send_wr_id++;
    CHECK_LT(wr_id, setup.src_memblock.size());
    auto src_buffer = setup.src_memblock.subspan(
        wr_id, 1);  // use wr_id as index to the buffer.
    ibv_sge sge = verbs_util::CreateSge(src_buffer, setup.src_mr);
    ibv_send_wr wqe;
    switch (work_type) {
      case WorkType::kSend: {
        wqe = verbs_util::CreateSendWr(wr_id, &sge, /*num_sge=*/1);
        break;
      }
      case WorkType::kWrite: {
        CHECK_LT(wr_id, setup.dst_memblock.size());
        auto dst_buffer = qp.dst_buffer.subspan(
            wr_id, 1);  // use wr_id as index to the buffer.
        wqe = verbs_util::CreateWriteWr(wr_id, &sge, /*num_sge=*/1,
                                        dst_buffer.data(), setup.dst_mr->rkey);
        break;
      }
    }
    ibv_send_wr* bad_wr;
    return ibv_post_send(qp.qp, &wqe, &bad_wr);
  }

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

  // Calls |work| |times_per_queue| on each QP in a separate thread per QP.
  // |Func| should have the signature: void foo(QpInfo* qp)
  template <typename Func>
  void ThreadedSubmission(std::vector<QpInfo> qps, int times_per_queue,
                          Func work) {
    std::vector<std::thread> threads;
    absl::Barrier wait_barrier(qps.size());
    for (auto& qp : qps) {
      threads.push_back(
          std::thread([&qp, times_per_queue, &wait_barrier, work]() {
            wait_barrier.Block();
            for (int i = 0; i < times_per_queue; ++i) {
              work(qp);
            }
          }));
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
};

TEST_F(CqBatchOpTest, SendSharedCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  static constexpr int kQueueCount = 2;
  const int writes_per_queue = (cq->cqe - 10) / kQueueCount;
  const int total_completions = writes_per_queue * kQueueCount;
  std::vector<QpInfo> qps =
      CreateTestQps(setup, cq, cq, writes_per_queue, kQueueCount);
  ThreadedSubmission(qps, writes_per_queue,
                     [&setup, this](QpInfo& qp) { QueueWrite(setup, qp); });
  WaitForAndVerifyCompletions(cq, total_completions);
}

// 2 CQs posting recv completions to a single completion queue.
TEST_F(CqBatchOpTest, RecvSharedCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  static constexpr int kQueueCount = 2;
  const int sends_per_queue = (recv_cq->cqe - 10) / kQueueCount;
  const int total_completions = sends_per_queue * kQueueCount;
  std::vector<QpInfo> qps =
      CreateTestQps(setup, send_cq, recv_cq, sends_per_queue, kQueueCount);
  for (auto& qp : qps) {
    for (int i = 0; i < sends_per_queue; ++i) {
      QueueRecv(setup, qp);
    }
  }
  ThreadedSubmission(qps, sends_per_queue,
                     [&setup, this](QpInfo& qp) { QueueSend(setup, qp); });
  WaitForAndVerifyCompletions(recv_cq, total_completions);
}

class CqOverflowTest : public CqBatchOpTest {
 protected:
  // Maximum amount of time for waiting for data to land in destination buffer.
  static constexpr absl::Duration kPollTime = absl::Seconds(10);
  // There is a time descrepancy between the data landing on the remote buffer
  // and the completion being generated in the local queue pair. Waiting a
  // small amount of time after WaitForData() returns before polling the
  // completion queue helps to reduce the race.
  static constexpr absl::Duration kCompletionWaitTime = absl::Seconds(1);

  // Wait for send/write data to land on the remote buffer. The function will
  // block until all bytes on |dst_buffer| is of |expected_value|. Notice there
  // is still a time descrepancy between the data landing on the desination
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
      for (int i = 0; i < dst_buffer.size(); ++i) {
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
  static constexpr int kQueueCount = 1;
  const int total_writes = cq->cqe + 10;
  std::vector<QpInfo> qps =
      CreateTestQps(setup, cq, cq, total_writes, kQueueCount);
  for (int i = 0; i < total_writes; ++i) {
    QueueWrite(setup, qps[0]);
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
  static constexpr int kQueueCount = 5;
  const int writes_per_queue = cq_size;
  std::vector<QpInfo> qps =
      CreateTestQps(setup, cq, cq, writes_per_queue, kQueueCount);
  ThreadedSubmission(qps, writes_per_queue,
                     [&setup, this](QpInfo& qp) { QueueWrite(setup, qp); });
  WaitForData(setup.dst_memblock.subspan(0, writes_per_queue * kQueueCount),
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
  std::vector<QpInfo> qps =
      CreateTestQps(setup, send_cq, recv_cq, total_sends, /*count=*/1);
  for (int i = 0; i < total_sends; ++i) {
    QueueRecv(setup, qps[0]);
  }
  for (int i = 0; i < total_sends; ++i) {
    QueueSend(setup, qps[0]);
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
  const int sends_per_queue = recv_cq->cqe;
  static constexpr int kQueueCount = 5;
  const int total_sends = sends_per_queue * kQueueCount;
  std::vector<QpInfo> qps =
      CreateTestQps(setup, send_cq, recv_cq, sends_per_queue, kQueueCount);
  for (auto& qp : qps) {
    for (int i = 0; i < sends_per_queue; ++i) {
      QueueRecv(setup, qp);
    }
  }
  ThreadedSubmission(qps, sends_per_queue,
                     [&setup, this](QpInfo& qp) { QueueSend(setup, qp); });
  WaitForData(setup.dst_memblock.subspan(0, total_sends), kSrcContent);
  absl::SleepFor(kCompletionWaitTime);
  WaitForAndVerifyCompletions(recv_cq, recv_cq->cqe);
  verbs_util::ExpectNoCompletion(recv_cq, absl::Seconds(1));
}

}  // namespace rdma_unit_test
