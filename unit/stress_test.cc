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

#include <atomic>
#include <cstdint>
#include <queue>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"

#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

using ::testing::NotNull;

class StressTest : public RdmaVerbsFixture {
 public:
  static constexpr char kRequestorMemContent = 'a';
  static constexpr char kResponderMemContent = 'b';

 protected:
  // Use this many (highest order) bits in wr_id to encode the index of Qp.
  static constexpr uint32_t kQpIndexBits = 8;

  struct BasicSetup {
    ibv_context* context;
    PortAttribute port_attr;
    ibv_pd* pd;
  };

  struct QpPair {
    int index;
    ibv_qp* requestor;
    ibv_qp* responder;
  };

  struct Memory {
    RdmaMemBlock buffer;
    ibv_mr* mr;
  };

  uint32_t EncodeQpIndex(uint32_t raw_wr_id, uint32_t qp_index) {
    DCHECK_LE(qp_index, 1ul << kQpIndexBits)
        << "Qp index too large to fit into " << kQpIndexBits << " bits.";
    return raw_wr_id | (qp_index << (32 - kQpIndexBits));
  }

  int ExtractQpIndex(uint32_t wr_id) { return wr_id >> (32 - kQpIndexBits); }

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd");
    }
    return setup;
  }

  absl::StatusOr<std::vector<QpPair>> CreateQpPairs(const BasicSetup& setup,
                                                    int num_qps,
                                                    uint32_t max_send_wr,
                                                    ibv_cq* cq) {
    std::vector<QpPair> qps;
    for (int i = 0; i < num_qps; ++i) {
      QpPair pair{
          .index = i,
          .requestor =
              ibv_.CreateQp(setup.pd, cq, IBV_QPT_RC,
                            QpInitAttribute().set_max_send_wr(max_send_wr)),
          .responder =
              ibv_.CreateQp(setup.pd, cq, IBV_QPT_RC,
                            QpInitAttribute().set_max_send_wr(max_send_wr)),
      };
      if (!pair.requestor) {
        return absl::InternalError("Failed to create requestor qp.");
      }
      if (!pair.responder) {
        return absl::InternalError("Failed to create responder qp.");
      }
      RETURN_IF_ERROR(ibv_.SetUpLoopbackRcQps(pair.requestor, pair.responder,
                                              setup.port_attr));
      qps.emplace_back(pair);
    }
    return qps;
  }

  absl::StatusOr<Memory> CreateMemory(BasicSetup& setup, uint32_t total_bytes) {
    Memory memory{
        .buffer = ibv_.AllocAlignedBufferByBytes(total_bytes),
        .mr = ibv_.RegMr(setup.pd, memory.buffer),
    };
    if (!memory.mr) {
      return absl::InternalError("Cannot register mr.");
    }
    return memory;
  }

  void PostRdmaOp(QpPair& pair, ibv_wr_opcode opcode, int op_size,
                  Memory& requestor, Memory& responder) {
    static uint32_t next_raw_wr_id = 0;
    uint32_t wr_id = EncodeQpIndex(next_raw_wr_id++, pair.index);
    ASSERT_EQ(requestor.buffer.size() % op_size, 0ul)
        << "Memory buffer size must be multiple of op_size";
    ASSERT_EQ(responder.buffer.size() % op_size, 0ul)
        << "Memory buffer size must be multiple of op_size";
    absl::Span<uint8_t> req_buf = requestor.buffer.subspan(
        (wr_id * op_size) % requestor.buffer.size(), op_size);
    absl::Span<uint8_t> resp_buf = responder.buffer.subspan(
        (wr_id * op_size) % responder.buffer.size(), op_size);
    ibv_send_wr wr;
    ibv_sge sge;
    switch (opcode) {
      case IBV_WR_RDMA_READ: {
        sge = verbs_util::CreateSge(req_buf, requestor.mr);
        wr = verbs_util::CreateReadWr(wr_id, &sge, /*num_sge=*/1,
                                      resp_buf.data(), responder.mr->rkey);
        break;
      }
      case IBV_WR_RDMA_WRITE: {
        sge = verbs_util::CreateSge(req_buf, requestor.mr);
        wr = verbs_util::CreateWriteWr(wr_id, &sge, /*num_sge=*/1,
                                       resp_buf.data(), responder.mr->rkey);
        break;
      }
      default: {
        LOG(FATAL) << "Opcode " << opcode << " not supported";  // Crash ok
      }
    }
    verbs_util::PostSend(pair.requestor, wr);
  }

  void ClosedLoopWorkLoadRoundRobin(BasicSetup& setup, std::vector<QpPair>& qps,
                                    ibv_cq* cq, ibv_wr_opcode opcode,
                                    int total_ops, int max_outstanding,
                                    int op_size) {
    // Set up memory.
    constexpr int kMemorySize = 1 * 1024 * 1024;  // 1 MB
    ASSERT_OK_AND_ASSIGN(Memory requestor_memory,
                         CreateMemory(setup, kMemorySize));
    ASSERT_OK_AND_ASSIGN(Memory responder_memory,
                         CreateMemory(setup, kMemorySize));
    std::vector<int> outstanding_ops(qps.size(), 0);
    uint32_t total_remaining_ops = total_ops;
    uint32_t total_issued_ops = 0;
    uint32_t total_outstanding = 0;
    uint32_t total_completion = 0;

    const absl::Duration kTimeout = absl::Seconds(20);
    absl::Time last_completion_time = absl::Now();
    uint32_t rr_next = 0;
    while ((total_remaining_ops > 0 || total_outstanding > 0) &&
           ((absl::Now() - last_completion_time) < kTimeout)) {
      // Poll completion.
      ibv_wc wc;
      if (total_outstanding > 0 && ibv_poll_cq(cq, 1, &wc) > 0) {
        ++total_completion;
        --outstanding_ops[ExtractQpIndex(wc.wr_id)];
        --total_outstanding;
        ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
        last_completion_time = absl::Now();
      }

      // Post next WR.
      if (outstanding_ops[rr_next] < max_outstanding &&
          total_remaining_ops > 0) {
        PostRdmaOp(qps[rr_next], opcode, op_size, requestor_memory,
                   responder_memory);
        ++total_issued_ops;
        --total_remaining_ops;
        ++outstanding_ops[rr_next];
        ++total_outstanding;
      }
      if (++rr_next == qps.size()) {
        rr_next = 0;
      }
    }
    LOG(INFO) << "Total remaining ops: " << total_remaining_ops;
    LOG(INFO) << "Total issued ops: " << total_issued_ops;
    LOG(INFO) << "Total completion: " << total_completion;
    EXPECT_EQ(total_issued_ops, total_completion);
  }

  void RunAtomicFetchAddCpuRdmaRace(
      std::vector<BasicSetup>& setups,
      const std::vector<std::vector<QpPair>>& all_qps,
      const std::vector<std::vector<ibv_cq*>>& all_cqs, int num_cpu_threads,
      absl::Duration test_duration, int backlog_size) {
    int num_nics = setups.size();

    // Set up shared atomic counter memory on NIC 0 and register on remaining
    // NICs
    ASSERT_OK_AND_ASSIGN(Memory base_counter, CreateMemory(setups[0], 8));
    ASSERT_EQ(reinterpret_cast<uintptr_t>(base_counter.buffer.data()) %
                  alignof(std::atomic<uint64_t>),
              0)
        << "RDMA atomic operations and std::atomic require proper alignment.";

    std::vector<ibv_mr*> counter_mrs;
    counter_mrs.push_back(base_counter.mr);
    for (int n = 1; n < num_nics; ++n) {
      ibv_mr* mr =
          ibv_.RegMr(setups[n].pd, base_counter.buffer,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
      ASSERT_THAT(mr, NotNull());
      counter_mrs.push_back(mr);
    }

    std::atomic<uint64_t>* cpu_counter =
        reinterpret_cast<std::atomic<uint64_t>*>(base_counter.buffer.data());
    *cpu_counter = 0;

    std::vector<Memory> fetch_mems;
    std::vector<ibv_qp*> qp_list;
    std::vector<ibv_cq*> cq_list;
    std::vector<uint32_t> rkeys;
    for (int n = 0; n < num_nics; ++n) {
      for (int i = 0; i < all_qps[n].size(); ++i) {
        ASSERT_OK_AND_ASSIGN(Memory fetch_mem, CreateMemory(setups[n], 8));
        fetch_mems.push_back(fetch_mem);
        qp_list.push_back(all_qps[n][i].requestor);
        cq_list.push_back(all_cqs[n][i]);
        rkeys.push_back(counter_mrs[n]->rkey);
      }
    }
    int num_rdma_threads_total = qp_list.size();

    std::vector<uint64_t> cpu_additions(num_cpu_threads, 0);
    std::vector<uint64_t> rdma_additions(num_rdma_threads_total, 0);

    LOG(INFO) << "Starting mixed CPU/RDMA fetch-and-add test across "
              << num_nics << " NICs with " << num_cpu_threads
              << " CPU threads and " << num_rdma_threads_total
              << " RDMA threads running for " << test_duration;

    absl::Time start_time = absl::Now();

    // CPU Worker: Bashing the counter via standard C++ atomics
    auto cpu_worker = [&](int thread_id) {
      while ((absl::Now() - start_time) < test_duration) {
        cpu_counter->fetch_add(1, std::memory_order_seq_cst);
        cpu_additions[thread_id]++;
      }
    };

    // RDMA Worker: Bashing the same counter via PCIe atomics
    auto rdma_worker = [&](int thread_id) {
      int outstanding_ops = 0;
      ibv_sge sge = verbs_util::CreateSge(
          fetch_mems[thread_id].buffer.subspan(0, 8), fetch_mems[thread_id].mr);
      uint64_t posted_ops = 0;

      while ((absl::Now() - start_time) < test_duration) {
        // Post operations until backlog is full
        while (outstanding_ops < backlog_size) {
          ibv_send_wr wr = verbs_util::CreateFetchAddWr(
              /*wr_id=*/posted_ops, &sge, /*num_sge=*/1,
              base_counter.buffer.data(), rkeys[thread_id],
              /*compare_add=*/1);
          verbs_util::PostSend(qp_list[thread_id], wr);
          outstanding_ops++;
          posted_ops++;
        }

        ibv_wc wc;
        int num_polled =
            ibv_poll_cq(cq_list[thread_id], /*num_entries=*/1, &wc);
        if (num_polled > 0) {
          ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
          outstanding_ops--;
          rdma_additions[thread_id]++;
        }
      }

      // Drain any remaining ops
      while (outstanding_ops > 0) {
        ibv_wc wc;
        int num_polled = ibv_poll_cq(cq_list[thread_id], 1, &wc);
        if (num_polled > 0) {
          ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
          outstanding_ops--;
          rdma_additions[thread_id]++;
        }
      }
    };

    // Launch the race
    std::vector<std::thread> cpu_thread_pool;
    std::vector<std::thread> rdma_thread_pool;
    for (int i = 0; i < num_cpu_threads; ++i) {
      cpu_thread_pool.emplace_back(cpu_worker, i);
    }
    for (int i = 0; i < num_rdma_threads_total; ++i) {
      rdma_thread_pool.emplace_back(rdma_worker, i);
    }

    for (auto& t : cpu_thread_pool) t.join();
    for (auto& t : rdma_thread_pool) t.join();

    uint64_t total_successes = 0;
    for (int i = 0; i < num_cpu_threads; ++i) {
      total_successes += cpu_additions[i];
    }
    for (int i = 0; i < num_rdma_threads_total; ++i) {
      total_successes += rdma_additions[i];
    }

    uint64_t final_val = cpu_counter->load();

    LOG(INFO) << "Final Mixed Atomic Counter: " << final_val
              << " | Expected (total successes): " << total_successes;

    if (num_cpu_threads > 0) {
      uint64_t cpu_sum = 0;
      for (uint64_t count : cpu_additions) cpu_sum += count;
      LOG(INFO) << "Average successful additions per CPU thread: "
                << static_cast<double>(cpu_sum) / num_cpu_threads;
    }
    if (num_rdma_threads_total > 0) {
      uint64_t rdma_sum = 0;
      for (uint64_t count : rdma_additions) rdma_sum += count;
      LOG(INFO) << "Average successful additions per RDMA thread: "
                << static_cast<double>(rdma_sum) / num_rdma_threads_total;
    }

    EXPECT_EQ(final_val, total_successes)
        << "Read-Modify-Write race detected! "
        << "The environment truly enforces IBV_ATOMIC_HCA (NIC-only coherence) "
        << "and drops atomicity when interacting with CPU operations.";
  }

  void RunAtomicCmpAndSwapCpuRdmaRace(
      std::vector<BasicSetup>& setups,
      const std::vector<std::vector<QpPair>>& all_qps,
      const std::vector<std::vector<ibv_cq*>>& all_cqs, int num_cpu_threads,
      int max_outstanding, absl::Duration test_duration) {
    int num_nics = setups.size();

    // Set up shared atomic counter memory
    ASSERT_OK_AND_ASSIGN(Memory base_counter, CreateMemory(setups[0], 8));
    ASSERT_EQ(reinterpret_cast<uintptr_t>(base_counter.buffer.data()) %
                  alignof(std::atomic<uint64_t>),
              0)
        << "RDMA atomic operations and std::atomic require proper alignment.";

    std::vector<ibv_mr*> counter_mrs;
    counter_mrs.push_back(base_counter.mr);
    for (int n = 1; n < num_nics; ++n) {
      ibv_mr* mr =
          ibv_.RegMr(setups[n].pd, base_counter.buffer,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
      ASSERT_THAT(mr, NotNull());
      counter_mrs.push_back(mr);
    }

    std::atomic<uint64_t>* cpu_counter =
        reinterpret_cast<std::atomic<uint64_t>*>(base_counter.buffer.data());
    *cpu_counter = 0;

    std::vector<Memory> fetch_mems;
    std::vector<ibv_qp*> qp_list;
    std::vector<ibv_cq*> cq_list;
    std::vector<uint32_t> rkeys;

    for (int n = 0; n < num_nics; ++n) {
      for (int i = 0; i < all_qps[n].size(); ++i) {
        ASSERT_OK_AND_ASSIGN(Memory fetch_mem,
                             CreateMemory(setups[n], 8 * max_outstanding));
        uint64_t* fetch_vals =
            reinterpret_cast<uint64_t*>(fetch_mem.buffer.data());
        for (int k = 0; k < max_outstanding; ++k) {
          fetch_vals[k] = 0;
        }
        fetch_mems.push_back(fetch_mem);
        qp_list.push_back(all_qps[n][i].requestor);
        cq_list.push_back(all_cqs[n][i]);
        rkeys.push_back(counter_mrs[n]->rkey);
      }
    }
    int num_rdma_threads_total = qp_list.size();

    std::vector<uint64_t> cpu_additions(num_cpu_threads, 0);
    std::vector<uint64_t> rdma_additions(num_rdma_threads_total, 0);

    LOG(INFO) << "Starting mixed CPU/RDMA compare-and-swap test across "
              << num_nics << " NICs with " << num_cpu_threads
              << " CPU threads and " << num_rdma_threads_total
              << " RDMA threads running for " << test_duration;

    absl::Time start_time = absl::Now();

    auto cpu_worker = [&](int thread_id) {
      uint64_t expected = cpu_counter->load(std::memory_order_relaxed);
      while ((absl::Now() - start_time) < test_duration) {
        // Ensure we succeed once before sleeping.
        while (!cpu_counter->compare_exchange_strong(
            expected, expected + 1, std::memory_order_seq_cst)) {
          if ((absl::Now() - start_time) >= test_duration) return;
        }
        cpu_additions[thread_id]++;
        expected = expected + 1;
        // Need to sleep to reduce starvation on the RDMA thread.
        absl::SleepFor(absl::Milliseconds(10));
      }
    };

    auto rdma_worker = [&](int thread_id) {
      int outstanding_ops = 0;
      uint64_t assumed_value = 0;
      uint64_t* fetched_values =
          reinterpret_cast<uint64_t*>(fetch_mems[thread_id].buffer.data());

      std::vector<ibv_sge> sges(max_outstanding);
      for (int i = 0; i < max_outstanding; ++i) {
        sges[i] = verbs_util::CreateSge(
            fetch_mems[thread_id].buffer.subspan(i * 8, 8),
            fetch_mems[thread_id].mr);
      }

      int next_post_idx = 0;
      std::queue<uint64_t> posted_assumes;

      while ((absl::Now() - start_time) < test_duration) {
        if (outstanding_ops < max_outstanding) {
          ibv_send_wr wr = verbs_util::CreateCompSwapWr(
              /*wr_id=*/next_post_idx, &sges[next_post_idx], /*num_sge=*/1,
              base_counter.buffer.data(), rkeys[thread_id], assumed_value,
              assumed_value + 1);
          verbs_util::PostSend(qp_list[thread_id], wr);
          posted_assumes.push(assumed_value);
          assumed_value++;
          outstanding_ops++;
          next_post_idx = (next_post_idx + 1) % max_outstanding;
        }

        ibv_wc wc;
        // Poll for completions if we are at the limit, or opportunistically
        // otherwise. But to avoid blocking we just poll 1.
        int num_polled =
            ibv_poll_cq(cq_list[thread_id], /*num_entries=*/1, &wc);
        if (num_polled > 0) {
          ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
          outstanding_ops--;

          int polled_idx = wc.wr_id;
          uint64_t actual_value = fetched_values[polled_idx];
          uint64_t polled_assume = posted_assumes.front();
          posted_assumes.pop();

          if (actual_value == polled_assume) {
            rdma_additions[thread_id]++;
          } else {
            assumed_value = actual_value;
          }
        }
      }

      // Drain any remaining ops
      while (outstanding_ops > 0) {
        ibv_wc wc;
        int num_polled = ibv_poll_cq(cq_list[thread_id], 1, &wc);
        if (num_polled > 0) {
          ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
          outstanding_ops--;

          int polled_idx = wc.wr_id;
          uint64_t actual_value = fetched_values[polled_idx];
          uint64_t polled_assume = posted_assumes.front();
          posted_assumes.pop();

          if (actual_value == polled_assume) {
            rdma_additions[thread_id]++;
          } else {
            assumed_value = actual_value;
          }
        }
      }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_cpu_threads; ++i) {
      threads.emplace_back(cpu_worker, i);
    }
    for (int i = 0; i < num_rdma_threads_total; ++i) {
      threads.emplace_back(rdma_worker, i);
    }

    for (auto& t : threads) {
      t.join();
    }

    uint64_t total_successes = 0;
    for (int i = 0; i < num_cpu_threads; ++i) {
      total_successes += cpu_additions[i];
    }
    for (int i = 0; i < num_rdma_threads_total; ++i) {
      total_successes += rdma_additions[i];
    }

    uint64_t final_val = cpu_counter->load();

    LOG(INFO) << "Final Mixed Atomic Counter: " << final_val
              << " | Expected (total successes): " << total_successes;

    if (num_cpu_threads > 0) {
      uint64_t cpu_sum = 0;
      for (uint64_t count : cpu_additions) cpu_sum += count;
      LOG(INFO) << "Average successful additions per CPU thread: "
                << static_cast<double>(cpu_sum) / num_cpu_threads;
    }
    if (num_rdma_threads_total > 0) {
      uint64_t rdma_sum = 0;
      for (uint64_t count : rdma_additions) rdma_sum += count;
      LOG(INFO) << "Average successful additions per RDMA thread: "
                << static_cast<double>(rdma_sum) / num_rdma_threads_total;
    }

    EXPECT_EQ(final_val, total_successes)
        << "Compare-And-Swap race detected! "
        << "The environment dropped atomicity when interacting with CPU ops.";
  }
};

TEST_F(StressTest, Write32B100Qp100kOps) {
  constexpr int kNumQps = 100;
  constexpr int kTotalOps = 100000;
  constexpr int kMaxOutstanding = 32;
  constexpr int kOpsSize = 32;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context, kMaxOutstanding * kNumQps + 10);
  ASSERT_THAT(cq, NotNull());
  ASSERT_OK_AND_ASSIGN(std::vector<QpPair> qps,
                       CreateQpPairs(setup, kNumQps, kMaxOutstanding + 10, cq));
  ASSERT_NO_FATAL_FAILURE(ClosedLoopWorkLoadRoundRobin(
      setup, qps, cq, IBV_WR_RDMA_WRITE, kTotalOps, kMaxOutstanding, kOpsSize));
}

TEST_F(StressTest, AtomicFetchAddCpuRdmaRace) {
  std::vector<ibv_context*> contexts;
  ASSERT_OK(ibv_.OpenAllDevices(contexts));

  bool global_atomic_supported = true;
  for (ibv_context* context : contexts) {
    ibv_device_attr attr;
    if (ibv_query_device(context, &attr) != 0 ||
        attr.atomic_cap != IBV_ATOMIC_GLOB) {
      global_atomic_supported = false;
      break;
    }
  }

  if (!global_atomic_supported) {
    GTEST_SKIP() << "Skipping atomic stress test because global atomic is not "
                    "supported by all HCAs.";
  }

  if (auto issue = Introspection().KnownIssue(); issue.has_value()) {
    GTEST_SKIP() << "Skipping atomic stress test due to known issue: "
                 << *issue;
  }
  constexpr int kNumQps = 20;
  constexpr int kNumCpuThreads = 20;
  constexpr int kBacklogSize = 1000;
  const absl::Duration kTestDuration =
      Introspection().IsSlowNic() ? absl::Seconds(3) : absl::Seconds(10);

  std::vector<BasicSetup> setups;
  std::vector<std::vector<QpPair>> all_qps;
  std::vector<std::vector<ibv_cq*>> all_cqs;

  for (ibv_context* context : contexts) {
    BasicSetup setup;
    setup.context = context;
    setup.port_attr = ibv_.GetPortAttribute(context);
    setup.pd = ibv_.AllocPd(context);
    ASSERT_THAT(setup.pd, NotNull());
    setups.push_back(setup);

    std::vector<ibv_cq*> cqs;
    std::vector<QpPair> qps;
    for (int i = 0; i < kNumQps; ++i) {
      ibv_cq* cq = ibv_.CreateCq(setup.context, kBacklogSize + 10);
      ASSERT_THAT(cq, NotNull());
      cqs.push_back(cq);
      ASSERT_OK_AND_ASSIGN(std::vector<QpPair> qp_pair,
                           CreateQpPairs(setup, 1, kBacklogSize + 10, cq));
      qps.push_back(qp_pair[0]);
    }
    all_cqs.push_back(cqs);
    all_qps.push_back(qps);
  }

  ASSERT_NO_FATAL_FAILURE(RunAtomicFetchAddCpuRdmaRace(
      setups, all_qps, all_cqs, kNumCpuThreads, kTestDuration, kBacklogSize));
}

TEST_F(StressTest, AtomicCmpAndSwapCpuRdmaRace) {
  std::vector<ibv_context*> contexts;
  ASSERT_OK(ibv_.OpenAllDevices(contexts));

  bool global_atomic_supported = true;
  for (ibv_context* context : contexts) {
    ibv_device_attr attr;
    if (ibv_query_device(context, &attr) != 0 ||
        attr.atomic_cap != IBV_ATOMIC_GLOB) {
      global_atomic_supported = false;
      break;
    }
  }

  if (!global_atomic_supported) {
    GTEST_SKIP() << "Skipping atomic stress test because global atomic is not "
                    "supported by all HCAs.";
  }

  if (auto issue = Introspection().KnownIssue(); issue.has_value()) {
    GTEST_SKIP() << "Skipping atomic stress test due to known issue: "
                 << *issue;
  }
  constexpr int kNumQps = 20;
  constexpr int kNumCpuThreads = 1;
  constexpr int kMaxOutstanding = 100;
  const absl::Duration kTestDuration =
      Introspection().IsSlowNic() ? absl::Seconds(3) : absl::Seconds(10);

  std::vector<BasicSetup> setups;
  std::vector<std::vector<QpPair>> all_qps;
  std::vector<std::vector<ibv_cq*>> all_cqs;

  for (ibv_context* context : contexts) {
    BasicSetup setup;
    setup.context = context;
    setup.port_attr = ibv_.GetPortAttribute(context);
    setup.pd = ibv_.AllocPd(context);
    ASSERT_THAT(setup.pd, NotNull());
    setups.push_back(setup);

    std::vector<ibv_cq*> cqs;
    std::vector<QpPair> qps;
    for (int i = 0; i < kNumQps; ++i) {
      ibv_cq* cq = ibv_.CreateCq(setup.context, kMaxOutstanding + 10);
      ASSERT_THAT(cq, NotNull());
      cqs.push_back(cq);
      ASSERT_OK_AND_ASSIGN(std::vector<QpPair> qp_pair,
                           CreateQpPairs(setup, 1, kMaxOutstanding + 10, cq));
      qps.push_back(qp_pair[0]);
    }
    all_cqs.push_back(cqs);
    all_qps.push_back(qps);
  }

  ASSERT_NO_FATAL_FAILURE(
      RunAtomicCmpAndSwapCpuRdmaRace(setups, all_qps, all_cqs, kNumCpuThreads,
                                     kMaxOutstanding, kTestDuration));
}

}  // namespace rdma_unit_test
