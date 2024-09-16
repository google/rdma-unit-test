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

#include <cstdint>
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

}  // namespace rdma_unit_test
