#include <algorithm>
#include <cstdint>
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
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::NotNull;

class StressTest : public BasicFixture {
 public:
  static constexpr char kRequestorMemContent = 'a';
  static constexpr char kResponderMemContent = 'b';

 protected:
  // Use this many (highest order) bits in wr_id to encode the index of Qp.
  static constexpr uint32_t kQpIndexBits = 8;

  struct BasicSetup {
    ibv_context* context;
    verbs_util::PortGid port_gid;
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
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
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
    ibv_qp_init_attr attr{.send_cq = cq,
                          .recv_cq = cq,
                          .srq = nullptr,
                          .cap = verbs_util::DefaultQpCap(),
                          .qp_type = IBV_QPT_RC,
                          .sq_sig_all = 0};
    attr.cap.max_send_wr = max_send_wr;
    for (int i = 0; i < num_qps; ++i) {
      QpPair pair{
          .index = i,
          .requestor = ibv_.CreateQp(setup.pd, attr),
          .responder = ibv_.CreateQp(setup.pd, attr),
      };
      if (!pair.requestor) {
        return absl::InternalError("Failed to create requestor qp.");
      }
      if (!pair.responder) {
        return absl::InternalError("Failed to create responder qp.");
      }
      ibv_.SetUpLoopbackRcQps(pair.requestor, pair.responder, setup.port_gid);
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
    switch (opcode) {
      case IBV_WR_RDMA_READ: {
        ibv_sge sge = verbs_util::CreateSge(req_buf, requestor.mr);
        wr = verbs_util::CreateReadWr(wr_id, &sge, /*num_sge=*/1,
                                      resp_buf.data(), responder.mr->rkey);
        break;
      }
      case IBV_WR_RDMA_WRITE: {
        ibv_sge sge = verbs_util::CreateSge(req_buf, requestor.mr);
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

    const absl::Duration kTimeout = absl::Seconds(100);
    absl::Time stop = absl::Now() + kTimeout;
    uint32_t rr_next = 0;
    while ((total_remaining_ops > 0 || total_outstanding > 0) &&
           absl::Now() < stop) {
      // Poll completion.
      ibv_wc wc;
      if (total_outstanding > 0 && ibv_poll_cq(cq, 1, &wc) > 0) {
        ++total_completion;
        --outstanding_ops[ExtractQpIndex(wc.wr_id)];
        --total_outstanding;
        ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
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
