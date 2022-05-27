/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_UNIT_OP_FIXTURE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_UNIT_OP_FIXTURE_H_

#include <cstdint>
#include <functional>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "public/rdma_memblock.h"
#include "public/verbs_util.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

// A test fixture for unit testing RDMA batch operations. Provides methods to
// create pairs of interconnected RC QPs on loopback port and methods to perform
// threaded send and receive operations.
class BatchOpFixture : public RdmaVerbsFixture {
 protected:
  static constexpr int kBufferSize = 16 * 1024 * 1024;  // 16 MB
  static constexpr uint8_t kSrcContent = 1;
  static constexpr uint8_t kDstContent = 0;

  struct QpPair {
    ibv_qp* send_qp;
    ibv_qp* recv_qp;
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
    PortAttribute port_attr;
    ibv_pd* pd;
    ibv_mr* src_mr;
    ibv_mr* dst_mr;
  };

  enum class WorkType : uint8_t { kSend, kWrite };

  absl::StatusOr<BasicSetup> CreateBasicSetup();

  // Posts send WQE.
  int QueueSend(BasicSetup& setup, QpPair& qp);

  // Posts write WQE.
  int QueueWrite(BasicSetup& setup, QpPair& qp);

  // Posts recv WQE.
  int QueueRecv(BasicSetup& setup, QpPair& qp);

  // Post a Send or Write WR to a QP. The WR uses a 1-byte buffer at byte 1,
  // then byte 2, and so on.
  int QueueWork(BasicSetup& setup, QpPair& qp, WorkType work_type);

  // Create |count| qps.
  absl::StatusOr<std::vector<QpPair>> CreateTestQpPairs(BasicSetup& setup,
                                                        ibv_cq* send_cq,
                                                        ibv_cq* recv_cq,
                                                        size_t max_qp_wr,
                                                        int count);

  // Creates a separate thread for each QP pair. Each thread calls `work`
  // `times_per_pair` on its corresponding QP pair.
  void ThreadedSubmission(std::vector<QpPair> qp_pairs, int times_per_pair,
                          std::function<void(QpPair&)> work);
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_UNIT_OP_FIXTURE_H_
