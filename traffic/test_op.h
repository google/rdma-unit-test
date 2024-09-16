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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_TEST_OP_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_TEST_OP_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "infiniband/verbs.h"
#include "traffic/op_types.h"
#include "traffic/qp_op_interface.h"

namespace rdma_unit_test {

// The TestOp structure represents a single RDMA operation being performed in
// the test. It stores all the information related to the op, such as op_type,
// op size, source/destination addresses, which qp it's being performed on and
// completion status.
struct TestOp {
  // Returns a string representation of OpTypes.
  static std::string ToString(OpTypes op_type);
  static constexpr uint64_t kAtomicWordSize = 8;

  // Return a string representation of the TestOp src or dest buffer contents.
  std::string SrcBuffer() const;
  std::string DestBuffer() const;

  // Unique per-qp op identifier.
  uint64_t op_id = 0;
  // Initiator qp_id on which this op has been posted.
  uint32_t qp_id = 0;
  // Remote qp_id on the target client. Stored, so that UD send operations can
  // be properly validated by keeping track of identifying information related
  // to the corresponding UD recv.
  QpOpInterface* remote_qp;
  // Source and destination buffer addresses.
  uint8_t* src_addr = nullptr;
  uint8_t* dest_addr = nullptr;
  // Operation length in bytes.
  uint64_t length = 0;
  OpTypes op_type = OpTypes::kInvalid;
  // Relevant only for atomic operations. Holds "compare" value for
  // "compare&add" or "add" value for "fetch&add" op.
  uint64_t compare_add = 0;
  // Relevant only for atomic "comp_swap" operation.
  uint64_t swap = 0;
  // Copy of op buffers for deferred validation.
  std::unique_ptr<std::vector<uint8_t>> src_buffer_copy = nullptr;
  std::unique_ptr<std::vector<uint8_t>> dest_buffer_copy = nullptr;

  // Assigned when completion is polled.
  ibv_wc_status status;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_TEST_OP_H_
