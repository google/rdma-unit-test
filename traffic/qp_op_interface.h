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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_QP_OP_INTERFACE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_QP_OP_INTERFACE_H_

#include <cstdint>
#include <list>
#include <optional>
#include <vector>

#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "traffic/op_types.h"

namespace rdma_unit_test {

class TestOp;

// An interface for accessing methods in QpState used by an Op. Remote QpState
// are stored in QpState (for RC QPs) or TestOp (for UD QP). The interface
// supports getting next OpAddress, getting qp pointer, rkeys and lkeys, etc.
class QpOpInterface {
 public:
  struct OpAddressesParams {
    // Defines whether allocation must happen on the src_buffer_ or dest_buffer_
    // of the Qp.
    enum class BufferType { kSrcBuffer, kDestBuffer, kInvalid };
    // Defines the number of RDMA ops to assign buffer space for.
    uint32_t num_ops = 0;
    // Number of data bytes in each op.
    uint32_t op_bytes = 0;

    BufferType buffer_to_use = BufferType::kInvalid;
    OpTypes op_type = OpTypes::kInvalid;
  };

  virtual ~QpOpInterface() = default;

  virtual ibv_qp* qp() const = 0;

  virtual uint32_t qp_id() const = 0;

  virtual uint32_t src_rkey() const = 0;

  virtual uint32_t dest_rkey() const = 0;

  virtual uint64_t GetLastOpId() const = 0;

  virtual uint64_t outstanding_ops_count() const = 0;

  virtual absl::StatusOr<std::vector<uint8_t*>> GetNextOpAddresses(
      const OpAddressesParams& op_params) = 0;

  virtual void IncrCompletedBytes(uint64_t bytes, OpTypes op_type) = 0;

  virtual void IncrCompletedOps(uint64_t op_cnt, OpTypes op_type) = 0;

  virtual std::optional<TestOp> TryValidateRecvOp(const TestOp& send) = 0;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_QP_OP_INTERFACE_H_
