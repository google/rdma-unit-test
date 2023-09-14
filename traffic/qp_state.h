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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_QP_STATE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_QP_STATE_H_

#include <stdlib.h>

#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/qp_op_interface.h"
#include "traffic/test_op.h"

ABSL_DECLARE_FLAG(bool, print_op_buffers);

namespace rdma_unit_test {

// The QpState class stores and manages all RDMA ops and buffers posted on a
// given QP. It provides methods to batch and post ops on the QP, and return
// stats of pending/completed ops on the QP.
class QpState : public QpOpInterface {
 public:
  // Contains information about a contiguous range of memory, defined by
  // base_addr and length. It also keeps track of a list of addresses on which
  // ops are posted, and can either be free or pending.
  struct BufferInfo {
    uint8_t* base_addr = nullptr;
    uint64_t length = 0;
    uint64_t max_op_size = 0;
    // List of free and active address. An active address is one with a pending
    // operation using it.
    absl::flat_hash_set<uint8_t*> free_addresses;
    absl::flat_hash_set<uint8_t*> active_addresses;
  };

  // Contains information that defines a UD op's destination, i.e. an address
  // handle and a remote (UD) qp id.
  struct UdDestination {
    ibv_ah* ah;
    QpOpInterface* qp_state;
  };

  QpState(int local_client_id, ibv_qp* qp, uint32_t qp_id, bool is_rc,
          absl::Span<uint8_t> src_buffer, absl::Span<uint8_t> dest_buffer,
          int max_outstanding_ops);
  ~QpState() override = default;
  virtual bool is_rc() const = 0;

  // Prints all properties of the qp to a string.
  std::string DumpState() const;

  ibv_qp* qp() const override { return qp_; }
  uint32_t qp_id() const override { return qp_id_; }

  uint32_t src_lkey() const { return src_lkey_; }
  uint32_t src_rkey() const override { return src_rkey_; }
  uint32_t dest_lkey() const { return dest_lkey_; }
  uint32_t dest_rkey() const override { return dest_rkey_; }

  // For RC QPs: set and get the unique destination QP id.
  virtual QpOpInterface* remote_qp_state() const {
    LOG(DFATAL) << "Function only available for RC QPs.";
    return nullptr;
  }
  virtual void set_remote_qp_state(QpOpInterface* remote_qp_state) {
    LOG(DFATAL) << "Function only available for RC QPs.";
  }

  // For UD QPs: add and get destinations for UD QPs and AHs.
  virtual void add_ud_destination(QpOpInterface* remote_qp_state, ibv_ah* ah) {
    LOG(DFATAL) << "Function only available for UD QPs.";
  }

  virtual const std::vector<UdDestination>* ud_destinations() const {
    LOG(DFATAL) << "Function only available for UD QPs.";
    return nullptr;
  }

  virtual UdDestination random_ud_destination() {
    LOG(DFATAL) << "Function only available for UD QPs.";
    return UdDestination{.ah = nullptr, .qp_state = nullptr};
  }

  uint64_t outstanding_ops_count() const override {
    return outstanding_ops_.size();
  }

  BufferInfo src_buffer() const { return src_buffer_; }
  BufferInfo dest_buffer() const { return dest_buffer_; }

  absl::flat_hash_map<uint64_t, std::unique_ptr<TestOp>>& outstanding_ops() {
    return outstanding_ops_;
  }
  std::list<std::unique_ptr<TestOp>>& unchecked_received_ops() {
    return unchecked_received_ops_;
  }
  std::list<std::unique_ptr<TestOp>>& unchecked_initiated_ops() {
    return unchecked_initiated_ops_;
  }

  void set_op_generator(OperationGenerator* op_generator) {
    op_generator_ = op_generator;
  }
  OperationGenerator* op_generator() { return op_generator_; }

  void set_src_lkey(uint32_t src_lkey) { src_lkey_ = src_lkey; }
  void set_src_rkey(uint32_t src_rkey) { src_rkey_ = src_rkey; }
  void set_dest_lkey(uint32_t dest_lkey) { dest_lkey_ = dest_lkey; }
  void set_dest_rkey(uint32_t dest_rkey) { dest_rkey_ = dest_rkey; }

  // Returns the current value of next_op_id_ and modifies it by incrementing.
  uint64_t GetOpIdAndIncr() { return next_op_id_++; }
  // Returns the id of the most recent op.
  uint64_t GetLastOpId() const override { return next_op_id_ - 1; }

  // Given the wqe associated with TestOp with op_id, this function saves the
  // wqe in a batch on the qp.
  void BatchRcSendWqe(std::unique_ptr<ibv_send_wr> wqe,
                      std::unique_ptr<ibv_sge> sge, uint32_t op_id);
  void FlushRcSendWqes();
  void BatchRcRecvWqe(std::unique_ptr<ibv_recv_wr> wqe,
                      std::unique_ptr<ibv_sge> sge, uint32_t op_id);
  void FlushRcRecvWqes();
  uint32_t SendRcBatchCount() const { return rc_send_batch_.size(); }
  uint32_t RecvRcBatchCount() const { return rc_recv_batch_.size(); }

  uint64_t TotalOpsCompleted() const;
  uint64_t OpsCompleted(OpTypes op_type) const;
  uint64_t BytesCompleted(OpTypes op_type) const;
  void IncrCompletedBytes(uint64_t bytes, OpTypes op_type) override;
  void IncrCompletedOps(uint64_t op_cnt, OpTypes op_type) override;

  // Returns buffer spaces for some RDMA ops, based on parameters provided in
  // the op_params. The function returns op addresses from the least recently
  // used chunks of the qp buffer. Since the length of the qp buffer is finite,
  // the op buffer allocation eventually wraps around; hence the function
  // doesn't guarantee that new ops won't over write previous inflight ops. The
  // call returns a failure if num_ops*op_bytes is larger than the length of the
  // target buffer.
  absl::StatusOr<std::vector<uint8_t*>> GetNextOpAddresses(
      const OpAddressesParams& op_params) override;

  // Iterate over all TestOps in outstanding_ops_ and see if the data in source
  // and destination buffers of the OP are the same. When a test fails and some
  // completions don't arrive (ie. ValidateCompletions fails), we can run this
  // function to check if the op's data has landed. We only call this function
  // at the end of test, when we know test failed or passed. The purpose of this
  // function is to check for incomplete ops, did data land or not? We'd like to
  // differentiate between when data landed but completions didn't arrived vs.
  // when data didn't land and completions didn't arrive.
  void CheckDataLanded();

  // Validate whether the recv end of a two-sided SEND/RECV op is successful.
  // Return the corresponding RECV op as a TestOp unique_ptr if it can be found.
  // Otherwise, return nullptr.
  std::unique_ptr<TestOp> TryValidateRecvOp(const TestOp& send) override;

  // Stores the given TestOp for future validation upon receiving a completion
  // for the TestOp. It moves the TestOp from outstanding_ops to unchecked_ops,
  // makes a copy of the src/dst buffers and frees up the src/dst addresses to
  // be used by subsequent ops.
  void StoreOpForValidation(TestOp* op_ptr) override;

  // Dumps the QP's internal stats into a string.
  virtual std::string ToString() const = 0;

 private:
  struct SendWork {
    std::unique_ptr<ibv_send_wr> wqe{};
    std::unique_ptr<ibv_sge> sge{};
  };

  struct RecvWork {
    std::unique_ptr<ibv_recv_wr> wqe{};
    std::unique_ptr<ibv_sge> sge{};
  };

  // Print buffers content if the flag print_op_buffers is true.
  static void MaybePrintBuffer(absl::string_view prefix_msg,
                               std::string op_buffer);

  // Frees the given address from the specified buffer type (src/dst). This is
  // called by StoreOpForValidation() function when a completion arrives for an
  // op and it is stored for future validation.
  void FreeBufferAddress(OpAddressesParams::BufferType buffer_type,
                         uint8_t* addr) override;

  ibv_qp* qp_ = nullptr;
  BufferInfo src_buffer_;
  BufferInfo dest_buffer_;
  uint64_t next_op_id_ = 0;

  // A map of inflight (submitted but not completed) ops on this qp.
  // Map format: op_id -> TestOp
  // Using unique_ptr to provide pointer_stability for the values.
  absl::flat_hash_map<uint64_t, std::unique_ptr<TestOp>> outstanding_ops_;

  // Stores ops of type kRecv that have been polled with ibv_post_recv, but
  // haven't been validated yet. The local_ client needs to store unchecked
  // completions in this list until the corresponding kSend op completes at the
  // remote_.
  std::list<std::unique_ptr<TestOp>> unchecked_received_ops_;
  // Similar to unchecked_received_ops, but for initiated ops: kSend, kWrite,
  // and kRead.
  std::list<std::unique_ptr<TestOp>> unchecked_initiated_ops_;

  // Keeps a pointer to the remote client and local client and qp_id.
  int local_client_id_ = 0;
  uint32_t qp_id_ = 0;

  // Determines the distribution of operation attributes for operations on this
  // qp. Can be shared among multiple qps.
  OperationGenerator* op_generator_;

  uint64_t write_bytes_completed_ = 0;
  uint64_t write_ops_completed_ = 0;
  uint64_t read_bytes_completed_ = 0;
  uint64_t read_ops_completed_ = 0;
  uint64_t send_bytes_completed_ = 0;
  uint64_t send_ops_completed_ = 0;
  uint64_t recv_bytes_completed_ = 0;
  uint64_t recv_ops_completed_ = 0;
  uint64_t fetchadd_bytes_completed_ = 0;
  uint64_t fetchadd_ops_completed_ = 0;
  uint64_t compswap_bytes_completed_ = 0;
  uint64_t compswap_ops_completed_ = 0;

  // The access keys to local and remote client memory regions.
  uint32_t src_lkey_ = 0;
  uint32_t src_rkey_ = 0;
  uint32_t dest_lkey_ = 0;
  uint32_t dest_rkey_ = 0;

  // Collection of work requests that have been prepared but not yet posted to
  // the device for processing. The items on these lists are intrusively linked
  // to allow to post them all to the RDMA device with a single post call of the
  // head element.
  std::deque<SendWork> rc_send_batch_;
  std::deque<RecvWork> rc_recv_batch_;
};

class RcQpState : public QpState {
 public:
  using QpState::QpState;
  ~RcQpState() override = default;

  bool is_rc() const override { return true; }

  QpOpInterface* remote_qp_state() const override { return remote_qp_state_; }

  void set_remote_qp_state(QpOpInterface* remote_qp_state) override {
    remote_qp_state_ = remote_qp_state;
  }

  std::string ToString() const override;

 private:
  QpOpInterface* remote_qp_state_ = nullptr;
};

class UdQpState : public QpState {
 public:
  using QpState::QpState;
  ~UdQpState() override = default;

  bool is_rc() const override { return false; }

  void add_ud_destination(QpOpInterface* remote_qp_state, ibv_ah* ah) override {
    UdDestination dest{.ah = ah, .qp_state = remote_qp_state};
    ud_destinations_.push_back(dest);
  }

  const std::vector<UdDestination>* ud_destinations() const override {
    return &ud_destinations_;
  }

  UdDestination random_ud_destination() override {
    return ud_destinations_[rand() % ud_destinations_.size()];
  }

  std::string ToString() const override;

 private:
  // Possible destinations for data from this UD qp.
  std::vector<UdDestination> ud_destinations_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_QP_STATE_H_
