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

#include "traffic/qp_state.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "traffic/op_types.h"
#include "traffic/test_op.h"

ABSL_FLAG(bool, print_op_buffers, false,
          "When true, prints the content of source and destination buffer for "
          "each op, before and after after op completes.");

namespace rdma_unit_test {

QpState::QpState(int local_client_id, ibv_qp* qp, uint32_t qp_id, bool is_rc,
                 absl::Span<uint8_t> src_buffer,
                 absl::Span<uint8_t> dest_buffer, int max_outstanding_ops)
    : qp_{qp},
      src_buffer_{.base_addr = src_buffer.data(),
                  .length = src_buffer.size(),
                  .max_op_size = src_buffer.size() / max_outstanding_ops},
      dest_buffer_{.base_addr = dest_buffer.data(),
                   .length = dest_buffer.size(),
                   .max_op_size = dest_buffer.size() / max_outstanding_ops},
      local_client_id_{local_client_id},
      qp_id_{qp_id} {
  // Generate the list of aligned addresses (offsets) in the src and dst buffer
  // to be used by ops posted on this qp. First, align *down* max_op_size. This
  // is safe as we added an extra buffer of sizeof(uintptr_t) bytes when
  // allocating the buffer. Next, align the src/dest buffer base addresses.
  // Finally, calculate offsets as aligned_base_address + i * aligned_op_size.

  src_buffer_.max_op_size -= src_buffer_.max_op_size % sizeof(uintptr_t);
  dest_buffer_.max_op_size -= dest_buffer_.max_op_size % sizeof(uintptr_t);

  void* aligned_src_buffer_base_addr = src_buffer_.base_addr;
  std::size_t length = src_buffer_.length;
  if (std::align(sizeof(uintptr_t),
                 src_buffer_.max_op_size * max_outstanding_ops,
                 aligned_src_buffer_base_addr, length) == nullptr) {
    LOG(FATAL) << "Could not align qp src buffer address."  // Crash OK.
               << " src_addr: " << aligned_src_buffer_base_addr
               << " max_ops: " << max_outstanding_ops
               << " max_op_size: " << src_buffer_.max_op_size
               << " buffer_length: " << length;
  }

  void* aligned_dest_buffer_base_addr = dest_buffer_.base_addr;
  length = dest_buffer_.length;
  if (std::align(sizeof(uintptr_t),
                 dest_buffer_.max_op_size * max_outstanding_ops,
                 aligned_dest_buffer_base_addr, length) == nullptr) {
    LOG(FATAL) << "Could not align qp dest buffer address."  // Crash OK.
               << " dest_addr: " << aligned_dest_buffer_base_addr
               << " max_ops: " << max_outstanding_ops
               << " max_op_size: " << dest_buffer_.max_op_size
               << " buffer_length: " << length;
  }

  for (int i = 0; i < max_outstanding_ops; ++i) {
    src_buffer_.free_addresses.insert(
        static_cast<uint8_t*>(aligned_src_buffer_base_addr) +
        i * src_buffer_.max_op_size);
    dest_buffer_.free_addresses.insert(
        static_cast<uint8_t*>(aligned_dest_buffer_base_addr) +
        i * dest_buffer_.max_op_size);
  }
}

void QpState::IncrCompletedBytes(uint64_t bytes, OpTypes op_type) {
  switch (op_type) {
    case OpTypes::kRead:
      read_bytes_completed_ += bytes;
      return;
    case OpTypes::kWrite:
      write_bytes_completed_ += bytes;
      return;
    case OpTypes::kSend:
      send_bytes_completed_ += bytes;
      return;
    case OpTypes::kRecv:
      recv_bytes_completed_ += bytes;
      return;
    case OpTypes::kFetchAdd:
      fetchadd_bytes_completed_ += bytes;
      return;
    case OpTypes::kCompSwap:
      compswap_bytes_completed_ += bytes;
      return;
    default:
      return;
  }
}

void QpState::IncrCompletedOps(uint64_t op_cnt, OpTypes op_type) {
  switch (op_type) {
    case OpTypes::kRead:
      read_ops_completed_ += op_cnt;
      return;
    case OpTypes::kWrite:
      write_ops_completed_ += op_cnt;
      return;
    case OpTypes::kSend:
      send_ops_completed_ += op_cnt;
      return;
    case OpTypes::kRecv:
      recv_ops_completed_ += op_cnt;
      return;
    case OpTypes::kFetchAdd:
      fetchadd_ops_completed_ += op_cnt;
      return;
    case OpTypes::kCompSwap:
      compswap_ops_completed_ += op_cnt;
      return;
    default:
      LOG(WARNING)
          << "Trying to increment completed ops for unsupported op type: "
          << TestOp::ToString(op_type);
      return;
  }
}

uint64_t QpState::TotalOpsCompleted() const {
  return read_ops_completed_ + write_ops_completed_ + send_ops_completed_ +
         recv_ops_completed_ + fetchadd_ops_completed_ +
         compswap_ops_completed_;
}

uint64_t QpState::OpsCompleted(OpTypes op_type) const {
  switch (op_type) {
    case OpTypes::kRead:
      return read_ops_completed_;
    case OpTypes::kWrite:
      return write_ops_completed_;
    case OpTypes::kSend:
      return send_ops_completed_;
    case OpTypes::kRecv:
      return recv_ops_completed_;
    case OpTypes::kFetchAdd:
      return fetchadd_ops_completed_;
    case OpTypes::kCompSwap:
      return compswap_ops_completed_;
    default:
      return 0;
  }
}

uint64_t QpState::BytesCompleted(OpTypes op_type) const {
  switch (op_type) {
    case OpTypes::kRead:
      return read_bytes_completed_;
    case OpTypes::kWrite:
      return write_bytes_completed_;
    case OpTypes::kSend:
      return send_bytes_completed_;
    case OpTypes::kRecv:
      return recv_bytes_completed_;
    case OpTypes::kFetchAdd:
      return fetchadd_bytes_completed_;
    case OpTypes::kCompSwap:
      return compswap_bytes_completed_;
    default:
      return 0;
  }
}

absl::StatusOr<std::vector<uint8_t*>> QpState::GetNextOpAddresses(
    const OpAddressesParams& op_params) {
  const uint64_t& op_bytes = op_params.op_bytes;
  const auto& op_type = op_params.op_type;
  const auto& num_ops = op_params.num_ops;
  BufferInfo* buffer = nullptr;
  switch (op_params.buffer_to_use) {
    case OpAddressesParams::BufferType::kSrcBuffer:
      buffer = &src_buffer_;
      break;
    case OpAddressesParams::BufferType::kDestBuffer:
      buffer = &dest_buffer_;
      break;
    default:
      return absl::InvalidArgumentError(
          "Op buffer must be allocated on src_buffer_ or dest_buffer_.");
  }

  if (num_ops > buffer->free_addresses.size()) {
    return absl::OutOfRangeError(
        "Allocation request exceeds available buffer space!");
  }

  std::vector<uint8_t*> op_addrs;
  op_addrs.reserve(num_ops);

  for (int i = 0; i < num_ops; ++i) {
    auto addr_iter = buffer->free_addresses.begin();
    op_addrs.push_back(*addr_iter);
    buffer->active_addresses.insert(*addr_iter);
    buffer->free_addresses.erase(addr_iter);
    VLOG(2) << "New " << TestOp::ToString(op_type) << " op allocated on client "
            << local_client_id_ << ", qp_id " << qp_id()
            << ", addr: " << static_cast<void*>(op_addrs.back())
            << ", op size: " << op_bytes;
  }
  return op_addrs;
}

void QpState::BatchRcSendWqe(std::unique_ptr<ibv_send_wr> wqe,
                             std::unique_ptr<ibv_sge> sge, uint32_t op_id) {
  TestOp* op_raw_ptr = reinterpret_cast<TestOp*>(wqe->wr_id);
  VLOG(2) << "Batching op " << op_id << ", type "
          << TestOp::ToString(op_raw_ptr->op_type) << ", size "
          << op_raw_ptr->length << " bytes, on qp " << qp_id();
  if (!rc_send_batch_.empty()) {
    rc_send_batch_.back().wqe->next = wqe.get();
  }
  rc_send_batch_.push_back({.wqe = std::move(wqe), .sge = std::move(sge)});
}

void QpState::FlushRcSendWqes() {
  if (rc_send_batch_.empty()) return;
  int send_batch_size = rc_send_batch_.size();
  ibv_send_wr* bad_wr;
  int ibv_ret = ibv_post_send(qp_, rc_send_batch_.front().wqe.get(), &bad_wr);
  if (ibv_ret != 0) {
    LOG(FATAL) << "ibv_post_send returned non-zero error: "  // Crash OK.
               << ibv_ret;
  }
  VLOG(2) << "posted a batch of size " << send_batch_size << ", on qp "
          << qp_id();
  do {
    std::unique_ptr<ibv_send_wr> wqe_head =
        std::move(rc_send_batch_.front().wqe);

    // reinterpret_cast is safe bc wr_id is a cookie in the wqe that we set to
    // the address of the corresponding TestOp, when the TestOp and its wqe are
    // created.
    TestOp* op_raw_ptr = reinterpret_cast<TestOp*>(wqe_head->wr_id);
    VLOG(2) << "\t\t  op id " << op_raw_ptr->op_id << ", type "
            << TestOp::ToString(op_raw_ptr->op_type) << ", size "
            << op_raw_ptr->length << " bytes.";
    rc_send_batch_.pop_front();
  } while (!rc_send_batch_.empty());
}

void QpState::BatchRcRecvWqe(std::unique_ptr<ibv_recv_wr> wqe,
                             std::unique_ptr<ibv_sge> sge, uint32_t op_id) {
  VLOG(2) << "Batching recv op " << op_id << ", size " << sge->length
          << " bytes, on qp " << qp_id();
  if (!rc_recv_batch_.empty()) {
    rc_recv_batch_.back().wqe->next = wqe.get();
  }
  rc_recv_batch_.push_back({.wqe = std::move(wqe), .sge = std::move(sge)});
}

void QpState::FlushRcRecvWqes() {
  if (rc_recv_batch_.empty()) return;
  int recv_batch_size = rc_recv_batch_.size();
  ibv_recv_wr* bad_wr;
  int ibv_ret = ibv_post_recv(qp_, rc_recv_batch_.front().wqe.get(), &bad_wr);
  if (ibv_ret != 0) {
    LOG(FATAL) << "ibv_post_recv returned non-zero error: "  // Crash OK.
               << ibv_ret;
  }
  VLOG(2) << "posted a batch of size " << recv_batch_size << ", on qp "
          << qp_id();
  do {
    std::unique_ptr<ibv_recv_wr> wqe_head =
        std::move(rc_recv_batch_.front().wqe);

    // reinterpret_cast is safe bc wr_id is a cookie in the wqe that we set to
    // the address of the corresponding TestOp, when the TestOp and its wqe are
    // created.
    TestOp* op_raw_ptr = reinterpret_cast<TestOp*>(wqe_head->wr_id);
    VLOG(2) << "\t\t  op id " << op_raw_ptr->op_id << ", type "
            << TestOp::ToString(op_raw_ptr->op_type) << ", size "
            << op_raw_ptr->length << " bytes.";
    rc_recv_batch_.pop_front();
  } while (!rc_recv_batch_.empty());
}

void QpState::CheckDataLanded() {
  if (outstanding_ops().empty()) {
    return;
  }

  LOG(INFO) << "client " << local_client_id_ << " qp_id " << qp_id() << ", has "
            << outstanding_ops().size() << " outstanding_ops "
            << unchecked_initiated_ops_.size() << " unchecked_initiated_ops_ "
            << unchecked_received_ops_.size() << " unchecked_received_ops_";

  for (const auto& [op_id, op_ptr] : outstanding_ops()) {
    if (op_ptr == nullptr) {
      LOG(INFO) << "op_id " << op_id
                << " completion received, but is pending validation.";
      continue;
    }
    if (op_ptr->src_addr && op_ptr->dest_addr &&
        std::memcmp(op_ptr->src_addr, op_ptr->dest_addr, op_ptr->length) == 0) {
      LOG(INFO) << "op_id " << op_ptr->op_id << " LANDED successfully.";
    } else {
      LOG(INFO) << "op_id " << op_ptr->op_id
                << " ---------DID NOT LAND successfully-------";
      VLOG(2) << absl::StrFormat("op_id: %lu, src after operation: ",
                                 op_ptr->op_id)
              << op_ptr->SrcBuffer();
      VLOG(2) << absl::StrFormat("op_id: %lu, dest after operation: ",
                                 op_ptr->op_id)
              << op_ptr->DestBuffer();
    }
  }

  LOG(INFO) << "Unchecked initiated ops:";
  for (const auto& op_ptr : unchecked_initiated_ops_) {
    LOG(INFO) << "op_id " << op_ptr->op_id << " "
              << static_cast<void*>(op_ptr->src_addr) << " "
              << static_cast<void*>(op_ptr->dest_addr);
    VLOG(2) << "src: " << op_ptr->SrcBuffer();
    VLOG(2) << "dst: " << op_ptr->DestBuffer();
  }

  LOG(INFO) << "Unchecked received ops:";
  for (const auto& op_ptr : unchecked_received_ops_) {
    LOG(INFO) << "op_id " << op_ptr->op_id << " "
              << static_cast<void*>(op_ptr->src_addr) << " "
              << static_cast<void*>(op_ptr->dest_addr);
    VLOG(2) << "src: " << op_ptr->SrcBuffer();
    VLOG(2) << "dst: " << op_ptr->DestBuffer();
  }
}

std::unique_ptr<TestOp> QpState::TryValidateRecvOp(const TestOp& send) {
  std::unique_ptr<TestOp> target_op_uptr = nullptr;
  if (is_rc()) {
    if (!unchecked_received_ops_.empty()) {
      target_op_uptr = std::move(unchecked_received_ops_.front());
      unchecked_received_ops_.pop_front();
    }
  } else {  // is UD
    // Because UD operations can arrive out of order, find the corresponding
    // receive operation to this send by comparing op buffers directly.
    for (auto dest_it = unchecked_received_ops_.begin();
         dest_it != unchecked_received_ops_.end(); ++dest_it) {
      if (std::memcmp(send.src_addr,
                      (*dest_it)->dest_addr + sizeof(struct ibv_grh),
                      send.length) == 0) {
        target_op_uptr = std::move(*dest_it);
        unchecked_received_ops_.erase(dest_it);
        break;
      }
    }
  }

  if (target_op_uptr == nullptr) {
    VLOG(2) << absl::StrFormat(
        "Store SEND op for future validation! qp_id "
        "%d, op_id %lu, src after %s: ",
        send.qp_id, send.op_id, TestOp::ToString(send.op_type)),
        send.SrcBuffer();
  } else {
    EXPECT_EQ(target_op_uptr->status, IBV_WC_SUCCESS);
    IncrCompletedBytes(send.length, OpTypes::kRecv);
    IncrCompletedOps(1, OpTypes::kRecv);
  }

  return target_op_uptr;
}

void QpState::StoreOpForValidation(TestOp* op_ptr) {
  using BufferType = QpState::OpAddressesParams::BufferType;
  auto iter = outstanding_ops_.find(op_ptr->op_id);
  if (iter == outstanding_ops_.end()) {
    LOG(FATAL) << "Trying to defer an op that is not pending.";  // Crash OK.
  }

  // Move the op from outstanding_ops to unchecked_ops.
  if (op_ptr->op_type == OpTypes::kRecv) {
    unchecked_received_ops().push_back(
        std::move(outstanding_ops().at(op_ptr->op_id)));
  } else {
    unchecked_initiated_ops().push_back(
        std::move(outstanding_ops().at(op_ptr->op_id)));
  }
  outstanding_ops().erase(op_ptr->op_id);

  // Make a copy of op src/dest buffer in a separate variable.
  if (op_ptr->src_addr != nullptr) {
    op_ptr->src_buffer_copy = std::make_unique<std::vector<uint8_t>>(
        op_ptr->src_addr, op_ptr->src_addr + src_buffer_.max_op_size);
  }
  if (op_ptr->dest_addr != nullptr) {
    op_ptr->dest_buffer_copy = std::make_unique<std::vector<uint8_t>>(
        op_ptr->dest_addr, op_ptr->dest_addr + dest_buffer_.max_op_size);
  }

  // Free up the initiator side buffer address.
  switch (op_ptr->op_type) {
    case OpTypes::kWrite:
    case OpTypes::kSend:
      FreeBufferAddress(BufferType::kSrcBuffer, op_ptr->src_addr);
      break;
    case OpTypes::kRead:
    case OpTypes::kRecv:
    case OpTypes::kCompSwap:
    case OpTypes::kFetchAdd:
      FreeBufferAddress(BufferType::kDestBuffer, op_ptr->dest_addr);
      break;
    case OpTypes::kInvalid:
      LOG(FATAL) << "Invalid op_type.";  // Crash OK.
  }
  // If one-sided op, then free up the buffer address on the target side too.
  if (op_ptr->op_type != OpTypes::kSend && op_ptr->op_type != OpTypes::kRecv) {
    QpOpInterface* target_qp;
    if (is_rc()) {
      target_qp = remote_qp_state();
    } else {  // is UD.
      target_qp = op_ptr->remote_qp;
    }
    if (target_qp == nullptr) {
      LOG(FATAL) << "Target QP cannot be null";  // Crash OK.
    }
    if (op_ptr->op_type == OpTypes::kWrite) {
      target_qp->FreeBufferAddress(BufferType::kDestBuffer, op_ptr->dest_addr);
    } else {
      target_qp->FreeBufferAddress(BufferType::kSrcBuffer, op_ptr->src_addr);
    }
  }

  // Set the src/dst addr to point to the copy data.
  if (op_ptr->src_addr != nullptr) {
    op_ptr->src_addr = op_ptr->src_buffer_copy->data();
  }
  if (op_ptr->dest_addr != nullptr) {
    op_ptr->dest_addr = op_ptr->dest_buffer_copy->data();
  }
}

void QpState::FreeBufferAddress(OpAddressesParams::BufferType buffer_type,
                                uint8_t* addr) {
  if (buffer_type == OpAddressesParams::BufferType::kSrcBuffer) {
    auto iter = src_buffer_.active_addresses.find(addr);
    if (iter == src_buffer_.active_addresses.end()) {
      LOG(FATAL) << "Freeing src buffer addr that is not pending";  // Crash OK.
    }
    src_buffer_.free_addresses.insert(addr);
    src_buffer_.active_addresses.erase(iter);
  } else {
    auto iter = dest_buffer_.active_addresses.find(addr);
    if (iter == dest_buffer_.active_addresses.end()) {
      LOG(FATAL) << "Freeing dst buffer addr that is not pending";  // Crash OK.
    }
    dest_buffer_.free_addresses.insert(addr);
    dest_buffer_.active_addresses.erase(iter);
  }
}

std::string QpState::DumpState() const {
  std::stringstream out;
  out << " qp_id: " << qp_id() << ",\n"
      << " qp_ptr: " << qp() << ",\n"
      << absl::StrFormat(" src_buffer: base: %p, length: %lu\n",
                         src_buffer().base_addr, src_buffer().length)
      << absl::StrFormat(" dest_buffer: base: %p, length: %lu\n",
                         dest_buffer().base_addr, dest_buffer().length)
      << " src_lkey: " << src_lkey() << ",\n"
      << " dest_lkey: " << dest_lkey() << ",\n"
      << " write_bytes_completed: " << BytesCompleted(OpTypes::kWrite) << ",\n"
      << " write_ops_completed: " << OpsCompleted(OpTypes::kWrite) << ",\n"
      << " read_bytes_completed: " << BytesCompleted(OpTypes::kRead) << ",\n"
      << " read_ops_completed: " << OpsCompleted(OpTypes::kRead) << ",\n"
      << " send_bytes_completed: " << BytesCompleted(OpTypes::kSend) << ",\n"
      << " send_ops_completed: " << OpsCompleted(OpTypes::kSend) << ",\n"
      << " receive_bytes_completed: " << BytesCompleted(OpTypes::kRecv) << ",\n"
      << " receive_ops_completed: " << OpsCompleted(OpTypes::kRecv) << ",\n"
      << " total_ops_completed: " << TotalOpsCompleted() << ",\n"
      << " total_ops_pending: " << outstanding_ops_count() << ",\n"
      << " unchecked_initiate_ops: " << unchecked_initiated_ops_.size() << ",\n"
      << " unchecked_receive_ops: " << unchecked_received_ops_.size() << ",\n";
  return out.str();
}

void QpState::MaybePrintBuffer(absl::string_view prefix_msg,
                               std::string op_buffer) {
  if (!absl::GetFlag(FLAGS_print_op_buffers)) {
    return;
  }
  LOG(INFO) << absl::StrCat(prefix_msg, op_buffer);
}

std::string RcQpState::ToString() const {
  std::stringstream out;
  out << "RcQpState:\n";
  out << DumpState();
  if (remote_qp_state() != nullptr) {
    out << " remote_qp_id: " << remote_qp_state()->qp_id() << "\n";
  } else {
    out << " remote_qp_id: Not Connected\n";
  }
  return out.str();
}

std::string UdQpState::ToString() const {
  std::stringstream out;
  out << "UdQpState:\n";
  out << DumpState();
  out << " remote_qp_ids: ";
  for (size_t i = 0; i < ud_destinations()->size(); i++) {
    if (i != 0) {
      out << " | ";
    }
    out << ud_destinations()->at(i).qp_state->qp_id();
  }
  out << "\n";
  return out.str();
}

}  // namespace rdma_unit_test
