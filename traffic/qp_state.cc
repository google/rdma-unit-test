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
                 absl::Span<uint8_t> dest_buffer)
    : qp_{qp},
      src_buffer_{.base_addr = src_buffer.data(),
                  .length = src_buffer.size(),
                  .next_op_offset = 0},
      dest_buffer_{.base_addr = dest_buffer.data(),
                   .length = dest_buffer.size(),
                   .next_op_offset = 0},
      local_client_id_{local_client_id},
      qp_id_{qp_id} {}

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

  if (op_bytes * num_ops > buffer->length) {
    return absl::OutOfRangeError(
        "Allocation request exceeds available buffer space!");
  }

  std::vector<uint8_t*> op_addrs;
  op_addrs.reserve(num_ops);

  uint64_t offset = buffer->next_op_offset;
  uint64_t len = buffer->length;
  uint8_t* base = buffer->base_addr;

  if (op_type == OpTypes::kFetchAdd || op_type == OpTypes::kCompSwap) {
    CHECK_EQ(op_bytes, TestOp::kAtomicWordSize)  // Crash OK
        << "Invalid op_bytes: " << op_bytes << " for RDMA atomic.";
    // RDMA atomic operations must always start at 8-bytes aligned addresses.
    if (offset % op_bytes != 0) {
      offset += op_bytes - (offset % op_bytes);
    }

    for (size_t i = 0; i < num_ops; ++i) {
      if (offset + op_bytes >= len) offset = 0;
      op_addrs.push_back(base + offset);
      VLOG(2) << "New " << TestOp::ToString(op_type)
              << " op allocated on client " << local_client_id_ << ", qp_id "
              << qp_id() << ", offset: " << offset << ", op size: " << op_bytes
              << ", buffer size: " << len;
      offset += op_bytes;
    }
  } else {
    // If num_ops don't fit in the remaining buffer (ie. len-offset), move the
    // offset forward to allow an integer number of ops in the remaining buffer
    // and then wrap the offset around.
    if (len - offset < op_bytes * num_ops) {
      offset = (((len - offset) % op_bytes) + offset) % len;
    }
    for (size_t i = 0; i < num_ops; ++i) {
      VLOG(2) << "New " << TestOp::ToString(op_type)
              << " op allocated on client " << local_client_id_ << ", qp_id "
              << qp_id() << ", offset: " << offset << ", op size: " << op_bytes
              << ", buffer size: " << len;
      op_addrs.push_back(base + offset);
      offset = (offset + op_bytes) % len;
    }
  }
  buffer->next_op_offset = offset;
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
            << outstanding_ops().size() << " outstanding_ops:";

  for (const auto& op_ptr_iter : outstanding_ops()) {
    if (std::memcmp(op_ptr_iter.second->src_addr, op_ptr_iter.second->dest_addr,
                    op_ptr_iter.second->length) == 0) {
      LOG(INFO) << "op_id " << op_ptr_iter.second->op_id
                << " LANDED successfully.";
    } else {
      LOG(INFO) << "op_id " << op_ptr_iter.second->op_id
                << " ---------DID NOT LAND successfully-------";
      LOG(INFO) << absl::StrFormat("op_id: %lu, src after operation: ",
                                   op_ptr_iter.second->op_id)
                << op_ptr_iter.second->SrcBuffer();
      LOG(INFO) << absl::StrFormat("op_id: %lu, dest after operation: ",
                                   op_ptr_iter.second->op_id)
                << op_ptr_iter.second->DestBuffer();
    }
  }
}

std::optional<TestOp> QpState::TryValidateRecvOp(const TestOp& send) {
  std::unique_ptr<TestOp> target_op_uptr = nullptr;
  if (is_rc()) {
    if (!unchecked_received_ops_.empty()) {
      target_op_uptr = std::move(unchecked_received_ops_.front());
      unchecked_received_ops_.pop_front();
    }
  } else {  // is UD
    // Because UD operations can arrive out of order, find the receive
    // corresponding receive operation to this send.
    for (auto dest_it = unchecked_received_ops_.begin();
         dest_it != unchecked_received_ops_.end(); ++dest_it) {
      // If the buffer is large enough to hold the remote op id, it will
      // be stored in the beginning of the buffer and we only need to
      // compare that.
      int compare_length = std::min(send.length, sizeof(uint64_t));
      if (std::memcmp(send.src_addr,
                      (*dest_it)->dest_addr + sizeof(struct ibv_grh),
                      compare_length) == 0) {
        target_op_uptr = std::move(*dest_it);
        unchecked_received_ops_.erase(dest_it);
        break;
      }
    }
  }

  if (target_op_uptr == nullptr) {
    MaybePrintBuffer(
        absl::StrFormat("Store SEND op for future validation! qp_id "
                        "%d, op_id %lu, src after %s: ",
                        send.qp_id, send.op_id, TestOp::ToString(send.op_type)),
        send.SrcBuffer());
    return absl::nullopt;
  }

  EXPECT_EQ(target_op_uptr->status, IBV_WC_SUCCESS);
  uint8_t* dest_addr = target_op_uptr->dest_addr;
  // The beginning of the UD receive buffer will contain an ibv_grh
  if (!is_rc()) {
    dest_addr += sizeof(ibv_grh);
  }
  outstanding_ops().erase(target_op_uptr->op_id);
  IncrCompletedBytes(send.length, OpTypes::kRecv);
  IncrCompletedOps(1, OpTypes::kRecv);

  return *target_op_uptr;
}

std::string QpState::DumpState() const {
  std::stringstream out;
  out << " qp_ptr: " << qp() << ",\n"
      << " qp_id: " << qp_id() << ",\n"
      << " write_bytes_completed: " << BytesCompleted(OpTypes::kWrite) << ",\n"
      << " write_ops_completed: " << OpsCompleted(OpTypes::kWrite) << ",\n"
      << " read_bytes_completed: " << BytesCompleted(OpTypes::kRead) << ",\n"
      << " read_ops_completed: " << OpsCompleted(OpTypes::kRead) << ",\n"
      << " send_bytes_completed: " << BytesCompleted(OpTypes::kSend) << ",\n"
      << " send_ops_completed: " << OpsCompleted(OpTypes::kSend) << ",\n"
      << " receive_bytes_completed: " << BytesCompleted(OpTypes::kRecv) << ",\n"
      << " receive_ops_completed: " << OpsCompleted(OpTypes::kRecv) << ",\n"
      << " src_lkey: " << src_lkey() << ",\n"
      << " dest_lkey: " << dest_lkey() << ",\n"
      << absl::StrFormat(" src_buffer: base: %p, length: %lu, offset: %lu\n",
                         src_buffer().base_addr, src_buffer().length,
                         src_buffer().next_op_offset)
      << absl::StrFormat(" dest_buffer: base: %p, length: %lu, offset: %lu\n",
                         dest_buffer().base_addr, dest_buffer().length,
                         dest_buffer().next_op_offset);
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
  out << " remote_qp_id: " << remote_qp_state()->qp_id() << "\n";
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
