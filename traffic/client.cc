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

#include "traffic/client.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <poll.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "internal/verbs_cleanup.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"

#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/qp_op_interface.h"
#include "traffic/qp_state.h"

ABSL_FLAG(bool, huge_page_buffers, false,
          "When true, each client allocates huge page buffers. If huge page is"
          "enabled, --page_align_buffer flag is ignored.");
ABSL_FLAG(bool, page_align_buffers, false,
          "When true, each client allocates page aligned source and "
          "destination buffers. If you need each qp's buffer to be "
          "page-aligned, also set --buffer_per_qp to a multiple of page size.");
ABSL_FLAG(absl::Duration, inter_op_delay_us, absl::Seconds(0),
          "Microseconds delay between consecutive op submission.");
ABSL_FLAG(
    absl::Duration, completion_timeout_s, absl::Seconds(5),
    "Timeout value for consecutive completion arrivals. The test times out if"
    " this number of seconds passes but no new completion has arrived. In a"
    " well-working non-faulty test, we expect consecutive completions to arrive"
    " much less than this timeout apart.");

namespace rdma_unit_test {
namespace {

void PrintAsyncEvent(struct ibv_context* ctx, struct ibv_async_event* event) {
  switch (event->event_type) {
    /* QP events */
    case IBV_EVENT_QP_FATAL:
      LOG(INFO) << "QP fatal event for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_QP_REQ_ERR:
      LOG(INFO) << "QP Requestor error for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_QP_ACCESS_ERR:
      LOG(INFO) << "QP access error event for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_COMM_EST:
      LOG(INFO) << "QP communication established event for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_SQ_DRAINED:
      LOG(INFO) << "QP Send Queue drained event for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_PATH_MIG:
      LOG(INFO) << "QP Path migration loaded event for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_PATH_MIG_ERR:
      LOG(INFO) << "QP Path migration error event for QP with handle "
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_QP_LAST_WQE_REACHED:
      LOG(INFO) << "QP last WQE reached event for QP with handle "
                << event->element.qp->qp_num;
      break;

    /* CQ events */
    case IBV_EVENT_CQ_ERR:
      LOG(INFO) << "CQ error for CQ with handle " << event->element.cq;
      break;

    /* SRQ events */
    case IBV_EVENT_SRQ_ERR:
      LOG(INFO) << "SRQ error for SRQ with handle " << event->element.srq;
      break;
    case IBV_EVENT_SRQ_LIMIT_REACHED:
      LOG(INFO) << "SRQ limit reached event for SRQ with handle "
                << event->element.srq;
      break;

    /* Port events */
    case IBV_EVENT_PORT_ACTIVE:
      LOG(INFO) << "Port active event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_PORT_ERR:
      LOG(INFO) << "Port error event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_LID_CHANGE:
      LOG(INFO) << "LID change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_PKEY_CHANGE:
      LOG(INFO) << "P_Key table change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_GID_CHANGE:
      LOG(INFO) << "GID table change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_SM_CHANGE:
      LOG(INFO) << "SM change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_CLIENT_REREGISTER:
      LOG(INFO) << "Client reregister event for port number "
                << event->element.port_num;
      break;

    /* RDMA device events */
    case IBV_EVENT_DEVICE_FATAL:
      LOG(INFO) << "Fatal error event for device "
                << ibv_get_device_name(ctx->device);
      break;

    default:
      LOG(INFO) << "Unknown event " << event->event_type;
  }
}

void SetQpKeys(QpState* const qp_state, const ibv_mr* const src_mr,
               const ibv_mr* const dest_mr) {
  qp_state->set_src_lkey(src_mr->lkey);
  qp_state->set_src_rkey(src_mr->rkey);
  qp_state->set_dest_lkey(dest_mr->lkey);
  qp_state->set_dest_rkey(dest_mr->rkey);
}

}  // namespace

Client::Client(int client_id, ibv_context* context, PortAttribute port_attr,
               const Config config)
    : context_(context),
      pd_(ibv_.AllocPd(context)),
      port_attr_(port_attr),
      send_cq_(nullptr),
      recv_cq_(nullptr),
      qps_{},
      client_id_(client_id),
      max_outstanding_ops_per_qp_(config.max_outstanding_ops_per_qp),
      buffer_per_qp_([config]() -> int {
        // Make sure that buffer is at least as large as necessary to hold the
        // maximum number of outstanding ops per qp plus ibv_grh header for UD
        // ops. sizeof(uintptr_t) is added to keep extra buffer in case we need
        // to align it to word size boundary.
        int min_buffer_size =
            (config.max_op_size + sizeof(ibv_grh) + sizeof(uintptr_t)) *
                config.max_outstanding_ops_per_qp +
            sizeof(uintptr_t);
        if (min_buffer_size > kDefaultBuffersPerQp) {
          LOG(INFO) << "Default buffer size (" << kDefaultBuffersPerQp
                    << "B) is not large enough to hold all outstanding ops. "
                       "Resizing to "
                    << min_buffer_size << "B.";
          return min_buffer_size;
        }
        return kDefaultBuffersPerQp;
      }()),
      max_qps_(config.max_qps) {
  // Buffer size is equal 'max_qps * buffer_per_qp',
  // rounded up to the nearest multiple integer of page size.
  if (absl::GetFlag(FLAGS_huge_page_buffers)) {
    uint64_t buffer_pages =
        ceil((max_qps_ * buffer_per_qp_ * 1.0) / kHugepageSize);
    src_buffer_ =
        std::make_unique<RdmaMemBlock>(ibv_.AllocHugepageBuffer(buffer_pages));
    dest_buffer_ =
        std::make_unique<RdmaMemBlock>(ibv_.AllocHugepageBuffer(buffer_pages));
  } else {
    uint64_t buffer_pages = ceil((max_qps_ * buffer_per_qp_ * 1.0) / kPageSize);
    if (absl::GetFlag(FLAGS_page_align_buffers)) {
      src_buffer_ =
          std::make_unique<RdmaMemBlock>(ibv_.AllocAlignedBuffer(buffer_pages));
      dest_buffer_ =
          std::make_unique<RdmaMemBlock>(ibv_.AllocAlignedBuffer(buffer_pages));
    } else {
      src_buffer_ =
          std::make_unique<RdmaMemBlock>(ibv_.AllocBuffer(buffer_pages));
      dest_buffer_ =
          std::make_unique<RdmaMemBlock>(ibv_.AllocBuffer(buffer_pages));
    }
  }

  // Init src buffer content to corresponding qp_id % 8 + 1.
  auto buffer = src_buffer_->span();
  for (size_t i = 0; i < buffer.size(); ++i) {
    buffer[i] = i / buffer_per_qp_ + 1;
  }

  // Create source and destination memory region.
  src_mr_.clear();
  src_mr_.reserve(1);
  src_mr_.push_back(ibv_.RegMr(pd_, *src_buffer_));
  CHECK(src_mr_[0]);  // Crash OK
  LOG(INFO) << "src_lkey: " << src_mr_.back()->lkey;

  dest_mr_.clear();
  dest_mr_.reserve(1);
  dest_mr_.push_back(ibv_.RegMr(pd_, *dest_buffer_));
  CHECK(dest_mr_[0]);  // Crash OK
  LOG(INFO) << "dest_lkey: " << dest_mr_.back()->lkey;

  // Create completion queues.
  int cq_size = max_outstanding_ops_per_qp_ * max_qps_;
  ibv_device_attr dev_attr = {};
  CHECK_EQ(0, ibv_query_device(context_, &dev_attr));  // Crash OK
  int cq_slots = std::min(dev_attr.max_cqe, cq_size);
  send_cc_ = ibv_.CreateChannel(context_);
  send_cq_ = ibv_.CreateCq(context_,
                           config.send_cq_size > 0
                               ? std::min(dev_attr.max_cqe, config.send_cq_size)
                               : cq_slots,
                           send_cc_);
  CHECK(send_cq_);  // Crash OK
  recv_cc_ = ibv_.CreateChannel(context_);
  recv_cq_ = ibv_.CreateCq(context_,
                           config.recv_cq_size > 0
                               ? std::min(dev_attr.max_cqe, config.recv_cq_size)
                               : cq_slots,
                           recv_cc_);
  CHECK(recv_cq_);  // Crash OK

  qps_.reserve(max_qps_);
}

Client::~Client() {
  if (send_epoll_fd_.has_value()) {
    CHECK_EQ(TEMP_FAILURE_RETRY(close(*send_epoll_fd_)), 0);  // Crash OK
  }
  if (recv_epoll_fd_.has_value()) {
    CHECK_EQ(TEMP_FAILURE_RETRY(close(*recv_epoll_fd_)), 0);  // Crash OK
  }
}

absl::StatusOr<uint32_t> Client::CreateQp(bool is_rc,
                                          QpInitAttribute qp_init_attribute) {
  if (qps_.size() == max_qps_)
    return absl::OutOfRangeError(
        absl::StrCat("Max allowed qps per client is ", max_qps_));

  ibv_qp* qp;
  if (is_rc) {
    qp = ibv_.CreateQp(pd_, send_cq_, recv_cq_, IBV_QPT_RC, qp_init_attribute);

    CHECK(qp);  // Crash OK
  } else {
    qp = ibv_.CreateQp(pd_, send_cq_, recv_cq_, IBV_QPT_UD, qp_init_attribute);
    CHECK(qp);                                                   // Crash OK
    CHECK_OK(ibv_.ModifyUdQpResetToRts(qp, port_attr_, kQKey));  // Crash OK
  }
  uint32_t qp_id = qps_.size();
  absl::Span<uint8_t> qp_src_buf = GetQpSrcBuffer(qp_id).span();
  absl::Span<uint8_t> qp_dest_buf = GetQpDestBuffer(qp_id).span();
  if (is_rc) {
    auto qp_state =
        std::make_unique<RcQpState>(client_id_, qp, qp_id, is_rc, qp_src_buf,
                                    qp_dest_buf, max_outstanding_ops_per_qp_);
    SetQpKeys(qp_state.get(), src_mr_[0], dest_mr_[0]);
    qps_[qp_id] = std::move(qp_state);
  } else {  // is UD
    auto qp_state =
        std::make_unique<UdQpState>(client_id_, qp, qp_id, is_rc, qp_src_buf,
                                    qp_dest_buf, max_outstanding_ops_per_qp_);
    SetQpKeys(qp_state.get(), src_mr_[0], dest_mr_[0]);
    qps_[qp_id] = std::move(qp_state);
  }

  LOG(INFO) << "Client" << client_id()
          << ", created Qp: " << qps_[qp_id]->ToString();
  return qp_id;
}

absl::Status Client::DeleteQp(uint32_t qp_id) {
  auto qp = qps_.at(qp_id)->qp();
  if (0 != ibv_.DestroyQp(qp)) {
    return absl::InternalError(absl::StrCat(
        "Client ", client_id(), " failed to destroy qp id ", qp_id));
  }
  qps_.erase(qp_id);
  return absl::OkStatus();
}

ibv_ah* Client::CreateAh(PortAttribute port_attr) {
  ibv_ah_attr ah_attr = AhAttribute().GetAttribute(
      port_attr.port, port_attr.gid_index, port_attr.gid);
  ibv_ah* ah = ibv_.CreateAh(pd_, ah_attr);
  if (ah != nullptr) {
    ahs_.push_back(ah);
  }
  return ah;
}

void Client::AddAh(ibv_ah* ah) {
  ahs_.push_back(ah);
  ibv_.clean_up().AddCleanup(ah);
}

absl::Status Client::PostOps(const OpAttributes& attributes) {
  using OpAddressesParams = QpState::OpAddressesParams;
  using BufferType = QpState::OpAddressesParams::BufferType;

  if (attributes.num_ops < 1) return absl::OkStatus();
  if (attributes.initiator_qp_id >= qps_.size()) {
    return absl::OutOfRangeError(absl::StrCat("qp ", attributes.initiator_qp_id,
                                              " is not created yet!"));
  }

  OpTypes op_type = attributes.op_type;
  std::string op_type_str = TestOp::ToString(op_type);
  std::unique_ptr<QpState>& initiator_qp_state =
      qps_[attributes.initiator_qp_id];
  LOG(INFO) << "Post " << attributes.num_ops << " " << op_type_str
          << " op on initiator client" << client_id() << ", "
          << initiator_qp_state->ToString();

  BufferType initiator_buffer_type;
  BufferType target_buffer_type;
  switch (op_type) {
    case OpTypes::kWrite:
    case OpTypes::kSend:
      initiator_buffer_type = BufferType::kSrcBuffer;
      target_buffer_type = BufferType::kDestBuffer;
      break;
    case OpTypes::kRead:
    case OpTypes::kRecv:
    case OpTypes::kCompSwap:
    case OpTypes::kFetchAdd:
      initiator_buffer_type = BufferType::kDestBuffer;
      target_buffer_type = BufferType::kSrcBuffer;
      break;
    default:
      return absl::InternalError(
          absl::StrCat("op_type '", op_type_str, "' not recognized."));
  }

  int op_bytes = attributes.op_bytes;
  if (!initiator_qp_state->is_rc() && op_type == OpTypes::kRecv) {
    // Add space for the Global Routing Header at the beginning of a UD receive
    // buffer.
    op_bytes += sizeof(ibv_grh);
  }
  OpAddressesParams op_buffer_info{.num_ops = attributes.num_ops,
                                   .op_bytes = op_bytes,
                                   .buffer_to_use = initiator_buffer_type,
                                   .op_type = op_type};
  ASSIGN_OR_RETURN(std::vector<uint8_t*> initiator_op_addrs,
                   initiator_qp_state->GetNextOpAddresses(op_buffer_info));

  // Send/Recv operations do not require a specific address on the target.
  // All other op types do.
  bool is_send_recv = op_type == OpTypes::kSend || op_type == OpTypes::kRecv;
  std::vector<uint8_t*> target_op_addrs = {};
  if (!is_send_recv) {
    op_buffer_info.buffer_to_use = target_buffer_type;
    QpState* initiator_rc_qp_state = qps_[attributes.initiator_qp_id].get();
    QpOpInterface* target_qp_state = initiator_rc_qp_state->remote_qp_state();
    ASSIGN_OR_RETURN(target_op_addrs,
                     target_qp_state->GetNextOpAddresses(op_buffer_info));
  }

  // Create TestOps, WQEs and submit WQEs.
  for (int i = 0; i < attributes.num_ops; ++i) {
    uint8_t* initiator_op_addr = initiator_op_addrs[i];
    const uint64_t op_id = initiator_qp_state->GetOpIdAndIncr();

    // Create scatter-gather entry.
    auto sge = std::make_unique<ibv_sge>();
    sge->addr = reinterpret_cast<uint64_t>(initiator_op_addr);
    sge->length = op_bytes;

    auto op = std::make_unique<TestOp>();
    op->op_id = op_id;
    op->qp_id = attributes.initiator_qp_id;
    op->length = sge->length;
    op->op_type = op_type;

    if (initiator_buffer_type == BufferType::kSrcBuffer) {
      sge->lkey = initiator_qp_state->src_lkey();
      op->src_addr = initiator_op_addr;
      if (!is_send_recv) {
        op->dest_addr = target_op_addrs[i];
      }
    } else {  // initiator_buffer_type == BufferType::kDestBuffer
      sge->lkey = initiator_qp_state->dest_lkey();
      op->dest_addr = initiator_op_addr;
      if (!is_send_recv) {
        op->src_addr = target_op_addrs[i];
      }
    }

    if (op_type == OpTypes::kRecv) {
      // Fill in the buffer with random bytes.
      std::generate_n(op->dest_addr, op->length, std::ref(random));
      uint64_t wr_id = reinterpret_cast<uint64_t>(op.get());
      auto wqe_recv = std::make_unique<ibv_recv_wr>(
          verbs_util::CreateRecvWr(wr_id, sge.get(), /*num_sge=*/1));
      initiator_qp_state->BatchRcRecvWqe(std::move(wqe_recv), std::move(sge),
                                         op->op_id);
    } else {
      uint32_t target_src_rkey;
      uint32_t target_dest_rkey;
      if (initiator_qp_state->is_rc()) {
        QpOpInterface* target_qp_state = initiator_qp_state->remote_qp_state();
        target_src_rkey = target_qp_state->src_rkey();
        target_dest_rkey = target_qp_state->dest_rkey();
      }
      std::unique_ptr<ibv_send_wr> wqe_send;
      uint64_t wr_id = reinterpret_cast<uint64_t>(op.get());
      switch (op_type) {
        case OpTypes::kWrite:
          wqe_send = std::make_unique<ibv_send_wr>(
              verbs_util::CreateWriteWr(wr_id, sge.get(), /*num_sge=*/1,
                                        op->dest_addr, target_dest_rkey));
          break;
        case OpTypes::kRead:
          wqe_send = std::make_unique<ibv_send_wr>(verbs_util::CreateReadWr(
              wr_id, sge.get(), /*num_sge=*/1, op->src_addr, target_src_rkey));
          break;
        case OpTypes::kCompSwap:
          if (!attributes.swap.has_value()) {
            return absl::InvalidArgumentError(
                "Cannot post kCompSwap operation without a valid swap value.");
          }
          op->swap = attributes.swap.value();
          if (attributes.compare != std::nullopt) {
            op->compare_add = *attributes.compare;
          } else {
            // When compare is not specified, we use the value of the
            // target address as the "compare" value in the wqe. This
            // guarantees that swap will succeed, modulo any concurrent
            // writes to the target address.
            op->compare_add = *reinterpret_cast<uint64_t*>(op->src_addr);
          }
          wqe_send = std::make_unique<ibv_send_wr>(verbs_util::CreateCompSwapWr(
              wr_id, sge.get(), /*num_sge=*/1, op->src_addr, target_src_rkey,
              op->compare_add, attributes.swap.value()));
          break;
        case OpTypes::kFetchAdd:
          if (!attributes.add.has_value()) {
            return absl::InvalidArgumentError(
                "Cannot post kFetchAdd operation without a valid add value.");
          }
          op->compare_add = attributes.add.value();
          wqe_send = std::make_unique<ibv_send_wr>(verbs_util::CreateFetchAddWr(
              wr_id, sge.get(), /*num_sge=*/1, op->src_addr, target_src_rkey,
              attributes.add.value()));
          break;
        case OpTypes::kSend:
          wqe_send = std::make_unique<ibv_send_wr>(
              verbs_util::CreateSendWr(wr_id, sge.get(), /*num_sge=*/1));
          break;
        default:
          return absl::InternalError(
              absl::StrCat("op_type '", op_type_str, "' not recognized."));
      }

      if (initiator_qp_state->is_rc()) {
        InitializeSrcBuffer(op->src_addr, op->length, op->op_id);
        initiator_qp_state->BatchRcSendWqe(std::move(wqe_send), std::move(sge),
                                           op->op_id);
      } else {
        if (!attributes.ud_send_attributes.has_value()) {
          return absl::InvalidArgumentError(
              "Must provide attributes for UD send operations.");
        }
        op->remote_qp = attributes.ud_send_attributes->remote_qp;
        wqe_send->wr.ud.ah = attributes.ud_send_attributes->remote_ah;
        uint32_t remote_qp_num = op->remote_qp->qp()->qp_num;
        wqe_send->wr.ud.remote_qpn = remote_qp_num;
        wqe_send->wr.ud.remote_qkey = kQKey;
        InitializeSrcBuffer(op->src_addr, op->length,
                            attributes.ud_send_attributes->remote_op_id);

        // Post the wqe.
        ibv_send_wr* bad_wr;
        EXPECT_EQ(0, ibv_post_send(initiator_qp_state->qp(), wqe_send.get(),
                                   &bad_wr));
      }
    }

    MaybePrintBuffer(
        absl::StrFormat("client %lu, qp_id %lu, op_id: %lu, src before %s: ",
                        client_id(), op->qp_id, op->op_id, op_type_str),
        op->SrcBuffer());
    MaybePrintBuffer(
        absl::StrFormat("client %lu, qp_id %lu, op_id: %lu, dest before %s: ",
                        client_id(), op->qp_id, op->op_id, op_type_str),
        op->DestBuffer());

    (initiator_qp_state->outstanding_ops())[op->op_id] = std::move(op);
  }

  // Flush all UD ops
  if (attributes.flush || !initiator_qp_state->is_rc()) {
    if (op_type == OpTypes::kRecv) {
      initiator_qp_state->FlushRcRecvWqes();
    } else {
      initiator_qp_state->FlushRcSendWqes();
    }
  }

  return absl::OkStatus();
}

int Client::ExecuteOps(Client& target, const size_t num_qps,
                       const size_t ops_per_qp, const size_t batch_per_qp,
                       const size_t max_inflight_per_qp,
                       const size_t max_inflight_ops_total,
                       const Client::CompletionMethod completion_method) {
  absl::Duration kTimeout = absl::GetFlag(FLAGS_completion_timeout_s);
  absl::Time no_completion_timeout = absl::Now() + kTimeout;

  size_t completions_at_once = max_inflight_ops_total + 1;
  size_t total_expected_ops = ops_per_qp * num_qps;
  absl::Time last_op_time = absl::InfinitePast();

  size_t completed_ops = 0;
  size_t issued_ops = 0;
  absl::flat_hash_map<OpTypes, int> issued_ops_by_type = {};
  size_t inflight_ops = 0;
  uint16_t next_qp_id = 0;
  size_t total_bytes = 0;

  // 'map: [qp_id -> num_ops_completed]' to account for previously finished ops.
  absl::flat_hash_map<int, int> previously_completed_ops;
  for (auto const& [qp_id, qp] : qps_) {
    previously_completed_ops[qp->qp_id()] = qp->TotalOpsCompleted();
  }

  // Lambda function that polls and validates completions depending on op_type.
  auto poll_completions_and_validate_them =
      [this, &completed_ops, &no_completion_timeout, &inflight_ops, &target,
       completions_at_once, kTimeout, completion_method]() {
        int recv_completions;
        int send_completions;
        switch (completion_method) {
          case Client::CompletionMethod::kPolling:
            recv_completions =
                target.TryPollRecvCompletions(completions_at_once);
            send_completions = TryPollSendCompletions(completions_at_once);
            break;
          case Client::CompletionMethod::kEventDrivenBlocking:
          case Client::CompletionMethod::kEventDrivenNonBlocking:
            recv_completions = target.TryPollRecvCompletionsEventDriven();
            send_completions = TryPollSendCompletionsEventDriven();
            break;
        }
        int validated = ValidateOrDeferCompletions();

        if (send_completions || recv_completions || validated) {
          LOG(INFO) << "Completions Fetched/Validated "
                  << send_completions + recv_completions << "/" << validated;
        }
        if (validated > 0) {
          completed_ops += validated;
          inflight_ops -= validated;
          no_completion_timeout = absl::Now() + kTimeout;
        }
      };

  // Lambda function that selects a qp to issue an op on. If inflight_ops is
  // less than max and we haven't issued all ops yet, then issue a new op.
  // Selects the qps in round robin fashion.
  auto maybe_issue_next_op = [this, &target, total_expected_ops,
                              max_inflight_ops_total, ops_per_qp, batch_per_qp,
                              max_inflight_per_qp, num_qps, &next_qp_id,
                              &inflight_ops, &issued_ops, &issued_ops_by_type,
                              &total_bytes, &previously_completed_ops,
                              &last_op_time](const absl::Time now) {
    if (now - last_op_time < absl::GetFlag(FLAGS_inter_op_delay_us)) return;

    if (issued_ops >= total_expected_ops ||
        inflight_ops >= max_inflight_ops_total)
      return;

    auto qp_new_ops = [&](const std::unique_ptr<QpState>& qp_state) {
      return qp_state->TotalOpsCompleted() +
             qp_state->unchecked_initiated_ops().size() +
             qp_state->outstanding_ops_count() -
             previously_completed_ops[qp_state->qp_id()];
    };

    // Find the next qp that has not ops_per_qp posted on.
    auto next_qp_id_old = next_qp_id;
    while (true) {
      std::unique_ptr<QpState>& qp_state = qps_[next_qp_id];
      uint64_t new_ops_issued = qp_new_ops(qp_state);
      if (new_ops_issued < ops_per_qp &&
          qp_state->outstanding_ops_count() < max_inflight_per_qp) {
        LOG(INFO) << "Selected qp  " << qp_state->qp_id()
                << " to issue new op(s) on, with " << new_ops_issued
                << " ops on it.";
        break;
      }

      next_qp_id = (next_qp_id + 1) % num_qps;
      if (next_qp_id == next_qp_id_old) {
        LOG(INFO) << "Searched over all qps. Not issuing a new op.";
        return;
      }
    }

    std::unique_ptr<QpState>& qp_state = qps_[next_qp_id];
    // Calculate how many ops we can post on the selected qp. This is the min of
    // 1. Number of ops required to complete a batch of send WQEs.
    // 2. Number of ops required to reach max inflight per qp.
    // 3. Number of ops required to reach total ops to be posted per qp.
    // 4. Number of ops required to reach total max inflight ops across all qps.
    int ops_to_batch = batch_per_qp - qp_state->SendRcBatchCount();
    int ops_to_max_inflight_per_qp = max_inflight_per_qp -
                                     qp_state->outstanding_ops_count() -
                                     qp_state->unchecked_initiated_ops().size();
    int ops_to_total_ops_per_qp = ops_per_qp - qp_new_ops(qp_state);
    int ops_to_total_inflight = max_inflight_ops_total - inflight_ops;

    int ops_to_post =
        std::min({ops_to_batch, ops_to_max_inflight_per_qp,
                  ops_to_total_ops_per_qp, ops_to_total_inflight});
    if (ops_to_post <= 0) {
      return;
    }

    for (size_t i = 0; i < ops_to_post; ++i) {
      // Determine the properties of the next operation
      const OperationGenerator::OpAttributes op_attributes =
          qp_state->op_generator()->NextOp();
      const OpTypes op_type = op_attributes.op_type;
      const int op_size_bytes = op_attributes.op_size_bytes;
      Client::OpAttributes initiator_attributes = {
          .op_type = op_type,
          .op_bytes = op_size_bytes,
          .num_ops = 1,
          .initiator_qp_id = next_qp_id,
          .flush = false};

      switch (op_type) {
        case OpTypes::kWrite:
        case OpTypes::kRead:
          ASSERT_OK(PostOps(initiator_attributes));
          break;
        case OpTypes::kFetchAdd:
          initiator_attributes.op_bytes = TestOp::kAtomicWordSize;
          initiator_attributes.add = i;
          ASSERT_OK(PostOps(initiator_attributes));
          break;
        case OpTypes::kCompSwap:
          initiator_attributes.op_bytes = TestOp::kAtomicWordSize;
          initiator_attributes.compare = {};
          initiator_attributes.swap = i;
          ASSERT_OK(PostOps(initiator_attributes));
          break;
        case OpTypes::kSend: {
          if (qp_state->is_rc()) {
            uint32_t remote_qp_id = qp_state->remote_qp_state()->qp_id();
            const Client::OpAttributes target_attributes = {
                .op_type = OpTypes::kRecv,
                .op_bytes = op_attributes.op_size_bytes,
                .num_ops = 1,
                .initiator_qp_id = remote_qp_id,
                .flush = false};
            ASSERT_OK(target.PostOps(target_attributes));
            ASSERT_OK(PostOps(initiator_attributes));
          } else {
            QpState::UdDestination remote_qp =
                qp_state->random_ud_destination();
            // Because UD ops are unordered, all UD receives must have space
            // for the largest ops.
            Client::OpAttributes target_attributes = {
                .op_type = OpTypes::kRecv,
                .op_bytes = qp_state->op_generator()->MaxOpSize(),
                .num_ops = 1,
                .initiator_qp_id = remote_qp.qp_state->qp_id()};
            ASSERT_OK(target.PostOps(target_attributes));
            initiator_attributes.ud_send_attributes = {
                .remote_qp = remote_qp.qp_state,
                .remote_op_id = remote_qp.qp_state->GetLastOpId(),
                .remote_ah = remote_qp.ah};
            ASSERT_OK(PostOps(initiator_attributes));
          }
          break;
        }

        default:
          LOG(ERROR) << "Unsupported op type: " << TestOp::ToString(op_type);
      }

      issued_ops_by_type[op_type] += 1;
      total_bytes += op_size_bytes;
    }

    inflight_ops += ops_to_post;
    issued_ops += ops_to_post;
    LOG(INFO) << "Client " << client_id() << ": Batched " << ops_to_post
            << " new ops. All issued ops: " << issued_ops
            << ", Total outstanding_ops_count: " << inflight_ops;

    if (qp_state->is_rc() && (qp_state->SendRcBatchCount() >= batch_per_qp ||
                              qp_new_ops(qp_state) >= ops_per_qp)) {
      target.qp_state(next_qp_id)->FlushRcRecvWqes();
      qp_state->FlushRcSendWqes();
    }

    last_op_time = absl::Now();
    next_qp_id = (next_qp_id + 1) % num_qps;
    LOG_IF(INFO, issued_ops == total_expected_ops)
        << "Posted all requested OPs.";
  };

  LOG(INFO) << "Client " << client_id() << ": Issue " << ops_per_qp
            << " ops per qp.";

  PrepareSendCompletionChannel(completion_method);
  target.PrepareRecvCompletionChannel(completion_method);

  // Experiment run loop.
  while (true) {
    // Fetch and validate available completions.
    poll_completions_and_validate_them();
    absl::Time now = absl::Now();
    // End the experiment if all ops are issued and completed or haven't
    // received new completions for over `kTimeout` since the last one.
    if (now > no_completion_timeout || completed_ops >= total_expected_ops) {
      break;
    }

    // Issue the next op if possible.
    maybe_issue_next_op(now);
  }

  LOG(INFO) << "Client " << client_id() << ": Asked for " << ops_per_qp
            << " ops per qp, on " << num_qps
            << " qps, total of : " << ops_per_qp * num_qps << "ops, totalling "
            << total_bytes << " bytes. Issued " << issued_ops << ". Completed "
            << completed_ops << ".";

  // Log the number of issued operations of each type.
  for (auto& elem : issued_ops_by_type) {
    LOG(INFO) << "Issued " << elem.second << " " << TestOp::ToString(elem.first)
              << " operations.";
  }

  return completed_ops;
}

int Client::TryPollSendCompletions(int count) {
  return TryPollCompletions(count, send_cq_);
}

int Client::TryPollRecvCompletions(int count) {
  return TryPollCompletions(count, recv_cq_);
}

int Client::TryPollCompletions(int count, ibv_cq* cq) {
  std::vector<ibv_wc> completions(count);
  int returned = ibv_poll_cq(cq, count, completions.data());
  CHECK_LE(returned, count);  // Crash OK
  if (returned == 0) return 0;

  if (returned < 0) {
    LOG(ERROR) << "Client " << client_id()
               << ": ERROR polling completion queue!";
    return 0;
  }

  int num_completed = 0;
  for (int i = 0; i < returned; ++i) {
    if (StoreCompletion(&completions[i])) {
      ++num_completed;
    } else {
      // If completion fails, check if we have async events and ack them to move
      // forward.
      // TODO(author5): Ideally we should handle AE in a separate thread
      // concurrently.
      HandleAsyncEvents();
    }
  }
  total_completions_ += num_completed;
  LOG_EVERY_N(INFO, 10) << "Client " << client_id() << ": Received "
                        << total_completions_ << " completions.";
  return num_completed;
}
absl::StatusOr<int> Client::PollSendCompletions(
    int count, absl::Duration timeout_duration) {
  return PollCompletions(count, send_cq_, timeout_duration);
}

absl::StatusOr<int> Client::PollRecvCompletions(
    int count, absl::Duration timeout_duration) {
  return PollCompletions(count, recv_cq_, timeout_duration);
}

absl::StatusOr<int> Client::PollCompletions(int count, ibv_cq* cq,
                                            absl::Duration timeout_duration) {
  absl::Time timeout = absl::Now() + timeout_duration;
  int num_completed = 0;
  while (true) {
    int num_polled = TryPollCompletions(count - num_completed, cq);
    num_completed += num_polled;
    if (num_completed == count) {
      break;
    }

    absl::Time now = absl::Now();
    if (num_polled > 0) {
      timeout = now + timeout_duration;
    }

    if (now >= timeout) {
      LOG(INFO) << "Client " << client_id() << ": Expected " << count
              << " completions, polled " << num_completed << " completions.";
      return absl::DeadlineExceededError("Timeout when polling completions.");
    }
    absl::SleepFor(absl::Milliseconds(10));
  }
  return num_completed;
}

void Client::PrepareSendCompletionChannel(CompletionMethod method) {
  PrepareCompletionChannel(method, send_cq_, send_epoll_fd_);
}
void Client::PrepareRecvCompletionChannel(CompletionMethod method) {
  PrepareCompletionChannel(method, recv_cq_, recv_epoll_fd_);
}
void Client::PrepareCompletionChannel(CompletionMethod method, ibv_cq* cq,
                                      std::optional<const int>& epoll_fd) {
  // Make sure that the file descriptor is in the correct blocking/non-blocking
  // mode.
  switch (method) {
    case CompletionMethod::kPolling:
      // There if nothing to prepare if not using event driven completions.
      return;
    case CompletionMethod::kEventDrivenBlocking:
      MakeCompletionChannelBlocking(cq);
      break;
    case CompletionMethod::kEventDrivenNonBlocking:
      MakeCompletionChannelNonBlocking(cq);
      break;
  }

  // Create an epoll instance for the completion channel, if one doesn't already
  // exist.
  if (!epoll_fd.has_value()) {
    epoll_fd.emplace(TEMP_FAILURE_RETRY(epoll_create1(0)));
    CHECK_NE(*epoll_fd, -1);  // Crash OK
    struct epoll_event event;
    event.events = EPOLLIN;
    CHECK_EQ(TEMP_FAILURE_RETRY(  // Crash OK
                 epoll_ctl(*epoll_fd, EPOLL_CTL_ADD, cq->channel->fd, &event)),
             0);
  }

  // Register for notifications.
  EXPECT_EQ(ibv_req_notify_cq(cq, /*solicited_only=*/0), 0);
}

void Client::MakeCompletionChannelNonBlocking(ibv_cq* cq) {
  int flags = TEMP_FAILURE_RETRY(fcntl(cq->channel->fd, F_GETFL));
  if ((flags & O_NONBLOCK) == O_NONBLOCK) {  // No-op if already non-blocking.
    return;
  }
  CHECK_EQ(
      TEMP_FAILURE_RETRY(fcntl(cq->channel->fd, F_SETFL, flags | O_NONBLOCK)),
      0);  // Crash OK
}

void Client::MakeCompletionChannelBlocking(ibv_cq* cq) {
  int flags = TEMP_FAILURE_RETRY(fcntl(cq->channel->fd, F_GETFL));
  if ((flags & O_NONBLOCK) == 0) {  // No-op if already blocking.
    return;
  }
  CHECK_EQ(
      TEMP_FAILURE_RETRY(fcntl(cq->channel->fd, F_SETFL, flags ^ O_NONBLOCK)),
      0);  // Crash OK
}

int Client::TryPollSendCompletionsEventDriven() {
  CHECK(send_epoll_fd_.has_value());  // Crash OK
  return TryPollCompletionsEventDriven(*send_epoll_fd_, send_cq_);
}

int Client::TryPollRecvCompletionsEventDriven() {
  CHECK(recv_epoll_fd_.has_value());  // Crash OK
  return TryPollCompletionsEventDriven(*recv_epoll_fd_, recv_cq_);
}

int Client::TryPollCompletionsEventDriven(const int epoll_fd, ibv_cq* cq) {
  int num_completed = 0;

  // Poll for new CQ events.
  int ms_timeout = 0;  // Don't block
  void* cq_ctx = nullptr;
  struct epoll_event event;
  int pollret = TEMP_FAILURE_RETRY(
      epoll_wait(epoll_fd, &event, /*maxevents=*/1, ms_timeout));
  if (pollret <= 0) {
    // res will be 0 when there are no new completions.
    // Check if poll return an error.
    if (pollret < 0) {
      LOG(WARNING) << "Client " << client_id()
                   << ": Polling event driven completions failed with errno "
                   << errno << ": " << std::strerror(errno);
    }
    return 0;
  }

  EXPECT_EQ(ibv_get_cq_event(cq->channel, &cq, &cq_ctx), 0);
  ibv_ack_cq_events(cq, 1);
  EXPECT_EQ(ibv_req_notify_cq(cq, /*solicited_only=*/0), 0);

  while (true) {
    int new_completions = TryPollCompletions(/*count=*/1, cq);
    if (new_completions == 0) break;
    EXPECT_EQ(new_completions, 1);
    num_completed += new_completions;
  }

  return num_completed;
}

int Client::ValidateOrDeferCompletions() {
  int num_validated = 0;
  for (const auto& [qp_id, qp_state] : qps_) {
    auto op_uptr_it = qp_state->unchecked_initiated_ops().begin();
    while (op_uptr_it != qp_state->unchecked_initiated_ops().end()) {
      auto& op_uptr = *op_uptr_it;
      EXPECT_EQ(IBV_WC_SUCCESS, op_uptr->status);
      std::string op_type_str = TestOp::ToString(op_uptr->op_type);
      uint8_t* src_addr = op_uptr->src_addr;
      uint8_t* dest_addr = op_uptr->dest_addr;
      // For two sided ops (ie. kSend, kRecv) we need to check that the
      // completion on the other side has arrived, then we can validate the data
      // landing. The expectation is that all Send/Recv ops on an RC qp
      // arrive in the order they've been issued.
      std::unique_ptr<TestOp> recv_op_ptr;
      if (op_uptr->op_type == OpTypes::kSend) {
        QpOpInterface* target_qp;
        if (qp_state->is_rc()) {
          target_qp = qp_state->remote_qp_state();
        } else {
          // UD qps can have multiple destinations, and the destination will be
          // stored in the TestOp struct.
          target_qp = op_uptr->remote_qp;
        }
        recv_op_ptr = target_qp->TryValidateRecvOp(*op_uptr);
        if (recv_op_ptr == nullptr) {
          // Cannot find the corresponding recv op.
          if (qp_state->is_rc()) {
            break;
          } else {
            ++op_uptr_it;
            continue;
          }
        } else {
          dest_addr = recv_op_ptr->dest_addr;
          if (!qp_state->is_rc()) {
            dest_addr += sizeof(ibv_grh);
          }
        }
      }

      // Print the content of the buffers and validate data landed successfully
      // in the destination buffer.
      MaybePrintBuffer(
          absl::StrFormat(
              "client %lu, qp_id %d, op_id %lu, src after %s: ", client_id(),
              op_uptr->qp_id, op_uptr->op_id, op_type_str),
          op_uptr->SrcBuffer());
      MaybePrintBuffer(
          absl::StrFormat(
              "client %lu, qp_id %d, op_id: %lu, dest after %s: ", client_id(),
              op_uptr->qp_id, op_uptr->op_id, op_type_str),
          op_uptr->DestBuffer());

      if (op_uptr->op_type == OpTypes::kFetchAdd) {
        DCHECK(dest_addr);
        EXPECT_EQ(
            *reinterpret_cast<uint64_t*>(dest_addr) + op_uptr->compare_add,
            *reinterpret_cast<uint64_t*>(src_addr));
      } else if (op_uptr->op_type == OpTypes::kCompSwap) {
        // If "compare" fails, swap fails. The dest address on the initiator
        // always holds the original value of the src address in the target.
        EXPECT_TRUE(
            (op_uptr->compare_add == *reinterpret_cast<uint64_t*>(dest_addr) &&
             op_uptr->swap == *reinterpret_cast<uint64_t*>(src_addr)) ||
            (op_uptr->compare_add != *reinterpret_cast<uint64_t*>(src_addr) &&
             *reinterpret_cast<uint64_t*>(src_addr) ==
                 *reinterpret_cast<uint64_t*>(dest_addr)));
      } else if (qp_state->is_rc()) {
        int buffer_content_compare =
            std::memcmp(src_addr, dest_addr, op_uptr->length);
        EXPECT_EQ(buffer_content_compare, 0);
        if (buffer_content_compare != 0) {
          LOG(INFO) << "Buffer mis-match:";
          LOG(INFO) << absl::StrFormat(
                         "client %lu, qp_id %d, op_id %lu, src after %s: ",
                         client_id(), op_uptr->qp_id, op_uptr->op_id,
                         op_type_str)
                  << " " << op_uptr->SrcBuffer();
          LOG(INFO) << absl::StrFormat(
                         "client %lu, qp_id %d, op_id %lu, dest after %s: ",
                         client_id(), op_uptr->qp_id, op_uptr->op_id,
                         op_type_str)
                  << " " << op_uptr->DestBuffer();
        }
        // This op's buffer was generated with op_id = op_ptr->op_id, but it
        // should match with ops_completed on this qp if the completion order is
        // correct.
        EXPECT_EQ(op_uptr->op_id, qp_state->TotalOpsCompleted());
        EXPECT_OK(
            ValidateDstBuffer(dest_addr, op_uptr->length, op_uptr->op_id));
      }

      // Update qp state after verification of buffers.
      qp_state->IncrCompletedBytes(op_uptr->length, op_uptr->op_type);
      qp_state->IncrCompletedOps(1, op_uptr->op_type);
      ++num_validated;

      op_uptr_it = qp_state->unchecked_initiated_ops().erase(op_uptr_it);
    }
  }
  return num_validated;
}

absl::Status Client::ValidateCompletions(int num_expected) {
  int num_validated = ValidateOrDeferCompletions();
  if (num_validated != num_expected) {
    return absl::DataLossError(
        "Completion validatation failed. Ops' data didn't land successfully!");
  }
  return absl::OkStatus();
}

void Client::CheckAllDataLanded() {
  for (auto& qp : qps_) {
    qp.second->CheckDataLanded();
  }
}

std::vector<ibv_qp*> Client::RetrieveQps() {
  std::vector<ibv_qp*> qps;
  for (auto& qpp : qps_) {
    qps.push_back(qpp.second->qp());
  }
  return qps;
}

int Client::HandleAsyncEvents() {
  pollfd poll_fd{};
  poll_fd.fd = context_->async_fd;
  poll_fd.events = POLLIN;
  int millisec_timeout = 0;
  int ret = TEMP_FAILURE_RETRY(poll(&poll_fd, 1, millisec_timeout));
  if (ret == 0) {
    LOG(INFO) << "No Async Events";
    return 0;
  }

  if (ret < 0) {
    LOG(ERROR) << "poll failed with errno " << errno;
    return ret;
  }

  ibv_async_event event{};
  int num_events = 0;
  while (true) {
    ret = ibv_get_async_event(context_, &event);
    if (ret) {
      return num_events;
    }
    PrintAsyncEvent(context_, &event);
    ibv_ack_async_event(&event);
    ++num_events;
  }
  return num_events;
}

void Client::DumpPendingOps() {
  LOG(INFO) << "Pending ops for client " << client_id() << ":";
  for (const auto& [qp_id, qp] : qps_) {
    LOG(INFO) << "QP id: " << qp->qp_id() << ", #Posted: "
              << qp->outstanding_ops_count() + qp->TotalOpsCompleted()
              << ", #Completed: " << qp->TotalOpsCompleted()
              << ", #Pending: " << qp->outstanding_ops_count();
    for (auto& [op_id, op] : qp->outstanding_ops()) {
      LOG(INFO) << op_id;
    }
  }
}

void Client::InitializeSrcBuffer(uint8_t* src_addr, uint32_t length,
                                 uint64_t id) {
  constexpr int kOpIdGapInBuffer = 32;

  // Fill in the buffer with random bytes.
  std::generate_n(src_addr, length, std::ref(random));

  // We want the generated byte sequence to have op_id embedded into it, but
  // also be distinguishable across MTU boundaries to validate ordering. We do
  // this by writing op id at the first byte, incrementing it and writing it
  // repeatedly at fixed intervals, by default every 32 bytes. Instead of
  // writing the full op id (uint64_t), we write it modulo (UCHAR_MAX) to fit it
  // in a single byte.
  //
  // [ ]................[ ]................[ ]................[ ]........
  //  |                  |                  |                  |
  //  v                  v                  v                  v
  // (op_id)            (op_id + 1)        (op_id + 2)        (op_id + 3)
  //

  for (uint32_t i = 0; i * kOpIdGapInBuffer < length; ++i) {
    src_addr[i * kOpIdGapInBuffer] = (id + i) % UCHAR_MAX;
  }
}

absl::Status Client::ValidateDstBuffer(uint8_t* dst_addr, uint32_t length,
                                       uint64_t id) {
  constexpr uint32_t kOpIdGapInBuffer = 32;
  // Validates the sequence of bytes in the buffer as generated by the
  // InitializeRcSrcBuffer function.
  for (uint32_t i = 0; i * kOpIdGapInBuffer < length; ++i) {
    EXPECT_EQ(dst_addr[i * kOpIdGapInBuffer], (id + i) % UCHAR_MAX);
  }
  LOG(INFO) << "Validation of dst_buffer successful.";
  return absl::OkStatus();
}

bool Client::StoreCompletion(const ibv_wc* completion) {
  if (completion->status != IBV_WC_SUCCESS) {
    LOG(INFO) << "Polled completion with error: ";
    verbs_util::PrintCompletion(*completion);
    return false;
  }
  auto* op = reinterpret_cast<TestOp*>(completion->wr_id);
  op->status = completion->status;
  auto& qp_state = qps_[op->qp_id];
  qp_state->StoreOpForValidation(op);
  return true;
}

void Client::MaybePrintBuffer(absl::string_view prefix_msg,
                              std::string op_buffer) {
  if (!absl::GetFlag(FLAGS_print_op_buffers)) {
    return;
  }
  LOG(INFO) << absl::StrCat(prefix_msg, op_buffer);
}

}  // namespace rdma_unit_test
