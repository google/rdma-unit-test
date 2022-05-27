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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <list>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/attributes.h"
#include "absl/container/fixed_array.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/introspection.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/rdma_verbs_fixture.h"

#define ASSERT_NOT_NULL(p) ASSERT_TRUE((p) != nullptr)
#define CHECK_NOT_NULL(p) CHECK((p) != nullptr)

namespace rdma_unit_test {
namespace {

// Local function to simplify initialization of const member attributes.
template <typename T>
T DieIfNull(T&& t) {
  if (t == nullptr) {
    CHECK(false);
  }
  return std::forward<T>(t);
}

using ::testing::NotNull;

}  // namespace

// Tracks usage of a buffer.
// Clients can Reserve and Return segments of the buffer. BufferTracker handles
// finding non-overlapping regions of the buffer to fulfill Reservations.
// Note: BufferTracker does not own the buffer.
class BufferTracker {
 public:
  // buffer is the buffer to track.
  // max_entries is the maximum number of reservations at once.
  BufferTracker(absl::Span<uint8_t> buffer, uint32_t max_entries)
      : buffer_size_(buffer.length()),
        max_entries_(max_entries),
        base_(buffer.data()) {}

  // Returns a buffer pointer to the start of the reservation.
  // Returns nullopt if the requested reservation could not be fulfilled.
  // count is the number bytes requested.
  absl::StatusOr<uint8_t*> ReserveBytes(uint64_t count) {
    if (allocations_.size() >= max_entries_) {
      return absl::ResourceExhaustedError(absl::StrFormat(
          "Reservation failed due to exceeding max allocation entries (%d).",
          max_entries_));
    }
    if (count > buffer_size_) {
      return absl::ResourceExhaustedError(absl::StrFormat(
          "Reservation failed due to exceeding max buffer size (%d > %d).",
          count, buffer_size_));
    }
    if (allocations_.empty()) {
      Allocation alloc = {.start = 0, .length = count};
      allocations_.emplace_back(alloc);
      return base_;
    }
    // Occupied range.
    uint64_t start = allocations_.front().start;
    uint64_t end = allocations_.back().start + allocations_.back().length;

    // xxxx__________xx
    //     ^ end     ^ start
    if (end < start) {
      if (count > start - end) {
        return absl::ResourceExhaustedError(
            "Failed to find space for reservation.");
      }
      Allocation alloc = {.start = end, .length = count};
      allocations_.emplace_back(alloc);
      return base_ + end;
    }

    // ___xxxxxxxxxx______
    //    ^ start   ^ end

    // First check if there is enough space in tail.
    if (count <= buffer_size_ - end) {
      Allocation alloc = {.start = end, .length = count};
      allocations_.emplace_back(alloc);
      return base_ + end;
    }
    // Otherwise place in front.
    if (count <= start) {
      Allocation alloc = {.start = 0, .length = count};
      allocations_.emplace_back(alloc);
      return base_;
    }
    // Out of space.
    return absl::ResourceExhaustedError(
        "Failed to find space for reservation.");
  }

  // Invalidates the oldest reservation and returns its contents the pool.
  // Reservations are FIFO> So the caller need not specify which should be
  // returned.
  // The return value of the call is the geometry of the reservation.
  absl::Span<uint8_t> ReturnReservation() {
    DCHECK(!allocations_.empty());
    Allocation result = allocations_.front();
    allocations_.pop_front();
    return absl::MakeSpan(base_ + result.start, result.length);
  }

 private:
  // Internal tracking structure for given out reservations.
  struct Allocation {
    // Represents the byte offset from the start of the buffer.
    uint64_t start;
    uint64_t length;
  };

  const uint64_t buffer_size_;
  const uint32_t max_entries_;
  uint8_t* const base_;
  std::list<Allocation> allocations_;
};

// A class encapsulating a RPC control path.
// The core functionality is to post recv buffers and send messages to the other
// side.
// Note: RpcControl is meant as a testing helper. Pretty much any unexpected
// behavior will result in a Check failure.
class RpcControl {
 public:
  // RpcControl takes a number of ibv objects in its constructor to allow
  // sharing with the data path. The QPs cannot be shared. RpcControl assumes
  // that all activity on its QP is control related.
  // |control_pages| is the number of memory pages to create for sending and
  // receiving payloads. Space is split 50/50 between send and recv.
  // |max_outstanding| is the maximum number of messages outstanding at a time.
  RpcControl(VerbsHelperSuite& ibv, ibv_context* context, ibv_pd* pd,
             int control_pages, int max_outstanding)
      : buffer_(ibv.AllocBuffer(control_pages)),
        cq_(DieIfNull(ibv.CreateCq(context, max_outstanding * 2))),
        qp_(DieIfNull(ibv.CreateQp(pd, cq_, IBV_QPT_RC,
                                   QpInitAttribute()
                                       .set_max_send_wr(max_outstanding)
                                       .set_max_recv_wr(max_outstanding)))),
        mr_(DieIfNull(ibv.RegMr(pd, buffer_))),
        send_tracker_(buffer_.subspan(0, buffer_.span().length() / 2),
                      max_outstanding),
        recv_tracker_(buffer_.subspan(buffer_.span().length() / 2),
                      max_outstanding) {
    // RPC is potentially a send and a response, so 2x the cq slots for
    // max_outstanding.
  }

  void Init(VerbsHelperSuite& ibv, RpcControl& other) {
    ASSERT_OK(ibv.ModifyRcQpResetToRts(
        qp_, ibv.GetPortAttribute(qp_->context),
        ibv.GetPortAttribute(other.qp_->context).gid, other.qp_->qp_num));
    other_rkey_ = other.mr_->rkey;
  }

  // Post a recv buffer for a single incoming message of size |size_of_message|.
  void PostRecv(int size_of_message) {
    auto buffer_or = recv_tracker_.ReserveBytes(size_of_message);
    CHECK_OK(buffer_or.status());
    uint8_t* buffer = buffer_or.value();
    ibv_sge sg;
    sg.addr = reinterpret_cast<uint64_t>(buffer);
    sg.length = size_of_message;
    sg.lkey = mr_->lkey;
    ibv_recv_wr recv;
    recv.wr_id = kRecvId;
    recv.next = nullptr;
    recv.sg_list = &sg;
    recv.num_sge = 1;
    ibv_recv_wr* bad_wr;
    ASSERT_EQ(ibv_post_recv(qp_, &recv, &bad_wr), 0);
  }

  // Sends |contents| to the other side.
  void SendMessage(absl::Span<uint8_t> contents) {
    VLOG(1) << "Sending message consuming reservation.";
    auto src_or = send_tracker_.ReserveBytes(contents.size());
    CHECK_OK(src_or.status());
    uint8_t* src = src_or.value();
    memcpy(src, contents.data(), contents.size());
    ibv_sge sg;
    sg.addr = reinterpret_cast<uint64_t>(src);
    sg.length = contents.size();
    sg.lkey = mr_->lkey;
    ibv_send_wr send;
    send.wr_id = kSendId;
    send.next = nullptr;
    send.sg_list = &sg;
    send.num_sge = 1;
    send.opcode = IBV_WR_SEND;
    send.send_flags = IBV_SEND_SIGNALED;
    ibv_send_wr* bad_wr;
    ASSERT_EQ(ibv_post_send(qp_, &send, &bad_wr), 0);
  }

  // Processes control completions.
  // Must be called periodically to return send space.
  // Any send completions mark sends as done.
  // The first recv completion results in returning the contents as a string.
  std::string CheckCompletions(absl::Duration timeout = absl::Seconds(1000)) {
    std::string message;
    absl::Time stop = absl::Now() + timeout;
    do {
      ibv_wc completion;
      int count = ibv_poll_cq(cq_, 1, &completion);
      if (count == 0) {
        absl::SleepFor(absl::Milliseconds(10));
        continue;
      }
      CHECK_EQ(1, count);
      CHECK_EQ(completion.status, IBV_WC_SUCCESS);
      switch (completion.opcode) {
        case IBV_WC_SEND: {
          CHECK_EQ(kSendId, completion.wr_id);
          VLOG(1) << "Got send completion returning reservation.";
          send_tracker_.ReturnReservation();
          break;
        }
        case IBV_WC_RECV: {
          CHECK_EQ(kRecvId, completion.wr_id);
          absl::Span<uint8_t> buf = recv_tracker_.ReturnReservation();
          message =
              std::string(reinterpret_cast<char*>(buf.data()), buf.length());
          break;
        }
        default:
          LOG(FATAL) << "Received unknown completion "
                     << static_cast<uint16_t>(completion.opcode);
      }
    } while (message.empty() && absl::Now() < stop);
    return message;
  }

 private:
  static constexpr uint64_t kRecvId = 10;
  static constexpr uint64_t kSendId = 20;

  // Assumption: incoming and outgoing are the same size so devote 1/2 the
  // buffer to each.
  RdmaMemBlock buffer_;
  ibv_cq* const cq_;
  ibv_qp* const qp_;
  ibv_mr* const mr_;
  uint32_t other_rkey_ = 0;
  BufferTracker send_tracker_;
  BufferTracker recv_tracker_;
};

// Randomize the contents of the buffer based on |seed|.
void RandomizeBuffer(absl::Span<uint8_t> target, uint32_t seed) {
  absl::SeedSeq seq = {seed};
  absl::BitGen bg(seq);
  for (size_t i = 0; i < target.size(); ++i) {
    target[i] = absl::Uniform<uint8_t>(bg);
  }
}

// Validate the contents of the buffer match what is expected for a call to
// RandomizeBuffer with |seed|.
bool ValidateBuffer(absl::Span<const uint8_t> target, uint32_t seed) {
  absl::SeedSeq seq = {seed};
  absl::BitGen bg(seq);
  for (size_t i = 0; i < target.size(); ++i) {
    if (target[i] != absl::Uniform<uint8_t>(bg)) {
      return false;
    }
  }
  return true;
}

enum class Operation { kRead, kWrite };

// A control message for a RPC request.
struct ControlRequest {
  Operation operation;
  uint8_t* addr;
  uint32_t length;
  uint32_t rkey;
  // Either the seed to generate data or confirm data.
  uint32_t seed;
} ABSL_ATTRIBUTE_PACKED;

// A control message for a RPC response.
struct ControlResponse {
  enum class Result { kSuccess, kInvalidWindow };

  Result result;
  // Echo back values so client need not track.
  ControlRequest req;
} ABSL_ATTRIBUTE_PACKED;

// A class to hold shared functionality between client and server.
class RpcBase {
 public:
  // |control_pages| is the number of pages to allocate for control messaging.
  // |data_pages| is the number of pages to allocate for data operations.
  // |max_outstanding| is the maximum number of RPCs allowed at a singel time.
  RpcBase(VerbsHelperSuite& ibv, int control_pages, int data_pages,
          int max_outstanding)
      : data_buffer_(ibv.AllocBuffer(data_pages)),
        data_tracker_(data_buffer_.span(), max_outstanding),
        context_(DieIfNull(ibv.OpenDevice().value())),
        pd_(DieIfNull(ibv.AllocPd(context_))),
        data_cq_(DieIfNull(ibv.CreateCq(context_, max_outstanding))),
        data_qp_(
            DieIfNull(ibv.CreateQp(pd_, data_cq_, IBV_QPT_RC,
                                   QpInitAttribute()
                                       .set_max_send_wr(max_outstanding)
                                       .set_max_recv_wr(max_outstanding)))),
        data_mr_(DieIfNull(ibv.RegMr(pd_, data_buffer_))),
        control_(ibv, context_, pd_, control_pages, max_outstanding) {}

  void Init(VerbsHelperSuite& ibv, RpcBase& other) {
    ASSERT_OK(ibv.ModifyRcQpResetToRts(
        data_qp_, ibv.GetPortAttribute(data_qp_->context),
        ibv.GetPortAttribute(data_qp_->context).gid, other.data_qp_->qp_num));
    control_.Init(ibv, other.control_);
  }

 protected:
  RdmaMemBlock data_buffer_;
  BufferTracker data_tracker_;
  ibv_context* const context_;
  ibv_pd* const pd_;
  ibv_cq* const data_cq_;
  ibv_qp* const data_qp_;
  ibv_mr* const data_mr_;
  RpcControl control_;
};

// Represents the client half of a client/server pair.
// Designed as a simple test driver. Anything unexpected will Check fail.
class RpcClient : public RpcBase {
 public:
  // See RpcBase.
  RpcClient(VerbsHelperSuite& ibv, int control_pages, int data_pages,
            int max_outstanding)
      : RpcBase(ibv, control_pages, data_pages, max_outstanding),
        // Having 1 additional slot in windows allows us to simplify tracking.
        // oldest == next happens when empty, oldest-1 == next happens when
        // full.
        data_windows_(max_outstanding + 1) {
    for (ibv_mw*& mw : data_windows_) {
      mw = ibv.AllocMw(pd_, IBV_MW_TYPE_2);
      CHECK(mw);
    }
  }

  // Pushes a single control buffer for an incoming message.
  void PostControl() { control_.PostRecv(sizeof(ControlResponse)); }

  // Initiates a new RPC operation of |length| bytes.
  // |seed| is used to populate the buffer on writes or to validate the buffer
  // for reads.
  // Note: this synchronously (post+poll) binds a window for data transfer.
  // ibv_mw* is returned to allow the client to cancel the transfer.
  // At the completion of this method a message has been sent to the server.
  // ProcessControl is needed to signal execution.
  ibv_mw* StartRpc(Operation operation, uint64_t length, uint32_t seed) {
    CHECK_NE(AdvanceWindow(next_window_), oldest_window_)
        << "Exceeded max outstanding";
    auto buffer_alloc_or = data_tracker_.ReserveBytes(length);
    CHECK_OK(buffer_alloc_or.status());
    uint8_t* buffer_alloc = buffer_alloc_or.value();
    ibv_mw* mw = data_windows_[next_window_];
    next_window_ = AdvanceWindow(next_window_);
    uint32_t rkey = next_rkey_++;

    // Post bind.
    ibv_send_wr bind;
    bind.wr_id = 1;
    bind.next = nullptr;
    bind.sg_list = nullptr;
    bind.num_sge = 0;
    bind.opcode = IBV_WR_BIND_MW;
    bind.send_flags = IBV_SEND_SIGNALED;
    bind.bind_mw.mw = mw;
    bind.bind_mw.rkey = rkey;
    bind.bind_mw.bind_info.mr = data_mr_;
    bind.bind_mw.bind_info.addr = reinterpret_cast<uint64_t>(buffer_alloc);
    bind.bind_mw.bind_info.length = length;
    switch (operation) {
      case Operation::kRead:
        bind.bind_mw.bind_info.mw_access_flags = IBV_ACCESS_REMOTE_WRITE;
        break;
      case Operation::kWrite:
        bind.bind_mw.bind_info.mw_access_flags = IBV_ACCESS_REMOTE_READ;
        break;
    }
    VLOG(2) << "Posting bind.";
    ibv_send_wr* bad_wr;
    int result = ibv_post_send(data_qp_, &bind, &bad_wr);
    CHECK_EQ(result, 0);

    // Setup data buffer.
    VLOG(2) << "Setting up data buffer.";
    switch (operation) {
      case Operation::kWrite:
        RandomizeBuffer(absl::MakeSpan(buffer_alloc, length), seed);
        break;
      case Operation::kRead:
        memset(buffer_alloc, 0, length);
        break;
    }

    // Wait for bind response.
    // Need to do this before issuing the send to avoid a race between the
    // remote RMA and the bind.
    // Synchronous bind for now.
    VLOG(2) << "Waiting for bind response.";
    ibv_wc completion = verbs_util::WaitForCompletion(data_cq_).value();
    CHECK_EQ(completion.status, IBV_WC_SUCCESS);
    // TODO(b/164096025): Re-enable once Simics is fixed.
    // CHECK_EQ(completion.opcode, IBV_WC_BIND_MW);
    mw->rkey = bind.bind_mw.rkey;

    VLOG(1) << "Client sending request.";
    // Send Control
    ControlRequest request;
    request.operation = operation;
    request.addr = buffer_alloc;
    request.length = length;
    request.rkey = rkey;
    request.seed = seed;
    control_.SendMessage(
        absl::MakeSpan(reinterpret_cast<uint8_t*>(&request), sizeof(request)));
    VLOG(2) << "RPC started.";

    return mw;
  }

  // Cancels an outstanding RPC.
  // Note: this may or may not stop data transfer depending on a race with the
  // server RMA. It may result in a partial transfer.
  // It is not allowed to cancel an already completed RPC.
  void CancelRpc(ibv_mw* mw) {
    ibv_send_wr invalidate;
    invalidate.wr_id = 1;
    invalidate.next = nullptr;
    invalidate.sg_list = nullptr;
    invalidate.num_sge = 0;
    invalidate.opcode = IBV_WR_LOCAL_INV;
    invalidate.send_flags = IBV_SEND_SIGNALED;
    invalidate.invalidate_rkey = mw->rkey;
    ibv_send_wr* bad_wr;
    int result = ibv_post_send(data_qp_, &invalidate, &bad_wr);
    ASSERT_EQ(result, 0);
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(data_cq_));
    ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
    ASSERT_EQ(completion.opcode, IBV_WC_LOCAL_INV);
  }

  // Handles a single control message (potentially validating data), cleans up
  // state for any outgoing messages, and reposts a recv buffer for the incoming
  // message.
  absl::StatusOr<ControlResponse::Result> ProcessControl(
      absl::Duration timeout = absl::Seconds(1000)) {
    std::string payload = control_.CheckCompletions(timeout);
    if (payload.empty()) {
      return absl::NotFoundError("Did not find completion.");
    }
    DCHECK_EQ(payload.size(), sizeof(ControlResponse));
    const auto* resp = reinterpret_cast<const ControlResponse*>(payload.data());

    // Validate that the Window is correct.
    CHECK_EQ(data_windows_[oldest_window_]->rkey, resp->req.rkey);
    if (resp->result == ControlResponse::Result::kSuccess &&
        resp->req.operation == Operation::kRead) {
      CHECK(ValidateBuffer(
          absl::MakeSpan(reinterpret_cast<uint8_t*>(resp->req.addr),
                         resp->req.length),
          resp->req.seed));
    }

    // Update tracking.
    oldest_window_ = AdvanceWindow(oldest_window_);
    absl::Span<uint8_t> buf = data_tracker_.ReturnReservation();
    CHECK_EQ(reinterpret_cast<uint64_t>(resp->req.addr),
             reinterpret_cast<uint64_t>(buf.data()));
    CHECK_EQ(resp->req.length, buf.size());

    // Repost buffer for recv.
    PostControl();
    return resp->result;
  }

 private:
  uint32_t AdvanceWindow(uint32_t target) const {
    ++target;
    return target % data_windows_.size();
  }

  absl::BitGen random_;
  absl::FixedArray<ibv_mw*, 0> data_windows_;
  uint32_t next_rkey_ = absl::Uniform<uint32_t>(random_);
  uint32_t oldest_window_ = 0;
  uint32_t next_window_ = 0;
};

// Represents the server half of a client/server pair.
// Designed as a simple test driver. Anything unexpected will Check fail.
class RpcServer : public RpcBase {
 public:
  // See RpcBase.
  RpcServer(VerbsHelperSuite& ibv, int control_pages, int data_pages,
            int max_outstanding)
      : RpcBase(ibv, control_pages, data_pages, max_outstanding) {}

  // Pushes a single control buffer for an incoming message.
  void PostControl() { control_.PostRecv(sizeof(ControlRequest)); }

  // Handles a single control message (potentially starting RMA), cleans up
  // state for any outgoing messages, and reposts a recv buffer.
  // Returns the request received (if any).
  absl::StatusOr<ControlRequest> ProcessControl(
      absl::Duration timeout = absl::Seconds(1000)) {
    std::string payload = control_.CheckCompletions(timeout);
    if (payload.empty()) {
      return absl::NotFoundError("Did not find completion.");
    }
    DCHECK_EQ(payload.size(), sizeof(ControlRequest));
    const auto* req = reinterpret_cast<const ControlRequest*>(payload.data());

    ASSIGN_OR_RETURN(uint8_t * buffer_alloc,
                     data_tracker_.ReserveBytes(req->length));
    ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(buffer_alloc);
    sge.length = req->length;
    sge.lkey = data_mr_->lkey;

    switch (req->operation) {
      case Operation::kWrite: {
        memset(buffer_alloc, 0, req->length);
        ibv_send_wr read;
        read.wr_id = 1;
        read.next = nullptr;
        read.sg_list = &sge;
        read.num_sge = 1;
        read.opcode = IBV_WR_RDMA_READ;
        read.send_flags = IBV_SEND_SIGNALED;
        read.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(req->addr);
        read.wr.rdma.rkey = req->rkey;
        ibv_send_wr* bad_wr;
        int result = ibv_post_send(data_qp_, &read, &bad_wr);
        CHECK_EQ(result, 0);
      } break;
      case Operation::kRead: {
        RandomizeBuffer(absl::MakeSpan(buffer_alloc, req->length), req->seed);
        ibv_send_wr write;
        write.wr_id = 1;
        write.next = nullptr;
        write.sg_list = &sge;
        write.num_sge = 1;
        write.opcode = IBV_WR_RDMA_WRITE;
        write.send_flags = IBV_SEND_SIGNALED;
        write.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(req->addr);
        write.wr.rdma.rkey = req->rkey;
        ibv_send_wr* bad_wr;
        int result = ibv_post_send(data_qp_, &write, &bad_wr);
        CHECK_EQ(result, 0);
        break;
      }
    }

    // Repost the recv buffer.
    PostControl();
    OutstandingRma tracking = {.local_buffer_start = buffer_alloc, .req = *req};
    outstanding_rmas_.emplace_back(tracking);
    return *req;
  }

  // Handles data completions (potentially validating data and queuing control
  // messages). Returns the result of the data operation.
  absl::StatusOr<ControlResponse::Result> ProcessData(
      absl::Duration timeout = absl::Seconds(1000)) {
    auto completion_or = verbs_util::WaitForCompletion(data_cq_, timeout);
    if (!completion_or.ok()) {
      return absl::NotFoundError("Did not find completion.");
    }
    ibv_wc completion = completion_or.value();
    ControlResponse::Result rma_result =
        completion.status == IBV_WC_SUCCESS
            ? ControlResponse::Result::kSuccess
            : ControlResponse::Result::kInvalidWindow;
    OutstandingRma& outstanding_rma = outstanding_rmas_.front();
    ControlRequest& req = outstanding_rma.req;
    switch (completion.opcode) {
      case IBV_WC_RDMA_WRITE:
        CHECK(Operation::kRead == req.operation);
        break;
      case IBV_WC_RDMA_READ:
        CHECK(Operation::kWrite == req.operation);
        // Validate Writes
        if (completion.status == IBV_WC_SUCCESS) {
          CHECK(ValidateBuffer(
              absl::MakeSpan(reinterpret_cast<uint8_t*>(
                                 outstanding_rma.local_buffer_start),
                             req.length),
              req.seed));
        }
        break;
      default:
        LOG(FATAL) << "Unexpected opcode: " << completion.opcode;
    }

    // Return the buffer.
    absl::Span<uint8_t> buf = data_tracker_.ReturnReservation();
    CHECK_EQ(reinterpret_cast<uint64_t>(outstanding_rma.local_buffer_start),
             reinterpret_cast<uint64_t>(buf.data()));
    CHECK_EQ(req.length, buf.size());

    // Queue ControlResponse
    ControlResponse resp;
    resp.result = rma_result;
    resp.req = req;
    control_.SendMessage(
        absl::MakeSpan(reinterpret_cast<uint8_t*>(&resp), sizeof(resp)));

    // Cleanup tracking
    outstanding_rmas_.pop_front();
    return rma_result;
  }

 private:
  struct OutstandingRma {
    uint8_t* local_buffer_start;
    ControlRequest req;
  };
  std::list<OutstandingRma> outstanding_rmas_;
};

// Set of tests to simulate expected RPC Rendezvous.
// The Rpc client/server strike a balance between resource sharing and
// complexity to somewhat approximate production use.
class RendezvousTest : public RdmaVerbsFixture {
 public:
  static constexpr int kControlPages = 4;
  static constexpr int kRpcSize = kPageSize;
  static constexpr int kDataPages = 101;
  static constexpr int kMaxOutstanding = 100;

  void SetUp() override {
    if (!Introspection().SupportsType2()) {
      GTEST_SKIP() << "Skipping due to lack of Type2 MW support.";
    }
  }

 protected:
  struct BasicSetup {
    RpcClient client;
    RpcServer server;
  };

  BasicSetup CreateBasicSetup() {
    BasicSetup setup{
        .client = RpcClient(ibv_, kControlPages, kDataPages, kMaxOutstanding),
        .server = RpcServer(ibv_, kControlPages, kDataPages, kMaxOutstanding)};
    setup.client.Init(ibv_, setup.server);
    setup.server.Init(ibv_, setup.client);
    return setup;
  }
};

TEST_F(RendezvousTest, Read) {
  BasicSetup setup = CreateBasicSetup();
  setup.client.PostControl();
  setup.server.PostControl();

  // Send request.
  LOG(INFO) << "Sending request.";
  setup.client.StartRpc(Operation::kRead, kRpcSize, 15);

  // Receive request.
  LOG(INFO) << "Waiting for request.";
  ASSERT_OK_AND_ASSIGN(ControlRequest request, setup.server.ProcessControl());
  ASSERT_EQ(Operation::kRead, request.operation);
  ASSERT_EQ(kRpcSize, request.length);

  // Wait for RMA to finish.
  LOG(INFO) << "Waiting for RMA finished.";
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rma_result,
                       setup.server.ProcessData());
  ASSERT_EQ(rma_result, ControlResponse::Result::kSuccess);

  // Wait for finish report.
  LOG(INFO) << "Waiting for response.";
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rpc_result,
                       setup.client.ProcessControl());
  EXPECT_EQ(rpc_result, ControlResponse::Result::kSuccess);
}

TEST_F(RendezvousTest, Batched) {
  BasicSetup setup = CreateBasicSetup();
  for (int i = 0; i < kMaxOutstanding; ++i) {
    setup.client.PostControl();
    setup.server.PostControl();
  }

  static constexpr uint64_t kMaxRpcSize = 10 * 1024;
  static constexpr uint64_t kMaxOutstanding =
      kDataPages * kPageSize / kMaxRpcSize - 2;
  absl::BitGen random;
  uint64_t outstanding = 0;
  uint64_t total_complete = 0;
  while (total_complete < kMaxOutstanding * 10) {
    VLOG(1) << "Sending requests.";
    while (outstanding < kMaxOutstanding) {
      Operation op =
          absl::Bernoulli(random, 0.5) ? Operation::kRead : Operation::kWrite;
      setup.client.StartRpc(op, absl::Uniform<uint32_t>(random, 1, kMaxRpcSize),
                            absl::Uniform<uint32_t>(random));
      ++outstanding;
    }
    VLOG(1) << "Checking for responses.";
    for (int i = 0; i < 10; ++i) {
      auto result = setup.server.ProcessControl(absl::ZeroDuration());
      LOG_IF(WARNING, !result.ok()) << result.status();
      auto result_or = setup.server.ProcessData(absl::ZeroDuration());
      if (result_or.ok()) {
        VLOG(1) << "Got data.";
        ASSERT_EQ(ControlResponse::Result::kSuccess, result_or.value());
      }
      auto rpc_result_or = setup.client.ProcessControl(absl::ZeroDuration());
      if (rpc_result_or.ok()) {
        VLOG(1) << "Finished RPC.";
        EXPECT_EQ(ControlResponse::Result::kSuccess, rpc_result_or.value());
        ++total_complete;
        --outstanding;
      }
    }
    // Wait a little.
    absl::SleepFor(absl::Milliseconds(50));
  }
}

TEST_F(RendezvousTest, ReadCancellation) {
  BasicSetup setup = CreateBasicSetup();
  setup.client.PostControl();
  setup.server.PostControl();

  // Send request.
  ibv_mw* mw = setup.client.StartRpc(Operation::kRead, kRpcSize, 15);

  // Cancel outstanding.
  ASSERT_NO_FATAL_FAILURE(setup.client.CancelRpc(mw));

  // Receive request.
  ASSERT_OK_AND_ASSIGN(ControlRequest request, setup.server.ProcessControl());
  ASSERT_EQ(request.operation, Operation::kRead);
  ASSERT_EQ(request.length, kRpcSize);

  // Wait for RMA to finish.
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rma_result,
                       setup.server.ProcessData());
  ASSERT_EQ(rma_result, ControlResponse::Result::kInvalidWindow);

  // Wait for finish report.
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rpc_result,
                       setup.client.ProcessControl());
  EXPECT_EQ(rpc_result, ControlResponse::Result::kInvalidWindow);
}

TEST_F(RendezvousTest, Write) {
  BasicSetup setup = CreateBasicSetup();
  setup.client.PostControl();
  setup.server.PostControl();

  // Send request.
  setup.client.StartRpc(Operation::kWrite, kRpcSize, 15);

  // Receive request.
  ASSERT_OK_AND_ASSIGN(ControlRequest request, setup.server.ProcessControl());
  ASSERT_EQ(request.operation, Operation::kWrite);
  ASSERT_EQ(request.length, kRpcSize);

  // Wait for RMA to finish.
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rma_result,
                       setup.server.ProcessData());
  ASSERT_EQ(rma_result, ControlResponse::Result::kSuccess);

  // Wait for finish report.
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rpc_result,
                       setup.client.ProcessControl());
  EXPECT_EQ(rpc_result, ControlResponse::Result::kSuccess);
}

TEST_F(RendezvousTest, WriteCancellation) {
  BasicSetup setup = CreateBasicSetup();
  setup.client.PostControl();
  setup.server.PostControl();

  // Send request.
  ibv_mw* mw = setup.client.StartRpc(Operation::kWrite, kRpcSize, 15);

  // Cancel outstanding.
  ASSERT_NO_FATAL_FAILURE(setup.client.CancelRpc(mw));

  // Receive request.
  ASSERT_OK_AND_ASSIGN(ControlRequest request, setup.server.ProcessControl());
  ASSERT_EQ(request.operation, Operation::kWrite);
  ASSERT_EQ(request.length, kRpcSize);

  // Wait for RMA to finish.
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rma_result,
                       setup.server.ProcessData());
  ASSERT_EQ(rma_result, ControlResponse::Result::kInvalidWindow);

  // Wait for finish report.
  ASSERT_OK_AND_ASSIGN(ControlResponse::Result rpc_result,
                       setup.client.ProcessControl());
  EXPECT_EQ(rpc_result, ControlResponse::Result::kInvalidWindow);
}

// Represents a pair of client and server objects.
class RendezvousPair {
 public:
  static constexpr uint64_t kMaxOutstanding = 25;
  static constexpr uint64_t kMaxRpcSize = kPageSize * 2;
  static constexpr uint64_t kControlPages = 4;
  static constexpr uint64_t kDataPages =
      kMaxOutstanding * kMaxRpcSize / kPageSize + kMaxRpcSize;
  explicit RendezvousPair(VerbsHelperSuite& ibv)
      : client_(ibv, kControlPages, kDataPages, kMaxOutstanding),
        server_(ibv, kControlPages, kDataPages, kMaxOutstanding) {
    client_.Init(ibv, server_);
    server_.Init(ibv, client_);
    for (uint64_t i = 0; i < kMaxOutstanding; ++i) {
      client_.PostControl();
      server_.PostControl();
    }
  }

  // Starts 1 thread each for the client and server adding them to |bundle|.
  // The threads execute until |target_rpcs| complete.
  void Start(std::vector<std::thread>& threads, int target_rpcs,
             const absl::Notification& cancelled_notification) {
    threads.push_back(
        std::thread([this, target_rpcs, &cancelled_notification]() {
          ASSERT_NO_FATAL_FAILURE(
              ClientRunloop(target_rpcs, cancelled_notification));
        }));
    threads.push_back(std::thread([this, &cancelled_notification]() {
      ASSERT_NO_FATAL_FAILURE(ServerRunloop(cancelled_notification));
    }));
  }

 private:
  void ClientRunloop(int target_rpcs,
                     const absl::Notification& cancelled_notification) {
    absl::BitGen random;
    struct OutstandingTracking {
      ibv_mw* id;
      bool cancelled;
    };
    std::list<OutstandingTracking> outstanding;
    int total_complete = 0;
    while (!cancelled_notification.HasBeenNotified() &&
           total_complete < target_rpcs) {
      VLOG(1) << "Sending requests.";
      while (outstanding.size() < kMaxOutstanding) {
        Operation op =
            absl::Bernoulli(random, 0.5) ? Operation::kRead : Operation::kWrite;
        ibv_mw* mw = client_.StartRpc(
            op, absl::Uniform<uint32_t>(random, 1, kMaxRpcSize),
            absl::Uniform<uint32_t>(random));
        ASSERT_THAT(mw, NotNull());
        OutstandingTracking entry = {.id = mw, .cancelled = false};
        outstanding.emplace_back(entry);
      }
      VLOG(1) << "Checking for responses.";
      for (uint64_t i = 0; i < kMaxOutstanding; ++i) {
        auto rpc_result_or = client_.ProcessControl(absl::ZeroDuration());
        bool should_cancel = false;
        if (rpc_result_or.ok()) {
          VLOG(1) << "Finished RPC.";
          if (!outstanding.front().cancelled) {
            ASSERT_EQ(rpc_result_or.value(), ControlResponse::Result::kSuccess);
          }
          ++total_complete;
          // Cancel 1 for every 4 which complete.
          should_cancel = total_complete % 4 == 0;
          outstanding.pop_front();
        }
        if (should_cancel && !outstanding.empty()) {
          int index = absl::Uniform<uint32_t>(random, 0, outstanding.size());
          auto iter = outstanding.begin();
          std::advance(iter, index);
          if (!iter->cancelled) {
            ASSERT_NO_FATAL_FAILURE(client_.CancelRpc(iter->id));
            iter->cancelled = true;
          }
        }
      }
      // Wait a little.
      absl::SleepFor(absl::Milliseconds(50));
    }
    finished_.store(true);
  }

  void ServerRunloop(const absl::Notification& cancelled_notification) {
    while (!cancelled_notification.HasBeenNotified() && !finished_.load()) {
      for (int i = 0; i < 10; ++i) {
        auto control_result = server_.ProcessControl(absl::ZeroDuration());
        LOG_IF(WARNING, !control_result.ok()) << control_result.status();
        auto data_result = server_.ProcessData(absl::ZeroDuration());
        LOG_IF(WARNING, !data_result.ok()) << data_result.status();
      }
      // Wait a little.
      absl::SleepFor(absl::Milliseconds(50));
    }
  }

  std::atomic<bool> finished_ = false;
  RpcClient client_;
  RpcServer server_;
};

}  // namespace rdma_unit_test
