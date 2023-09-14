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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_CLIENT_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_CLIENT_H_

#include <stddef.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/rdma_memblock.h"
#include "public/verbs_helper_suite.h"
#include "traffic/op_types.h"
#include "traffic/qp_op_interface.h"
#include "traffic/qp_state.h"

ABSL_DECLARE_FLAG(bool, page_align_buffers);
ABSL_DECLARE_FLAG(absl::Duration, inter_op_delay_us);
ABSL_DECLARE_FLAG(absl::Duration, completion_timeout_s);

namespace rdma_unit_test {

// A client is a collection of qps, each qp associated with two distinct
// ranges of memory buffer for src and dest in write operations. Each client
// is associated with a protection domain (pd). This class is not thread safe.
class Client {
 public:
  // Information necessary to initialize ibverbs resources for a Client.
  struct Config {
    int max_op_size;  // in bytes
    int max_outstanding_ops_per_qp;
    int max_qps;
    int send_cq_size = -1;
    int recv_cq_size = -1;
  };

  // Methods of getting completion entries.
  enum class CompletionMethod {
    kPolling,
    kEventDrivenNonBlocking,
    kEventDrivenBlocking
  };

  // Specifies the attributes required for UD send operations that are not
  // already included in `OpAttributes`.
  struct UdSendAttributes {
    QpOpInterface* remote_qp = nullptr;
    uint64_t remote_op_id = 0;
    ibv_ah* remote_ah = nullptr;
  };

  // Specifies all the required and optional characteristics of a batch of RDMA
  // ops of the same type to be posted to RDMA device.
  struct OpAttributes {
    OpTypes op_type = OpTypes::kInvalid;
    int op_bytes = 0;
    int num_ops = 0;
    uint32_t initiator_qp_id = 0;
    // Used for atomic Fetch&Add op. A value must be provided for kFetchAdd
    // operations.
    std::optional<uint64_t> add = std::nullopt;
    // Used for atomic Compare&Swap op.If "compare" is not specified, the "swap"
    // operation always succeeds (when the RDMA op completes with success
    // status). When a value is not provided, we compare against the value
    // residing in target address.
    std::optional<uint64_t> compare = std::nullopt;
    // Used for atomic Compare&Swap op. A value must be provided for kCompSwap
    // operations.
    std::optional<uint64_t> swap = std::nullopt;
    // When flush is set, the ops will be submitted to h/w. The value of this
    // field is ignored for UD operations, which are always flushed.
    bool flush = true;
    std::optional<UdSendAttributes> ud_send_attributes = std::nullopt;
  };

  // Default constants.
  static constexpr int kQKey = 200;
  static constexpr int kDefaultBuffersPerQp = 4096;

  // Constructs a client associated with the fixture. Sets up all required
  // resources like pd, mr, cq, and src and dest buffers. For src and dest
  // buffers, it allocates `buffer_per_qp` * `max_qps_per_client`, where
  // `buffer_per_qp` is equal to `max_op_size * max_outstanding_ops_per_qp`.
  // This is rounded up to the nearest integer multile of pagesize. The src and
  // dest buffers are divided to pieces per qps (buffer_per_qp) and each
  // qp's src buffer is initialized by the qp id. This facilitates to
  // verify the correct values are written in the dest_buffer_ entries. If
  // send_cq_size is positive, it is used as the minimum allocated send
  // completion queue size capped by dev_attr.max_cqe. If recv_cq_size is
  // positive, it is used as the minimum allocated receive completion queue size
  // capped by dev_attr.max_cqe.
  // Note: It is the responsibility of the caller ot guarantee uniqueness of
  // client_id.
  Client(int client_id, ibv_context* context, PortAttribute port_attr,
         Config config);
  ~Client();
  Client(const Client& other) = delete;
  Client& operator=(const Client& other) = delete;
  Client(Client&& other) = default;
  Client& operator=(Client&& other) = default;

  QpState* qp_state(uint32_t qp_id) const {
    auto iter = qps_.find(qp_id);
    if (iter != qps_.end()) {
      return iter->second.get();
    }
    return nullptr;
  }
  size_t num_qps() const { return qps_.size(); }
  ibv_pd* pd() const { return pd_; }
  int client_id() const { return client_id_; }

  // Constructs a qp for this client and returns its qp_id. The qp will have
  // FLAGS_buffer_per_qp of src and dest buffer to send/accept data to/from
  // other qps. 'is_rc' specifies whether the qps are in rc mode or ud mode.
  // Returns OutOfRangeError if creating the qp goes over max_qps_per_client.
  absl::StatusOr<uint32_t> CreateQp(
      bool is_rc, QpInitAttribute qp_init_attribute = QpInitAttribute());
  absl::Status DeleteQp(uint32_t qp_id);

  // Create an address handle and store it in the Client. Since the RDMA unit
  // test framework currently only supports loopback traffic, `port_attr`
  // represents both source and destination port attributes.
  ibv_ah* CreateAh(PortAttribute port_attr);

  // Add an externally created address handle used to the Client to be used
  // (for UD traffic). The AH is not created by VerbsHelperSuite thus is not
  // owned by anyone prior.
  void AddAh(ibv_ah* ah);

  // Creates num_ops WQEs according to the attributes provided, and saves them
  // in the QpState. Buffers for initiated operations are from distinct
  // non-overlapping ranges of the queue pair's src_buffer_. Data from
  // non-send/recv operations is destined and written to distinct
  // non-overlapping ranges on the dest_buffer_ of the destination queue pair.
  // Buffers for recv operations are also from the queue pair's dest_buffer_.
  // For each submitted op, also creates a TestOp object and adds it to the
  // qp's set of inflight ops (ie. outstanding_ops_).
  // Only send/recv op types are supported for UD queue pairs. For UD send
  // operations, attributes.ud_send_attributes must be populated.
  // Return error if qp with attributes.initiator_qp_id isn't
  // constructed or 'op_bytes * num_ops' exceeds the capacity of src_buffer_
  // (ie. FLAGS_buffer_per_qp).
  absl::Status PostOps(const OpAttributes& attributes);

  // Issues a pre-specified number of ops on num_qps of qps, round-robin'ing
  // between qps to issue the ops. If ops don't complete in a timely manner, the
  // function eventually times out and returns the number of ops completed. To
  // avoid a deadlock when `batch_per_qp > 1`, make sure that
  // batch_per_qp * num_qps >= max_inflight_ops_total.
  int ExecuteOps(Client& target, size_t num_qps, size_t ops_per_qp,
                 size_t batch_per_qp, size_t max_inflight_per_qp,
                 size_t max_inflight_ops_total,
                 Client::CompletionMethod completion_method =
                     Client::CompletionMethod::kPolling);

  // Tries poll count completions, returns a lower number if fewer completions
  // are available. Also see TryPollCompletions() below.
  int TryPollSendCompletions(int count);
  int TryPollRecvCompletions(int count);
  // Tries to poll count completions on a completion queue of this client, polls
  // fewer if fewer completions are available. Returns the number of completed
  // operations.
  int TryPollCompletions(int count, ibv_cq* cq);

  // Poll count completions or fail. Also see PollCompletions() below.
  absl::StatusOr<int> PollSendCompletions(
      int count, absl::Duration timeout_duration = absl::Seconds(30));
  absl::StatusOr<int> PollRecvCompletions(
      int count, absl::Duration timeout_duration = absl::Seconds(30));
  // Tries to poll count completions on cq. Will keep polling until count
  // numbers of completions are collected, or return error on timeout. Completed
  // ops are stored internally.
  absl::StatusOr<int> PollCompletions(int count, ibv_cq* cq,
                                      absl::Duration timeout_duration);

  // Gets a completion channel ready to receive operation completions. Sets the
  // flags of the file descriptor according to the desired method, makes sure
  // that there is an epoll instance created to listen for events on the
  // completion channel, and requests completion notifications.
  void PrepareSendCompletionChannel(CompletionMethod method);
  void PrepareRecvCompletionChannel(CompletionMethod method);
  void PrepareCompletionChannel(CompletionMethod method, ibv_cq* cq,
                                std::optional<const int>& epoll_fd);

  // Sets the file descriptor for the completion channel for the cq to
  // blocking mode.
  void MakeCompletionChannelBlocking(ibv_cq* cq);
  // Sets the file descriptor for the completion channel for the cq to
  // non-blocking mode.
  void MakeCompletionChannelNonBlocking(ibv_cq* cq);

  // Same as `TryPollCompletions`, but uses completion event notifications.
  // Must be preceded by a call to `PrepareCompletionChannel`. Returns the
  // number of completed operations. We must poll all completed operations, as
  // there will not be a new notification for them.
  int TryPollSendCompletionsEventDriven();
  int TryPollRecvCompletionsEventDriven();
  int TryPollCompletionsEventDriven(int epoll_fd, ibv_cq* cq);

  // For RDMA RC ops (Read/Write/Send/Recv), this function tries to verify that
  // ops completed successfully; ie. data landed correctly from the src_buffer
  // of the op to the destination buffer.
  // Returns the number of completions that were actually validated.
  // For one-sided RC Read/Write completions, this function is expected to
  // validate them all at once (prefer to use ValidateCompletions() for these
  // ops). However, when this function is called on two-sided RC Send/Recv
  // completions, it tries to validate them in the order ops were issued on the
  // qp, but for completions that it can't validate, stores them internally
  // for future validation. That's due to the asynchronous nature of two-sided
  // ops when the Recv/Send completions on the other side may not have arrived
  // yet. A successful validation of a completion, removes the completed TestOp
  // from QP's outstanding_ops map (if user doesn't call this function or
  // ValidateCompletions(), qp's outstanding_ops map may grow boundlessly).
  // Send/Recv ops must be validated on the sender.
  int ValidateOrDeferCompletions();

  // Verifies that RDMA RC ops (Read/Write/Send/Recv) correctly completed;
  // ie. src_buffer_ of src_qp and dest_buffer_ of dest_qp contain the same
  // values. Input argument is the number of completions expected, returned by
  // PollSendCompletions, PollRecvCompletions, TryPollSendCompletions,
  // TryPollRecvCompletions. Prefer calling this function for RC Write/Read ops;
  // for RC Send/Recv ops, prefer calling ValidateOrDeferCompletions since the
  // Send and Recv completions arrive asynchronously. Send/Recv ops must be
  // validated on the sender.
  absl::Status ValidateCompletions(int num_expected);

  // This function should be called at the end of the test, after
  // PollSendCompletions and ValidateCompletions calls; by then test has
  // already failed or succeeded. Under common case, by the end of the test, all
  // operations completed, all data landed, and data landing is validated with
  // those two functions, the call to this function then simply returns without
  // any stdout output. Under rare cases where some completions don't arrive, a
  // call to this function checks if data landed for un-completed ops (ie.
  // outstanding_ops) on all qps of this client. The purpose is to test that
  // for the operations that we didn't receive completion, did data landed
  // successfully or not (in some cases the data may land but completions are
  // not delivered to the software). This function prints out which un-completed
  // op has landed data or not.
  void CheckAllDataLanded();

  std::vector<ibv_qp*> RetrieveQps();

  // Fetches, prints and acks any asynchronous events (AEs) on the client. Does
  // not wait or poll for an AE, but simply handles and returns existing AEs.
  // Returns a negative value if polling for events failed.
  // Returns 0 if there are no error events.
  // Returns a positive number indicating the number of AEs acked.
  int HandleAsyncEvents();

  // Prints number of pending ops on all QPs for this client.
  void DumpPendingOps();

  RdmaMemBlock GetQpSrcBuffer(uint32_t qp_id) {
    return src_buffer_->subblock(buffer_per_qp_ * qp_id, buffer_per_qp_);
  }

  RdmaMemBlock GetQpDestBuffer(uint32_t qp_id) {
    return dest_buffer_->subblock(buffer_per_qp_ * qp_id, buffer_per_qp_);
  }

  ibv_context* GetContext() { return context_; }

 protected:
  inline void InitializeSrcBuffer(uint8_t* src_addr, uint32_t length,
                                  uint64_t id);
  inline absl::Status ValidateDstBuffer(uint8_t* dst_addr, uint32_t length,
                                        uint64_t id);

  // Stores the TestOp associated with the completion in
  // `unchecked_received_ops` if it was a Recv op, or `unchecked_initiated_ops`
  // otherwise. Returns true if the completions was successful, false if it was
  // not.
  bool StoreCompletion(const ibv_wc* completion);

  VerbsHelperSuite ibv_;
  ibv_context* const context_;
  ibv_pd* const pd_;
  const PortAttribute port_attr_;
  std::unique_ptr<RdmaMemBlock> src_buffer_;
  std::unique_ptr<RdmaMemBlock> dest_buffer_;
  std::vector<ibv_mr*> src_mr_;
  std::vector<ibv_mr*> dest_mr_;
  ibv_comp_channel* send_cc_ = nullptr;
  ibv_comp_channel* recv_cc_ = nullptr;
  ibv_cq* send_cq_ = nullptr;
  ibv_cq* recv_cq_ = nullptr;
  int total_completions_ = 0;
  absl::flat_hash_map<uint32_t, std::unique_ptr<QpState>> qps_;
  std::vector<ibv_ah*> ahs_;
  const int client_id_ = 0;
  const int max_outstanding_ops_per_qp_;
  const int buffer_per_qp_;
  const size_t max_qps_;

  // The file descriptor corresponding to an epoll instances for a completion
  // channel. Will be initialized in `PrepareCompletionChannel`.
  std::optional<const int> send_epoll_fd_;
  std::optional<const int> recv_epoll_fd_;

 private:
  // Print buffers content if the flag print_op_buffers is true.
  static void MaybePrintBuffer(absl::string_view prefix_msg,
                               std::string op_buffer);
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_CLIENT_H_
