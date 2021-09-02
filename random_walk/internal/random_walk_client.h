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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_RANDOM_WALK_CLIENT_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_RANDOM_WALK_CLIENT_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/rdma_memblock.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "random_walk/internal/bind_ops_tracker.h"
#include "random_walk/internal/client_update_service.grpc.pb.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/ibv_resource_manager.h"
#include "random_walk/internal/inbound_update_interface.h"
#include "random_walk/internal/invalidate_ops_tracker.h"
#include "random_walk/internal/logging.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/sampling.h"
#include "random_walk/internal/types.h"
#include "random_walk/internal/update_dispatcher_interface.h"

namespace rdma_unit_test {
namespace random_walk {

// The class that simulates a libibverbs client. It provides methods for
// carrying out random but mostly valid libibverbs commands. It exchanges OOB
// (out-of-band) information with other RandomWalkClients via
// InboundUpdateInterface. In the context of IbVerbs, a RandomWalkClient
// represent a client on a single ibv_context (device).
class RandomWalkClient : public InboundUpdateInterface {
 public:
  // The number of recent Actions recording by RandomWalkLogger.
  static constexpr size_t kLogSize = 200;
  // Controls the probability that a messaging command (send/recv)
  // will be done at a pair of UD QPs (vs RC QPs).
  static constexpr double kMessagingUdProbability = 0.3;
  // Controls the probability that a send op will carry immediate data.
  static constexpr double kSendImmProbability = 0.2;
  // The capacity of the queues storing oustanding inbound/outbound updates.
  static constexpr size_t kMaxOustandingUpdates = 50;

  // ---------------------------------------------------------------------------

  // Initializes the client with necessary attributes.
  // - client_id: the id of client, assigned by the test orchestrator.
  // - action_weights: weight for each action in random walk.
  RandomWalkClient(ClientId client_id, const ActionWeights& action_weights);
  // Movable but not copyable.
  RandomWalkClient(RandomWalkClient&& client) = default;
  RandomWalkClient& operator=(RandomWalkClient&& client) = default;
  RandomWalkClient(const RandomWalkClient& client) = delete;
  RandomWalkClient& operator=(const RandomWalkClient& client) = delete;
  ~RandomWalkClient() = default;

  // ---------------------------------------------------------------------------

  // Registers the a remote RandomWalkClient. The remote RandomWalkClient will
  // be identified by an id and the its gid. After this, the RandomWalkClient
  // will be able to (randomly) interact with the new client in its random walk.
  void AddRemoteClient(ClientId client_id, const ibv_gid& gid);

  // Returns the gid the RandomWalkClient is running on.
  ibv_gid GetGid() const;

  // Registers a dispatcher implementing UpdateDispatcherInterface to the
  // RandomWalkClient.
  void RegisterUpdateDispatcher(
      std::shared_ptr<UpdateDispatcherInterface> dispatcher);

  // Implements InboundUpdateInterface.
  void PushInboundUpdate(const ClientUpdate& update) override;

  // Run the client for a fixed amount of time.
  void Run(absl::Duration duration);

  // Run the client for a fixed amount of random walk steps.
  void Run(size_t steps);

 private:
  using CqInfo = IbvResourceManager::CqInfo;
  using PdInfo = IbvResourceManager::PdInfo;
  using MrInfo = IbvResourceManager::MrInfo;
  using Type1MwBindInfo = IbvResourceManager::Type1MwBindInfo;
  using Type2MwBindInfo = IbvResourceManager::Type2MwBindInfo;
  using RdmaMemory = IbvResourceManager::RdmaMemory;
  using AhInfo = IbvResourceManager::AhInfo;
  using RcQpInfo = IbvResourceManager::RcQpInfo;
  using UdQpInfo = IbvResourceManager::UdQpInfo;
  using RemoteUdQpInfo = IbvResourceManager::RemoteUdQpInfo;

  // Statistics.
  struct Stats {
    // Number of commands (of different types) issued.
    size_t commands = 0;
    size_t create_cq = 0;
    size_t destroy_cq = 0;
    size_t alloc_pd = 0;
    size_t dealloc_pd = 0;
    size_t reg_mr = 0;
    size_t dereg_mr = 0;
    size_t alloc_type_1_mw = 0;
    size_t alloc_type_2_mw = 0;
    size_t dealloc_type_1_mw = 0;
    size_t dealloc_type_2_mw = 0;
    size_t bind_type_1_mw = 0;
    size_t bind_type_2_mw = 0;
    size_t create_rc_qp_pair = 0;
    size_t create_ud_qp = 0;
    size_t modify_qp_error = 0;
    size_t destroy_qp = 0;
    size_t create_ah = 0;
    size_t destroy_ah = 0;
    size_t send = 0;
    size_t send_with_inv = 0;
    size_t recv = 0;
    size_t read = 0;
    size_t write = 0;
    size_t fetch_add = 0;
    size_t comp_swap = 0;
    size_t completions = 0;
    std::array<size_t, 22> completion_statuses = {
        0};  // There are a total of 22 completion
             // statuses in ibverbs, from 0 to 21.
  };

  // Performs some initial commands to bootstrap the random walk. This mostly
  // includes creating the minimum number of MRs, MWs, PDs to start the
  // random walk.
  void BootstrapRandomWalk();
  // Does one step of random walk. This includes:
  // 1. Fetch inbound ClientUpdate from any remote clients.
  // 2. Carry out one random but via Action (see type.h). See each corresponding
  //    method (e.g. TryRegMr) for the set of Action and what each Action does.
  // 3. Fetch any remaining entries from the completion queue.
  absl::Status RandomWalk();
  // The same as RandomWalk, but instead of carrying out random Action, carry
  // out a specific Action.
  absl::Status DoAction(Action action);

  // Print via LOG(INFO) the the running log of the client, which includes:
  // 1. Recent action logs: records the most recent |kLogSize| events (commands
  // and completion) witnessed by the client. See RandomWalkLogger for more
  // details.
  void PrintLogs() const;
  // Prints via LOG(INFO) the running statistics of the client, such as number
  // of (each type of) commands issued.
  void PrintStats() const;

  // Helper function to create a RC QP.
  ibv_qp* CreateLocalRcQp(ClientId peer_id, ibv_pd* pd);

  // Finish the process for creating an interconnected qp pair. This will take
  // in a remote gid and qpn, where the remote (responder) qp is assumed
  // to be in RTS state. Then the function will bring up the local qp from RESET
  // state to RTS state.
  absl::Status ModifyRcQpResetToRts(ibv_qp* local_qp, ibv_gid remote_gid,
                                    uint32_t remote_qpn,
                                    ClientId remote_client_id);

  // Perform actions with specific input. These are usually done when trying to
  // destroy specific resources to clear out refcounts for destroying PDs, MRs
  // and QPs. Function names are self-explanatory.
  absl::StatusCode DeregMr(ibv_mr* mr);
  absl::StatusCode DeallocType1Mw(ibv_mw* mw, bool is_bound);
  absl::StatusCode DeallocType2Mw(ibv_mw* mw, bool is_bound);
  absl::StatusCode DestroyQp(ibv_qp* qp);

  // Performs a viable random action with random input. This function repeatedly
  // calls TryDoRandomAction until one of the runs returns Ok or InternalError,
  // i.e. the action is viable and the command is issued with random and valid
  // inputs.
  absl::Status DoRandomAction();

  // Tries to perform a random action with random input.
  // Returns a StatusCode specified below.
  absl::StatusCode TryDoRandomAction();

  // Tries a specific action with random input. Returns a absl::StatusCode:
  // -- Ok: when the action succeeded.
  // -- FailedPreconditionError: when we cannot create the ops due to the client
  //                             in a wrong state, e.g. trying to dealloc an MW
  //                             when the client doesn't have an MW.
  // -- InternalError: when client fails to issue the command, typically
  //                   triggered by the ibv_* function call returning nonzero
  //                   value.
  absl::StatusCode TryCreateCq();
  absl::StatusCode TryDestroyCq();
  absl::StatusCode TryAllocPd();
  absl::StatusCode TryDeallocPd();
  absl::StatusCode TryRegMr();
  absl::StatusCode TryDeregMr();
  absl::StatusCode TryAllocType1Mw();
  absl::StatusCode TryAllocType2Mw();
  absl::StatusCode TryDeallocType1Mw();
  absl::StatusCode TryDeallocType2Mw();
  absl::StatusCode TryBindType1Mw();
  absl::StatusCode TryBindType2Mw();
  absl::StatusCode TryCreateRcQpPair();
  absl::StatusCode TryCreateUdQp();
  absl::StatusCode TryModifyQpError();
  absl::StatusCode TryDestroyQp();
  absl::StatusCode TryCreateAh();
  absl::StatusCode TryDestroyAh();
  absl::StatusCode TrySend();
  absl::StatusCode TrySendWithInv();
  absl::StatusCode TryRecv();
  absl::StatusCode TryRead();
  absl::StatusCode TryWrite();
  absl::StatusCode TryFetchAdd();
  absl::StatusCode TryCompSwap();

  // Pushes an ClientUpdate to outbound_updates queue.
  void PushOutboundUpdate(ClientUpdate& update);
  // Pulls an inbound ClientUpdate from the inbound_updates_ queue. Returns
  // nullopt if the queue is empty.
  absl::optional<ClientUpdate> PullInboundUpdate();
  // Flushes the inbound_updates_ queue, process all ClientUpdate flushed.
  void FlushInboundUpdateQueue();
  // Processes a connection update request from remote.
  void ProcessUpdate(const ClientUpdate& update);
  // Polls completion entries and process them from all completion queue until
  // they are empty.
  void FlushAllCompletionQueues();
  // Polls completion entries and process them from a completion queue until
  // it is empty.
  void FlushCompletionQueue(ibv_cq* cq);
  // Processes a completion.
  void ProcessCompletion(ibv_wc completion);

  // ---------------------------------------------------------------------------
  VerbsHelperSuite ibv_;
  RandomWalkLogger log_;

  const ClientId id_;

  // - The memory_ field represents the "ground" memory buffer for the client.
  // - Memory Regions/Windows are allocated from it.
  RdmaMemBlock memory_;
  // - Other static resource for clients.
  ibv_context* context_;
  verbs_util::PortGid port_gid_;
  uint64_t next_wr_id_ = 0;

  // Resource manager for MW, MR, and QP.
  IbvResourceManager resource_manager_;
  // For storing and retrieving ops.
  BindOpsTracker bind_ops_;
  InvalidateOpsTracker invalidate_ops_;

  absl::flat_hash_map<ClientId, ibv_gid> client_gids_;

  std::shared_ptr<UpdateDispatcherInterface> dispatcher_ = nullptr;

  // Remote updates received from other clients.
  std::deque<ClientUpdate> inbound_updates_ ABSL_GUARDED_BY(mtx_in_updates_);

  // Related to sampling (actions and its parameters) in the random walker.
  const ActionWeights action_weights_;
  const MinimumObjects minimum_objects_;
  RandomWalkSampler sampler_;
  ActionSampler action_sampler_;

  // Statistics.
  Stats stats_;

  // Mutexes.
  mutable absl::Mutex mtx_in_updates_;

  // absl::BitGen for random number generators.
  mutable absl::BitGen bitgen_;
};

// TODO(author2): Ops with multiple SGEs and different MRs.
// TODO(author2): Implement forced deletion for PD.
// TODO(author2): Implement forced deletion for CQ.
// TODO(author2): Implement forced deletion for QP.
// TODO(author2): Implement forced deletion for MR.

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_RANDOM_WALK_CLIENT_H_
