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

#include "random_walk/internal/random_walk_client.h"

#include <sched.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/random/distributions.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include <magic_enum.hpp>
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/introspection.h"
#include "public/map_util.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "random_walk/internal/bind_ops_tracker.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/ibv_resource_manager.h"
#include "random_walk/internal/invalidate_ops_tracker.h"
#include "random_walk/internal/logging.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/sampling.h"
#include "random_walk/internal/types.h"
#include "random_walk/internal/update_dispatcher_interface.h"

ABSL_FLAG(bool, allow_outstanding_ops, false,
          "Controls if client will destroy qp with outstanding ops.");

namespace rdma_unit_test {
namespace random_walk {

RandomWalkClient::RandomWalkClient(ClientId client_id,
                                   const ActionWeights& action_weights)
    : log_(kLogSize),
      id_(client_id),
      allow_outstanding_ops_(absl::GetFlag(FLAGS_allow_outstanding_ops)),
      action_sampler_([action_weights]() -> ActionWeights {
        if (Introspection().SupportsType2()) {
          return action_weights;
        } else {
          // Zero out weights of type 2 MW actions when the NIC does not support
          // type 2 MW.
          ActionWeights new_weights = action_weights;
          new_weights.set_allocate_type_2_mw(0);
          new_weights.set_bind_type_2_mw(0);
          new_weights.set_deallocate_type_2_mw(0);
          return new_weights;
        }
      }()) {
  memory_ = ibv_.AllocBuffer(RandomWalkSampler::kGroundMemoryPages);
  memset(memory_.data(), '-', memory_.size());
  context_ = ibv_.OpenDevice().value();
  CHECK(context_);  // Crash ok
  port_attr_ = ibv_.GetPortAttribute(context_);
}

void RandomWalkClient::AddRemoteClient(ClientId client_id, const ibv_gid& gid) {
  map_util::InsertOrDie(client_gids_, client_id, gid);
}

ibv_gid RandomWalkClient::GetGid() const { return port_attr_.gid; }

void RandomWalkClient::RegisterUpdateDispatcher(
    std::shared_ptr<UpdateDispatcherInterface> dispatcher) {
  DCHECK(dispatcher);
  DCHECK(!dispatcher_);
  dispatcher_ = dispatcher;
}

void RandomWalkClient::PushInboundUpdate(const ClientUpdate& update) {
  absl::MutexLock guard(&mtx_in_updates_);
  CHECK_LT(inbound_updates_.size(), kMaxOustandingUpdates)  // Crash ok
      << "Too many outstanding inbound updates.";
  inbound_updates_.push_back(update);
}

void RandomWalkClient::Run(absl::Duration duration) {
  size_t step_count = 0;
  absl::Time start = absl::Now();
  absl::Time finish = start + duration;

  BootstrapRandomWalk();
  absl::SleepFor(absl::Milliseconds(10));
  while (absl::Now() < finish) {
    absl::Status result = RandomWalk();
    if (!result.ok()) {
      LOG(INFO) << result;
      LOG(DFATAL) << "Random walk fails at step " << step_count << ".";
      break;
    }
    ++step_count;
  }
  LOG(INFO) << "Random walk completes " << step_count << " steps in "
            << duration << ".";
}

void RandomWalkClient::Run(size_t steps) {
  size_t step_count = 0;

  BootstrapRandomWalk();
  absl::SleepFor(absl::Milliseconds(10));
  while (step_count < steps) {
    absl::Status result = RandomWalk();
    if (!result.ok()) {
      LOG(INFO) << result;
      LOG(DFATAL) << "Random walk fails at step " << step_count << ".";
      break;
    }
    ++step_count;
  }
  LOG(INFO) << "Random walk completes " << step_count << "steps.";
}

void RandomWalkClient::BootstrapRandomWalk() {
  for (size_t i = 0; i < caps_.min_cq(); ++i) {
    CHECK_OK(DoAction(Action::CREATE_CQ));  // Crash ok
  }
  for (size_t i = 0; i < caps_.min_pd(); ++i) {
    CHECK_OK(DoAction(Action::ALLOC_PD));  // Crash ok
  }
  for (size_t i = 0; i < caps_.min_mr(); ++i) {
    CHECK_OK(DoAction(Action::REG_MR));  // Crash ok
  }
  for (size_t i = 0; i < caps_.min_type_1_mw(); ++i) {
    CHECK_OK(DoAction(Action::ALLOC_TYPE_1_MW));  // Crash ok
  }
  if (Introspection().SupportsType2()) {
    for (size_t i = 0; i < caps_.min_type_2_mw(); ++i) {
      CHECK_OK(DoAction(Action::ALLOC_TYPE_2_MW));  // Crash ok
    }
  }
}

absl::Status RandomWalkClient::RandomWalk() {
  FlushInboundUpdateQueue();
  RETURN_IF_ERROR(DoRandomAction());
  sched_yield();
  absl::SleepFor(absl::Milliseconds(2));
  FlushAllCompletionQueues();
  return absl::OkStatus();
}

absl::Status RandomWalkClient::DoAction(Action action) {
  // Process all incoming updates first.
  FlushInboundUpdateQueue();
  absl::StatusCode result = absl::StatusCode::kOk;
  switch (action) {
    case Action::CREATE_CQ: {
      result = TryCreateCq();
      break;
    }
    case Action::DESTROY_CQ: {
      result = TryDestroyCq();
      break;
    }
    case Action::ALLOC_PD: {
      result = TryAllocPd();
      break;
    }
    case Action::DEALLOC_PD: {
      result = TryDeallocPd();
      break;
    }
    case Action::REG_MR: {
      result = TryRegMr();
      break;
    }
    case Action::DEREG_MR: {
      result = TryDeregMr();
      break;
    }
    case Action::ALLOC_TYPE_1_MW: {
      result = TryAllocType1Mw();
      break;
    }
    case Action::ALLOC_TYPE_2_MW: {
      result = TryAllocType2Mw();
      break;
    }
    case Action::BIND_TYPE_1_MW: {
      result = TryBindType1Mw();
      break;
    }
    case Action::BIND_TYPE_2_MW: {
      result = TryBindType2Mw();
      break;
    }
    case Action::DEALLOC_TYPE_1_MW: {
      result = TryDeallocType1Mw();
      break;
    }
    case Action::DEALLOC_TYPE_2_MW: {
      result = TryDeallocType2Mw();
      break;
    }
    case Action::SEND: {
      result = TrySend();
      break;
    }
    case Action::SEND_WITH_INV: {
      result = TrySendWithInv();
      break;
    }
    case Action::RECV: {
      result = TryRecv();
      break;
    }
    case Action::READ: {
      result = TryRead();
      break;
    }
    case Action::WRITE: {
      result = TryWrite();
      break;
    }
    case Action::FETCH_ADD: {
      result = TryFetchAdd();
      break;
    }
    case Action::COMP_SWAP: {
      result = TryCompSwap();
      break;
    }
    default: {
      return absl::InternalError("Try to carry out unknown action.");
    }
  }

  if (result != absl::StatusCode::kOk) {
    return absl::InternalError(absl::StrCat(
        "Cannot do action (", magic_enum::enum_name(action), ")."));
  }

  sched_yield();
  absl::SleepFor(absl::Milliseconds(2));

  FlushAllCompletionQueues();

  return absl::OkStatus();
}

void RandomWalkClient::PrintLogs() const { log_.PrintLogs(); }

void RandomWalkClient::PrintStats() const {
  LOG(INFO) << "Dumping stats for client " << id_;
  LOG(INFO) << "Statistics:";
  LOG(INFO) << "commands = " << stats_.commands;
  LOG(INFO) << "create_cq = " << stats_.create_cq;
  LOG(INFO) << "destroy_cq = " << stats_.destroy_cq;
  LOG(INFO) << "alloc_pd = " << stats_.alloc_pd;
  LOG(INFO) << "dealloc_pd = " << stats_.dealloc_pd;
  LOG(INFO) << "reg_mr = " << stats_.reg_mr;
  LOG(INFO) << "dererg_mr = " << stats_.dereg_mr;
  LOG(INFO) << "alloc_type_1_mw = " << stats_.alloc_type_1_mw;
  LOG(INFO) << "alloc_type_2_mw = " << stats_.alloc_type_2_mw;
  LOG(INFO) << "dealloc_type_1_mw = " << stats_.dealloc_type_1_mw;
  LOG(INFO) << "dealloc_type_2_mw = " << stats_.dealloc_type_2_mw;
  LOG(INFO) << "create_qp_pair = " << stats_.create_rc_qp_pair;
  LOG(INFO) << "create_ud_qp = " << stats_.create_ud_qp;
  LOG(INFO) << "modify_qp_error = " << stats_.modify_qp_error;
  LOG(INFO) << "destroy_qp = " << stats_.destroy_qp;
  LOG(INFO) << "create_ah = " << stats_.create_ah;
  LOG(INFO) << "destroy_ah = " << stats_.destroy_ah;
  LOG(INFO) << "bind_type_1_mw = " << stats_.bind_type_1_mw_success << "/"
            << stats_.bind_type_1_mw;
  LOG(INFO) << "bind_type_2_mw = " << stats_.bind_type_2_mw_success << "/"
            << stats_.bind_type_2_mw;
  LOG(INFO) << "send = " << stats_.send_success << "/" << stats_.send;
  LOG(INFO) << "send_with_inv = " << stats_.send_with_inv_success << "/"
            << stats_.send_with_inv;
  LOG(INFO) << "recv = " << stats_.recv_success << "/" << stats_.recv;
  LOG(INFO) << "read = " << stats_.read_success << "/" << stats_.read;
  LOG(INFO) << "write = " << stats_.write_success << "/" << stats_.write;
  LOG(INFO) << "fetch_add = " << stats_.fetch_add_success << "/"
            << stats_.fetch_add;
  LOG(INFO) << "comp_swap = " << stats_.comp_swap_success << "/"
            << stats_.comp_swap;
  LOG(INFO) << "completion_statuses: ";
  LOG(INFO) << "total completions = " << stats_.completions;
  for (ibv_wc_status status :
       {IBV_WC_SUCCESS,           IBV_WC_LOC_LEN_ERR,
        IBV_WC_LOC_QP_OP_ERR,     IBV_WC_LOC_EEC_OP_ERR,
        IBV_WC_LOC_PROT_ERR,      IBV_WC_WR_FLUSH_ERR,
        IBV_WC_MW_BIND_ERR,       IBV_WC_BAD_RESP_ERR,
        IBV_WC_LOC_ACCESS_ERR,    IBV_WC_REM_INV_REQ_ERR,
        IBV_WC_REM_ACCESS_ERR,    IBV_WC_REM_OP_ERR,
        IBV_WC_RETRY_EXC_ERR,     IBV_WC_RNR_RETRY_EXC_ERR,
        IBV_WC_LOC_RDD_VIOL_ERR,  IBV_WC_REM_INV_RD_REQ_ERR,
        IBV_WC_REM_ABORT_ERR,     IBV_WC_INV_EECN_ERR,
        IBV_WC_INV_EEC_STATE_ERR, IBV_WC_FATAL_ERR,
        IBV_WC_RESP_TIMEOUT_ERR,  IBV_WC_GENERAL_ERR}) {
    LOG(INFO) << ibv_wc_status_str(status) << " = "
              << stats_.completion_statuses[status];
  }
  LOG(INFO) << profiler_.DumpStats();
}

// ---------------------------- Private -----------------------------------//

ibv_qp* RandomWalkClient::CreateLocalRcQp(ibv_pd* pd) {
  auto send_cq_sample = resource_manager_.GetRandomCq();
  if (!send_cq_sample.has_value()) {
    return nullptr;
  }
  ibv_cq* send_cq = send_cq_sample.value();
  DCHECK(send_cq);
  auto recv_cq_sample = resource_manager_.GetRandomCq();
  if (!recv_cq_sample.has_value()) {
    return nullptr;
  }
  ibv_cq* recv_cq = send_cq_sample.value();
  DCHECK(recv_cq);

  ibv_qp_init_attr init_attr =
      QpInitAttribute()
          .set_max_send_wr(absl::Uniform(
              bitgen_, kMinQpWr, Introspection().device_attr().max_qp_wr))
          .set_max_recv_wr(absl::Uniform(
              bitgen_, kMinQpWr, Introspection().device_attr().max_qp_wr))
          .set_max_send_sge(
              absl::Uniform(bitgen_, 1, Introspection().device_attr().max_sge))
          .set_max_recv_sge(
              absl::Uniform(bitgen_, 1, Introspection().device_attr().max_sge))
          .GetAttribute(send_cq, recv_cq, IBV_QPT_RC);
  ibv_qp* qp = ibv_.CreateQp(pd, init_attr);
  if (!qp) {
    return nullptr;
  }
  resource_manager_.InsertRcQp(qp, init_attr.cap);
  CqInfo* send_cq_info = resource_manager_.GetMutableCqInfo(send_cq);
  DCHECK(send_cq_info);
  map_util::InsertOrDie(send_cq_info->send_qps, qp);
  CqInfo* recv_cq_info = resource_manager_.GetMutableCqInfo(recv_cq);
  DCHECK(recv_cq_info);
  map_util::InsertOrDie(recv_cq_info->recv_qps, qp);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::InsertOrDie(pd_info->rc_qps, qp);
  return qp;
}

absl::Status RandomWalkClient::ModifyRcQpResetToRts(ibv_qp* local_qp,
                                                    ibv_gid remote_gid,
                                                    uint32_t remote_qpn,
                                                    ClientId remote_client_id,
                                                    uint32_t remote_pd_handle) {
  RcQpInfo* qp_info = resource_manager_.GetMutableRcQpInfo(local_qp);
  DCHECK(qp_info);
  IbvResourceManager::RemoteRcQpInfo remote_qp = {
      .client_id = remote_client_id,
      .qp_num = remote_qpn,
      .pd_handle = remote_pd_handle,
  };
  qp_info->remote_qp = remote_qp;
  DCHECK_EQ(IBV_QPS_RESET, verbs_util::GetQpState(local_qp));
  RETURN_IF_ERROR(
      ibv_.ModifyRcQpResetToRts(local_qp, port_attr_, remote_gid, remote_qpn));
  return absl::OkStatus();
}

absl::Status RandomWalkClient::DoRandomAction() {
  constexpr size_t kMaxAttempt = 1000;
  for (size_t attempt = 0; attempt < kMaxAttempt; ++attempt) {
    absl::StatusCode result = TryDoRandomAction();
    if (result == absl::StatusCode::kOk) {
      ++stats_.commands;
      return absl::OkStatus();
    } else if (result == absl::StatusCode::kInternal) {
      return absl::InternalError("Failed to issue the command");
    }
  }
  return absl::InternalError("Too many attempts to do random actions.");
}

absl::StatusCode RandomWalkClient::DeregMr(ibv_mr* mr) {
  uint32_t rkey = mr->rkey;
  ibv_pd* pd = mr->pd;
  int result = ibv_.DeregMr(mr);
  log_.PushDeregMr(mr);
  if (result) {
    LOG(DFATAL) << "Failed to deregister mr (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.dereg_mr;
  resource_manager_.EraseMr(mr);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::CheckPresentAndErase(pd_info->mrs, mr);

  ClientUpdate update;
  RemoveRKey* remove_rkey = update.mutable_remove_rkey();
  remove_rkey->set_rkey(rkey);
  remove_rkey->set_owner_id(id_);
  PushOutboundUpdate(update);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::DeallocType1Mw(ibv_mw* mw, bool is_bound) {
  DCHECK(mw);
  DCHECK_EQ(IBV_MW_TYPE_1, mw->type);

  ibv_pd* pd = mw->pd;
  uint32_t mw_rkey = mw->rkey;
  int result = ibv_.DeallocMw(mw);
  log_.PushDeallocMw(mw);
  if (result) {
    LOG(DFATAL) << "Failed to deallocate (bound) mw (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.dealloc_type_1_mw;
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::CheckPresentAndErase(pd_info->type_1_mws, mw);
  if (is_bound) {
    Type1MwBindInfo mw_info = resource_manager_.GetType1BindInfo(mw);
    ibv_mr* mr = mw_info.mr;
    MrInfo* mr_info = resource_manager_.GetMutableMrInfo(mr);
    DCHECK(mr_info);
    map_util::CheckPresentAndErase(mr_info->bound_mws, mw);
    resource_manager_.EraseBoundType1Mw(mw);

    ClientUpdate update;
    RemoveRKey* remove_rkey = update.mutable_remove_rkey();
    remove_rkey->set_owner_id(id_);
    remove_rkey->set_rkey(mw_rkey);
    PushOutboundUpdate(update);
  } else {
    resource_manager_.EraseUnboundType1Mw(mw);
  }

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::DeallocType2Mw(ibv_mw* mw, bool is_bound) {
  DCHECK(mw);
  DCHECK_EQ(IBV_MW_TYPE_2, mw->type);

  uint32_t rkey = mw->rkey;
  ibv_pd* pd = mw->pd;
  int result = ibv_.DeallocMw(mw);
  log_.PushDeallocMw(mw);
  if (result) {
    LOG(DFATAL) << "Failed to deallocate (bound) mw (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.dealloc_type_2_mw;
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::CheckPresentAndErase(pd_info->type_2_mws, mw);
  if (is_bound) {
    Type2MwBindInfo mw_info = resource_manager_.GetType2BindInfo(rkey);
    resource_manager_.EraseBoundType2Mw(rkey);
    MrInfo* mr_info = resource_manager_.GetMutableMrInfo(mw_info.bind_info.mr);
    DCHECK(mr_info);
    map_util::CheckPresentAndErase(mr_info->bound_mws, mw);
    RcQpInfo* qp_info = resource_manager_.GetMutableRcQpInfo(mw_info.qp_num);
    DCHECK(qp_info);
    map_util::CheckPresentAndErase(qp_info->type_2_mws, mw);

    ClientUpdate update;
    update.set_destination_id(qp_info->remote_qp->client_id);
    RemoveRKey* remove_rkey = update.mutable_remove_rkey();
    remove_rkey->set_owner_id(id_);
    remove_rkey->set_rkey(rkey);
    PushOutboundUpdate(update);
  } else {
    resource_manager_.EraseUnboundType2Mw(mw);
  }
  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::DestroyQp(ibv_qp* qp) {
  // Check that Qp satisfies precondition.
  DCHECK_EQ(verbs_util::GetQpState(qp), IBV_QPS_ERR);
  DCHECK(resource_manager_.GetQpInfo(qp).inflight_ops.empty());

  ibv_pd* pd = qp->pd;
  ibv_cq* send_cq = qp->send_cq;
  ibv_cq* recv_cq = qp->recv_cq;
  uint32_t qp_num = qp->qp_num;
  ibv_qp_type qp_type = qp->qp_type;
  int result = ibv_.DestroyQp(qp);
  if (result) {
    LOG(ERROR) << "Failed to destroy qp (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  resource_manager_.EraseQp(qp_num, qp_type);
  ++stats_.destroy_qp;
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::CheckPresentAndErase(pd_info->rc_qps, qp);
  CqInfo* send_cq_info = resource_manager_.GetMutableCqInfo(send_cq);
  DCHECK(send_cq_info);
  map_util::CheckPresentAndErase(send_cq_info->send_qps, qp);
  CqInfo* recv_cq_info = resource_manager_.GetMutableCqInfo(recv_cq);
  DCHECK(recv_cq_info);
  map_util::CheckPresentAndErase(recv_cq_info->recv_qps, qp);
  if (qp_type == IBV_QPT_UD) {
    ClientUpdate update;
    RemoveUdQp* remove_ud_qp = update.mutable_remove_ud_qp();
    remove_ud_qp->set_owner_id(id_);
    remove_ud_qp->set_qp_num(qp_num);
    PushOutboundUpdate(update);
  }
  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDoRandomAction() {
  Action action = action_sampler_.RandomAction();
  absl::StatusCode result;
  switch (action) {
    case Action::CREATE_CQ: {
      result = TryCreateCq();
      break;
    }
    case Action::DESTROY_CQ: {
      result = TryDestroyCq();
      break;
    }
    case Action::ALLOC_PD: {
      result = TryAllocPd();
      break;
    }
    case Action::DEALLOC_PD: {
      result = TryDeallocPd();
      break;
    }
    case Action::REG_MR: {
      result = TryRegMr();
      break;
    }
    case Action::DEREG_MR: {
      result = TryDeregMr();
      break;
    }
    case Action::ALLOC_TYPE_1_MW: {
      result = TryAllocType1Mw();
      break;
    }
    case Action::ALLOC_TYPE_2_MW: {
      result = TryAllocType2Mw();
      break;
    }
    case Action::DEALLOC_TYPE_1_MW: {
      result = TryDeallocType1Mw();
      break;
    }
    case Action::DEALLOC_TYPE_2_MW: {
      result = TryDeallocType2Mw();
      break;
    }
    case Action::BIND_TYPE_1_MW: {
      result = TryBindType1Mw();
      break;
    }
    case Action::BIND_TYPE_2_MW: {
      result = TryBindType2Mw();
      break;
    }
    case Action::CREATE_RC_QP_PAIR: {
      result = TryCreateRcQpPair();
      break;
    }
    case Action::CREATE_UD_QP: {
      result = TryCreateUdQp();
      break;
    }
    case Action::MODIFY_QP_ERROR: {
      result = TryModifyQpError();
      break;
    }
    case Action::DESTROY_QP: {
      result = TryDestroyQp();
      break;
    }
    case Action::CREATE_AH: {
      result = TryCreateAh();
      break;
    }
    case Action::DESTROY_AH: {
      result = TryDestroyAh();
      break;
    }
    case Action::SEND: {
      result = TrySend();
      break;
    }
    case Action::SEND_WITH_INV: {
      result = TrySendWithInv();
      break;
    }
    case Action::RECV: {
      result = TryRecv();
      break;
    }
    case Action::READ: {
      result = TryRead();
      break;
    }
    case Action::WRITE: {
      result = TryWrite();
      break;
    }
    case Action::FETCH_ADD: {
      result = TryFetchAdd();
      break;
    }
    case Action::COMP_SWAP: {
      result = TryCompSwap();
      break;
    }
    default: {
      LOG(DFATAL) << "Unknown action code.";
      result = absl::StatusCode::kInternal;
    }
  }
  return result;
}

absl::StatusCode RandomWalkClient::TryCreateCq() {
  if (resource_manager_.CqCount() >= caps_.max_cq()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  int cqe =
      absl::Uniform(bitgen_, kMinCqe, Introspection().device_attr().max_cqe);
  ibv_cq* cq = ibv_.CreateCq(context_, cqe);
  log_.PushCreateCq(cq);
  if (!cq) {
    LOG(DFATAL) << "Failed to create cq (" << errno << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.create_cq;
  resource_manager_.InsertCq(cq);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDestroyCq() {
  if (resource_manager_.CqCount() <= caps_.min_cq()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  // TODO(author2): Implements force CQ destruction.
  auto cq_sample = resource_manager_.GetRandomCqNoReference();
  if (!cq_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_cq* cq = cq_sample.value();
  DCHECK(cq);

  int result = ibv_.DestroyCq(cq);
  log_.PushDestroyCq(cq);
  if (result) {
    LOG(DFATAL) << "Failed to destroy CQ (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.destroy_cq;
  resource_manager_.EraseCq(cq);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryAllocPd() {
  if (resource_manager_.PdCount() >= caps_.max_pd()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = ibv_.AllocPd(context_);
  log_.PushAllocPd(pd);
  if (!pd) {
    LOG(DFATAL) << "Failed to allocate PD. (errno = " << errno << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.alloc_pd;
  resource_manager_.InsertPd(pd);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDeallocPd() {
  if (resource_manager_.PdCount() <= caps_.min_pd()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  // TODO(author2): Implements force PD deallocation.
  auto pd_sample = resource_manager_.GetRandomPdNoReference();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);

  int result = ibv_.DeallocPd(pd);
  log_.PushDeallocPd(pd);
  if (result) {
    LOG(DFATAL) << "Failed to deallocate PD (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.dealloc_pd;
  resource_manager_.ErasePd(pd);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryRegMr() {
  if (resource_manager_.MrCount() >= caps_.max_mr()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RdmaMemBlock memblock = sampler_.RandomMrRdmaMemblock(memory_);
  auto pd_sample = resource_manager_.GetRandomPd();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);

  ibv_mr* mr = ibv_.RegMr(pd, memblock);
  log_.PushAllocPd(pd);
  if (!mr) {
    LOG(DFATAL) << "Failed to register mr.";
    return absl::StatusCode::kInternal;
  }
  ++stats_.reg_mr;
  resource_manager_.InsertMr(mr);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::InsertOrDie(pd_info->mrs, mr);

  ClientUpdate update;
  AddRKey* add_rkey = update.mutable_add_rkey();
  add_rkey->set_addr(reinterpret_cast<uint64_t>(mr->addr));
  add_rkey->set_length(mr->length);
  add_rkey->set_rkey(mr->rkey);
  add_rkey->set_owner_id(id_);
  add_rkey->set_pd_handle(mr->pd->handle);
  PushOutboundUpdate(update);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDeregMr() {
  if (resource_manager_.MrCount() << caps_.min_mr()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto mr_sample = resource_manager_.GetRandomMrNoReference();
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  return DeregMr(mr);
}

absl::StatusCode RandomWalkClient::TryAllocType1Mw() {
  if (resource_manager_.Type1MwCount() >= caps_.max_type_1_mw()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto pd_sample = resource_manager_.GetRandomPd();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);

  ibv_mw* mw = ibv_.AllocMw(pd, IBV_MW_TYPE_1);
  log_.PushAllocMw(pd, IBV_MW_TYPE_1, mw);
  if (!mw) {
    LOG(DFATAL) << "Failed to allocate mw.";
    return absl::StatusCode::kInternal;
  }
  ++stats_.alloc_type_1_mw;
  resource_manager_.InsertUnboundType1Mw(mw);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::InsertOrDie(pd_info->type_1_mws, mw);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryAllocType2Mw() {
  DCHECK(Introspection().SupportsType2()) << "NIC does not support type 2.";
  if (resource_manager_.Type2MwCount() >= caps_.max_type_2_mw()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto pd_sample = resource_manager_.GetRandomPd();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);

  ibv_mw* mw = ibv_.AllocMw(pd, IBV_MW_TYPE_2);
  log_.PushAllocMw(pd, IBV_MW_TYPE_2, mw);
  if (!mw) {
    LOG(DFATAL) << "Failed to allocate mw.";
    return absl::StatusCode::kInternal;
  }
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::InsertOrDie(pd_info->type_2_mws, mw);
  ++stats_.alloc_type_2_mw;
  resource_manager_.InsertUnboundType2Mw(mw);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDeallocType1Mw() {
  if (resource_manager_.Type1MwCount() <= caps_.min_type_1_mw()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  bool deallocate_bound = absl::Bernoulli(bitgen_, 0.5);
  absl::optional<ibv_mw*> mw_sample;
  if (deallocate_bound) {
    mw_sample = resource_manager_.GetRandomBoundType1Mw();
  } else {
    mw_sample = resource_manager_.GetRandomUnboundType1Mw();
  }
  if (!mw_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mw* mw = mw_sample.value();
  DCHECK(mw);
  return DeallocType1Mw(mw, deallocate_bound);
}

absl::StatusCode RandomWalkClient::TryDeallocType2Mw() {
  DCHECK(Introspection().SupportsType2()) << "NIC does not support type 2.";
  if (resource_manager_.Type2MwCount() <= caps_.min_type_2_mw()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  bool bound = absl::Bernoulli(bitgen_, 0.5);
  absl::optional<ibv_mw*> mw_sample;
  if (bound) {
    mw_sample = resource_manager_.GetRandomBoundType2Mw();
  } else {
    mw_sample = resource_manager_.GetRandomUnboundType2Mw();
  }
  if (!mw_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mw* mw = mw_sample.value();
  DCHECK(mw);
  return DeallocType2Mw(mw, bound);
}

absl::StatusCode RandomWalkClient::TryBindType1Mw() {
  auto qp_sample = resource_manager_.GetRandomQpForBind();
  if (!qp_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_qp* qp = qp_sample.value();
  DCHECK(qp);
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  auto mw_sample = resource_manager_.GetRandomUnboundType1Mw(qp->pd);
  if (!mw_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mw* mw = mw_sample.value();
  DCHECK(mw);
  absl::Span<uint8_t> buffer = sampler_.RandomMwSpan(mr);
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::BIND_TYPE_1_MW);
  ibv_mw_bind bind_wr = verbs_util::CreateType1MwBind(wr_id, buffer, mr);
  int result = ibv_bind_mw(qp, mw, &bind_wr);
  log_.PushBindMw(bind_wr, mw);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  resource_manager_.GetMutableRcQpInfo(qp)->inflight_ops.insert(wr_id);
  log_.PushBindMw(bind_wr, mw);
  ++stats_.bind_type_1_mw;
  MrInfo* mr_info = resource_manager_.GetMutableMrInfo(mr);
  DCHECK(mr_info);
  map_util::InsertOrDie(mr_info->bound_mws, mw);
  resource_manager_.EraseUnboundType1Mw(mw);
  bind_ops_.PushType1MwBind(mw, bind_wr);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryBindType2Mw() {
  DCHECK(Introspection().SupportsType2()) << "NIC does not support type 2.";
  auto qp_sample = resource_manager_.GetRandomQpForBind();
  if (!qp_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_qp* qp = qp_sample.value();
  DCHECK(qp);
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  auto mw_sample = resource_manager_.GetRandomUnboundType2Mw(qp->pd);
  if (!mw_sample.has_value()) {
    // The PD still might not have an unbound MW.
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mw* mw = mw_sample.value();
  DCHECK(mw);
  absl::Span<uint8_t> buffer = sampler_.RandomMwSpan(mr);
  uint32_t rkey = absl::Uniform<uint32_t>(bitgen_);
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::BIND_TYPE_2_MW);
  ibv_send_wr bind_wr =
      verbs_util::CreateType2BindWr(wr_id, mw, buffer, rkey, mr);
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, &bind_wr, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  resource_manager_.GetMutableRcQpInfo(qp)->inflight_ops.insert(wr_id);
  log_.PushBindMw(bind_wr);
  ++stats_.bind_type_2_mw;
  MrInfo* mr_info = resource_manager_.GetMutableMrInfo(mr);
  DCHECK(mr_info);
  map_util::InsertOrDie(mr_info->bound_mws, mw);
  resource_manager_.EraseUnboundType2Mw(mw);
  bind_ops_.PushType2MwBind(bind_wr);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryCreateRcQpPair() {
  if (resource_manager_.QpCount(IBV_QPT_RC) >= caps_.max_rc_qp()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ClientId peer_id = sampler_.GetRandomMapKey(client_gids_).value();
  auto pd_sample = resource_manager_.GetRandomPd();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);
  ibv_qp* qp = CreateLocalRcQp(pd);
  log_.PushCreateQp(qp);
  DCHECK(qp);
  ++stats_.create_rc_qp_pair;

  ClientUpdate update;
  update.set_destination_id(peer_id);
  InitiatorCreateRcQp* request = update.mutable_initiator_create_rc_qp();
  request->set_initiator_id(id_);
  request->set_initiator_qpn(qp->qp_num);
  request->set_initiator_pd_handle(qp->pd->handle);
  PushOutboundUpdate(update);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryCreateUdQp() {
  if (resource_manager_.QpCount(IBV_QPT_UD) >= caps_.max_ud_qp()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto pd_sample = resource_manager_.GetRandomPd();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);
  auto send_cq_sample = resource_manager_.GetRandomCq();
  if (!send_cq_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_cq* send_cq = send_cq_sample.value();
  DCHECK(send_cq);
  auto recv_cq_sample = resource_manager_.GetRandomCq();
  if (!recv_cq_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_cq* recv_cq = send_cq_sample.value();
  DCHECK(recv_cq);

  ibv_qp_init_attr init_attr =
      QpInitAttribute()
          .set_max_send_wr(absl::Uniform(
              bitgen_, kMinQpWr, Introspection().device_attr().max_qp_wr))
          .set_max_recv_wr(absl::Uniform(
              bitgen_, kMinQpWr, Introspection().device_attr().max_qp_wr))
          .set_max_send_sge(
              absl::Uniform(bitgen_, 1, Introspection().device_attr().max_sge))
          .set_max_recv_sge(
              absl::Uniform(bitgen_, 1, Introspection().device_attr().max_sge))
          .GetAttribute(send_cq, recv_cq, IBV_QPT_UD);
  ibv_qp* qp = ibv_.CreateQp(pd, init_attr);
  log_.PushCreateQp(qp);
  if (!qp) {
    LOG(DFATAL) << "Failed to create UD QP (" << errno << ")";
    return absl::StatusCode::kInternal;
  }
  uint32_t qkey = absl::Uniform<uint32_t>(bitgen_);
  auto status = ibv_.ModifyUdQpResetToRts(qp, qkey);
  if (!status.ok()) {
    CHECK_EQ(0, ibv_.DestroyQp(qp));  // Crash ok
    LOG(DFATAL) << "Failed to bring up UD QP (" << status << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.create_ud_qp;
  resource_manager_.InsertUdQp(qp, qkey, init_attr.cap);
  CqInfo* send_cq_info = resource_manager_.GetMutableCqInfo(send_cq);
  DCHECK(send_cq_info);
  map_util::InsertOrDie(send_cq_info->send_qps, qp);
  CqInfo* recv_cq_info = resource_manager_.GetMutableCqInfo(recv_cq);
  DCHECK(recv_cq_info);
  map_util::InsertOrDie(recv_cq_info->recv_qps, qp);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::InsertOrDie(pd_info->rc_qps, qp);

  ClientUpdate update;
  AddUdQp* ud_qp = update.mutable_add_ud_qp();
  ud_qp->set_owner_id(id_);
  ud_qp->set_qp_num(qp->qp_num);
  ud_qp->set_q_key(qkey);
  PushOutboundUpdate(update);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryModifyQpError() {
  auto qp_sample =
      resource_manager_.GetRandomQpForModifyError(allow_outstanding_ops_);
  if (!qp_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_qp* qp = qp_sample.value();
  DCHECK(qp);

  CHECK_OK(ibv_.ModifyQpToError(qp));  // Crash ok
  ++stats_.modify_qp_error;

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDestroyQp() {
  auto qp_sample =
      resource_manager_.GetRandomQpForDestroy(allow_outstanding_ops_);
  if (!qp_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_qp* qp = qp_sample.value();
  DCHECK(qp);

  return DestroyQp(qp);
}

absl::StatusCode RandomWalkClient::TryCreateAh() {
  auto pd_sample = resource_manager_.GetRandomPd();
  if (!pd_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_pd* pd = pd_sample.value();
  DCHECK(pd);
  auto gid_sample = sampler_.GetRandomMapKeyValuePair(client_gids_);
  ClientId client_id;
  ibv_gid gid;
  std::tie(client_id, gid) = gid_sample.value();

  ibv_ah* ah = ibv_.CreateLoopbackAh(pd, port_attr_);
  log_.PushCreateAh(pd, client_id, ah);
  if (!ah) {
    LOG(DFATAL) << "Failed to create ah (" << errno << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.create_ah;
  resource_manager_.InsertAh(ah, client_id);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::InsertOrDie(pd_info->ahs, ah);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryDestroyAh() {
  auto ah_sample = resource_manager_.GetRandomAh();
  if (!ah_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_ah* ah = ah_sample.value();
  DCHECK(ah);

  ibv_pd* pd = ah->pd;
  int result = ibv_.DestroyAh(ah);
  log_.PushDestroyAh(ah);
  if (result) {
    LOG(DFATAL) << "Failed to destroy ah (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  ++stats_.destroy_ah;
  resource_manager_.EraseAh(ah);
  PdInfo* pd_info = resource_manager_.GetMutablePdInfo(pd);
  DCHECK(pd_info);
  map_util::CheckPresentAndErase(pd_info->ahs, ah);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TrySend() {
  ibv_qp_type qp_type = absl::Bernoulli(bitgen_, kMessagingUdProbability)
                            ? IBV_QPT_UD
                            : IBV_QPT_RC;
  auto qp_sample = resource_manager_.GetRandomQpForMessaging(qp_type);
  if (!qp_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_qp* qp = qp_sample.value();
  DCHECK(qp);
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  uint32_t max_send_sge;
  std::vector<absl::Span<uint8_t>> buffers;
  if (qp_type == IBV_QPT_RC) {
    max_send_sge = resource_manager_.GetRcQpInfo(qp).cap.max_send_sge;
    buffers = sampler_.RandomRcSendRecvSpans(mr, max_send_sge);
  } else {
    max_send_sge = resource_manager_.GetUdQpInfo(qp).cap.max_send_sge;
    buffers = sampler_.RandomUdSendSpans(mr, max_send_sge);
  }

  std::vector<ibv_sge> sges;
  sges.reserve(buffers.size());
  for (const auto& buffer : buffers) {
    sges.push_back(verbs_util::CreateSge(buffer, mr));
  }
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::SEND);
  ibv_send_wr send = verbs_util::CreateSendWr(wr_id, sges.data(), sges.size());
  if (qp_type == IBV_QPT_UD) {
    auto ah_sample = resource_manager_.GetRandomAh(qp->pd);
    if (!ah_sample.has_value()) {
      return absl::StatusCode::kFailedPrecondition;
    }
    ibv_ah* ah = ah_sample.value();
    DCHECK(ah);
    AhInfo ah_info = resource_manager_.GetAhInfo(ah);
    auto remote_ud_sample =
        resource_manager_.GetRandomRemoteUdQp(ah_info.client_id);
    if (!remote_ud_sample.has_value()) {
      return absl::StatusCode::kFailedPrecondition;
    }
    RemoteUdQpInfo remote_ud = remote_ud_sample.value();
    send.wr.ud.ah = ah;
    send.wr.ud.remote_qpn = remote_ud.qp_num;
    send.wr.ud.remote_qkey = remote_ud.q_key;
  }
  if (absl::Bernoulli(bitgen_, kSendImmProbability)) {
    send.opcode = IBV_WR_SEND_WITH_IMM;
    send.imm_data = absl::Uniform<uint32_t>(bitgen_);
  }
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, &send, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  QpInfo* qp_info = resource_manager_.GetMutableQpInfo(qp);
  DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
  qp_info->inflight_ops.insert(wr_id);
  log_.PushSend(send);
  ++stats_.send;

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TrySendWithInv() {
  auto remote_mw_sample = resource_manager_.GetRandomRemoteBoundType2Mw();
  if (!remote_mw_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RdmaMemory remote_mw = remote_mw_sample.value();
  DCHECK(remote_mw.qp_num.has_value());
  ibv_qp* qp = resource_manager_.GetLocalRcQp(remote_mw.client_id,
                                              remote_mw.qp_num.value());
  if (!qp || verbs_util::GetQpState(qp) != IBV_QPS_RTS) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RcQpInfo rc_info = resource_manager_.GetRcQpInfo(qp);
  // Half of the time, just do the send with an empty SGL.
  ibv_send_wr send_inv;
  std::vector<ibv_sge> sges;
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::SEND_WITH_INV);
  if (absl::Bernoulli(bitgen_, 0.5)) {
    send_inv = verbs_util::CreateSendWr(wr_id, nullptr, /*num_sge=*/0);
  } else {
    auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
    if (!mr_sample.has_value()) {
      return absl::StatusCode::kFailedPrecondition;
    }
    ibv_mr* mr = mr_sample.value();
    DCHECK(mr);
    uint32_t max_sge = resource_manager_.GetRcQpInfo(qp).cap.max_send_sge;
    std::vector<absl::Span<uint8_t>> buffers =
        sampler_.RandomRcSendRecvSpans(mr, max_sge);
    for (const auto& buffer : buffers) {
      sges.push_back(verbs_util::CreateSge(buffer, mr));
    }
    send_inv = verbs_util::CreateSendWr(wr_id, sges.data(), sges.size());
  }
  send_inv.opcode = IBV_WR_SEND_WITH_INV;
  send_inv.invalidate_rkey = remote_mw.rkey;
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, &send_inv, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  QpInfo* qp_info = resource_manager_.GetMutableQpInfo(qp);
  DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
  qp_info->inflight_ops.insert(wr_id);
  log_.PushSend(send_inv);
  ++stats_.send_with_inv;
  invalidate_ops_.PushInvalidate(send_inv.wr_id, send_inv.invalidate_rkey,
                                 remote_mw.client_id);

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryRecv() {
  ibv_qp_type qp_type = absl::Bernoulli(bitgen_, kMessagingUdProbability)
                            ? IBV_QPT_UD
                            : IBV_QPT_RC;
  auto qp_sample = resource_manager_.GetRandomQpForMessaging(qp_type);
  if (!qp_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_qp* qp = qp_sample.value();
  DCHECK(qp);
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  uint32_t max_recv_sge;
  std::vector<absl::Span<uint8_t>> buffers;
  if (qp_type == IBV_QPT_RC) {
    max_recv_sge = resource_manager_.GetRcQpInfo(qp).cap.max_recv_sge;
    buffers = sampler_.RandomRcSendRecvSpans(mr, max_recv_sge);
  } else {
    max_recv_sge = resource_manager_.GetUdQpInfo(qp).cap.max_recv_sge;
    buffers = sampler_.RandomUdRecvSpans(mr, max_recv_sge);
  }

  std::vector<ibv_sge> sges;
  sges.reserve(buffers.size());
  for (const absl::Span<uint8_t>& buffer : buffers) {
    sges.push_back(verbs_util::CreateSge(buffer, mr));
  }
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::RECV);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(wr_id, sges.data(), sges.size());
  ibv_recv_wr* bad_wr = nullptr;
  int result = ibv_post_recv(qp, &recv, &bad_wr);
  if (result == 0) {
    QpInfo* qp_info = resource_manager_.GetMutableQpInfo(qp);
    DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
    qp_info->inflight_ops.insert(wr_id);
    log_.PushRecv(recv);
    ++stats_.recv;
    return absl::StatusCode::kOk;
  } else if (result == ENOMEM) {
    return absl::StatusCode::kOk;
  } else {
    LOG(DFATAL) << "Failed to post to recv queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
}

absl::StatusCode RandomWalkClient::TryRead() {
  auto memory_opt = resource_manager_.GetRandomRdmaMemory();
  if (!memory_opt) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RdmaMemory memory = memory_opt.value();
  ibv_qp* qp = nullptr;
  if (memory.qp_num.has_value()) {
    ibv_qp* local_qp =
        resource_manager_.GetLocalRcQp(memory.client_id, memory.qp_num.value());
    if (local_qp != nullptr &&
        verbs_util::GetQpState(local_qp) == IBV_QPS_RTS) {
      qp = local_qp;
    }
  } else {
    auto qp_sample = resource_manager_.GetRandomQpForRdma(memory.client_id,
                                                          memory.pd_handle);
    if (qp_sample.has_value()) {
      qp = qp_sample.value();
    }
  }
  if (!qp) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  std::vector<absl::Span<uint8_t>> local_buffers;
  uint8_t* remote_addr;
  std::tie(local_buffers, remote_addr) = sampler_.RandomRdmaBuffersPair(
      mr, memory.addr, memory.length,
      resource_manager_.GetRcQpInfo(qp).cap.max_send_sge);

  std::vector<ibv_sge> sges;
  sges.reserve(local_buffers.size());
  for (const auto& local_buffer : local_buffers) {
    sges.push_back(verbs_util::CreateSge(local_buffer, mr));
  }
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::READ);
  ibv_send_wr read = verbs_util::CreateReadWr(wr_id, sges.data(), sges.size(),
                                              remote_addr, memory.rkey);
  ibv_send_wr* bad_wr = nullptr;
  if (verbs_util::GetQpState(qp) == IBV_QPS_RTS) {
    return absl::StatusCode::kFailedPrecondition;
  }
  int result = ibv_post_send(qp, &read, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  RcQpInfo* qp_info = resource_manager_.GetMutableRcQpInfo(qp);
  DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
  qp_info->inflight_ops.insert(wr_id);
  log_.PushRead(read);
  ++stats_.read;

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryWrite() {
  auto memory_opt = resource_manager_.GetRandomRdmaMemory();
  if (!memory_opt) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RdmaMemory memory = memory_opt.value();
  ibv_qp* qp = nullptr;
  if (memory.qp_num.has_value()) {
    ibv_qp* local_qp =
        resource_manager_.GetLocalRcQp(memory.client_id, memory.qp_num.value());
    if (local_qp != nullptr &&
        verbs_util::GetQpState(local_qp) == IBV_QPS_RTS) {
      qp = local_qp;
    }
  } else {
    auto qp_sample = resource_manager_.GetRandomQpForRdma(memory.client_id,
                                                          memory.pd_handle);
    if (qp_sample.has_value()) {
      qp = qp_sample.value();
    }
  }
  if (!qp) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  std::vector<absl::Span<uint8_t>> local_buffers;
  uint8_t* remote_addr;
  std::tie(local_buffers, remote_addr) = sampler_.RandomRdmaBuffersPair(
      mr, memory.addr, memory.length,
      resource_manager_.GetRcQpInfo(qp).cap.max_send_sge);

  std::vector<ibv_sge> sges;
  sges.reserve(local_buffers.size());
  for (const auto& local_buffer : local_buffers) {
    sges.push_back(verbs_util::CreateSge(local_buffer, mr));
  }
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::WRITE);
  ibv_send_wr write = verbs_util::CreateWriteWr(wr_id, sges.data(), sges.size(),
                                                remote_addr, memory.rkey);
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, &write, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  RcQpInfo* qp_info = resource_manager_.GetMutableRcQpInfo(qp);
  DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
  qp_info->inflight_ops.insert(wr_id);
  log_.PushWrite(write);
  ++stats_.write;

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryFetchAdd() {
  auto memory_opt = resource_manager_.GetRandomRdmaMemory();
  if (!memory_opt) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RdmaMemory memory = memory_opt.value();
  ibv_qp* qp = nullptr;
  if (memory.qp_num.has_value()) {
    ibv_qp* local_qp =
        resource_manager_.GetLocalRcQp(memory.client_id, memory.qp_num.value());
    if (local_qp != nullptr &&
        verbs_util::GetQpState(local_qp) == IBV_QPS_RTS) {
      qp = local_qp;
    }
  } else {
    auto qp_sample = resource_manager_.GetRandomQpForRdma(memory.client_id,
                                                          memory.pd_handle);
    if (qp_sample.has_value()) {
      qp = qp_sample.value();
    }
  }
  if (!qp) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  uint8_t* local_addr = sampler_.RandomAtomicAddr(
      reinterpret_cast<uint8_t*>(mr->addr), mr->length);
  uint8_t* remote_addr = sampler_.RandomAtomicAddr(
      reinterpret_cast<uint8_t*>(memory.addr), memory.length);
  uint64_t add = absl::Uniform<uint64_t>(bitgen_);

  ibv_sge sge = verbs_util::CreateAtomicSge(local_addr, mr);
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::FETCH_ADD);
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      wr_id, &sge, /*num_sge=*/1, remote_addr, memory.rkey, add);
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, &fetch_add, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  RcQpInfo* qp_info = resource_manager_.GetMutableRcQpInfo(qp);
  DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
  qp_info->inflight_ops.insert(wr_id);
  log_.PushFetchAdd(fetch_add);
  ++stats_.fetch_add;

  return absl::StatusCode::kOk;
}

absl::StatusCode RandomWalkClient::TryCompSwap() {
  auto memory_opt = resource_manager_.GetRandomRdmaMemory();
  if (!memory_opt) {
    return absl::StatusCode::kFailedPrecondition;
  }
  RdmaMemory memory = memory_opt.value();
  ibv_qp* qp = nullptr;
  if (memory.qp_num.has_value()) {
    ibv_qp* local_qp =
        resource_manager_.GetLocalRcQp(memory.client_id, memory.qp_num.value());
    if (local_qp != nullptr &&
        verbs_util::GetQpState(local_qp) == IBV_QPS_RTS) {
      qp = local_qp;
    }
  } else {
    auto qp_sample = resource_manager_.GetRandomQpForRdma(memory.client_id,
                                                          memory.pd_handle);
    if (qp_sample.has_value()) {
      qp = qp_sample.value();
    }
  }
  if (!qp) {
    return absl::StatusCode::kFailedPrecondition;
  }
  auto mr_sample = resource_manager_.GetRandomMr(qp->pd);
  if (!mr_sample.has_value()) {
    return absl::StatusCode::kFailedPrecondition;
  }
  ibv_mr* mr = mr_sample.value();
  DCHECK(mr);
  uint8_t* local_addr = sampler_.RandomAtomicAddr(
      reinterpret_cast<uint8_t*>(mr->addr), mr->length);
  uint8_t* remote_addr = sampler_.RandomAtomicAddr(
      reinterpret_cast<uint8_t*>(memory.addr), memory.length);
  uint64_t add = absl::Uniform<uint64_t>(bitgen_);
  uint64_t swap = absl::Uniform<uint64_t>(bitgen_);

  ibv_sge sge = verbs_util::CreateAtomicSge(local_addr, mr);
  uint64_t wr_id = EncodeAction(next_raw_wr_id_++, Action::COMP_SWAP);
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      wr_id, &sge, /*num_sge=*/1, remote_addr, memory.rkey, add, swap);
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, &comp_swap, &bad_wr);
  if (result) {
    LOG(DFATAL) << "Failed to post to send queue (" << result << ").";
    return absl::StatusCode::kInternal;
  }
  RcQpInfo* qp_info = resource_manager_.GetMutableRcQpInfo(qp);
  DCHECK(qp_info) << "Cannot find info for QP " << qp->qp_num;
  qp_info->inflight_ops.insert(wr_id);
  log_.PushCompSwap(comp_swap);
  ++stats_.comp_swap;

  return absl::StatusCode::kOk;
}

void RandomWalkClient::PushOutboundUpdate(ClientUpdate& update) {
  dispatcher_->DispatchUpdate(update);
}

absl::optional<ClientUpdate> RandomWalkClient::PullInboundUpdate() {
  absl::MutexLock guard(&mtx_in_updates_);
  if (inbound_updates_.empty()) {
    return absl::nullopt;
  }
  ClientUpdate update = inbound_updates_.front();
  inbound_updates_.pop_front();
  return update;
}

void RandomWalkClient::FlushInboundUpdateQueue() {
  for (auto maybe_update = PullInboundUpdate(); maybe_update.has_value();
       maybe_update = PullInboundUpdate()) {
    ProcessUpdate(maybe_update.value());
  }
}

void RandomWalkClient::ProcessUpdate(const ClientUpdate& update) {
  switch (update.contents_case()) {
    case ClientUpdate::kAddRkey: {
      AddRKey add_rkey = update.add_rkey();
      if (add_rkey.has_qpn()) {
        resource_manager_.InsertRdmaMemory(
            add_rkey.owner_id(), add_rkey.rkey(), add_rkey.addr(),
            add_rkey.length(), add_rkey.pd_handle(), add_rkey.qpn());
      } else {
        resource_manager_.InsertRdmaMemory(add_rkey.owner_id(), add_rkey.rkey(),
                                           add_rkey.addr(), add_rkey.length(),
                                           add_rkey.pd_handle());
      }
      break;
    }
    case ClientUpdate::kRemoveRkey: {
      RemoveRKey remove_rkey = update.remove_rkey();
      // The local metadata for a type 2 memory windows might have already been
      // removed by an ongoing remote invalidate op.
      resource_manager_.TryEraseRdmaMemory(remove_rkey.owner_id(),
                                           remove_rkey.rkey());
      break;
    }
    case ClientUpdate::kInitiatorCreateRcQp: {
      const InitiatorCreateRcQp& create_qp = update.initiator_create_rc_qp();
      auto pd_sample = resource_manager_.GetRandomPd();
      DCHECK(pd_sample.has_value());
      ibv_pd* pd = pd_sample.value();
      DCHECK(pd);
      ibv_qp* qp = CreateLocalRcQp(pd);
      DCHECK(qp);
      RcQpInfo qp_info = resource_manager_.GetRcQpInfo(qp);
      absl::Status result = ModifyRcQpResetToRts(
          qp, client_gids_.at(create_qp.initiator_id()),
          create_qp.initiator_qpn(), create_qp.initiator_id(),
          create_qp.initiator_pd_handle());
      CHECK_OK(result);  // Crash ok
      ClientUpdate out_update;
      out_update.set_destination_id(create_qp.initiator_id());
      ResponderCreateModifyRcQpRts* create_mod_rts =
          out_update.mutable_responder_create_modify_rc_qp_rts();
      create_mod_rts->set_responder_id(id_);
      create_mod_rts->set_responder_qpn(qp_info.qp->qp_num);
      create_mod_rts->set_initiator_qpn(create_qp.initiator_qpn());
      create_mod_rts->set_responder_pd_handle(pd->handle);
      PushOutboundUpdate(out_update);
      break;
    }
    case ClientUpdate::kResponderCreateModifyRcQpRts: {
      const ResponderCreateModifyRcQpRts& create_mod_rts =
          update.responder_create_modify_rc_qp_rts();
      RcQpInfo qp_info =
          resource_manager_.GetRcQpInfo(create_mod_rts.initiator_qpn());
      absl::Status result = ModifyRcQpResetToRts(
          qp_info.qp, client_gids_.at(create_mod_rts.responder_id()),
          create_mod_rts.responder_qpn(), create_mod_rts.responder_id(),
          create_mod_rts.responder_pd_handle());
      CHECK_OK(result);  // Crash ok
      ClientUpdate out_update;
      out_update.set_destination_id(create_mod_rts.responder_id());
      InitiatorModifyRcQpRts* mod_rts =
          out_update.mutable_initiator_modify_rc_qp_rts();
      mod_rts->set_responder_qpn(create_mod_rts.responder_qpn());
      PushOutboundUpdate(out_update);
      break;
    }
    case ClientUpdate::kInitiatorModifyRcQpRts: {
      const InitiatorModifyRcQpRts& mod_rts =
          update.initiator_modify_rc_qp_rts();
      RcQpInfo* qp_info =
          resource_manager_.GetMutableRcQpInfo(mod_rts.responder_qpn());
      // TODO(author2): The QP could have been asynchronously destroyed
      // resulting in a bad status. For now simply log the condition.  Update to
      // flush last RPC upon qp destruction.
      LOG_IF(WARNING, !qp_info)
          << "kInitiatorModifyRcQpRts failed. qpn=" << mod_rts.responder_qpn();
      break;
    }
    case ClientUpdate::kAddUdQp: {
      AddUdQp add_ud_qp = update.add_ud_qp();
      resource_manager_.InsertRemoteUdQp(add_ud_qp.owner_id(),
                                         add_ud_qp.qp_num(), add_ud_qp.q_key());
      break;
    }
    case ClientUpdate::kRemoveUdQp: {
      RemoveUdQp remove_ud_qp = update.remove_ud_qp();
      resource_manager_.EraseRemoteUdQp(remove_ud_qp.owner_id(),
                                        remove_ud_qp.qp_num());
      break;
    }
    default: {
    }
  }
}

void RandomWalkClient::FlushAllCompletionQueues() {
  std::vector<ibv_cq*> cqs = resource_manager_.GetAllCqs();
  for (const auto& cq : cqs) {
    DCHECK(cq);
    FlushCompletionQueue(cq);
  }
}

void RandomWalkClient::FlushCompletionQueue(ibv_cq* cq) {
  ibv_wc completion;
  while (true) {
    int count = ibv_poll_cq(cq, 1, &completion);
    if (count == 0) {
      break;
    }
    ProcessCompletion(completion);
  }
}

void RandomWalkClient::ProcessCompletion(ibv_wc completion) {
  log_.PushCompletion(completion);
  // Check for validity of error status.
  CHECK_LT(completion.status, stats_.completion_statuses.size());  // Crash ok
  ++stats_.completions;
  ++stats_.completion_statuses[completion.status];
  profiler_.RegisterCompletion(completion);
  auto result = resource_manager_.GetMutableQpInfo(completion.qp_num)
                    ->inflight_ops.erase(completion.wr_id);
  DCHECK_GT(result, 0ul) << "Cannot find " << completion.wr_id << "("
                         << magic_enum::enum_name(
                                DecodeAction(completion.wr_id))
                         << ").";

  if (completion.status != IBV_WC_SUCCESS) {
    return;
  }

  Action action = DecodeAction(completion.wr_id);

  switch (action) {
    case Action::BIND_TYPE_1_MW:
    case Action::BIND_TYPE_2_MW: {
      DCHECK_EQ(completion.opcode, IBV_WC_BIND_MW);
      BindOpsTracker::BindWr bind_args =
          bind_ops_.ExtractBindWr(completion.wr_id);
      ibv_mw* mw = bind_args.mw;
      ibv_mw_bind_info bind_info = bind_args.bind_info;
      ClientUpdate update;
      AddRKey* add_rkey = update.mutable_add_rkey();
      if (action == Action::BIND_TYPE_1_MW) {
        ++stats_.bind_type_1_mw_success;
        resource_manager_.InsertBoundType1Mw(mw, bind_info);

        add_rkey->set_addr(bind_info.addr);
        add_rkey->set_length(bind_info.length);
        add_rkey->set_rkey(mw->rkey);
        add_rkey->set_owner_id(id_);
        add_rkey->set_pd_handle(mw->pd->handle);
      } else {
        ++stats_.bind_type_2_mw_success;
        DCHECK(bind_args.rkey.has_value());
        uint32_t rkey = bind_args.rkey.value();
        resource_manager_.InsertBoundType2Mw(mw, bind_info, completion.qp_num);
        RcQpInfo* qp_info =
            resource_manager_.GetMutableRcQpInfo(completion.qp_num);
        DCHECK(qp_info);
        map_util::InsertOrDie(qp_info->type_2_mws, mw);

        add_rkey->set_addr(bind_info.addr);
        add_rkey->set_length(bind_info.length);
        add_rkey->set_rkey(rkey);
        add_rkey->set_owner_id(id_);
        add_rkey->set_qpn(completion.qp_num);
        add_rkey->set_pd_handle(mw->pd->handle);
        update.set_destination_id(qp_info->remote_qp->client_id);
      }
      PushOutboundUpdate(update);
      break;
    }
    case Action::SEND: {
      DCHECK_EQ(completion.opcode, IBV_WC_SEND);
      ++stats_.send_success;
      break;
    }
    case Action::SEND_WITH_INV: {
      DCHECK_EQ(completion.opcode, IBV_WC_SEND);
      ++stats_.send_with_inv;
      InvalidateOpsTracker::InvalidateWr invalidate =
          invalidate_ops_.ExtractInvalidateWr(completion.wr_id);
      // RKey might already been invalidated, either by remote deallocation of
      // MW or by a precedeed invalidation.
      resource_manager_.TryEraseRdmaMemory(invalidate.client_id,
                                           invalidate.rkey);
      break;
    }
    case Action::RECV: {
      DCHECK_EQ(completion.opcode, IBV_WC_RECV);
      ++stats_.recv_success;
      if (completion.wc_flags & IBV_WC_WITH_INV) {
        uint32_t rkey = completion.invalidated_rkey;
        auto mw_info_opt = resource_manager_.TryGetType2BindInfo(rkey);
        if (!mw_info_opt.has_value()) {
          // mw already deallocated.
          return;
        }
        Type2MwBindInfo mw_info = mw_info_opt.value();
        resource_manager_.EraseBoundType2Mw(rkey);
        ibv_mr* mr = mw_info.bind_info.mr;
        MrInfo* mr_info = resource_manager_.GetMutableMrInfo(mr);
        DCHECK(mr_info);
        map_util::CheckPresentAndErase(mr_info->bound_mws, mw_info.mw);
        RcQpInfo* qp_info =
            resource_manager_.GetMutableRcQpInfo(completion.qp_num);
        DCHECK(qp_info);
        map_util::CheckPresentAndErase(qp_info->type_2_mws, mw_info.mw);
        resource_manager_.InsertUnboundType2Mw(mw_info.mw);
      }
      break;
    }
    case Action::READ: {
      DCHECK_EQ(completion.opcode, IBV_WC_RDMA_READ);
      ++stats_.read_success;
      break;
    }
    case Action::WRITE: {
      DCHECK_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
      ++stats_.write_success;
      break;
    }
    case Action::FETCH_ADD: {
      DCHECK_EQ(completion.opcode, IBV_WC_FETCH_ADD);
      ++stats_.fetch_add_success;
      break;
    }
    case Action::COMP_SWAP: {
      DCHECK_EQ(completion.opcode, IBV_WC_COMP_SWAP);
      ++stats_.comp_swap_success;
      break;
    }
    default: {
      LOG(DFATAL) << magic_enum::enum_name(action)
                  << " should not receive completion.";
    }
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
