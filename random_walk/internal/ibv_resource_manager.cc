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

#include "random_walk/internal/ibv_resource_manager.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/types/optional.h"
#include "infiniband/verbs.h"
#include "public/map_util.h"
#include "public/verbs_util.h"
#include "random_walk/internal/sampling.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

bool IbvResourceManager::RdmaMemory::operator==(
    const RdmaMemory& memory) const {
  return client_id == memory.client_id && rkey == memory.rkey;
}

bool IbvResourceManager::RdmaMemory::operator==(
    const RdmaMemoryKey& key) const {
  return client_id == key.client_id && rkey == key.rkey;
}

bool IbvResourceManager::RemoteUdQpInfo::operator==(
    const RemoteUdQpInfo& ud_qp) const {
  return client_id == ud_qp.client_id && qp_num == ud_qp.qp_num;
}

bool IbvResourceManager::RemoteUdQpInfo::operator==(
    const RemoteUdQpKey& key) const {
  return client_id == key.client_id && qp_num == key.qp_num;
}

void IbvResourceManager::InsertCq(ibv_cq* cq) {
  map_util::InsertOrDie(cqs_, cq, CqInfo());
}

IbvResourceManager::CqInfo* IbvResourceManager::GetMutableCqInfo(ibv_cq* cq) {
  return map_util::FindOrNull(cqs_, cq);
}

IbvResourceManager::CqInfo IbvResourceManager::GetCqInfo(ibv_cq* cq) const {
  return map_util::FindOrDie(cqs_, cq);
}

absl::optional<ibv_cq*> IbvResourceManager::GetRandomCq() const {
  return sampler_.GetRandomMapKey(cqs_);
}

absl::optional<ibv_cq*> IbvResourceManager::GetRandomCqNoReference() const {
  StreamSampler<ibv_cq*> stream_sampler;
  for (const auto& [cq, cq_info] : cqs_) {
    if (cq_info.send_qps.empty() && cq_info.recv_qps.empty()) {
      stream_sampler.UpdateSample(cq);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::EraseCq(ibv_cq* cq) {
  map_util::CheckPresentAndErase(cqs_, cq);
}

std::vector<ibv_cq*> IbvResourceManager::GetAllCqs() const {
  std::vector<ibv_cq*> cqs;
  for (const auto& [cq, cq_info] : cqs_) {
    cqs.push_back(cq);
  }
  return cqs;
}

size_t IbvResourceManager::CqCount() const { return cqs_.size(); }

void IbvResourceManager::InsertPd(ibv_pd* pd) {
  map_util::InsertOrDie(pds_, pd, PdInfo());
}

IbvResourceManager::PdInfo* IbvResourceManager::GetMutablePdInfo(ibv_pd* pd) {
  return map_util::FindOrNull(pds_, pd);
}

IbvResourceManager::PdInfo IbvResourceManager::GetPdInfo(ibv_pd* pd) const {
  return map_util::FindOrDie(pds_, pd);
}

absl::optional<ibv_pd*> IbvResourceManager::GetRandomPd() const {
  return sampler_.GetRandomMapKey(pds_);
}

absl::optional<ibv_pd*> IbvResourceManager::GetRandomPdNoReference() const {
  StreamSampler<ibv_pd*> stream_sampler;
  for (const auto& [pd, pd_info] : pds_) {
    if (pd_info.mrs.empty() && pd_info.type_1_mws.empty() &&
        pd_info.ahs.empty() && pd_info.type_2_mws.empty() &&
        pd_info.rc_qps.empty()) {
      stream_sampler.UpdateSample(pd);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::ErasePd(ibv_pd* pd) {
  map_util::CheckPresentAndErase(pds_, pd);
}

size_t IbvResourceManager::PdCount() const { return pds_.size(); }

void IbvResourceManager::InsertMr(ibv_mr* mr) {
  map_util::InsertOrDie(mrs_, mr, MrInfo());
}

IbvResourceManager::MrInfo* IbvResourceManager::GetMutableMrInfo(ibv_mr* mr) {
  return map_util::FindOrNull(mrs_, mr);
}

IbvResourceManager::MrInfo IbvResourceManager::GetMrInfo(ibv_mr* mr) const {
  return map_util::FindOrDie(mrs_, mr);
}

absl::optional<ibv_mr*> IbvResourceManager::GetRandomMr() const {
  return sampler_.GetRandomMapKey(mrs_);
}

absl::optional<ibv_mr*> IbvResourceManager::GetRandomMrNoReference() const {
  StreamSampler<ibv_mr*> stream_sampler;
  for (const auto& [mr, mr_info] : mrs_) {
    if (mr_info.bound_mws.empty()) {
      stream_sampler.UpdateSample(mr);
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_mr*> IbvResourceManager::GetRandomMr(ibv_pd* pd) const {
  StreamSampler<ibv_mr*> stream_sampler;
  for (const auto& [mr, mr_info] : mrs_) {
    if (mr->pd == pd) {
      stream_sampler.UpdateSample(mr);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::EraseMr(ibv_mr* mr) {
  map_util::CheckPresentAndErase(mrs_, mr);
}

size_t IbvResourceManager::MrCount() const { return mrs_.size(); }

void IbvResourceManager::InsertUnboundType1Mw(ibv_mw* mw) {
  DCHECK_EQ(mw->type, IBV_MW_TYPE_1);
  map_util::InsertOrDie(type_1_mws_unbound_, mw);
}

void IbvResourceManager::InsertBoundType1Mw(ibv_mw* mw,
                                            ibv_mw_bind_info bind_info) {
  DCHECK_EQ(mw->type, IBV_MW_TYPE_1);
  map_util::InsertOrDie(type_1_mws_bound_, mw, bind_info);
}

IbvResourceManager::Type1MwBindInfo IbvResourceManager::GetType1BindInfo(
    ibv_mw* mw) const {
  return map_util::FindOrDie(type_1_mws_bound_, mw);
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomUnboundType1Mw() const {
  return sampler_.GetRandomSetElement(type_1_mws_unbound_);
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomUnboundType1Mw(
    ibv_pd* pd) const {
  StreamSampler<ibv_mw*> stream_sampler;
  for (const auto& mw : type_1_mws_unbound_) {
    if (mw->pd == pd) {
      stream_sampler.UpdateSample(mw);
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomBoundType1Mw() const {
  return sampler_.GetRandomMapKey(type_1_mws_bound_);
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomBoundType1Mw(
    ibv_pd* pd) const {
  StreamSampler<ibv_mw*> stream_sampler;
  for (const auto& [mw, mw_info] : type_1_mws_bound_) {
    if (mw->pd == pd) {
      stream_sampler.UpdateSample(mw);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::EraseUnboundType1Mw(ibv_mw* mw) {
  map_util::CheckPresentAndErase(type_1_mws_unbound_, mw);
}

void IbvResourceManager::EraseBoundType1Mw(ibv_mw* mw) {
  map_util::CheckPresentAndErase(type_1_mws_bound_, mw);
}

size_t IbvResourceManager::Type1MwCount() const {
  return type_1_mws_bound_.size() + type_1_mws_unbound_.size();
}

void IbvResourceManager::InsertUnboundType2Mw(ibv_mw* mw) {
  map_util::InsertOrDie(type_2_mws_unbound_, mw);
}

void IbvResourceManager::InsertBoundType2Mw(ibv_mw* mw,
                                            ibv_mw_bind_info bind_info,
                                            uint32_t qp_num) {
  Type2MwBindInfo bind{.mw = mw, .bind_info = bind_info, .qp_num = qp_num};
  map_util::InsertOrDie(type_2_mws_bound_, mw->rkey, bind);
}

IbvResourceManager::Type2MwBindInfo IbvResourceManager::GetType2BindInfo(
    uint32_t rkey) const {
  return map_util::FindOrDie(type_2_mws_bound_, rkey);
}

absl::optional<IbvResourceManager::Type2MwBindInfo>
IbvResourceManager::TryGetType2BindInfo(uint32_t rkey) const {
  auto iter = type_2_mws_bound_.find(rkey);
  if (iter == type_2_mws_bound_.end()) {
    return absl::nullopt;
  }
  return iter->second;
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomUnboundType2Mw() const {
  return sampler_.GetRandomSetElement(type_2_mws_unbound_);
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomUnboundType2Mw(
    ibv_pd* pd) const {
  StreamSampler<ibv_mw*> stream_sampler;
  for (const auto& mw : type_2_mws_unbound_) {
    if (mw->pd == pd) {
      stream_sampler.UpdateSample(mw);
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomBoundType2Mw() const {
  auto sample = sampler_.GetRandomMapValue(type_2_mws_bound_);
  if (!sample.has_value()) {
    return absl::nullopt;
  }
  return sample.value().mw;
}

absl::optional<ibv_mw*> IbvResourceManager::GetRandomBoundType2Mw(
    ibv_pd* pd) const {
  StreamSampler<ibv_mw*> stream_sampler;
  for (const auto& [rkey, mw_info] : type_2_mws_bound_) {
    if (mw_info.mw->pd == pd) {
      stream_sampler.UpdateSample(mw_info.mw);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::EraseUnboundType2Mw(ibv_mw* mw) {
  map_util::CheckPresentAndErase(type_2_mws_unbound_, mw);
}

void IbvResourceManager::EraseBoundType2Mw(uint32_t rkey) {
  map_util::CheckPresentAndErase(type_2_mws_bound_, rkey);
}

size_t IbvResourceManager::Type2MwCount() const {
  return type_2_mws_bound_.size() + type_2_mws_unbound_.size();
}

void IbvResourceManager::InsertRdmaMemory(ClientId client_id, uint32_t rkey,
                                          uint64_t addr, uint64_t length,
                                          uint32_t pd_handle) {
  RdmaMemory value{.client_id = client_id,
                   .rkey = rkey,
                   .addr = addr,
                   .length = length,
                   .pd_handle = pd_handle,
                   .qp_num = absl::nullopt};
  map_util::InsertOrDie(rdma_memories_, value);
}

void IbvResourceManager::InsertRdmaMemory(ClientId client_id, uint32_t rkey,
                                          uint64_t addr, uint64_t length,
                                          uint32_t pd_handle, uint32_t qp_num) {
  RdmaMemory memory{.client_id = client_id,
                    .rkey = rkey,
                    .addr = addr,
                    .length = length,
                    .pd_handle = pd_handle,
                    .qp_num = qp_num};
  map_util::InsertOrDie(rdma_memories_, memory);
}

absl::optional<IbvResourceManager::RdmaMemory>
IbvResourceManager::GetRandomRdmaMemory() const {
  return sampler_.GetRandomSetElement(rdma_memories_);
}

absl::optional<IbvResourceManager::RdmaMemory>
IbvResourceManager::GetRandomRemoteBoundType2Mw() const {
  StreamSampler<RdmaMemory> stream_sampler;
  for (const auto& memory : rdma_memories_) {
    if (memory.qp_num.has_value()) {
      stream_sampler.UpdateSample(memory);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::EraseRdmaMemory(ClientId client_id, uint32_t rkey) {
  map_util::CheckPresentAndErase(rdma_memories_, {client_id, rkey});
}

void IbvResourceManager::TryEraseRdmaMemory(ClientId client_id, uint32_t rkey) {
  rdma_memories_.erase({client_id, rkey});
}

void IbvResourceManager::InsertRcQp(ibv_qp* qp, const ibv_qp_cap& cap) {
  RcQpInfo rc_qp_info;
  rc_qp_info.qp = qp;
  rc_qp_info.cap = cap;
  map_util::InsertOrDie(rc_qps_, qp->qp_num, rc_qp_info);
}

void IbvResourceManager::InsertUdQp(ibv_qp* qp, uint32_t qkey, ibv_qp_cap cap) {
  UdQpInfo ud_qp_info;
  ud_qp_info.qp = qp;
  ud_qp_info.cap = cap;
  ud_qp_info.qkey = qkey;
  map_util::InsertOrDie(ud_qps_, qp->qp_num, ud_qp_info);
}

IbvResourceManager::RcQpInfo IbvResourceManager::GetRcQpInfo(ibv_qp* qp) const {
  return GetRcQpInfo(qp->qp_num);
}

IbvResourceManager::RcQpInfo IbvResourceManager::GetRcQpInfo(
    uint32_t qp_num) const {
  return map_util::FindOrDie(rc_qps_, qp_num);
}

IbvResourceManager::UdQpInfo IbvResourceManager::GetUdQpInfo(ibv_qp* qp) const {
  return map_util::FindOrDie(ud_qps_, qp->qp_num);
}

IbvResourceManager::QpInfo IbvResourceManager::GetQpInfo(ibv_qp* qp) const {
  switch (qp->qp_type) {
    case (IBV_QPT_RC): {
      return map_util::FindOrDie(rc_qps_, qp->qp_num);
    }
    case (IBV_QPT_UD): {
      return map_util::FindOrDie(ud_qps_, qp->qp_num);
    }
    default: {
      LOG(FATAL) << "Unknown QP type " << qp->qp_type;  // Crash ok
    }
  }
}

IbvResourceManager::RcQpInfo* IbvResourceManager::GetMutableRcQpInfo(
    ibv_qp* qp) {
  return GetMutableRcQpInfo(qp->qp_num);
}

IbvResourceManager::RcQpInfo* IbvResourceManager::GetMutableRcQpInfo(
    uint32_t qp_num) {
  return map_util::FindOrNull(rc_qps_, qp_num);
}

IbvResourceManager::UdQpInfo* IbvResourceManager::GetMutableUdQpInfo(
    ibv_qp* qp) {
  return GetMutableUdQpInfo(qp->qp_num);
}

IbvResourceManager::UdQpInfo* IbvResourceManager::GetMutableUdQpInfo(
    uint32_t qp_num) {
  return map_util::FindOrNull(ud_qps_, qp_num);
}

IbvResourceManager::QpInfo* IbvResourceManager::GetMutableQpInfo(ibv_qp* qp) {
  switch (qp->qp_type) {
    case (IBV_QPT_RC): {
      return map_util::FindOrNull(rc_qps_, qp->qp_num);
    }
    case (IBV_QPT_UD): {
      return map_util::FindOrNull(ud_qps_, qp->qp_num);
    }
    default: {
      LOG(FATAL) << "Unknown QP type " << qp->qp_type;
      return nullptr;
    }
  }
}

IbvResourceManager::QpInfo* IbvResourceManager::GetMutableQpInfo(
    uint32_t qp_num) {
  QpInfo* qp_info = map_util::FindOrNull(rc_qps_, qp_num);
  if (qp_info == nullptr) {
    qp_info = map_util::FindOrNull(ud_qps_, qp_num);
  }
  return qp_info;
}

absl::optional<ibv_qp*> IbvResourceManager::GetRandomQpForModifyError(
    bool allow_outstanding_ops) const {
  StreamSampler<ibv_qp*> stream_sampler;
  for (const auto& [qp_num, qp_info] : rc_qps_) {
    if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_RTS &&
        qp_info.remote_qp.has_value() &&
        (allow_outstanding_ops || qp_info.inflight_ops.empty())) {
      stream_sampler.UpdateSample(qp_info.qp);
    }
  }
  for (const auto& [qp_num, qp_info] : ud_qps_) {
    if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_RTS &&
        (allow_outstanding_ops || qp_info.inflight_ops.empty())) {
      stream_sampler.UpdateSample(qp_info.qp);
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_qp*> IbvResourceManager::GetRandomQpForBind() const {
  StreamSampler<ibv_qp*> stream_sampler;
  for (const auto& [qp_num, qp_info] : rc_qps_) {
    if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_RTS &&
        qp_info.remote_qp.has_value()) {
      stream_sampler.UpdateSample(qp_info.qp);
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_qp*> IbvResourceManager::GetRandomQpForMessaging(
    ibv_qp_type qp_type) const {
  StreamSampler<ibv_qp*> stream_sampler;
  switch (qp_type) {
    case (IBV_QPT_RC): {
      for (const auto& [qp_num, qp_info] : rc_qps_) {
        if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_RTS &&
            qp_info.remote_qp.has_value()) {
          stream_sampler.UpdateSample(qp_info.qp);
        }
      }
      break;
    }
    case (IBV_QPT_UD): {
      for (const auto& [qp_num, qp_info] : ud_qps_) {
        if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_RTS) {
          stream_sampler.UpdateSample(qp_info.qp);
        }
      }
      break;
    }
    default: {
      LOG(FATAL) << "Unknown QP type " << qp_type;
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_qp*> IbvResourceManager::GetRandomQpForRdma(
    ClientId client_id, uint32_t pd_handle) const {
  StreamSampler<ibv_qp*> stream_sampler;
  for (const auto& [qp_num, qp_info] : rc_qps_) {
    if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_RTS &&
        qp_info.remote_qp.has_value() &&
        qp_info.remote_qp->client_id == client_id &&
        qp_info.remote_qp->pd_handle == pd_handle) {
      DCHECK_EQ(qp_info.qp->qp_type, IBV_QPT_RC);
      stream_sampler.UpdateSample(qp_info.qp);
    }
  }
  return stream_sampler.ExtractSample();
}

absl::optional<ibv_qp*> IbvResourceManager::GetRandomQpForDestroy(
    bool allow_outstanding_ops) const {
  StreamSampler<ibv_qp*> stream_sampler;
  for (const auto& [qp_num, qp_info] : rc_qps_) {
    if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_ERR &&
        qp_info.type_2_mws.empty() &&
        (allow_outstanding_ops || qp_info.inflight_ops.empty())) {
      stream_sampler.UpdateSample(qp_info.qp);
    }
  }
  for (const auto& [qp_num, qp_info] : ud_qps_) {
    if (verbs_util::GetQpState(qp_info.qp) == IBV_QPS_ERR &&
        (allow_outstanding_ops || qp_info.inflight_ops.empty())) {
      stream_sampler.UpdateSample(qp_info.qp);
    }
  }
  return stream_sampler.ExtractSample();
}

ibv_qp* IbvResourceManager::GetLocalRcQp(ClientId client_id,
                                         uint32_t remote_qpn) const {
  for (const auto& [qpn, qp_info] : rc_qps_) {
    if (qp_info.remote_qp.has_value() &&
        qp_info.remote_qp->client_id == client_id &&
        qp_info.remote_qp->qp_num == remote_qpn) {
      return qp_info.qp;
    }
  }
  return nullptr;
}

void IbvResourceManager::EraseQp(uint32_t qp_num, ibv_qp_type qp_type) {
  switch (qp_type) {
    case (IBV_QPT_RC): {
      map_util::CheckPresentAndErase(rc_qps_, qp_num);
      break;
    }
    case (IBV_QPT_UD): {
      map_util::CheckPresentAndErase(ud_qps_, qp_num);
      break;
    }
    default: {
      LOG(FATAL) << "Unknown QP type: " << qp_type;
    }
  }
}

uint32_t IbvResourceManager::QpCount(ibv_qp_type qp_type) const {
  switch (qp_type) {
    case (IBV_QPT_RC): {
      return rc_qps_.size();
    }
    case (IBV_QPT_UD): {
      return ud_qps_.size();
    }
    default: {
      LOG(FATAL) << "Unknown QP type :" << qp_type;
      return 0;
    }
  }
}

void IbvResourceManager::InsertRemoteUdQp(ClientId client_id, uint32_t qp_num,
                                          uint32_t qkey) {
  RemoteUdQpInfo qp_info{
      .client_id = client_id, .qp_num = qp_num, .q_key = qkey};
  map_util::InsertOrDie(remote_ud_qps_, qp_info);
}

void IbvResourceManager::EraseRemoteUdQp(ClientId client_id, uint32_t qp_num) {
  map_util::CheckPresentAndErase(remote_ud_qps_, {client_id, qp_num});
}

absl::optional<IbvResourceManager::RemoteUdQpInfo>
IbvResourceManager::GetRandomRemoteUdQp(ClientId client_id) const {
  StreamSampler<RemoteUdQpInfo> stream_sampler;
  for (const auto& remote_ud_info : remote_ud_qps_) {
    if (remote_ud_info.client_id == client_id) {
      stream_sampler.UpdateSample(remote_ud_info);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::InsertAh(ibv_ah* ah, ClientId client_id) {
  AhInfo ah_info{.client_id = client_id};
  map_util::InsertOrDie(ahs_, ah, ah_info);
}

IbvResourceManager::AhInfo IbvResourceManager::GetAhInfo(ibv_ah* ah) const {
  return map_util::FindOrDie(ahs_, ah);
}

absl::optional<ibv_ah*> IbvResourceManager::GetRandomAh() const {
  return sampler_.GetRandomMapKey(ahs_);
}

absl::optional<ibv_ah*> IbvResourceManager::GetRandomAh(ibv_pd* pd) const {
  StreamSampler<ibv_ah*> stream_sampler;
  for (const auto& [ah, ah_info] : ahs_) {
    if (ah->pd == pd) {
      stream_sampler.UpdateSample(ah);
    }
  }
  return stream_sampler.ExtractSample();
}

void IbvResourceManager::EraseAh(ibv_ah* ah) {
  map_util::CheckPresentAndErase(ahs_, ah);
}

}  // namespace random_walk
}  // namespace rdma_unit_test
