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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_IBV_RESOURCE_MANAGER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_IBV_RESOURCE_MANAGER_H_

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/types/optional.h"
#include "infiniband/verbs.h"
#include "random_walk/internal/sampling.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

// IbvResource tracks metadadta of IbVerbs resources, such as queue pairs,
// memory windows and memory regions. It provides methods for adding, removing
// and modifying these resources. It also features methods for sampling random
// objects subject to different constraints.
class IbvResourceManager {
 public:
  // Metadata for CQ.
  struct CqInfo {
    // References to the CQ.
    absl::flat_hash_set<ibv_qp*> send_qps;
    absl::flat_hash_set<ibv_qp*> recv_qps;
  };

  // Metadata for PD.
  struct PdInfo {
    // Reference to the PD.
    absl::flat_hash_set<ibv_qp*> rc_qps;
    absl::flat_hash_set<ibv_mr*> mrs;
    absl::flat_hash_set<ibv_mw*> type_1_mws;
    absl::flat_hash_set<ibv_mw*> type_2_mws;
    absl::flat_hash_set<ibv_ah*> ahs;
  };

  // Metadata for a bound type 1 MW.
  using Type1MwBindInfo = ibv_mw_bind_info;

  // Metadata for a bound type 2 MW.
  struct Type2MwBindInfo {
    ibv_mw* mw;
    ibv_mw_bind_info bind_info;
    uint32_t qp_num;
  };

  // Key for the Metadata for a remote RKey memory.
  struct RdmaMemoryKey {
    ClientId client_id;
    uint32_t rkey;
  };

  // Metadata for a remote memory with an RKey.
  struct RdmaMemory {
    ClientId client_id;
    uint32_t rkey;
    uint64_t addr;
    uint64_t length;
    uint32_t pd_handle;
    absl::optional<uint32_t> qp_num;

    template <typename H>
    friend H AbslHashValue(H h, const RdmaMemory& memory) {
      return H::combine(std::move(h), memory.client_id, memory.rkey);
    }

    template <typename H>
    friend H AbslHashValue(H h, const RdmaMemoryKey& key) {
      return H::combine(std::move(h), key.client_id, key.rkey);
    }

    bool operator==(const RdmaMemory& memory) const;
    bool operator==(const RdmaMemoryKey& key) const;
  };

  // Metadata for a remote RC QP.
  struct RemoteRcQpInfo {
    ClientId client_id;
    uint32_t qp_num;
    uint32_t pd_handle;
  };

  // Key for the metadata of remote UD QPs.
  struct RemoteUdQpKey {
    ClientId client_id;
    uint32_t qp_num;
  };

  // Metadata for a remote UD QP.
  struct RemoteUdQpInfo {
    ClientId client_id;
    uint32_t qp_num;
    uint32_t q_key;

    template <typename H>
    friend H AbslHashValue(H h, const RemoteUdQpInfo& ud_qp) {
      return H::combine(std::move(h), ud_qp.client_id, ud_qp.qp_num);
    }

    template <typename H>
    friend H AbslHashValue(H h, const RemoteUdQpKey& key) {
      return H::combine(std::move(h), key.client_id, key.qp_num);
    }

    bool operator==(const RemoteUdQpInfo& ud_qp) const;
    bool operator==(const RemoteUdQpKey& key) const;
  };

  // Metadata for a local QP, RC or UD.
  struct QpInfo {
    ibv_qp* qp;
    ibv_qp_cap cap;
    // In flight ops, keep tracked of by its wr_id.
    absl::flat_hash_set<uint64_t> inflight_ops;
  };

  // Metadata specific for a RC QP.
  struct RcQpInfo : public QpInfo {
    std::optional<RemoteRcQpInfo> remote_qp = std::nullopt;
    absl::flat_hash_set<ibv_mw*> type_2_mws;
  };

  // Metadata specific for a UD QP.
  struct UdQpInfo : public QpInfo {
    uint32_t qkey;
  };

  // Metadata for an MR.
  struct MrInfo {
    absl::flat_hash_set<ibv_mw*> bound_mws;
  };

  // Metadata for an AH.
  struct AhInfo {
    ClientId client_id;
  };

  IbvResourceManager() = default;
  // Movable but not copyable.
  IbvResourceManager(IbvResourceManager&& manager) = default;
  IbvResourceManager& operator=(IbvResourceManager&& manager) = default;
  IbvResourceManager(const IbvResourceManager& manager) = delete;
  IbvResourceManager& operator=(const IbvResourceManager& manager) = delete;
  ~IbvResourceManager() = default;

  // Inserts a CQ into the sampling pool.
  void InsertCq(ibv_cq* cq);
  // Returns a pointer to the CqInfo object for a CQ.
  // Returns nullptr when the PD is not in the sampling pool.
  CqInfo* GetMutableCqInfo(ibv_cq* cq);
  // Returns a CqInfo object for a CQ.
  CqInfo GetCqInfo(ibv_cq* cq) const;
  // Returns a Uniformly random CQ in the sampling pool.
  absl::optional<ibv_cq*> GetRandomCq() const;
  // Returns a Uniformly random CQ with no reference.
  absl::optional<ibv_cq*> GetRandomCqNoReference() const;
  void EraseCq(ibv_cq* cq);
  // Returns a vector consists of all CQs.
  std::vector<ibv_cq*> GetAllCqs() const;
  // Returns the total number of CQs in the pool.
  size_t CqCount() const;

  // Inserts a PD into the sampling pool.
  void InsertPd(ibv_pd* pd);
  // Returns a pointer to the PdInfo object for a PD.
  // Returns nullptr when the PD is not in the sampling pool.
  PdInfo* GetMutablePdInfo(ibv_pd* pd);
  // Returns a PdInfo object for a PD.
  PdInfo GetPdInfo(ibv_pd* pd) const;
  // Returns a uniformly random PD from the sampling pool.
  absl::optional<ibv_pd*> GetRandomPd() const;
  // Returns a uniformly random PD with zero reference, i.e. without any MR, MW
  // or QP on it.
  absl::optional<ibv_pd*> GetRandomPdNoReference() const;
  // Erases a PD from the sampling pool.
  void ErasePd(ibv_pd* pd);
  // Returns the total number of PDs in the pool.
  size_t PdCount() const;

  // Inserts an MR into the sampling pool.
  void InsertMr(ibv_mr* mr);
  // Returns a pointer to the MrInfo for an MR. Returns nullptr
  // if the MR doesn't exist in the sampling pool.
  MrInfo* GetMutableMrInfo(ibv_mr* mr);
  // Returns a MrInfo for an MR in the sampling pool.
  MrInfo GetMrInfo(ibv_mr* mr) const;
  // Returns a uniformly random MR from the sampling pool.
  absl::optional<ibv_mr*> GetRandomMr() const;
  // Returns a uniformly random MR with specific PD from the sampling pool.
  absl::optional<ibv_mr*> GetRandomMr(ibv_pd* pd) const;
  // Returns a uniformly random MR with zero reference.
  absl::optional<ibv_mr*> GetRandomMrNoReference() const;
  // Erases an MR from the sampling pool.
  void EraseMr(ibv_mr* mr);
  // Returns the total number of MRs in the pool.
  size_t MrCount() const;

  // Inserts an unbound type 1 MW into the sampling pool.
  void InsertUnboundType1Mw(ibv_mw* mw);
  // Inserts a bound type 1 MW into the sampling pool.
  void InsertBoundType1Mw(ibv_mw* mw, ibv_mw_bind_info bind_info);
  // Returns a Type1MwBindInfo for a type 1 MW.
  Type1MwBindInfo GetType1BindInfo(ibv_mw* mw) const;
  // Returns a uniformly random unbound type 1 MW.
  absl::optional<ibv_mw*> GetRandomUnboundType1Mw() const;
  // Returns a uniformly random unbound type 1 MW with specific PD.
  absl::optional<ibv_mw*> GetRandomUnboundType1Mw(ibv_pd* pd) const;
  // Returns a uniformly random bound type 1 MW.
  absl::optional<ibv_mw*> GetRandomBoundType1Mw() const;
  // Returns a uniformly random bound type 1 MW with specific PD.
  absl::optional<ibv_mw*> GetRandomBoundType1Mw(ibv_pd* pd) const;
  // Erases an unbound type 1 MW from the sampling pool.
  void EraseUnboundType1Mw(ibv_mw* mw);
  // Erases a bound type 1 MW from the sampling pool.
  void EraseBoundType1Mw(ibv_mw* mw);
  // Returns the total number of Type 1 MWs in the pool.
  size_t Type1MwCount() const;

  // Inserts an unbound type 2 MW into the sampling pool.
  void InsertUnboundType2Mw(ibv_mw* mw);
  // Inserts a bound type 2 MW into the sampling pool.
  void InsertBoundType2Mw(ibv_mw* mw, ibv_mw_bind_info bind_info,
                          uint32_t qp_num);
  // Returns a Type2MwBindInfo for a type 2 MW specified by its rkey.
  Type2MwBindInfo GetType2BindInfo(uint32_t rkey) const;
  // Returns a Type2MwBindInfo for a type 2 MW specified by its rkey if exists.
  // Otherwise, returns nullopt.
  absl::optional<Type2MwBindInfo> TryGetType2BindInfo(uint32_t rkey) const;
  // Returns a uniformly random unbound type 2 MW.
  absl::optional<ibv_mw*> GetRandomUnboundType2Mw() const;
  // Returns a uniformly random unbound type 2 MW.
  absl::optional<ibv_mw*> GetRandomUnboundType2Mw(ibv_pd* pd) const;
  // Returns a uniformly random bound type 2 MW.
  absl::optional<ibv_mw*> GetRandomBoundType2Mw() const;
  // Returns a uniformly random bound type 2 MW.
  absl::optional<ibv_mw*> GetRandomBoundType2Mw(ibv_pd* pd) const;
  // Erases an unbound type 2 MW from the sampling pool.
  void EraseUnboundType2Mw(ibv_mw* mw);
  // Erases a bound type 2 MW from the sampling pool, specified using its rkey.
  void EraseBoundType2Mw(uint32_t rkey);
  // Returns the total number of Type 2 MWs in the pool.
  size_t Type2MwCount() const;

  // Inserts a RdmaMemory into the sampling pool.
  void InsertRdmaMemory(ClientId client_id, uint32_t rkey, uint64_t addr,
                        uint64_t length, uint32_t pd_handle);
  void InsertRdmaMemory(ClientId client_id, uint32_t rkey, uint64_t addr,
                        uint64_t length, uint32_t pd_handle, uint32_t qp_num);
  // Returns a uniformly random RdmaMemory from the sampling pool.
  absl::optional<RdmaMemory> GetRandomRdmaMemory() const;
  // Returns a random RdmaMemory which corresponds to a remote bound type 2 MW.
  absl::optional<RdmaMemory> GetRandomRemoteBoundType2Mw() const;
  // Erases a RdmaMemory from the sampling pool.
  void EraseRdmaMemory(ClientId client_id, uint32_t rkey);
  // Erases a RdmaMemory from the sampling pool. Does not crash if the element
  // is not present.
  void TryEraseRdmaMemory(ClientId client_id, uint32_t rkey);

  // Inserts a RC QP into the sampling pool.
  void InsertRcQp(ibv_qp* qp, const ibv_qp_cap& cap);
  // Insert a UD QP into the sampling pool.
  void InsertUdQp(ibv_qp* qp, uint32_t qkey, ibv_qp_cap cap);
  // Returns a RcQpInfo.
  RcQpInfo GetRcQpInfo(ibv_qp* qp) const;
  RcQpInfo GetRcQpInfo(uint32_t qp_num) const;
  // Returns a UdQpInfo.
  UdQpInfo GetUdQpInfo(ibv_qp* qp) const;
  // Returns a QpInfo.
  QpInfo GetQpInfo(ibv_qp* qp) const;
  // Returns a pointer to a RcQpInfo.
  RcQpInfo* GetMutableRcQpInfo(ibv_qp* qp);
  RcQpInfo* GetMutableRcQpInfo(uint32_t qp_num);
  // Returns a pointer to a UdQPInfo.
  UdQpInfo* GetMutableUdQpInfo(ibv_qp* qp);
  UdQpInfo* GetMutableUdQpInfo(uint32_t qp_num);
  // Returns a pointier to a QpInfo.
  QpInfo* GetMutableQpInfo(ibv_qp* qp);
  QpInfo* GetMutableQpInfo(uint32_t qp_num);
  // Returns a random QP to modify to error. The QP must be:
  // 1. In RTS state.
  // 2. If the QP is RC, the corresponding remote QP must be once brought to
  //    RTS.
  // 3. If allow_outstanding_ops is false, qp have no outstanding ops.
  absl::optional<ibv_qp*> GetRandomQpForModifyError(
      bool allow_outstanding_ops) const;
  // Returns a random QP to carry out a bind op. The QP must be:
  // 1. RC QP.
  // 2. must be RTS.
  absl::optional<ibv_qp*> GetRandomQpForBind() const;
  // Returns a random QP to carry out messaging. The QP must be:
  // 1. In RTS state.
  // 2. If the QP is RC, the corresponding remote QP must be once brought to
  //    RTS.
  absl::optional<ibv_qp*> GetRandomQpForMessaging(ibv_qp_type qp_type) const;
  // Returns a random Qp to carry out RDMA and atomics. The QP must be:
  // 1. RC QP.
  // 2. The corresponding remote QP must be once brought to RTS.
  // 3. The client_id of the remote QP must match with the provided client_id.
  // 4. The PD handle of the remote QP must match with the provided pd_handle.
  absl::optional<ibv_qp*> GetRandomQpForRdma(ClientId client_id,
                                             uint32_t pd_handle) const;
  // Gets a random QP:
  // 1. In ERROR state.
  // 2. With no Type 2 MW bound to it.
  // 3. If allow_outstanding_ops is false, qp have no outstanding ops.
  absl::optional<ibv_qp*> GetRandomQpForDestroy(
      bool allow_outstanding_ops) const;
  // Returns the local RC QP connected to a specific remote QP, specified by the
  // remote cient_id and qp_num. Used specifically for Rdma/Atomics on type 2
  // MWs.
  ibv_qp* GetLocalRcQp(ClientId client_id, uint32_t remote_qpn) const;
  // Erases a QP from the sampling pool.
  void EraseQp(uint32_t qp_num, ibv_qp_type qp_type);
  // Returns the total number of QPs of specific type.
  uint32_t QpCount(ibv_qp_type qp_type) const;

  // Insert a remote UD QP into the sampling pool.
  void InsertRemoteUdQp(ClientId client_id, uint32_t qp_num, uint32_t qkey);
  // Returns an arbitrary remote UD QP info on a specific client.
  absl::optional<RemoteUdQpInfo> GetRandomRemoteUdQp(ClientId client_id) const;
  // Erase a remote UD QP from the sampling pool.
  void EraseRemoteUdQp(ClientId client_id, uint32_t qp_num);

  // Inserts an AH into the sampling pool.
  void InsertAh(ibv_ah* ah, ClientId client_id);
  // Returns the corresponding AhInfo for an AH.
  AhInfo GetAhInfo(ibv_ah* ah) const;
  // Returns a uniformly random Ah from the sampling pool,
  absl::optional<ibv_ah*> GetRandomAh() const;
  // Returns a uniformly random Ah from the sampling pool with particular PD.
  absl::optional<ibv_ah*> GetRandomAh(ibv_pd* pd) const;
  // Erases an AH from the sampling pool.
  void EraseAh(ibv_ah* ah);

 private:
  absl::flat_hash_map<ibv_cq*, CqInfo> cqs_;
  absl::flat_hash_map<ibv_pd*, PdInfo> pds_;
  absl::flat_hash_map<ibv_mr*, MrInfo> mrs_;
  absl::flat_hash_map<ibv_mw*, Type1MwBindInfo> type_1_mws_bound_;
  absl::flat_hash_set<ibv_mw*> type_1_mws_unbound_;
  absl::flat_hash_map<uint32_t, Type2MwBindInfo>
      type_2_mws_bound_;  // Key is rkey of the MW.
  absl::flat_hash_set<ibv_mw*> type_2_mws_unbound_;
  absl::flat_hash_map<uint32_t, RcQpInfo> rc_qps_;  // Key is qp number.
  absl::flat_hash_map<uint32_t, UdQpInfo> ud_qps_;  // Key is qp number.
  absl::flat_hash_set<RdmaMemory> rdma_memories_;
  absl::flat_hash_set<RemoteUdQpInfo> remote_ud_qps_;
  absl::flat_hash_map<ibv_ah*, AhInfo> ahs_;
  RandomWalkSampler sampler_;
};

};  // namespace random_walk
};  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_IBV_RESOURCE_MANAGER_H_
