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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_VERBS_ALLOCATOR_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_VERBS_ALLOCATOR_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "infiniband/verbs.h"
#include "public/rdma-memblock.h"
#include "public/util.h"

namespace rdma_unit_test {

// This is a thread-safe class that provides smart creation and deletion of
// ibverbs objects such as Queue Pairs, Memory Regions, Memory Windows, etc.
// Objects that are created are stored and automatically
// destroyed with libibverbs calls.
class VerbsAllocator {
 public:
  VerbsAllocator() = default;
  virtual ~VerbsAllocator() = default;

  // Set of helpers for automatically cleaning up objects when the tracker is
  // torn down.
  static void ContextDeleter(ibv_context* context);
  static void AhDeleter(ibv_ah* ah);
  static void MrDeleter(ibv_mr* mr);
  static void MwDeleter(ibv_mw* mw);
  static void PdDeleter(ibv_pd* pd);
  static void ChannelDeleter(ibv_comp_channel* channel);
  static void CqDeleter(ibv_cq* cq);
  static void SrqDeleter(ibv_srq* srq);
  static void QpDeleter(ibv_qp* qp);

  RdmaMemBlock AllocBuffer(int pages, bool requires_shared_memory = false);
  RdmaMemBlock AllocAlignedBuffer(int pages,
                                  size_t alignment = verbs_util::kPageSize);
  RdmaMemBlock AllocBufferByBytes(
      size_t bytes, size_t alignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  // Opens an ibv device. Uses the first device listed.
  absl::StatusOr<ibv_context*> OpenDevice(bool no_ipv6_for_gid = false);
  ibv_ah* CreateAh(ibv_pd* pd);
  ibv_pd* AllocPd(ibv_context* context);
  int DeallocPd(ibv_pd* pd);
  ibv_mr* RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC |
                             IBV_ACCESS_MW_BIND);
  int DeregMr(ibv_mr* mr);
  ibv_mw* AllocMw(ibv_pd* pd, ibv_mw_type type);
  int DeallocMw(ibv_mw* mw);
  ibv_comp_channel* CreateChannel(ibv_context* context);
  int DestroyChannel(ibv_comp_channel* channel);
  ibv_cq* CreateCq(ibv_context* context, int max_wr = verbs_util::kDefaultMaxWr,
                   ibv_comp_channel* channel = nullptr);
  int DestroyCq(ibv_cq* cq);
  ibv_srq* CreateSrq(ibv_pd* pd, uint32_t max_wr = verbs_util::kDefaultMaxWr);
  ibv_srq* CreateSrq(ibv_pd* pd, ibv_srq_init_attr& attr);
  int DestroySrq(ibv_srq* srq);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* cq);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* cq, ibv_srq* srq);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq, ibv_srq* srq,
                   uint32_t max_send_wr, uint32_t max_recv_wr,
                   ibv_qp_type qp_type, int sig_all);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr);
  int DestroyQp(ibv_qp* qp);
  // Returns the first available GID from the device context.
  verbs_util::LocalVerbsAddress GetContextAddressInfo(
      ibv_context* context) const;

 private:
  // This only creates Ah/Qp/Mr without putting it into qps_ for auto-cleanup.
  virtual ibv_mr* RegMrInternal(ibv_pd* pd, const RdmaMemBlock& memblock,
                                int access) = 0;
  virtual ibv_ah* CreateAhInternal(ibv_pd* pd) = 0;
  virtual ibv_qp* CreateQpInternal(ibv_pd* pd,
                                   ibv_qp_init_attr& basic_attr) = 0;

  std::vector<std::unique_ptr<RdmaMemBlock>> memblocks_
      ABSL_GUARDED_BY(mtx_memblocks_);
  std::vector<std::unique_ptr<ibv_context, decltype(&ContextDeleter)>> contexts_
      ABSL_GUARDED_BY(mtx_contexts_);
  std::vector<std::unique_ptr<ibv_pd, decltype(&PdDeleter)>> pds_
      ABSL_GUARDED_BY(mtx_pds_);
  std::vector<std::unique_ptr<ibv_ah, decltype(&AhDeleter)>> ahs_
      ABSL_GUARDED_BY(mtx_ahs_);
  std::vector<std::unique_ptr<ibv_comp_channel, decltype(&ChannelDeleter)>>
      channels_ ABSL_GUARDED_BY(mtx_channels_);
  std::vector<std::unique_ptr<ibv_cq, decltype(&CqDeleter)>> cqs_
      ABSL_GUARDED_BY(mtx_cqs_);
  std::vector<std::unique_ptr<ibv_srq, decltype(&SrqDeleter)>> srqs_
      ABSL_GUARDED_BY(mtx_srqs_);
  std::vector<std::unique_ptr<ibv_qp, decltype(&QpDeleter)>> qps_
      ABSL_GUARDED_BY(mtx_qps_);
  std::vector<std::unique_ptr<ibv_mr, decltype(&MrDeleter)>> mrs_
      ABSL_GUARDED_BY(mtx_mrs_);
  std::vector<std::unique_ptr<ibv_mw, decltype(&MwDeleter)>> mws_
      ABSL_GUARDED_BY(mtx_mws_);
  // Tracks address info for a given context.
  absl::flat_hash_map<ibv_context*, std::vector<verbs_util::LocalVerbsAddress>>
      address_info_ ABSL_GUARDED_BY(mtx_address_info_);

  // locks for containers above.
  absl::Mutex mtx_memblocks_;
  absl::Mutex mtx_contexts_;
  absl::Mutex mtx_pds_;
  absl::Mutex mtx_ahs_;
  absl::Mutex mtx_channels_;
  absl::Mutex mtx_cqs_;
  absl::Mutex mtx_srqs_;
  absl::Mutex mtx_qps_;
  absl::Mutex mtx_mrs_;
  absl::Mutex mtx_mws_;
  mutable absl::Mutex mtx_address_info_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_VERBS_ALLOCATOR_H_
