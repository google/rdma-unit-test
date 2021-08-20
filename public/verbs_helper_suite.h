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
#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_HELPER_SUITE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_HELPER_SUITE_H_

#include <cstdint>
#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "internal/verbs_allocator.h"
#include "internal/verbs_backend.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// VerbsHelperSuite bundles VerbsAllocator and VerbsBackend to provide
// a unified interface of frequently used helper function for rdma unit test.
// They includes:
//    (1) Allocation of ibverbs objects, such as PD, QP, CQ, etc.
//    (2) Automatic cleanup for objects in (1).
//    (3) Bring up QP for connections.
// The class is written mainly for user-friendliness purposes: providing a
// single interface free the user from the trouble of memorizing individual
// components of the backend and hide the coupling between VerbsAllocator and
// VerbsBackend over different transport types, and avoids potential
// inconsistency.
class VerbsHelperSuite {
 public:
  VerbsHelperSuite();
  // Movable but not copyable.
  VerbsHelperSuite(VerbsHelperSuite&& helper) = default;
  VerbsHelperSuite& operator=(VerbsHelperSuite&& helper) = default;
  VerbsHelperSuite(const VerbsHelperSuite& helper) = delete;
  VerbsHelperSuite& operator=(const VerbsHelperSuite& helper) = delete;
  ~VerbsHelperSuite() = default;

  // See VerbsBackend.
  absl::Status SetUpRcQp(ibv_qp* local_qp, const verbs_util::PortGid& local,
                         ibv_gid remote_gid, uint32_t remote_qpn);
  void SetUpSelfConnectedRcQp(ibv_qp* qp, const verbs_util::PortGid& local);
  void SetUpLoopbackRcQps(ibv_qp* qp1, ibv_qp* qp2,
                          const verbs_util::PortGid& local);
  absl::Status SetUpUdQp(ibv_qp* qp, const verbs_util::PortGid& local,
                         uint32_t qkey);
  absl::Status SetQpInit(ibv_qp* qp, uint8_t port);
  absl::Status SetQpRtr(ibv_qp* qp, const verbs_util::PortGid& local,
                        ibv_gid remote_gid, uint32_t remote_qpn);
  absl::Status SetQpRts(ibv_qp* qp);
  absl::Status SetQpRts(ibv_qp* qp, ibv_qp_attr custom_attr, int mask);
  absl::Status SetQpError(ibv_qp* qp);

  // See VerbsAllocator.
  RdmaMemBlock AllocBuffer(int pages, bool requires_shared_memory = false);
  RdmaMemBlock AllocAlignedBuffer(int pages,
                                  size_t alignment = verbs_util::kPageSize);
  RdmaMemBlock AllocBufferByBytes(
      size_t bytes, size_t alignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  absl::StatusOr<ibv_context*> OpenDevice(bool no_ipv6_for_gid = false);
  ibv_ah* CreateAh(ibv_pd* pd, ibv_gid remote_gid);
  int DestroyAh(ibv_ah* ah);
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
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq, ibv_srq* srq);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                   ibv_srq* srq = nullptr,
                   uint32_t max_send_wr = verbs_util::kDefaultMaxWr,
                   uint32_t max_recv_wr = verbs_util::kDefaultMaxWr,
                   ibv_qp_type qp_type = IBV_QPT_RC, int sig_all = 0);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr);
  int DestroyQp(ibv_qp* qp);
  verbs_util::PortGid GetLocalPortGid(ibv_context* context) const;

 private:
  std::unique_ptr<VerbsAllocator> allocator_;
  std::unique_ptr<VerbsBackend> backend_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_HELPER_SUITE_H_
