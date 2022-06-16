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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "internal/verbs_cleanup.h"
#include "internal/verbs_extension.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// Attributes of a port obtained from ibv_query_port.
// Each port can be configured with multiple GID (Global Identifier, ibv_gid).
// Each PortAttributes represent one GID and the associated port's attribute.
struct PortAttribute {
  uint8_t port;
  ibv_gid gid;
  int gid_index;
  ibv_port_attr attr;
};

// A class of helper functions used for libibverbs test.
// VerbsHelperSuite is an attempt to improve test readability by making
// ibverbs objects "RAII". The idea is to use a set of unique_ptr to store all
// ibverbs objects created so that it automatically tears down as the test
// terminates. The class also abstacts away transport specific extension to
// ibverbs so that the same libibverbs test can run on multiple different
// extensions. The class function includes includes:
//    (1) Allocation of ibverbs objects, such as PD, QP, CQ, etc.
//    (2) Automatic cleanup for objects in (1).
//    (3) Bring up QP for connections.
class VerbsHelperSuite {
 public:
  VerbsHelperSuite();
  // Movable but not copyable.
  VerbsHelperSuite(VerbsHelperSuite&& helper) = default;
  VerbsHelperSuite& operator=(VerbsHelperSuite&& helper) = default;
  VerbsHelperSuite(const VerbsHelperSuite& helper) = delete;
  VerbsHelperSuite& operator=(const VerbsHelperSuite& helper) = delete;
  virtual ~VerbsHelperSuite() = default;

  // Modify a RC `local_qp` from RESET state to RTS state.
  absl::Status ModifyRcQpResetToRts(ibv_qp* local_qp,
                                    const PortAttribute& local,
                                    ibv_gid remote_gid, uint32_t remote_qpn,
                                    QpAttribute qp_attr = QpAttribute());
  // Modify a RC `source_qp` from RESET state to RTS state connecting to
  // `destination_qp`.
  absl::Status ModifyLoopbackRcQpResetToRts(
      ibv_qp* source_qp, ibv_qp* destination_qp, const PortAttribute& port_attr,
      QpAttribute qp_attr = QpAttribute());
  // Modify a RC `qp` from RESET to INIT state.
  int ModifyRcQpResetToInit(ibv_qp* qp, uint8_t port,
                            QpAttribute qp_attr = QpAttribute());
  // Modify a RC `qp` from INIT to RTR state.
  int ModifyRcQpInitToRtr(ibv_qp* qp, const PortAttribute& local,
                          ibv_gid remote_gid, uint32_t remote_qpn,
                          QpAttribute qp_attr = QpAttribute());
  // Modify a RC `qp` from RTR to RTS state.
  int ModifyRcQpRtrToRts(ibv_qp* qp, QpAttribute qp_attr = QpAttribute());
  // Modify two RC QPs `qp_1` and `qp_2` from RESET state to RTS state.
  absl::Status SetUpLoopbackRcQps(ibv_qp* source_qp, ibv_qp* destination_qp,
                                  const PortAttribute& port_attr,
                                  QpAttribute qp_attr = QpAttribute());
  // Modify a UD `qp` from RESET to RTS state.
  absl::Status ModifyUdQpResetToRts(ibv_qp* qp, const PortAttribute& local,
                                    uint32_t qkey,
                                    QpAttribute qp_attr = QpAttribute());
  // Modify a UD `qp` from RESET to RTS state.
  absl::Status ModifyUdQpResetToRts(ibv_qp* qp, uint32_t qkey,
                                    QpAttribute qp_attr = QpAttribute());
  // Modify `qp` with user supplied attributes.
  int ModifyQp(ibv_qp* qp, ibv_qp_attr& attr, int mask) const;
  // Modify `qp` to ERROR state.
  absl::Status ModifyQpToError(ibv_qp* qp) const;
  // Modify `qp` to RESET state.
  absl::Status ModifyQpToReset(ibv_qp* qp) const;

  // Helper functions to create/destroy objects which will be automatically
  // cleaned up when VerbsHelperSuite is destroyed.
  RdmaMemBlock AllocBuffer(int pages, bool requires_shared_memory = false);
  RdmaMemBlock AllocAlignedBuffer(int pages, size_t alignment = kPageSize);
  RdmaMemBlock AllocHugepageBuffer(int pages);
  RdmaMemBlock AllocAlignedBufferByBytes(
      size_t bytes, size_t alignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__,
      bool huge_page = false);
  absl::StatusOr<ibv_context*> OpenDevice();

  // Creates an address handle.
  ibv_ah* CreateAh(ibv_pd* pd, uint8_t port, uint8_t sgid_index,
                   ibv_gid remote_gid, AhAttribute ah_attr = AhAttribute());

  // Creates a loopback address handle with the same source and destination
  // port, specified by `port_attr`.
  ibv_ah* CreateLoopbackAh(ibv_pd* pd, const PortAttribute& port_attr,
                           AhAttribute ah_attr = AhAttribute());

  // Creates an address handle.
  ibv_ah* CreateAh(ibv_pd* pd, ibv_ah_attr& attr);

  // Destroy an address handle.
  int DestroyAh(ibv_ah* ah);

  ibv_pd* AllocPd(ibv_context* context);
  int DeallocPd(ibv_pd* pd);
  ibv_mr* RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC |
                             IBV_ACCESS_MW_BIND);
  int ReregMr(ibv_mr* mr, int flags, ibv_pd* pd, const RdmaMemBlock* memblock,
              int access);
  int DeregMr(ibv_mr* mr);
  ibv_mw* AllocMw(ibv_pd* pd, ibv_mw_type type);
  int DeallocMw(ibv_mw* mw);
  ibv_comp_channel* CreateChannel(ibv_context* context);
  int DestroyChannel(ibv_comp_channel* channel);
  ibv_cq* CreateCq(ibv_context* context, int cqe = verbs_util::kDefaultMaxWr,
                   ibv_comp_channel* channel = nullptr);
  int DestroyCq(ibv_cq* cq);
  ibv_cq_ex* CreateCqEx(ibv_context* context, ibv_cq_init_attr_ex& cq_attr);
  ibv_cq_ex* CreateCqEx(ibv_context* context,
                        uint32_t cqe = verbs_util::kDefaultMaxWr);
  int DestroyCqEx(ibv_cq_ex* cq_ex);
  ibv_srq* CreateSrq(ibv_pd* pd, uint32_t max_wr = 200, uint32_t max_sge = 1);
  ibv_srq* CreateSrq(ibv_pd* pd, ibv_srq_init_attr& attr);
  int DestroySrq(ibv_srq* srq);
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* cq, ibv_qp_type type = IBV_QPT_RC,
                   QpInitAttribute qp_init_attr = QpInitAttribute());
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                   ibv_qp_type type = IBV_QPT_RC,
                   QpInitAttribute qp_init_attr = QpInitAttribute());
  ibv_qp* CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq, ibv_srq* srq,
                   ibv_qp_type type = IBV_QPT_RC,
                   QpInitAttribute qp_init_attr = QpInitAttribute());
  ibv_qp* CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr);
  int DestroyQp(ibv_qp* qp);

  // Returns a PortAttribute for an opened device.
  PortAttribute GetPortAttribute(ibv_context* context) const;

  // Returns a reference the already initialized ibverbs extension interface.
  // This is for any user which requires runtime indirection for different
  // ibverbs extensions but does not want the overhead/synchronization incurred
  // by the automatic deletion setup.
  VerbsExtension& extension();

  // Returns the VerbsCleanup object for registering for auto-deletion.
  VerbsCleanup& clean_up();

 private:
  // Enumerates all possible GID for all ports under a device. Each GID will be
  // a separate entry.
  static absl::StatusOr<std::vector<PortAttribute>> EnumeratePorts(
      ibv_context* context);

  // Tracks RdmaMemblocks to make sure it outlive MRs.
  std::vector<std::unique_ptr<RdmaMemBlock>> memblocks_
      ABSL_GUARDED_BY(mtx_memblocks_);
  // Tracks address info for a given context.
  absl::flat_hash_map<ibv_context*, std::vector<PortAttribute>> port_attrs_
      ABSL_GUARDED_BY(mtx_port_attrs_);

  // locks for containers above.
  absl::Mutex mtx_memblocks_;
  mutable absl::Mutex mtx_port_attrs_;

  std::unique_ptr<VerbsExtension> extension_;
  VerbsCleanup cleanup_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_HELPER_SUITE_H_
