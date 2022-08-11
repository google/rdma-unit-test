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

#include "public/verbs_helper_suite.h"

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include <magic_enum.hpp>
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "internal/verbs_cleanup.h"
#include "internal/verbs_extension.h"
#include "public/flags.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

VerbsHelperSuite::VerbsHelperSuite()
    : extension_([]() -> std::unique_ptr<VerbsExtension> {
        return std::make_unique<VerbsExtension>();
      }()) {}

absl::Status VerbsHelperSuite::ModifyRcQpResetToRts(ibv_qp* local_qp,
                                                    const PortAttribute& local,
                                                    ibv_gid remote_gid,
                                                    uint32_t remote_qpn,
                                                    QpAttribute qp_attr) {
  int result_code = ModifyRcQpResetToInit(local_qp, local.port, qp_attr);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from RESET to INIT failed (%d).", result_code));
  }
  result_code =
      ModifyRcQpInitToRtr(local_qp, local, remote_gid, remote_qpn, qp_attr);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from INIT to RTR failed (%d).", result_code));
  }
  result_code = ModifyRcQpRtrToRts(local_qp, qp_attr);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from RTR to RTS failed (%d).", result_code));
  }
  return absl::OkStatus();
}

absl::Status VerbsHelperSuite::ModifyLoopbackRcQpResetToRts(
    ibv_qp* source_qp, ibv_qp* destination_qp, const PortAttribute& port_attr,
    QpAttribute qp_attr) {
  return ModifyRcQpResetToRts(source_qp, port_attr, port_attr.gid,
                              destination_qp->qp_num, qp_attr);
}

int VerbsHelperSuite::ModifyRcQpResetToInit(ibv_qp* qp, uint8_t port,
                                            QpAttribute qp_attr) {
  ibv_qp_attr mod_init = qp_attr.GetRcResetToInitAttr(port);
  int mask = qp_attr.GetRcResetToInitMask();

  int result_code = ibv_modify_qp(qp, &mod_init, mask);
  VLOG(1) << absl::StrFormat("Modify QP (%p) from RESET to INIT (%d).", qp,
                             result_code);
  return result_code;
}

int VerbsHelperSuite::ModifyRcQpInitToRtr(ibv_qp* qp,
                                          const PortAttribute& local,
                                          ibv_gid remote_gid,
                                          uint32_t remote_qpn,
                                          QpAttribute qp_attr) {
  ibv_qp_attr mod_rtr = qp_attr.GetRcInitToRtrAttr(local.port, local.gid_index,
                                                   remote_gid, remote_qpn);
  int mask = qp_attr.GetRcInitToRtrMask();
  LOG_IF(DFATAL, mod_rtr.path_mtu > local.attr.active_mtu)  // Crash OK
      << absl::StrFormat("Path mtu %d bigger than port active mtu %d.",
                         128 << mod_rtr.path_mtu, 128 << local.attr.active_mtu);
  int result_code = extension().ModifyRcQpInitToRtr(qp, mod_rtr, mask);
  VLOG(1) << absl::StrFormat("Modified QP (%p) from INIT to RTR (%d).", qp,
                             result_code);
  return result_code;
}

int VerbsHelperSuite::ModifyRcQpRtrToRts(ibv_qp* qp, QpAttribute qp_attr) {
  ibv_qp_attr mod_rts = qp_attr.GetRcRtrToRtsAttr();
  int mask = qp_attr.GetRcRtrToRtsMask();
  int result_code = ibv_modify_qp(qp, &mod_rts, mask);
  VLOG(1) << absl::StrFormat("Modify QP (%p) from RTR to RTS (%d).", qp,
                             result_code);
  return result_code;
}

absl::Status VerbsHelperSuite::SetUpLoopbackRcQps(
    ibv_qp* source_qp, ibv_qp* destination_qp, const PortAttribute& port_attr,
    QpAttribute qp_attr) {
  RETURN_IF_ERROR(ModifyRcQpResetToRts(source_qp, port_attr, port_attr.gid,
                                       destination_qp->qp_num, qp_attr));
  return ModifyRcQpResetToRts(destination_qp, port_attr, port_attr.gid,
                              source_qp->qp_num, qp_attr);
}

absl::Status VerbsHelperSuite::ModifyUdQpResetToRts(ibv_qp* qp,
                                                    const PortAttribute& local,
                                                    uint32_t qkey,
                                                    QpAttribute qp_attr) {
  ibv_qp_attr mod_init = qp_attr.GetUdResetToInitAttr(local.port, qkey);
  int mask = qp_attr.GetUdResetToInitMask();
  int result_code = ibv_modify_qp(qp, &mod_init, mask);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modify QP from RESET to INIT failed (%d).", result_code));
  }
  VLOG(1) << absl::StrFormat("Modify QP (%p) from RESET to INIT (%d).", qp,
                             result_code);

  // Ready to receive.
  ibv_qp_attr mod_rtr = qp_attr.GetUdInitToRtrAttr();
  mask = qp_attr.GetUdInitToRtrMask();
  result_code = ibv_modify_qp(qp, &mod_rtr, mask);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from INIT to RTR failed (%d).", result_code));
  }
  VLOG(1) << absl::StrFormat("Modify QP (%p) from INIT to RTR (%d).", qp,
                             result_code);

  // Ready to send.
  ibv_qp_attr mod_rts = qp_attr.GetUdRtrToRtsAttr();
  mask = qp_attr.GetUdRtrToRtsMask();
  result_code = ibv_modify_qp(qp, &mod_rts, mask);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from RTR to RTS failed (%d).", result_code));
  }
  VLOG(1) << absl::StrFormat("Modify QP (%p) from RTR to RTS (%d).", qp,
                             result_code);
  return absl::OkStatus();
}

absl::Status VerbsHelperSuite::ModifyUdQpResetToRts(ibv_qp* qp, uint32_t qkey,
                                                    QpAttribute qp_attr) {
  return ModifyUdQpResetToRts(qp, GetPortAttribute(qp->context), qkey, qp_attr);
}

absl::Status VerbsHelperSuite::ModifyQpToError(ibv_qp* qp) const {
  ibv_qp_attr attr{.qp_state = IBV_QPS_ERR};
  int result_code = ModifyQp(qp, attr, IBV_QP_STATE);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP to ERROR state failed (%d).", result_code));
  }
  return absl::OkStatus();
}

absl::Status VerbsHelperSuite::ModifyQpToReset(ibv_qp* qp) const {
  ibv_qp_attr attr{.qp_state = IBV_QPS_RESET};
  int result_code = ModifyQp(qp, attr, IBV_QP_STATE);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP to ERROR state failed (%d).", result_code));
  }
  return absl::OkStatus();
}

int VerbsHelperSuite::ModifyQp(ibv_qp* qp, ibv_qp_attr& attr, int mask) const {
  int result_code = ibv_modify_qp(qp, &attr, mask);
  VLOG(1) << absl::StrFormat("Modify QP (%p) to %s (%d).", qp,
                             magic_enum::enum_name(attr.qp_state), result_code);
  return result_code;
}

RdmaMemBlock VerbsHelperSuite::AllocBuffer(int pages,
                                           bool requires_shared_memory) {
  return AllocAlignedBufferByBytes(
      pages * kPageSize,
      requires_shared_memory ? kPageSize : __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}

RdmaMemBlock VerbsHelperSuite::AllocAlignedBuffer(int pages, size_t alignment) {
  return AllocAlignedBufferByBytes(pages * kPageSize, alignment);
}

RdmaMemBlock VerbsHelperSuite::AllocHugepageBuffer(int pages) {
  return AllocAlignedBufferByBytes(pages * kHugepageSize, kHugepageSize,
                                   /*huge_page=*/true);
}

RdmaMemBlock VerbsHelperSuite::AllocAlignedBufferByBytes(size_t bytes,
                                                         size_t alignment,
                                                         bool huge_page) {
  auto block = std::make_unique<RdmaMemBlock>(bytes, alignment, huge_page);
  DCHECK(block);
  memset(block->data(), '-', block->size());
  RdmaMemBlock result = *block;
  absl::MutexLock guard(&mtx_memblocks_);
  memblocks_.emplace_back(std::move(block));
  return result;
}

absl::StatusOr<ibv_context*> VerbsHelperSuite::OpenDevice() {
  std::vector<std::string> device_names;
  if (!absl::GetFlag(FLAGS_device_name).empty()) {
    device_names.push_back(absl::GetFlag(FLAGS_device_name));
  } else {
    absl::StatusOr<std::vector<std::string>> enum_results =
        verbs_util::EnumerateDeviceNames();
    if (!enum_results.ok()) return enum_results.status();
    device_names = enum_results.value();
  }

  ibv_context* context = nullptr;
  std::vector<PortAttribute> port_attrs;
  for (auto& device_name : device_names) {
    absl::StatusOr<ibv_context*> context_or =
        verbs_util::OpenUntrackedDevice(device_name);
    LOG(INFO) << "Opening device: " << device_name;
    if (!context_or.ok()) {
      LOG(INFO) << "Failed to open device: " << device_name;
      continue;
    }
    context = context_or.value();
    absl::StatusOr<std::vector<PortAttribute>> enum_result =
        EnumeratePorts(context);
    if (enum_result.ok() && !enum_result.value().empty()) {
      port_attrs = enum_result.value();
      VLOG(1) << "Found (" << port_attrs.size()
              << ") active ports for device: " << device_name;
      // Just need one device with active ports. Break at this point.
      break;
    }
    LOG(INFO) << "Failed to get ports for device: " << device_name;
    int result = ibv_close_device(context);
    LOG_IF(DFATAL, result != 0) << "Failed to close device: " << device_name;
    context = nullptr;
  }
  if (!context || port_attrs.empty()) {
    return absl::InternalError("Failed to open a device with active ports.");
  }
  cleanup_.AddCleanup(context);

  absl::MutexLock guard(&mtx_port_attrs_);
  port_attrs_[context] = port_attrs;

  VLOG(1) << "Opened device " << context;

  return context;
}

ibv_ah* VerbsHelperSuite::CreateAh(ibv_pd* pd, uint8_t port, uint8_t sgid_index,
                                   ibv_gid remote_gid, AhAttribute ah_attr) {
  ibv_ah_attr attr = ah_attr.GetAttribute(port, sgid_index, remote_gid);
  return CreateAh(pd, attr);
}

ibv_ah* VerbsHelperSuite::CreateLoopbackAh(ibv_pd* pd,
                                           const PortAttribute& port_attr,
                                           AhAttribute ah_attr) {
  ibv_ah_attr attr =
      ah_attr.GetAttribute(port_attr.port, port_attr.gid_index, port_attr.gid);
  return CreateAh(pd, attr);
}

ibv_ah* VerbsHelperSuite::CreateAh(ibv_pd* pd, ibv_ah_attr& ah_attr) {
  ibv_ah* ah = extension().CreateAh(pd, ah_attr);
  if (ah) {
    VLOG(1) << "Created AH " << ah;
    cleanup_.AddCleanup(ah);
  }
  return ah;
}

int VerbsHelperSuite::DestroyAh(ibv_ah* ah) {
  int result = ibv_destroy_ah(ah);
  if (result == 0) {
    VLOG(1) << "Destroyed AH " << ah;
    cleanup_.ReleaseCleanup(ah);
  }
  return result;
}

ibv_pd* VerbsHelperSuite::AllocPd(ibv_context* context) {
  ibv_pd* pd = ibv_alloc_pd(context);
  if (pd) {
    VLOG(1) << "Allocated PD " << pd;
    cleanup_.AddCleanup(pd);
  }
  return pd;
}

int VerbsHelperSuite::DeallocPd(ibv_pd* pd) {
  int result = ibv_dealloc_pd(pd);
  if (result == 0) {
    VLOG(1) << "Deallocated PD " << pd;
    cleanup_.ReleaseCleanup(pd);
  }
  return result;
}

ibv_mr* VerbsHelperSuite::RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                                int access) {
  ibv_mr* mr = extension().RegMr(pd, memblock, access);
  if (mr) {
    VLOG(1) << "Registered MR " << mr;
    cleanup_.AddCleanup(mr);
  }
  return mr;
}

int VerbsHelperSuite::ReregMr(ibv_mr* mr, int flags, ibv_pd* pd,
                              const RdmaMemBlock* memblock, int access) {
  int result = extension().ReregMr(mr, flags, pd, memblock, access);
  if (result == 0) {
    VLOG(1) << "Reregistered MR " << mr;
  }
  return result;
}

int VerbsHelperSuite::DeregMr(ibv_mr* mr) {
  int result = ibv_dereg_mr(mr);
  if (result == 0) {
    VLOG(1) << "Deregistered MR " << mr;
    cleanup_.ReleaseCleanup(mr);
  }
  return result;
}

ibv_mw* VerbsHelperSuite::AllocMw(ibv_pd* pd, ibv_mw_type type) {
  ibv_mw* mw = ibv_alloc_mw(pd, type);
  if (mw) {
    VLOG(1) << "Allocated MW " << mw;
    cleanup_.AddCleanup(mw);
  }
  return mw;
}

int VerbsHelperSuite::DeallocMw(ibv_mw* mw) {
  int result = ibv_dealloc_mw(mw);
  if (result == 0) {
    VLOG(1) << "Deallocated MW " << mw;
    cleanup_.ReleaseCleanup(mw);
  }
  return result;
}

ibv_comp_channel* VerbsHelperSuite::CreateChannel(ibv_context* context) {
  ibv_comp_channel* channel = ibv_create_comp_channel(context);
  if (channel) {
    VLOG(1) << "Created channel " << channel;
    cleanup_.AddCleanup(channel);
  }
  return channel;
}

int VerbsHelperSuite::DestroyChannel(ibv_comp_channel* channel) {
  int result = ibv_destroy_comp_channel(channel);
  if (result == 0) {
    VLOG(1) << "Destroyed channel " << channel;
    cleanup_.ReleaseCleanup(channel);
  }
  return result;
}

ibv_cq* VerbsHelperSuite::CreateCq(ibv_context* context, int cqe,
                                   ibv_comp_channel* channel) {
  ibv_cq* cq = ibv_create_cq(context, cqe, /*cq_context=*/nullptr, channel,
                             /*cq_vector=*/0);
  if (cq) {
    VLOG(1) << "Created CQ " << cq;
    cleanup_.AddCleanup(cq);
  }
  return cq;
}

int VerbsHelperSuite::DestroyCq(ibv_cq* cq) {
  int result = ibv_destroy_cq(cq);
  if (result == 0) {
    VLOG(1) << "Destroyed CQ " << cq;
    cleanup_.ReleaseCleanup(cq);
  }
  return result;
}

ibv_cq_ex* VerbsHelperSuite::CreateCqEx(ibv_context* context,
                                        ibv_cq_init_attr_ex& cq_attr) {
  ibv_cq_ex* cq = ibv_create_cq_ex(context, &cq_attr);
  if (cq) {
    VLOG(1) << "Created CQ " << cq;
    cleanup_.AddCleanup(cq);
  }
  return cq;
}

ibv_cq_ex* VerbsHelperSuite::CreateCqEx(ibv_context* context,
                                        uint32_t max_cqe) {
  ibv_cq_init_attr_ex attr{.cqe = max_cqe};
  return CreateCqEx(context, attr);
}

int VerbsHelperSuite::DestroyCqEx(ibv_cq_ex* cq_ex) {
  ibv_cq* cq = ibv_cq_ex_to_cq(cq_ex);
  int result = ibv_destroy_cq(cq);
  if (result == 0) {
    VLOG(1) << "Destroyed CQ " << cq;
    cleanup_.ReleaseCleanup(cq_ex);
  }
  return result;
}

ibv_srq* VerbsHelperSuite::CreateSrq(ibv_pd* pd, uint32_t max_wr,
                                     uint32_t max_sge) {
  ibv_srq_init_attr attr{.attr{.max_wr = max_wr, .max_sge = max_sge}};
  return CreateSrq(pd, attr);
}

ibv_srq* VerbsHelperSuite::CreateSrq(ibv_pd* pd, ibv_srq_init_attr& attr) {
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  if (srq) {
    VLOG(1) << "Created SRQ " << srq;
    cleanup_.AddCleanup(srq);
  }
  return srq;
}

int VerbsHelperSuite::DestroySrq(ibv_srq* srq) {
  int result = ibv_destroy_srq(srq);
  if (result == 0) {
    VLOG(1) << "Destroyed SRQ " << srq;
    cleanup_.ReleaseCleanup(srq);
  }
  return result;
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_cq* cq, ibv_qp_type type,
                                   QpInitAttribute qp_init_attr) {
  return CreateQp(pd, cq, cq, type, qp_init_attr);
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                                   ibv_qp_type type,
                                   QpInitAttribute qp_init_attr) {
  return CreateQp(pd, send_cq, recv_cq, nullptr, type, qp_init_attr);
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                                   ibv_srq* srq, ibv_qp_type type,
                                   QpInitAttribute qp_init_attr) {
  ibv_qp_init_attr attr =
      qp_init_attr.GetAttribute(send_cq, recv_cq, type, srq);
  return CreateQp(pd, attr);
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr) {
  ibv_qp* qp = extension().CreateQp(pd, basic_attr);
  if (qp) {
    VLOG(1) << "Created QP " << qp;
    cleanup_.AddCleanup(qp);
  }
  return qp;
}

int VerbsHelperSuite::DestroyQp(ibv_qp* qp) {
  int result = ibv_destroy_qp(qp);
  if (result == 0) {
    VLOG(1) << "Destroyed QP " << qp;
    cleanup_.ReleaseCleanup(qp);
  }
  return result;
}

PortAttribute VerbsHelperSuite::GetPortAttribute(ibv_context* context) const {
  absl::MutexLock guard(&mtx_port_attrs_);
  auto iter = port_attrs_.find(context);
  CHECK(iter != port_attrs_.end());  // Crash ok
  auto& info_array = iter->second;
  return info_array[0];
}

VerbsExtension& VerbsHelperSuite::extension() { return *extension_; }

VerbsCleanup& VerbsHelperSuite::clean_up() { return cleanup_; }

namespace {

std::string GidToString(const ibv_gid& gid) {
  return absl::StrFormat("GID: %x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x",
                         gid.raw[0], gid.raw[1], gid.raw[2], gid.raw[3],
                         gid.raw[4], gid.raw[5], gid.raw[6], gid.raw[7],
                         gid.raw[8], gid.raw[9], gid.raw[10], gid.raw[11],
                         gid.raw[12], gid.raw[13], gid.raw[14], gid.raw[15]);
}

}  // namespace

absl::StatusOr<std::vector<PortAttribute>> VerbsHelperSuite::EnumeratePorts(
    ibv_context* context) {
  std::vector<PortAttribute> result;
  bool ipv4_only = absl::GetFlag(FLAGS_ipv4_only);
  LOG(INFO) << "Enumerating Ports for " << context
            << " ipv4_only: " << ipv4_only;
  ibv_device_attr dev_attr = {};
  int query_result = ibv_query_device(context, &dev_attr);
  if (query_result != 0) {
    return absl::InternalError("Failed to query device ports.");
  }

  // libibverbs port numbers start at 1.
  for (uint8_t port = 1; port <= dev_attr.phys_port_cnt; ++port) {
    ibv_port_attr port_attr = {};
    query_result = ibv_query_port(context, port, &port_attr);
    if (query_result != 0) {
      return absl::InternalError("Failed to query port attributes.");
    }
    VLOG(1) << "Found port: " << static_cast<uint32_t>(port) << std::endl
            << "\t"
            << "state: " << port_attr.state << std::endl
            << "\t"
            << " mtu: " << (128 << port_attr.active_mtu) << std::endl
            << "\t"
            << "max_msg_sz: " << port_attr.max_msg_sz;
    if (port_attr.state != IBV_PORT_ACTIVE) {
      continue;
    }
    for (int gid_index = 0; gid_index < port_attr.gid_tbl_len; ++gid_index) {
      ibv_gid gid = {};
      query_result = ibv_query_gid(context, port, gid_index, &gid);
      if (query_result != 0) {
        return absl::InternalError("Failed to query gid.");
      }
      auto ip_type = verbs_util::GetIpAddressType(gid);
      if (ip_type == -1) {
        continue;
      }
      if (ipv4_only && (ip_type == AF_INET6)) {
        continue;
      }

      VLOG(2) << absl::StrFormat("Adding port %u with gid %s", port,
                                 GidToString(gid));
      PortAttribute match{
          .port = port, .gid = gid, .gid_index = gid_index, .attr = port_attr};
      result.push_back(match);
    }
  }
  return result;
}

}  // namespace rdma_unit_test
