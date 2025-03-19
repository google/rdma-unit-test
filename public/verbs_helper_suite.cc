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

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
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
  LOG(INFO) << absl::StrFormat("Modify QP (%p) from RESET to INIT (%d).", qp,
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
  LOG_IF(FATAL, mod_rtr.path_mtu > local.attr.active_mtu)  // Crash OK
      << absl::StrFormat("Path mtu %d bigger than port active mtu %d.",
                         128 << mod_rtr.path_mtu, 128 << local.attr.active_mtu);
  int result_code = extension().ModifyRcQpInitToRtr(qp, mod_rtr, mask);
  LOG(INFO) << absl::StrFormat("Modified QP (%p) from INIT to RTR (%d).", qp,
                             result_code);
  return result_code;
}

int VerbsHelperSuite::ModifyRcQpRtrToRts(ibv_qp* qp, QpAttribute qp_attr) {
  ibv_qp_attr mod_rts = qp_attr.GetRcRtrToRtsAttr();
  int mask = qp_attr.GetRcRtrToRtsMask();
  int result_code = ibv_modify_qp(qp, &mod_rts, mask);
  LOG(INFO) << absl::StrFormat("Modify QP (%p) from RTR to RTS (%d).", qp,
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
  LOG(INFO) << absl::StrFormat("Modify QP (%p) from RESET to INIT (%d).", qp,
                             result_code);

  // Ready to receive.
  ibv_qp_attr mod_rtr = qp_attr.GetUdInitToRtrAttr();
  mask = qp_attr.GetUdInitToRtrMask();
  result_code = ibv_modify_qp(qp, &mod_rtr, mask);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from INIT to RTR failed (%d).", result_code));
  }
  LOG(INFO) << absl::StrFormat("Modify QP (%p) from INIT to RTR (%d).", qp,
                             result_code);

  // Ready to send.
  ibv_qp_attr mod_rts = qp_attr.GetUdRtrToRtsAttr();
  mask = qp_attr.GetUdRtrToRtsMask();
  result_code = ibv_modify_qp(qp, &mod_rts, mask);
  if (result_code) {
    return absl::InternalError(absl::StrFormat(
        "Modified QP from RTR to RTS failed (%d).", result_code));
  }
  LOG(INFO) << absl::StrFormat("Modify QP (%p) from RTR to RTS (%d).", qp,
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
        "Modified QP to RESET state failed (%d).", result_code));
  }
  return absl::OkStatus();
}

int VerbsHelperSuite::ModifyQp(ibv_qp* qp, ibv_qp_attr& attr, int mask) const {
  int result_code = ibv_modify_qp(qp, &attr, mask);
  LOG(INFO) << absl::StrFormat("Modify QP (%p) to %s (%d).", qp,
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
      LOG(INFO) << "Found (" << port_attrs.size()
              << ") active ports for device: " << device_name;
      // Just need one device with active ports. Break at this point.
      break;
    }
    LOG(INFO) << "Failed to get ports for device: " << device_name;
    int result = ibv_close_device(context);
    LOG_IF(FATAL, result != 0) << "Failed to close device: " << device_name;
    context = nullptr;
  }
  if (!context || port_attrs.empty()) {
    return absl::InternalError("Failed to open a device with active ports.");
  }
  cleanup_.AddCleanup(context);

  absl::MutexLock guard(&mtx_port_attrs_);
  port_attrs_[context] = port_attrs;

  LOG(INFO) << "Opened device " << context;

  return context;
}

absl::Status VerbsHelperSuite::OpenAllDevices(
    std::vector<ibv_context*>& contexts) {
  std::vector<std::string> device_names;
  if (!absl::GetFlag(FLAGS_device_name).empty()) {
    device_names = absl::StrSplit(absl::GetFlag(FLAGS_device_name), ',');
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
      LOG(INFO) << "Found (" << port_attrs.size()
              << ") active ports for device: " << device_name;
      contexts.push_back(context);
    } else {
      LOG(INFO) << "Failed to get ports for device: " << device_name;
      LOG(INFO) << "Getting error" << enum_result.status().message();
      int result = ibv_close_device(context);
      LOG_IF(FATAL, result != 0) << "Failed to close device: " << device_name;
    }
    if (!context || port_attrs.empty()) {
      return absl::InternalError(absl::StrCat("Failed to open device ",
                                              context->device->name,
                                              " with active ports."));
    }
    cleanup_.AddCleanup(context);
    absl::MutexLock guard(&mtx_port_attrs_);
    port_attrs_[context] = port_attrs;
    LOG(INFO) << "Opened device " << context;
    context = nullptr;
  }
  if (contexts.empty()) {
    return absl::InternalError("Failed to open any device with active ports.");
  }

  return absl::OkStatus();
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
    LOG(INFO) << "Created AH " << ah;
    cleanup_.AddCleanup(ah);
  } else {
    LOG(INFO) << "Failed to create AH: " << std::strerror(errno);
  }
  return ah;
}

int VerbsHelperSuite::DestroyAh(ibv_ah* ah) {
  cleanup_.ReleaseCleanup(ah);
  int result = ibv_destroy_ah(ah);
  if (result == 0) {
    LOG(INFO) << "Destroyed AH " << ah;
  } else {
    cleanup_.AddCleanup(ah);
    LOG(INFO) << "Failed to destroy AH: " << std::strerror(errno);
  }
  return result;
}

ibv_pd* VerbsHelperSuite::AllocPd(ibv_context* context) {
  ibv_pd* pd = ibv_alloc_pd(context);
  if (pd) {
    LOG(INFO) << "Allocated PD " << pd;
    cleanup_.AddCleanup(pd);
  } else {
    LOG(INFO) << "Failed to allocate PD: " << std::strerror(errno);
  }
  return pd;
}

int VerbsHelperSuite::DeallocPd(ibv_pd* pd) {
  cleanup_.ReleaseCleanup(pd);
  int result = ibv_dealloc_pd(pd);
  if (result == 0) {
    LOG(INFO) << "Deallocated PD " << pd;
  } else {
    cleanup_.AddCleanup(pd);
    LOG(INFO) << "Failed to deallocate PD: " << std::strerror(errno);
  }
  return result;
}

ibv_mr* VerbsHelperSuite::RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                                int access) {
  ibv_mr* mr = extension().RegMr(pd, memblock, access);
  if (mr) {
    LOG(INFO) << "Registered MR " << mr;
    cleanup_.AddCleanup(mr);
  } else {
    LOG(INFO) << "Failed to register MR: " << std::strerror(errno);
  }
  return mr;
}

int VerbsHelperSuite::ReregMr(ibv_mr* mr, int flags, ibv_pd* pd,
                              const RdmaMemBlock* memblock, int access) {
  int result = extension().ReregMr(mr, flags, pd, memblock, access);
  if (result == 0) {
    LOG(INFO) << "Reregistered MR " << mr;
  } else {
    LOG(INFO) << "Failed to reregister MR: " << std::strerror(errno);
  }
  return result;
}

int VerbsHelperSuite::DeregMr(ibv_mr* mr) {
  cleanup_.ReleaseCleanup(mr);
  int result = ibv_dereg_mr(mr);
  if (result == 0) {
    LOG(INFO) << "Deregistered MR " << mr;
  } else {
    cleanup_.AddCleanup(mr);
    LOG(INFO) << "Failed to deregister MR: " << std::strerror(errno);
  }
  return result;
}

ibv_mw* VerbsHelperSuite::AllocMw(ibv_pd* pd, ibv_mw_type type) {
  ibv_mw* mw = ibv_alloc_mw(pd, type);
  if (mw) {
    LOG(INFO) << "Allocated MW " << mw;
    cleanup_.AddCleanup(mw);
  } else {
    LOG(INFO) << "Failed to allocate MW: " << std::strerror(errno);
  }
  return mw;
}

int VerbsHelperSuite::DeallocMw(ibv_mw* mw) {
  cleanup_.ReleaseCleanup(mw);
  int result = ibv_dealloc_mw(mw);
  if (result == 0) {
    LOG(INFO) << "Deallocated MW " << mw;
  } else {
    cleanup_.AddCleanup(mw);
    LOG(INFO) << "Failed to deallocate MW: " << std::strerror(errno);
  }
  return result;
}

ibv_comp_channel* VerbsHelperSuite::CreateChannel(ibv_context* context) {
  ibv_comp_channel* channel = ibv_create_comp_channel(context);
  if (channel) {
    LOG(INFO) << "Created channel " << channel;
    cleanup_.AddCleanup(channel);
  } else {
    LOG(INFO) << "Failed to create comp channel: " << std::strerror(errno);
  }
  return channel;
}

int VerbsHelperSuite::DestroyChannel(ibv_comp_channel* channel) {
  cleanup_.ReleaseCleanup(channel);
  int result = ibv_destroy_comp_channel(channel);
  if (result == 0) {
    LOG(INFO) << "Destroyed channel " << channel;
  } else {
    cleanup_.AddCleanup(channel);
    LOG(INFO) << "Failed to destroy comp channel: " << std::strerror(errno);
  }
  return result;
}

ibv_cq* VerbsHelperSuite::CreateCq(ibv_context* context, int cqe,
                                   ibv_comp_channel* channel) {
  ibv_cq* cq = ibv_create_cq(context, cqe, /*cq_context=*/nullptr, channel,
                             /*cq_vector=*/0);
  if (cq) {
    LOG(INFO) << "Created CQ " << cq;
    cleanup_.AddCleanup(cq);
  } else {
    LOG(INFO) << "Failed to create CQ: " << std::strerror(errno);
  }
  return cq;
}

int VerbsHelperSuite::DestroyCq(ibv_cq* cq) {
  cleanup_.ReleaseCleanup(cq);
  int result = ibv_destroy_cq(cq);
  if (result == 0) {
    LOG(INFO) << "Destroyed CQ " << cq;
  } else {
    cleanup_.AddCleanup(cq);
    LOG(INFO) << "Failed to destroy CQ: " << std::strerror(errno);
  }
  return result;
}

ibv_cq_ex* VerbsHelperSuite::CreateCqEx(ibv_context* context,
                                        ibv_cq_init_attr_ex& cq_attr) {
  ibv_cq_ex* cq = ibv_create_cq_ex(context, &cq_attr);
  if (cq) {
    LOG(INFO) << "Created CQ " << cq;
    cleanup_.AddCleanup(cq);
  } else {
    LOG(INFO) << "Failed to create extended CQ: " << std::strerror(errno);
  }
  return cq;
}

ibv_cq_ex* VerbsHelperSuite::CreateCqEx(ibv_context* context,
                                        uint32_t max_cqe) {
  ibv_cq_init_attr_ex attr{.cqe = max_cqe};
  return CreateCqEx(context, attr);
}

int VerbsHelperSuite::DestroyCqEx(ibv_cq_ex* cq_ex) {
  cleanup_.ReleaseCleanup(cq_ex);
  ibv_cq* cq = ibv_cq_ex_to_cq(cq_ex);
  int result = ibv_destroy_cq(cq);
  if (result == 0) {
    LOG(INFO) << "Destroyed CQ " << cq;
  } else {
    cleanup_.AddCleanup(cq_ex);
    LOG(INFO) << "Failed to destroy extended CQ: " << std::strerror(errno);
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
    LOG(INFO) << "Created SRQ " << srq;
    cleanup_.AddCleanup(srq);
  } else {
    LOG(INFO) << "Failed to create SRQ: " << std::strerror(errno);
  }
  return srq;
}

int VerbsHelperSuite::DestroySrq(ibv_srq* srq) {
  cleanup_.ReleaseCleanup(srq);
  int result = ibv_destroy_srq(srq);
  if (result == 0) {
    LOG(INFO) << "Destroyed SRQ " << srq;
  } else {
    cleanup_.AddCleanup(srq);
    LOG(INFO) << "Failed to destroy SRQ: " << std::strerror(errno);
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
    LOG(INFO) << "Created QP " << qp;
    cleanup_.AddCleanup(qp);
  } else {
    LOG(INFO) << "Failed to create QP: " << std::strerror(errno);
  }
  return qp;
}

int VerbsHelperSuite::DestroyQp(ibv_qp* qp) {
  cleanup_.ReleaseCleanup(qp);
  int result = ibv_destroy_qp(qp);
  if (result == 0) {
    LOG(INFO) << "Destroyed QP " << qp;
  } else {
    cleanup_.AddCleanup(qp);
    LOG(INFO) << "Failed to destroy QP: " << std::strerror(errno);
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

std::string IpTypeToString(int ip_type) {
  switch (ip_type) {
    case AF_INET:
      return "AF_INET";
    case AF_INET6:
      return "AF_INET6";
    default:
      return "UNKNOWN";
  }
}

}  // namespace

absl::StatusOr<std::vector<PortAttribute>> VerbsHelperSuite::EnumeratePorts(
    ibv_context* context) {
  std::vector<PortAttribute> result;
  std::vector<PortAttribute> result_ipv4;
  ibv_port_attr port_attr = {};
  bool ipv4_only = absl::GetFlag(FLAGS_ipv4_only);
  LOG(INFO) << "Enumerating Ports for " << context
            << " ipv4_only: " << ipv4_only;
  ibv_device_attr dev_attr = {};
  int query_result = ibv_query_device(context, &dev_attr);
  if (query_result != 0) {
    return absl::InternalError("Failed to query device ports.");
  }

  int32_t port = absl::GetFlag(FLAGS_port_num);
  int gid_index = absl::GetFlag(FLAGS_gid_index);
  if (port) {
    query_result = ibv_query_port(context, port, &port_attr);
  } else {
    // libibverbs port numbers start at 1.
    for (port = 1; port <= dev_attr.phys_port_cnt; ++port) {
      port_attr = {};
      query_result = ibv_query_port(context, port, &port_attr);
      if (query_result != 0) {
        return absl::InternalError("Failed to query port attributes.");
      }
      LOG(INFO) << "Found port: " << static_cast<uint32_t>(port) << std::endl
              << "\t"
              << "state: " << port_attr.state << std::endl
              << "\t"
              << " mtu: " << (128 << port_attr.active_mtu) << std::endl
              << "\t"
              << "max_msg_sz: " << port_attr.max_msg_sz;
      if (port_attr.state == IBV_PORT_ACTIVE) {
        break;
      }
    }
  }

  if (gid_index >= 0) {
    ibv_gid gid = {};
    query_result = ibv_query_gid(context, port, gid_index, &gid);
    if (query_result != 0) {
      return absl::InternalError("Failed to query gid.");
    }
    PortAttribute match{.port = static_cast<uint8_t>(port),
                        .gid = gid,
                        .gid_index = gid_index,
                        .attr = port_attr};
    result.push_back(match);
  } else {
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
      LOG(INFO) << absl::StrFormat("Adding port %d with gid %s type %s", port,
                                 GidToString(gid), IpTypeToString(ip_type));
      PortAttribute match{.port = static_cast<uint8_t>(port),
                          .gid = gid,
                          .gid_index = gid_index,
                          .attr = port_attr};
      if (ip_type == AF_INET) {
        result_ipv4.push_back(match);
      } else if (ip_type == AF_INET6) {
        result.push_back(match);
      } else {
        LOG(INFO) << absl::StrFormat("Skipping port %d with gid %s: unknown type",
                                   port, GidToString(gid));
      }
    }
    // Add ipv4 GIDs after IPv6 ones
    result.insert(result.end(), result_ipv4.begin(), result_ipv4.end());
  }
  return result;
}

}  // namespace rdma_unit_test
