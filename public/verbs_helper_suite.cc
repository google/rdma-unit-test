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
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "infiniband/verbs.h"
#include "internal/verbs_cleanup.h"
#include "internal/verbs_extension.h"
#include "public/flags.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

VerbsHelperSuite::VerbsHelperSuite()
    : extension_(InitVerbsExtension()),
      cleanup_(std::make_unique<VerbsCleanup>()) {}

absl::Status VerbsHelperSuite::SetUpRcQp(ibv_qp* local_qp,
                                         const verbs_util::PortGid& local,
                                         ibv_gid remote_gid,
                                         uint32_t remote_qpn) {
  RETURN_IF_ERROR(SetQpInit(local_qp, local.port));
  RETURN_IF_ERROR(SetQpRtr(local_qp, local, remote_gid, remote_qpn));
  return SetQpRts(local_qp);
}

absl::Status VerbsHelperSuite::SetUpRcQp(ibv_qp* local_qp, ibv_qp* remote_qp) {
  verbs_util::PortGid local = GetLocalPortGid(local_qp->context);
  verbs_util::PortGid remote = GetLocalPortGid(remote_qp->context);
  return SetUpRcQp(local_qp, local, remote.gid, remote_qp->qp_num);
}

absl::Status VerbsHelperSuite::SetUpLoopbackRcQps(ibv_qp* qp1, ibv_qp* qp2) {
  verbs_util::PortGid local1 = GetLocalPortGid(qp1->context);
  verbs_util::PortGid local2 = GetLocalPortGid(qp2->context);
  RETURN_IF_ERROR(SetUpRcQp(qp1, local1, local2.gid, qp2->qp_num));
  return SetUpRcQp(qp2, local2, local1.gid, qp1->qp_num);
}

absl::Status VerbsHelperSuite::SetUpUdQp(ibv_qp* qp, verbs_util::PortGid local,
                                         uint32_t qkey) {
  ibv_qp_attr mod_init = {};
  mod_init.qp_state = IBV_QPS_INIT;
  mod_init.pkey_index = 0;
  mod_init.port_num = local.port;
  mod_init.qkey = qkey;
  constexpr int kInitMask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY;
  if (ibv_modify_qp(qp, &mod_init, kInitMask) != 0) {
    // TODO(author1): Go through and return errno for all of these
    return absl::InternalError("Modify Qp (init) failed.");
  }

  // Ready to receive.
  ibv_qp_attr mod_rtr = {};
  mod_rtr.qp_state = IBV_QPS_RTR;
  constexpr int kRtrMask = IBV_QP_STATE;
  if (ibv_modify_qp(qp, &mod_rtr, kRtrMask) != 0) {
    return absl::InternalError("Modify QP (RtR) failed.");
  }

  // Ready to send.
  ibv_qp_attr mod_rts = {};
  mod_rts.qp_state = IBV_QPS_RTS;
  mod_rts.sq_psn =
      1225;  // TODO(author1): Eventually randomize for reality.
  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_SQ_PSN;
  if (ibv_modify_qp(qp, &mod_rts, kRtsMask) != 0) {
    return absl::InternalError("Modify Qp (RtS) failed.");
  }
  return absl::OkStatus();
}

absl::Status VerbsHelperSuite::SetUpUdQp(ibv_qp* qp, uint32_t qkey) {
  return SetUpUdQp(qp, GetLocalPortGid(qp->context), qkey);
}

absl::Status VerbsHelperSuite::SetQpInit(ibv_qp* qp, uint8_t port) {
  ibv_qp_attr mod_init = verbs_util::CreateBasicQpAttrInit(port);
  int result_code = ibv_modify_qp(qp, &mod_init, verbs_util::kQpAttrInitMask);
  if (result_code) {
    return absl::InternalError(
        absl::StrCat("Modify QP (Init) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

absl::Status VerbsHelperSuite::SetQpRtr(ibv_qp* qp,
                                        const verbs_util::PortGid& local,
                                        ibv_gid remote_gid,
                                        uint32_t remote_qpn) {
  return extension()->SetQpRtr(qp, local, remote_gid, remote_qpn);
}

absl::Status VerbsHelperSuite::SetQpRts(ibv_qp* qp) {
  ibv_qp_attr mod_rts = verbs_util::CreateBasicQpAttrRts();
  int result_code = ibv_modify_qp(qp, &mod_rts, verbs_util::kQpAttrRtsMask);
  if (result_code != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (Rts) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

absl::Status VerbsHelperSuite::SetQpError(ibv_qp* qp) {
  ibv_qp_attr modify_error = {};
  modify_error.qp_state = IBV_QPS_ERR;
  int result_code = ibv_modify_qp(qp, &modify_error, IBV_QP_STATE);
  if (result_code != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (Error) failed (", result_code, ")."));
  }
  return absl::OkStatus();
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
  auto block = absl::make_unique<RdmaMemBlock>(bytes, alignment, huge_page);
  DCHECK(block);
  memset(block->data(), '-', block->size());
  RdmaMemBlock result = *block;
  absl::MutexLock guard(&mtx_memblocks_);
  memblocks_.emplace_back(std::move(block));
  return result;
}

absl::StatusOr<ibv_context*> VerbsHelperSuite::OpenDevice(
    bool no_ipv6_for_gid) {
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
  std::vector<verbs_util::PortGid> port_gids;
  for (auto& device_name : device_names) {
    absl::StatusOr<ibv_context*> context_or =
        verbs_util::OpenUntrackedDevice(device_name);
    LOG(INFO) << "Opening device: " << device_name;
    if (!context_or.ok()) {
      LOG(INFO) << "Failed to open device: " << device_name;
      continue;
    }
    context = context_or.value();
    absl::StatusOr<std::vector<verbs_util::PortGid>> enum_result =
        verbs_util::EnumeratePortGidsForContext(context);
    if (enum_result.ok() && !enum_result.value().empty()) {
      port_gids = enum_result.value();
      VLOG(1) << "Found (" << port_gids.size()
              << ") active ports for device: " << device_name;
      // Just need one device with active ports. Break at this point.
      break;
    }
    LOG(INFO) << "Failed to get ports for device: " << device_name;
    int result = ibv_close_device(context);
    LOG_IF(DFATAL, result != 0) << "Failed to close device: " << device_name;
    context = nullptr;
  }
  if (!context || port_gids.empty()) {
    return absl::InternalError("Failed to open a device with active ports.");
  }
  cleanup_->AddCleanup(context);

  absl::MutexLock guard(&mtx_port_gids_);
  port_gids_[context] = port_gids;

  return context;
}

ibv_ah* VerbsHelperSuite::CreateAh(ibv_pd* pd, ibv_gid remote_gid,
                                   uint8_t traffic_class) {
  verbs_util::PortGid local = GetLocalPortGid(pd->context);
  ibv_ah* ah = extension()->CreateAh(pd, local, remote_gid, traffic_class);
  if (ah) {
    cleanup_->AddCleanup(ah);
  }
  return ah;
}

int VerbsHelperSuite::DestroyAh(ibv_ah* ah) {
  int result = ibv_destroy_ah(ah);
  if (result == 0) {
    cleanup_->ReleaseCleanup(ah);
  }
  return result;
}

ibv_pd* VerbsHelperSuite::AllocPd(ibv_context* context) {
  ibv_pd* pd = ibv_alloc_pd(context);
  if (pd) {
    cleanup_->AddCleanup(pd);
  }
  return pd;
}

int VerbsHelperSuite::DeallocPd(ibv_pd* pd) {
  int result = ibv_dealloc_pd(pd);
  if (result == 0) {
    cleanup_->ReleaseCleanup(pd);
  }
  return result;
}

ibv_mr* VerbsHelperSuite::RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                                int access) {
  ibv_mr* mr = extension()->RegMr(pd, memblock, access);
  if (mr) {
    cleanup_->AddCleanup(mr);
  }
  return mr;
}

int VerbsHelperSuite::ReregMr(ibv_mr* mr, int flags, ibv_pd* pd,
                              const RdmaMemBlock* memblock, int access) {
  return extension_->ReregMr(mr, flags, pd, memblock, access);
}

int VerbsHelperSuite::DeregMr(ibv_mr* mr) {
  int result = ibv_dereg_mr(mr);
  if (result == 0) {
    cleanup_->ReleaseCleanup(mr);
  }
  return result;
}

ibv_mw* VerbsHelperSuite::AllocMw(ibv_pd* pd, ibv_mw_type type) {
  ibv_mw* mw = ibv_alloc_mw(pd, type);
  if (mw) {
    cleanup_->AddCleanup(mw);
  }
  return mw;
}

int VerbsHelperSuite::DeallocMw(ibv_mw* mw) {
  int result = ibv_dealloc_mw(mw);
  if (result == 0) {
    cleanup_->ReleaseCleanup(mw);
  }
  return result;
}

ibv_comp_channel* VerbsHelperSuite::CreateChannel(ibv_context* context) {
  ibv_comp_channel* channel = ibv_create_comp_channel(context);
  if (channel) {
    cleanup_->AddCleanup(channel);
  }
  return channel;
}

int VerbsHelperSuite::DestroyChannel(ibv_comp_channel* channel) {
  int result = ibv_destroy_comp_channel(channel);
  if (result == 0) {
    cleanup_->ReleaseCleanup(channel);
  }
  return result;
}

ibv_cq* VerbsHelperSuite::CreateCq(ibv_context* context, int cqe,
                                   ibv_comp_channel* channel) {
  ibv_cq* cq = ibv_create_cq(context, cqe, /*cq_context=*/nullptr, channel,
                             /*cq_vector=*/0);
  if (cq) {
    cleanup_->AddCleanup(cq);
  }
  return cq;
}

int VerbsHelperSuite::DestroyCq(ibv_cq* cq) {
  int result = ibv_destroy_cq(cq);
  if (result == 0) {
    cleanup_->ReleaseCleanup(cq);
  }
  return result;
}

ibv_cq_ex* VerbsHelperSuite::CreateCqEx(ibv_context* context,
                                        ibv_cq_init_attr_ex& cq_attr) {
  ibv_cq_ex* cq = ibv_create_cq_ex(context, &cq_attr);
  if (cq) {
    cleanup_->AddCleanup(cq);
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
    cleanup_->ReleaseCleanup(cq_ex);
  }
  return result;
}

ibv_srq* VerbsHelperSuite::CreateSrq(ibv_pd* pd, uint32_t max_wr) {
  ibv_srq_init_attr init_attr;
  init_attr.attr = verbs_util::DefaultSrqAttr();
  init_attr.attr.max_wr = max_wr;
  return CreateSrq(pd, init_attr);
}

ibv_srq* VerbsHelperSuite::CreateSrq(ibv_pd* pd, ibv_srq_init_attr& attr) {
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  if (srq) {
    cleanup_->AddCleanup(srq);
  }
  return srq;
}

int VerbsHelperSuite::DestroySrq(ibv_srq* srq) {
  int result = ibv_destroy_srq(srq);
  if (result == 0) {
    cleanup_->ReleaseCleanup(srq);
  }
  return result;
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_cq* cq) {
  return CreateQp(pd, cq, nullptr);
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_cq* cq, ibv_srq* srq) {
  ibv_qp_init_attr attr{.send_cq = cq,
                        .recv_cq = cq,
                        .srq = srq,
                        .cap = verbs_util::DefaultQpCap(),
                        .qp_type = IBV_QPT_RC,
                        .sq_sig_all = 0};
  return CreateQp(pd, attr);
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                                   ibv_srq* srq, uint32_t max_send_wr,
                                   uint32_t max_recv_wr, ibv_qp_type qp_type,
                                   int sig_all) {
  ibv_qp_init_attr attr{.send_cq = send_cq,
                        .recv_cq = recv_cq,
                        .srq = srq,
                        .cap = verbs_util::DefaultQpCap(),
                        .qp_type = qp_type,
                        .sq_sig_all = sig_all};
  attr.cap.max_send_wr = max_send_wr;
  attr.cap.max_recv_wr = max_recv_wr;
  return CreateQp(pd, attr);
}

ibv_qp* VerbsHelperSuite::CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr) {
  ibv_qp* qp = extension()->CreateQp(pd, basic_attr);
  if (qp) {
    cleanup_->AddCleanup(qp);
  }
  return qp;
}

int VerbsHelperSuite::DestroyQp(ibv_qp* qp) {
  int result = ibv_destroy_qp(qp);
  if (result == 0) {
    cleanup_->ReleaseCleanup(qp);
  }
  return result;
}

verbs_util::PortGid VerbsHelperSuite::GetLocalPortGid(
    ibv_context* context) const {
  absl::MutexLock guard(&mtx_port_gids_);
  auto iter = port_gids_.find(context);
  CHECK(iter != port_gids_.end());  // Crash ok
  auto& info_array = iter->second;
  return info_array[0];
}

VerbsExtension* VerbsHelperSuite::extension() { return extension_.get(); }

std::unique_ptr<VerbsExtension> VerbsHelperSuite::InitVerbsExtension() {
  return std::make_unique<VerbsExtension>();
}

VerbsCleanup& VerbsHelperSuite::cleanup() { return *cleanup_; }

}  // namespace rdma_unit_test
