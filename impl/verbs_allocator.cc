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

#include "impl/verbs_allocator.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/rdma_memblock.h"
#include "public/util.h"

namespace rdma_unit_test {

void VerbsAllocator::ContextDeleter(ibv_context* context) {
  int result = ibv_close_device(context);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::AhDeleter(ibv_ah* ah) {
  int result = ibv_destroy_ah(ah);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::MrDeleter(ibv_mr* mr) {
  int result = ibv_dereg_mr(mr);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::MwDeleter(ibv_mw* mw) {
  int result = ibv_dealloc_mw(mw);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::PdDeleter(ibv_pd* pd) {
  int result = ibv_dealloc_pd(pd);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::ChannelDeleter(ibv_comp_channel* channel) {
  int result = ibv_destroy_comp_channel(channel);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::CqDeleter(ibv_cq* cq) {
  int result = ibv_destroy_cq(cq);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::SrqDeleter(ibv_srq* srq) {
  int result = ibv_destroy_srq(srq);
  DCHECK_EQ(0, result);
}

void VerbsAllocator::QpDeleter(ibv_qp* qp) {
  int result = ibv_destroy_qp(qp);
  DCHECK_EQ(0, result);
}

RdmaMemBlock VerbsAllocator::AllocBuffer(int pages,
                                         bool requires_shared_memory) {
  return AllocAlignedBuffer(pages, requires_shared_memory
                                       ? verbs_util::kPageSize
                                       : __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}

RdmaMemBlock VerbsAllocator::AllocAlignedBuffer(int pages, size_t alignment) {
  auto block =
      absl::make_unique<RdmaMemBlock>(pages * verbs_util::kPageSize, alignment);
  DCHECK(block);
  memset(block->data(), '-', block->size());
  RdmaMemBlock result = *block;
  absl::MutexLock guard(&mtx_memblocks_);
  memblocks_.emplace_back(std::move(block));
  return result;
}

RdmaMemBlock VerbsAllocator::AllocBufferByBytes(size_t bytes,
                                                size_t alignment) {
  auto block = absl::make_unique<RdmaMemBlock>(bytes, alignment);
  DCHECK(block);
  memset(block->data(), '-', block->size());
  RdmaMemBlock result = *block;
  absl::MutexLock guard(&mtx_memblocks_);
  memblocks_.emplace_back(std::move(block));
  return result;
}

absl::StatusOr<ibv_context*> VerbsAllocator::OpenDevice(bool no_ipv6_for_gid) {
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

  {
    absl::MutexLock guard(&mtx_contexts_);
    contexts_.emplace(context, &ContextDeleter);
  }
  absl::MutexLock guard(&mtx_port_gids_);
  port_gids_[context] = port_gids;
  return context;
}

ibv_pd* VerbsAllocator::AllocPd(ibv_context* context) {
  auto pd = ibv_alloc_pd(context);
  if (pd) {
    VLOG(1) << "Created pd " << pd;
    absl::MutexLock guard(&mtx_pds_);
    pds_.emplace(pd, &PdDeleter);
  }
  return pd;
}

int VerbsAllocator::DeallocPd(ibv_pd* pd) {
  absl::MutexLock guard(&mtx_pds_);
  int result = ibv_dealloc_pd(pd);
  if (result != 0) {
    return result;
  }
  auto node = pds_.extract(pd);
  DCHECK(!node.empty());
  ibv_pd* found = node.value().release();
  DCHECK_EQ(pd, found);
  return result;
}

ibv_ah* VerbsAllocator::CreateAh(ibv_pd* pd, ibv_gid remote_gid) {
  ibv_ah* ah = CreateAhInternal(pd, remote_gid);
  if (ah) {
    VLOG(1) << "Created ah " << ah;
    absl::MutexLock guard(&mtx_ahs_);
    ahs_.emplace(ah, &AhDeleter);
  }
  return ah;
}

int VerbsAllocator::DestroyAh(ibv_ah* ah) {
  absl::MutexLock guard(&mtx_ahs_);
  int result = ibv_destroy_ah(ah);
  if (result != 0) {
    return result;
  }
  auto node = ahs_.extract(ah);
  DCHECK(!node.empty());
  ibv_ah* found = node.value().release();
  DCHECK_EQ(ah, found);
  return result;
}

ibv_mr* VerbsAllocator::RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                              int access) {
  ibv_mr* mr = RegMrInternal(pd, memblock, access);
  if (mr) {
    VLOG(1) << "Created Memory Region " << mr;
    absl::MutexLock guard(&mtx_mrs_);
    mrs_.emplace(mr, &MrDeleter);
  }
  return mr;
}

int VerbsAllocator::DeregMr(ibv_mr* mr) {
  absl::MutexLock guard(&mtx_mrs_);
  int result = ibv_dereg_mr(mr);
  if (result != 0) {
    return result;
  }
  auto node = mrs_.extract(mr);
  DCHECK(!node.empty());
  ibv_mr* found = node.value().release();
  DCHECK_EQ(mr, found);
  return result;
}

ibv_mw* VerbsAllocator::AllocMw(ibv_pd* pd, ibv_mw_type type) {
  ibv_mw* mw = ibv_alloc_mw(pd, type);
  if (mw) {
    VLOG(1) << "Created Memory Window " << mw;
    absl::MutexLock guard(&mtx_mws_);
    mws_.emplace(mw, &MwDeleter);
  }
  return mw;
}

int VerbsAllocator::DeallocMw(ibv_mw* mw) {
  absl::MutexLock guard(&mtx_mws_);
  int result = ibv_dealloc_mw(mw);
  if (result != 0) {
    return result;
  }
  auto node = mws_.extract(mw);
  DCHECK(!node.empty());
  ibv_mw* found = node.value().release();
  DCHECK_EQ(mw, found);
  return result;
}

ibv_comp_channel* VerbsAllocator::CreateChannel(ibv_context* context) {
  auto channel = ibv_create_comp_channel(context);
  if (channel) {
    VLOG(1) << "Created channel " << channel;
    absl::MutexLock guard(&mtx_channels_);
    channels_.emplace(channel, &ChannelDeleter);
  }
  return channel;
}

int VerbsAllocator::DestroyChannel(ibv_comp_channel* channel) {
  absl::MutexLock guard(&mtx_channels_);
  int result = ibv_destroy_comp_channel(channel);
  if (result != 0) {
    return result;
  }
  auto node = channels_.extract(channel);
  DCHECK(!node.empty());
  ibv_comp_channel* found = node.value().release();
  DCHECK_EQ(channel, found);
  return result;
}

ibv_cq* VerbsAllocator::CreateCq(ibv_context* context, int max_wr,
                                 ibv_comp_channel* channel) {
  auto cq = ibv_create_cq(context, max_wr, nullptr, channel, 0);
  if (cq) {
    VLOG(1) << "Created Cq " << cq;
    absl::MutexLock guard(&mtx_cqs_);
    cqs_.emplace(cq, &CqDeleter);
  }
  return cq;
}

int VerbsAllocator::DestroyCq(ibv_cq* cq) {
  absl::MutexLock guard(&mtx_cqs_);
  int result = ibv_destroy_cq(cq);
  if (result != 0) {
    return result;
  }
  auto node = cqs_.extract(cq);
  DCHECK(!node.empty());
  ibv_cq* found = node.value().release();
  DCHECK_EQ(cq, found);
  return result;
}

ibv_srq* VerbsAllocator::CreateSrq(ibv_pd* pd, uint32_t max_wr) {
  ibv_srq_init_attr attr;
  attr.attr.max_wr = max_wr;
  attr.attr.max_sge = verbs_util::kDefaultMaxSge;
  attr.attr.srq_limit = 0;  // not used for infiniband.
  return CreateSrq(pd, attr);
}

ibv_srq* VerbsAllocator::CreateSrq(ibv_pd* pd, ibv_srq_init_attr& attr) {
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  if (srq) {
    VLOG(1) << "Created srq: " << srq;
    absl::MutexLock guard(&mtx_srqs_);
    srqs_.emplace(srq, &SrqDeleter);
  }
  return srq;
}

int VerbsAllocator::DestroySrq(ibv_srq* srq) {
  absl::MutexLock guard(&mtx_srqs_);
  int result = ibv_destroy_srq(srq);
  if (result != 0) {
    return result;
  }
  auto node = srqs_.extract(srq);
  DCHECK(!node.empty());
  ibv_srq* found = node.value().release();
  DCHECK_EQ(srq, found);
  return result;
}

ibv_qp* VerbsAllocator::CreateQp(ibv_pd* pd, ibv_cq* cq) {
  return CreateQp(pd, cq, nullptr);
}

ibv_qp* VerbsAllocator::CreateQp(ibv_pd* pd, ibv_cq* cq, ibv_srq* srq) {
  return CreateQp(pd, cq, cq, srq, verbs_util::kDefaultMaxWr,
                  verbs_util::kDefaultMaxWr, IBV_QPT_RC, /*sig_all=*/0);
}

ibv_qp* VerbsAllocator::CreateQp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                                 ibv_srq* srq, uint32_t max_send_wr,
                                 uint32_t max_recv_wr, ibv_qp_type qp_type,
                                 int sig_all) {
  ibv_qp_init_attr basic_attr{};
  basic_attr.send_cq = send_cq;
  basic_attr.recv_cq = recv_cq;
  basic_attr.srq = srq;
  basic_attr.cap.max_send_wr = max_send_wr;
  basic_attr.cap.max_recv_wr = max_recv_wr;
  basic_attr.cap.max_send_sge = verbs_util::kDefaultMaxSge;
  basic_attr.cap.max_recv_sge = verbs_util::kDefaultMaxSge;
  basic_attr.cap.max_inline_data = verbs_util::kDefaultMaxInlineSize;
  basic_attr.qp_type = qp_type;
  basic_attr.sq_sig_all = sig_all;
  return CreateQp(pd, basic_attr);
}

ibv_qp* VerbsAllocator::CreateQp(ibv_pd* pd, ibv_qp_init_attr& attr) {
  ibv_qp* qp = CreateQpInternal(pd, attr);
  if (qp) {
    VLOG(1) << "Created qp " << qp;
    absl::MutexLock guard(&mtx_qps_);
    qps_.emplace(qp, &QpDeleter);
  }
  return qp;
}

int VerbsAllocator::DestroyQp(ibv_qp* qp) {
  absl::MutexLock guard(&mtx_qps_);
  int result = ibv_destroy_qp(qp);
  if (result != 0) {
    return result;
  }
  auto node = qps_.extract(qp);
  DCHECK(!node.empty());
  ibv_qp* found = node.value().release();
  DCHECK_EQ(qp, found);
  return result;
}

verbs_util::PortGid VerbsAllocator::GetLocalPortGid(
    ibv_context* context) const {
  absl::MutexLock guard(&mtx_port_gids_);
  auto iter = port_gids_.find(context);
  CHECK(iter != port_gids_.end());  // Crash ok
  auto& info_array = iter->second;
  return info_array[0];
}

}  // namespace rdma_unit_test
