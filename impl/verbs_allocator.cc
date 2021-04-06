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
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "infiniband/verbs.h"
#include "public/rdma-memblock.h"
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
  ibv_device** devices = nullptr;
  auto free_list = absl::MakeCleanup([&devices]() {
    if (devices) {
      ibv_free_device_list(devices);
    }
  });
  int num_devices = 0;
  devices = ibv_get_device_list(&num_devices);
  if (num_devices == 0) {
    return absl::InternalError("No devices found.");
  }
  ibv_device* device = devices[0];
  const std::string device_name = device->name;
  LOG(INFO) << "Opening " << device_name;
  ibv_context* context = ibv_open_device(device);
  if (!context) {
    return absl::InternalError("Failed to open a context.");
  }
  auto enum_result = verbs_util::EnumeratePortsForContext(context);
  if (!enum_result.ok()) {
    int result = ibv_close_device(context);
    LOG_IF(DFATAL, result != 0) << "Failed to close device";
    return enum_result.status();
  }
  std::vector<verbs_util::VerbsAddress> addresses = enum_result.value();
  {
    absl::MutexLock guard(&mtx_contexts_);
    contexts_.emplace_back(context, &ContextDeleter);
  }
  absl::MutexLock guard(&mtx_address_info_);
  address_info_[context] = addresses;
  return context;
}

ibv_pd* VerbsAllocator::AllocPd(ibv_context* context) {
  auto result = ibv_alloc_pd(context);
  if (result) {
    LOG(INFO) << "Created pd " << result;
    absl::MutexLock guard(&mtx_pds_);
    pds_.emplace_back(result, &PdDeleter);
  }
  return result;
}

int VerbsAllocator::DeallocPd(ibv_pd* pd) {
  absl::MutexLock guard(&mtx_pds_);
  int result = ibv_dealloc_pd(pd);
  if (result != 0) {
    return result;
  }
  auto iter = pds_.begin();
  while (iter != pds_.end() && iter->get() != pd) {
    ++iter;
  }
  DCHECK(iter != pds_.end());
  ibv_pd* found = iter->release();
  DCHECK_EQ(found, pd);
  pds_.erase(iter);
  return result;
}

ibv_ah* VerbsAllocator::CreateAh(ibv_pd* pd) {
  ibv_ah* ah = CreateAhInternal(pd);
  if (ah) {
    LOG(INFO) << "Created ah " << ah;
    absl::MutexLock guard(&mtx_ahs_);
    ahs_.emplace_back(ah, &AhDeleter);
  }
  return ah;
}

ibv_mr* VerbsAllocator::RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                              int access) {
  ibv_mr* mr = RegMrInternal(pd, memblock, access);
  if (mr) {
    LOG(INFO) << "Created Memory Region " << mr;
    absl::MutexLock guard(&mtx_mrs_);
    mrs_.emplace_back(mr, &MrDeleter);
  }
  return mr;
}

int VerbsAllocator::DeregMr(ibv_mr* mr) {
  absl::MutexLock guard(&mtx_mrs_);
  int result = ibv_dereg_mr(mr);
  if (result != 0) {
    return result;
  }
  auto iter = mrs_.begin();
  while (iter != mrs_.end() && iter->get() != mr) {
    ++iter;
  }
  DCHECK(iter != mrs_.end());
  ibv_mr* found = iter->release();
  DCHECK_EQ(found, mr);
  mrs_.erase(iter);
  return result;
}

ibv_mw* VerbsAllocator::AllocMw(ibv_pd* pd, ibv_mw_type type) {
  ibv_mw* mw = ibv_alloc_mw(pd, type);
  if (mw) {
    LOG(INFO) << "Created Memory Window " << mw;
    absl::MutexLock guard(&mtx_mws_);
    mws_.emplace_back(mw, &MwDeleter);
  }
  return mw;
}

int VerbsAllocator::DeallocMw(ibv_mw* mw) {
  absl::MutexLock guard(&mtx_mws_);
  int result = ibv_dealloc_mw(mw);
  if (result != 0) {
    return result;
  }
  auto iter = mws_.begin();
  while (iter != mws_.end() && iter->get() != mw) {
    ++iter;
  }
  DCHECK(iter != mws_.end());
  ibv_mw* found = iter->release();
  DCHECK_EQ(found, mw);
  mws_.erase(iter);
  return result;
}

ibv_comp_channel* VerbsAllocator::CreateChannel(ibv_context* context) {
  auto result = ibv_create_comp_channel(context);
  if (result) {
    LOG(INFO) << "Created channel " << result;
    absl::MutexLock guard(&mtx_channels_);
    channels_.emplace_back(result, &ChannelDeleter);
  }
  return result;
}

int VerbsAllocator::DestroyChannel(ibv_comp_channel* channel) {
  absl::MutexLock guard(&mtx_channels_);
  int result = ibv_destroy_comp_channel(channel);
  if (result != 0) {
    return result;
  }
  auto iter = channels_.begin();
  while (iter != channels_.end() && iter->get() != channel) {
    ++iter;
  }
  DCHECK(iter != channels_.end());
  ibv_comp_channel* found = iter->release();
  DCHECK_EQ(found, channel);
  channels_.erase(iter);
  return result;
}

ibv_cq* VerbsAllocator::CreateCq(ibv_context* context, int max_wr,
                                 ibv_comp_channel* channel) {
  auto result = ibv_create_cq(context, max_wr, nullptr, channel, 0);
  if (result) {
    LOG(INFO) << "Created Cq " << result;
    absl::MutexLock guard(&mtx_cqs_);
    cqs_.emplace_back(result, &CqDeleter);
  }
  return result;
}

int VerbsAllocator::DestroyCq(ibv_cq* cq) {
  absl::MutexLock guard(&mtx_cqs_);
  int result = ibv_destroy_cq(cq);
  if (result != 0) {
    return result;
  }
  auto iter = cqs_.begin();
  while (iter != cqs_.end() && iter->get() != cq) {
    ++iter;
  }
  DCHECK(iter != cqs_.end());
  ibv_cq* found = iter->release();
  DCHECK_EQ(found, cq);
  cqs_.erase(iter);
  return result;
}

ibv_srq* VerbsAllocator::CreateSrq(ibv_pd* pd, uint32_t max_wr) {
  ibv_srq_init_attr attr;
  attr.attr.max_wr = max_wr;
  attr.attr.max_sge = verbs_util::kDefaultMaxSge;
  attr.attr.srq_limit = 0;  // not used for infiniband.
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  if (srq) {
    LOG(INFO) << "Created srq: " << srq;
    absl::MutexLock guard(&mtx_srqs_);
    srqs_.emplace_back(srq, &SrqDeleter);
  }
  return srq;
}

ibv_srq* VerbsAllocator::CreateSrq(ibv_pd* pd, ibv_srq_init_attr& attr) {
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  if (srq) {
    LOG(INFO) << "Created srq: " << srq;
    absl::MutexLock guard(&mtx_srqs_);
    srqs_.emplace_back(srq, &SrqDeleter);
  }
  return srq;
}

int VerbsAllocator::DestroySrq(ibv_srq* srq) {
  absl::MutexLock guard(&mtx_srqs_);
  int result = ibv_destroy_srq(srq);
  if (result != 0) {
    return result;
  }
  auto iter = srqs_.begin();
  while (iter != srqs_.end() && iter->get() != srq) {
    ++iter;
  }
  DCHECK(iter != srqs_.end());
  ibv_srq* found = iter->release();
  DCHECK_EQ(found, srq);
  srqs_.erase(iter);
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
    LOG(INFO) << "Created qp " << qp;
    absl::MutexLock guard(&mtx_qps_);
    qps_.emplace_back(qp, &QpDeleter);
  }
  return qp;
}

int VerbsAllocator::DestroyQp(ibv_qp* qp) {
  absl::MutexLock guard(&mtx_qps_);
  int result = ibv_destroy_qp(qp);
  if (result != 0) {
    return result;
  }
  auto iter = qps_.begin();
  while (iter != qps_.end() && iter->get() != qp) {
    ++iter;
  }
  DCHECK(iter != qps_.end());
  ibv_qp* found = iter->release();
  DCHECK_EQ(found, qp);
  qps_.erase(iter);
  return result;
}

verbs_util::VerbsAddress VerbsAllocator::GetContextAddressInfo(
    ibv_context* context) const {
  absl::MutexLock guard(&mtx_address_info_);
  auto iter = address_info_.find(context);
  CHECK(iter != address_info_.end());  // Crash ok
  auto& info_array = iter->second;
  return info_array[0];
}

}  // namespace rdma_unit_test
