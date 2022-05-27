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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_UTIL_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_UTIL_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"

#include "infiniband/verbs.h"


namespace rdma_unit_test {
namespace verbs_util {


//////////////////////////////////////////////////////////////////////////////
//                          Constants
//////////////////////////////////////////////////////////////////////////////

// Default Wr capacity for send queue, receive queue, completion queue and
// shared receive queue.
constexpr uint32_t kDefaultMaxWr = 200;
// Default Sge capacity for single Wr in send queue, receive queue, compleetion
// queue and shared receive queue.
constexpr uint32_t kDefaultMaxSge = 1;
// Default timeout waiting for completion.
constexpr absl::Duration kDefaultCompletionTimeout = absl::Seconds(2);
// Default timeout waiting for completion on a known qp error.
constexpr absl::Duration kDefaultErrorCompletionTimeout = absl::Seconds(2);
// Definition for IPv4 Loopback Address.
constexpr std::string_view kIpV4LoopbackAddress{"127.0.0.1"};
// Definition for IPv6 Loopback Address.
constexpr std::string_view kIpV6LoopbackAddress{"::1"};

//////////////////////////////////////////////////////////////////////////////
//                          Helper Functions
//////////////////////////////////////////////////////////////////////////////

// Converts an ibv_mtu to int.
int VerbsMtuToInt(ibv_mtu mtu);

int GetIpAddressType(const ibv_gid& gid);

// Converts an uint64_t mtu to a ibv_mtu object.
ibv_mtu ToVerbsMtu(uint64_t mtu);


// Enumerates the names of all the devices available for the host.
absl::StatusOr<std::vector<std::string>> EnumerateDeviceNames();

// Create a defaulted ibv_srq_attr.
ibv_srq_attr DefaultSrqAttr();

// Returns the defaulted ibv_srq_attr value.
ibv_srq_attr DefaultSrqAttr();

///////////////////////////////////////////////////////////////////////////////
//                           Verbs Utilities
// Useful helper functions to eliminate the tediousness of filling repetitive
// attributes and flags. Simplifies the verb API.
///////////////////////////////////////////////////////////////////////////////

ibv_qp_state GetQpState(ibv_qp* qp);

ibv_qp_cap GetQpCap(ibv_qp* qp);

ibv_sge CreateSge(absl::Span<uint8_t> buffer, ibv_mr* mr);

// Create an SGE for atomic operation. Addr must be 8-byte aligned.
ibv_sge CreateAtomicSge(void* addr, ibv_mr* mr);

ibv_mw_bind_info CreateMwBindInfo(absl::Span<uint8_t> buffer, ibv_mr* mr,
                                  int access = IBV_ACCESS_REMOTE_READ |
                                               IBV_ACCESS_REMOTE_WRITE |
                                               IBV_ACCESS_REMOTE_ATOMIC);

ibv_mw_bind CreateType1MwBind(uint64_t wr_id, absl::Span<uint8_t> buffer,
                              ibv_mr* mr,
                              int access = IBV_ACCESS_REMOTE_READ |
                                           IBV_ACCESS_REMOTE_WRITE |
                                           IBV_ACCESS_REMOTE_ATOMIC);

ibv_send_wr CreateType2BindWr(uint64_t wr_id, ibv_mw* mw,
                              const absl::Span<uint8_t> buffer, uint32_t rkey,
                              ibv_mr* mr,
                              int access = IBV_ACCESS_REMOTE_READ |
                                           IBV_ACCESS_REMOTE_WRITE |
                                           IBV_ACCESS_REMOTE_ATOMIC);

ibv_send_wr CreateLocalInvalidateWr(uint64_t wr_id, uint32_t rkey);

ibv_send_wr CreateSendWr(uint64_t wr_id, ibv_sge* sge, int num_sge);

ibv_send_wr CreateSendWithInvalidateWr(uint64_t wr_id, uint32_t rkey);

ibv_recv_wr CreateRecvWr(uint64_t wr_id, ibv_sge* sge, int num_sge);

ibv_send_wr CreateReadWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                         void* remote_buffer, uint32_t rkey);

ibv_send_wr CreateWriteWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                          void* remote_buffer, uint32_t rkey);

ibv_send_wr CreateFetchAddWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                             void* remote_buffer, uint32_t rkey,
                             uint64_t compare_add);

ibv_send_wr CreateCompSwapWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                             void* remote_buffer, uint32_t rkey,
                             uint64_t compare_add, uint64_t swap);

void PostType1Bind(ibv_qp* qp, ibv_mw* mw, const ibv_mw_bind& bind_args);

void PostSend(ibv_qp* qp, const ibv_send_wr& wr);

void PostRecv(ibv_qp* qp, const ibv_recv_wr& wr);

void PostSrqRecv(ibv_srq* srq, const ibv_recv_wr& wr);

absl::StatusOr<ibv_wc> WaitForCompletion(
    ibv_cq* cq, absl::Duration timeout = kDefaultCompletionTimeout);

absl::Status WaitForPollingExtendedCompletion(
    ibv_cq_ex* cq, absl::Duration timeout = kDefaultCompletionTimeout);

absl::Status WaitForNextExtendedCompletion(
    ibv_cq_ex* cq, absl::Duration timeout = kDefaultCompletionTimeout);

bool CheckExtendedCompletionHasCapability(ibv_context* context,
                                          uint64_t wc_flag);

bool ExpectNoCompletion(
    ibv_cq* cq, absl::Duration timeout = kDefaultErrorCompletionTimeout);

bool ExpectNoExtendedCompletion(
    ibv_cq_ex* cq, absl::Duration timeout = kDefaultErrorCompletionTimeout);

void PrintCompletion(const ibv_wc& completion);

// Synchronously execute ops:
// 1. Create WR.
// 2. Post WR to QP.
// 3. Wait for completion and return completion status.
absl::StatusOr<ibv_wc_status> BindType1MwSync(
    ibv_qp* qp, ibv_mw* mw, absl::Span<uint8_t> buffer, ibv_mr* mr,
    int access = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_ATOMIC);

absl::StatusOr<ibv_wc_status> BindType2MwSync(
    ibv_qp* qp, ibv_mw* mw, absl::Span<uint8_t> buffer, uint32_t rkey,
    ibv_mr* mr,
    int access = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_ATOMIC);

absl::StatusOr<ibv_wc_status> ReadSync(ibv_qp* qp,
                                       absl::Span<uint8_t> local_buffer,
                                       ibv_mr* local_mr, void* remote_buffer,
                                       uint32_t rkey);

absl::StatusOr<ibv_wc_status> WriteSync(ibv_qp* qp,
                                        absl::Span<uint8_t> local_buffer,
                                        ibv_mr* local_mr, void* remote_buffer,
                                        uint32_t rkey);

absl::StatusOr<ibv_wc_status> FetchAddSync(ibv_qp* qp, void* local_buffer,
                                           ibv_mr* local_mr,
                                           void* remote_buffer, uint32_t rkey,
                                           uint64_t comp_add);

absl::StatusOr<ibv_wc_status> CompSwapSync(ibv_qp* qp, void* local_buffer,
                                           ibv_mr* local_mr,
                                           void* remote_buffer, uint32_t rkey,
                                           uint64_t comp_add, uint64_t swap);

// Execute a local invalidate op and return completion status.
absl::StatusOr<ibv_wc_status> LocalInvalidateSync(ibv_qp* qp, uint32_t rkey);

// The return pair consists of first the send side completion status then the
// recv side completion status.
absl::StatusOr<std::pair<ibv_wc_status, ibv_wc_status>> SendRecvSync(
    ibv_qp* src_qp, ibv_qp* dst_qp, absl::Span<uint8_t> src_buffer,
    ibv_mr* src_mr, absl::Span<uint8_t> dst_buffer, ibv_mr* dst_mr);

// Opens device. If device_name is not empty, tries to open the first device
// with the name; else, tries to open devices[0]. Returns the context. This
// function, OpenUntrackedDevice(), is mainly used as an internal util function.
// VerbsAllocator::OpenDevice() is preferred for most end user calls.
absl::StatusOr<ibv_context*> OpenUntrackedDevice(const std::string device_name);

}  // namespace verbs_util
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_VERBS_UTIL_H_
