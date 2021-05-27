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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_UTIL_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_UTIL_H_

#include <net/ethernet.h>

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "glog/logging.h"
#include "linux/if_ether.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"



#include "infiniband/verbs.h"

namespace rdma_unit_test {
namespace verbs_util {

//////////////////////////////////////////////////////////////////////////////
//                          Classes and Data Types
//////////////////////////////////////////////////////////////////////////////

// Encapsulates the various attributes that are used to define a local verbs
// endpoint. Note: a remote endpoint is solely defined by an ibv_gid.
class LocalEndpointAttr {
 public:
  LocalEndpointAttr() = default;
  LocalEndpointAttr(uint8_t port, ibv_gid gid, uint8_t gid_index);
  ~LocalEndpointAttr() = default;
  // Copyable.
  LocalEndpointAttr(const LocalEndpointAttr& other) = default;
  LocalEndpointAttr& operator=(const LocalEndpointAttr& other) = default;
  LocalEndpointAttr(LocalEndpointAttr&& other) = default;
  LocalEndpointAttr& operator=(LocalEndpointAttr&& other) = default;

  uint8_t port() const;
  ibv_gid gid() const;
  uint8_t gid_index() const;


 private:
  uint8_t port_;
  ibv_gid gid_;
  uint8_t gid_index_;
};

// Creates an abstraction for ibv_ah_attr.
class AddressHandleAttr {
 public:
  AddressHandleAttr() = delete;
  // Creates an AddressHandle with default attributes with the info from the
  // passed endpoint attributes.
  explicit AddressHandleAttr(const verbs_util::LocalEndpointAttr& local,
                             ibv_gid remote_gid);
  ~AddressHandleAttr() = default;

  // Returns the underlying ibv_ah_attr.
  ibv_ah_attr GetAttributes() const;

 private:
  ibv_ah_attr ibv_ah_attr_;
};

//////////////////////////////////////////////////////////////////////////////
//                          Constants
//////////////////////////////////////////////////////////////////////////////

// By default QPs will be constructed with max_inline_data set to this.
constexpr uint32_t kDefaultMaxInlineSize = 36;
// Default Wr capacity for send queue, receive queue, completion queue and
// shared receive queue.
constexpr uint32_t kDefaultMaxWr = 200;
// Default Sge capacity for single Wr in send queue, receive queue, compleetion
// queue and shared receive queue.
constexpr uint32_t kDefaultMaxSge = 1;
// Default timeout waiting for completion.
constexpr absl::Duration kDefaultCompletionTimeout = absl::Seconds(2);
// Default timeout waiting for completion on a known qp error
constexpr absl::Duration kDefaultErrorCompletionTimeout = absl::Seconds(10);
// Definition for IPv6 Loopback Address.
constexpr std::string_view kIpV6LoopbackAddress{"::1"};

// Programmatically define the page size in lieue of calling sysconf
#if defined(__x86_64__) || defined(__i386__)
constexpr uint32_t kPageSize = 4 * 1024;  // 4K
#else
#error "No page size defined for this architecture"
#endif

//////////////////////////////////////////////////////////////////////////////
//                          Helper Functions
//////////////////////////////////////////////////////////////////////////////

// Returns of vector of attached interface names of type AF_PACKET.
std::vector<std::string> GetInterfaces();

// Returns the ethernet address of the given interface.
std::array<uint8_t, ETH_ALEN> GetEthernetAddress(std::string_view interface);

// Converts an uint64_t mtu to a ibv_mtu object.
ibv_mtu ToVerbsMtu(uint64_t mtu);

// Convert Gid to a string
std::string GidToString(const ibv_gid& gid);

// Enumerate all ports with (one of) their sgid(s).
absl::StatusOr<std::vector<LocalEndpointAttr>> EnumeratePortsForContext(
    ibv_context* context);

// Verbs utilities:
// Useful helper functions to eliminate the tediousness of filling repetitive
// attributes and flags. Simplifies the verb API.

ibv_qp_state GetQpState(ibv_qp* qp);

ibv_sge CreateSge(absl::Span<uint8_t> buffer, ibv_mr* mr);
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

ibv_send_wr CreateInvalidateWr(uint64_t wr_id, uint32_t rkey);
ibv_send_wr CreateSendWr(uint64_t wr_id, ibv_sge* sge, int num_sge);
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

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_UTIL_H_
