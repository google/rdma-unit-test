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

#include "public/verbs_util.h"

#include <arpa/inet.h>
#include <resolv.h>
#include <sys/socket.h>

#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"

#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/status_matchers.h"

namespace rdma_unit_test {
namespace verbs_util {

int VerbsMtuToInt(ibv_mtu mtu) {
  // The enum ibv_mtu use value 1 to 5 for IBV_MTU_256 to IBV_MTU_4096.
  return 128 << mtu;
}

// Determines whether the gid is a valid ipv4 or ipv6 ip address.
// Returns AF_INET if ipv4.
// Returns AF_INET6 if ipv6.
// Reterns -1 if an invalid ip address.
int GetIpAddressType(const ibv_gid& gid) {
  char ip_str[INET6_ADDRSTRLEN];
  const char* result = inet_ntop(
      AF_INET6, reinterpret_cast<const char*>(gid.raw), ip_str, sizeof(ip_str));
  if (result == nullptr) return -1;

  const in6_addr* addr6 = reinterpret_cast<const in6_addr*>(gid.raw);
  if (addr6->s6_addr32[0] != 0 || addr6->s6_addr32[1] != 0 ||
      addr6->s6_addr16[4] != 0 || addr6->s6_addr16[5] != 0xffff) {
    return AF_INET6;
  }
  return AF_INET;
}

std::string GidToString(const ibv_gid& gid) {
  return absl::StrFormat("GID: %x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x",
                         gid.raw[0], gid.raw[1], gid.raw[2], gid.raw[3],
                         gid.raw[4], gid.raw[5], gid.raw[6], gid.raw[7],
                         gid.raw[8], gid.raw[9], gid.raw[10], gid.raw[11],
                         gid.raw[12], gid.raw[13], gid.raw[14], gid.raw[15]);
}

absl::StatusOr<std::vector<std::string>> EnumerateDeviceNames() {
  ibv_device** devices = nullptr;
  absl::Cleanup free_list = [&devices]() {
    if (devices) {
      ibv_free_device_list(devices);
    }
  };
  int num_devices = 0;
  devices = ibv_get_device_list(&num_devices);
  std::vector<std::string> device_names;
  if (num_devices <= 0 || !devices) {
    return device_names;
  }
  for (int i = 0; i < num_devices; ++i) {
    ibv_device* device = devices[i];
    VLOG(1) << "Found device " << device->name << ".";
    device_names.push_back(device->name);
  }
  return device_names;
}

ibv_srq_attr DefaultSrqAttr() {
  ibv_srq_attr attr;
  attr.max_wr = verbs_util::kDefaultMaxWr;
  attr.max_sge = verbs_util::kDefaultMaxSge;
  attr.srq_limit = 0;  // not used for infiniband.
  return attr;
}

ibv_qp_state GetQpState(ibv_qp* qp) {
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  int result = ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr);
  DCHECK_EQ(0, result);
  return attr.qp_state;
}

ibv_qp_cap GetQpCap(ibv_qp* qp) {
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  int result = ibv_query_qp(qp, &attr, IBV_QP_CAP, &init_attr);
  DCHECK_EQ(0, result);
  return attr.cap;
}

ibv_sge CreateSge(absl::Span<uint8_t> buffer, ibv_mr* mr) {
  ibv_sge sge;
  sge.addr = reinterpret_cast<uint64_t>(buffer.data());
  sge.length = buffer.length();
  sge.lkey = mr->lkey;
  return sge;
}

ibv_sge CreateAtomicSge(void* addr, ibv_mr* mr) {
  DCHECK_EQ(reinterpret_cast<uint64_t>(addr) % 8, 0ul)
      << "Address is not 8 byte aligned.";
  return CreateSge(absl::MakeSpan(reinterpret_cast<uint8_t*>(addr), 8), mr);
}

ibv_mw_bind_info CreateMwBindInfo(absl::Span<uint8_t> buffer, ibv_mr* mr,
                                  int access) {
  ibv_mw_bind_info info;
  info.addr = reinterpret_cast<uint64_t>(buffer.data());
  info.length = buffer.length();
  info.mr = mr;
  info.mw_access_flags = access;
  return info;
}

ibv_mw_bind CreateType1MwBind(uint64_t wr_id, absl::Span<uint8_t> buffer,
                              ibv_mr* mr, int access) {
  ibv_mw_bind bind;
  bind.wr_id = wr_id;
  bind.send_flags = IBV_SEND_SIGNALED;
  bind.bind_info = CreateMwBindInfo(buffer, mr, access);
  return bind;
}

ibv_send_wr CreateType2BindWr(uint64_t wr_id, ibv_mw* mw,
                              const absl::Span<uint8_t> buffer, uint32_t rkey,
                              ibv_mr* mr, int access) {
  ibv_send_wr bind;
  bind.wr_id = wr_id;
  bind.next = nullptr;
  bind.sg_list = nullptr;
  bind.num_sge = 0;
  bind.opcode = IBV_WR_BIND_MW;
  bind.send_flags = IBV_SEND_SIGNALED;
  bind.bind_mw.mw = mw;
  bind.bind_mw.rkey = rkey;
  bind.bind_mw.bind_info = CreateMwBindInfo(buffer, mr, access);
  return bind;
}

ibv_send_wr CreateLocalInvalidateWr(uint64_t wr_id, uint32_t rkey) {
  ibv_send_wr invalidate;
  invalidate.wr_id = wr_id;
  invalidate.next = nullptr;
  invalidate.sg_list = nullptr;
  invalidate.num_sge = 0;
  invalidate.opcode = IBV_WR_LOCAL_INV;
  invalidate.send_flags = IBV_SEND_SIGNALED;
  invalidate.invalidate_rkey = rkey;
  return invalidate;
}

ibv_send_wr CreateSendWr(uint64_t wr_id, ibv_sge* sge, int num_sge) {
  ibv_send_wr send;
  send.wr_id = wr_id;
  send.next = nullptr;
  send.sg_list = sge;
  send.num_sge = num_sge;
  send.opcode = IBV_WR_SEND;
  send.send_flags = IBV_SEND_SIGNALED;
  return send;
}

ibv_send_wr CreateSendWithInvalidateWr(uint64_t wr_id, uint32_t rkey) {
  ibv_send_wr inv;
  inv.wr_id = wr_id;
  inv.next = nullptr;
  inv.sg_list = nullptr;
  inv.num_sge = 0;
  inv.opcode = IBV_WR_SEND_WITH_INV;
  inv.invalidate_rkey = rkey;
  inv.send_flags = IBV_SEND_SIGNALED;
  return inv;
}

ibv_recv_wr CreateRecvWr(uint64_t wr_id, ibv_sge* sge, int num_sge) {
  ibv_recv_wr recv;
  recv.wr_id = wr_id;
  recv.next = nullptr;
  recv.sg_list = sge;
  recv.num_sge = num_sge;
  return recv;
}

ibv_send_wr CreateReadWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                         void* remote_buffer, uint32_t rkey) {
  ibv_send_wr read;
  read.wr_id = wr_id;
  read.next = nullptr;
  read.sg_list = sge;
  read.num_sge = num_sge;
  read.opcode = IBV_WR_RDMA_READ;
  read.send_flags = IBV_SEND_SIGNALED;
  read.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_buffer);
  read.wr.rdma.rkey = rkey;
  return read;
}

ibv_send_wr CreateWriteWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                          void* remote_buffer, uint32_t rkey) {
  ibv_send_wr write;
  write.wr_id = wr_id;
  write.next = nullptr;
  write.sg_list = sge;
  write.num_sge = num_sge;
  write.opcode = IBV_WR_RDMA_WRITE;
  write.send_flags = IBV_SEND_SIGNALED;
  write.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_buffer);
  write.wr.rdma.rkey = rkey;
  return write;
}

ibv_send_wr CreateFetchAddWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                             void* remote_buffer, uint32_t rkey,
                             uint64_t compare_add) {
  ibv_send_wr fetch_add;
  fetch_add.wr_id = wr_id;
  fetch_add.next = nullptr;
  fetch_add.sg_list = sge;
  fetch_add.num_sge = num_sge;
  fetch_add.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  fetch_add.send_flags = IBV_SEND_SIGNALED;

  fetch_add.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_buffer);
  fetch_add.wr.atomic.rkey = rkey;
  fetch_add.wr.atomic.compare_add = compare_add;
  return fetch_add;
}

ibv_send_wr CreateCompSwapWr(uint64_t wr_id, ibv_sge* sge, int num_sge,
                             void* remote_buffer, uint32_t rkey,
                             uint64_t compare_add, uint64_t swap) {
  ibv_send_wr cmp_and_swp;
  cmp_and_swp.wr_id = wr_id;
  cmp_and_swp.next = nullptr;
  cmp_and_swp.sg_list = sge;
  cmp_and_swp.num_sge = num_sge;
  cmp_and_swp.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  cmp_and_swp.send_flags = IBV_SEND_SIGNALED;

  cmp_and_swp.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_buffer);
  cmp_and_swp.wr.atomic.rkey = rkey;
  cmp_and_swp.wr.atomic.compare_add = compare_add;
  cmp_and_swp.wr.atomic.swap = swap;
  return cmp_and_swp;
}

void PostType1Bind(ibv_qp* qp, ibv_mw* mw, const ibv_mw_bind& bind_args) {
  int result = ibv_bind_mw(qp, mw, const_cast<ibv_mw_bind*>(&bind_args));
  ASSERT_EQ(0, result);
}

void PostSend(ibv_qp* qp, const ibv_send_wr& wr) {
  ibv_send_wr* bad_wr = nullptr;
  int result = ibv_post_send(qp, const_cast<ibv_send_wr*>(&wr), &bad_wr);
  ASSERT_EQ(0, result);
}

void PostRecv(ibv_qp* qp, const ibv_recv_wr& wr) {
  ibv_recv_wr* bad_wr = nullptr;
  int result = ibv_post_recv(qp, const_cast<ibv_recv_wr*>(&wr), &bad_wr);
  ASSERT_EQ(0, result);
}

void PostSrqRecv(ibv_srq* srq, const ibv_recv_wr& wr) {
  ibv_recv_wr* bad_wr = nullptr;
  int result = ibv_post_srq_recv(srq, const_cast<ibv_recv_wr*>(&wr), &bad_wr);
  ASSERT_EQ(0, result);
}

absl::Duration GetSlowDownTimeout(absl::Duration timeout, uint64_t multiplier) {
  if (!multiplier) {
    LOG(ERROR) << "completion_wait_multiplier should be a positive value";
    multiplier = 1;
  } else if (multiplier > 1) {
    LOG(INFO) << "Excepted timeout: " << timeout
              << ", multiplier: " << multiplier;
  }
  return timeout * multiplier;
}

absl::StatusOr<ibv_wc> WaitForCompletion(ibv_cq* cq, absl::Duration timeout) {
  ibv_wc result;
  absl::Time stop =
      absl::Now() +
      GetSlowDownTimeout(timeout,
                         absl::GetFlag(FLAGS_completion_wait_multiplier));
  int count = ibv_poll_cq(cq, 1, &result);
  while (count == 0 && absl::Now() < stop) {
    absl::SleepFor(absl::Milliseconds(10));
    count = ibv_poll_cq(cq, 1, &result);
  }
  if (count > 0) {
    return result;
  }
  return absl::DeadlineExceededError("Timeout while waiting for a completion.");
}

absl::Status WaitForPollingExtendedCompletion(ibv_cq_ex* cq,
                                              absl::Duration timeout) {
  ibv_poll_cq_attr poll_attr = {};
  int result = ibv_start_poll(cq, &poll_attr);
  absl::Time stop =
      absl::Now() +
      GetSlowDownTimeout(timeout,
                         absl::GetFlag(FLAGS_completion_wait_multiplier));
  while (result == ENOENT && absl::Now() < stop) {
    absl::SleepFor(absl::Milliseconds(10));
    result = ibv_start_poll(cq, &poll_attr);
  }
  if (result == 0) {
    return absl::OkStatus();
  }
  if (result != ENOENT) {
    return absl::InternalError("Failed to start polling completion.");
  }
  return absl::DeadlineExceededError("Timeout while waiting for a completion.");
}

absl::Status WaitForNextExtendedCompletion(ibv_cq_ex* cq,
                                           absl::Duration timeout) {
  int result = ibv_next_poll(cq);
  absl::Time stop =
      absl::Now() +
      GetSlowDownTimeout(timeout,
                         absl::GetFlag(FLAGS_completion_wait_multiplier));
  while (result == ENOENT && absl::Now() < stop) {
    absl::SleepFor(absl::Milliseconds(10));
    result = ibv_next_poll(cq);
  }
  if (result == 0) {
    return absl::OkStatus();
  }
  ibv_end_poll(cq);
  if (result != ENOENT) {
    return absl::InternalError("Failed to get next completion.");
  }
  return absl::DeadlineExceededError("Timeout while waiting for a completion.");
}

bool CheckExtendedCompletionHasCapability(ibv_context* context,
                                          uint64_t wc_flag) {
  ibv_cq_init_attr_ex cq_attr = {.cqe = 1, .wc_flags = wc_flag};
  ibv_cq_ex* cq = ibv_create_cq_ex(context, &cq_attr);
  if (cq != nullptr) {
    int result = ibv_destroy_cq(ibv_cq_ex_to_cq(cq));
    DCHECK_EQ(result, 0);
    return true;
  }
  return false;
}

bool ExpectNoCompletion(ibv_cq* cq, absl::Duration timeout) {
  return absl::IsDeadlineExceeded(WaitForCompletion(cq, timeout).status());
}

bool ExpectNoExtendedCompletion(ibv_cq_ex* cq, absl::Duration timeout) {
  return absl::IsDeadlineExceeded(
      WaitForPollingExtendedCompletion(cq, timeout));
}

void PrintCompletion(const ibv_wc& completion) {
  LOG(INFO) << "  status = " << completion.status;
  LOG(INFO) << "  vendor_err = " << completion.vendor_err;
  LOG(INFO) << "  wr_id = " << completion.wr_id;
  LOG(INFO) << "  opcode = " << completion.opcode;
  LOG(INFO) << "  qp_num = " << completion.qp_num;
}

absl::StatusOr<ibv_wc_status> BindType1MwSync(ibv_qp* qp, ibv_mw* mw,
                                              absl::Span<uint8_t> buffer,
                                              ibv_mr* mr, int access) {
  static uint32_t wr_id = 1;
  ibv_mw_bind bind = CreateType1MwBind(wr_id++, buffer, mr, access);
  PostType1Bind(qp, mw, bind);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  EXPECT_EQ(completion.wr_id, bind.wr_id);
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(completion.opcode, IBV_WC_BIND_MW);
  }
  return completion.status;
}

absl::StatusOr<ibv_wc_status> BindType2MwSync(ibv_qp* qp, ibv_mw* mw,
                                              absl::Span<uint8_t> buffer,
                                              uint32_t rkey, ibv_mr* mr,
                                              int access) {
  static uint32_t wr_id = 1;
  ibv_send_wr bind = CreateType2BindWr(wr_id++, mw, buffer, rkey, mr, access);
  PostSend(qp, bind);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  EXPECT_EQ(completion.wr_id, bind.wr_id);
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_BIND_MW, completion.opcode);
  }
  return completion.status;
}

absl::StatusOr<ibv_wc_status> ReadSync(ibv_qp* qp,
                                       absl::Span<uint8_t> local_buffer,
                                       ibv_mr* local_mr, void* remote_buffer,
                                       uint32_t rkey) {
  static uint32_t wr_id = 1;
  ibv_sge sge = CreateSge(local_buffer, local_mr);
  ibv_send_wr read =
      CreateReadWr(wr_id++, &sge, /*num_sge=*/1, remote_buffer, rkey);
  PostSend(qp, read);
  ASSIGN_OR_RETURN(ibv_wc completion, WaitForCompletion(qp->send_cq));
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  EXPECT_EQ(completion.wr_id, read.wr_id);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  }
  return completion.status;
}

absl::StatusOr<ibv_wc_status> WriteSync(ibv_qp* qp,
                                        absl::Span<uint8_t> local_buffer,
                                        ibv_mr* local_mr, void* remote_buffer,
                                        uint32_t rkey) {
  static uint32_t wr_id = 1;
  ibv_sge sge = CreateSge(local_buffer, local_mr);
  ibv_send_wr write =
      CreateWriteWr(wr_id++, &sge, /*num_sge=*/1, remote_buffer, rkey);
  PostSend(qp, write);
  ASSIGN_OR_RETURN(ibv_wc completion, WaitForCompletion(qp->send_cq));
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  EXPECT_EQ(completion.wr_id, write.wr_id);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  }
  return completion.status;
}

absl::StatusOr<ibv_wc_status> FetchAddSync(ibv_qp* qp, void* local_buffer,
                                           ibv_mr* local_mr,
                                           void* remote_buffer, uint32_t rkey,
                                           uint64_t comp_add) {
  static uint32_t wr_id = 1;
  ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(local_buffer),
      .length = 8,
      .lkey = local_mr->lkey,
  };
  ibv_send_wr fetch_add = CreateFetchAddWr(wr_id++, &sge, /*num_sge=*/1,
                                           remote_buffer, rkey, comp_add);
  PostSend(qp, fetch_add);
  ASSIGN_OR_RETURN(ibv_wc completion, WaitForCompletion(qp->send_cq));
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  EXPECT_EQ(completion.wr_id, fetch_add.wr_id);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_FETCH_ADD, completion.opcode);
  }
  return completion.status;
}

absl::StatusOr<ibv_wc_status> CompSwapSync(ibv_qp* qp, void* local_buffer,
                                           ibv_mr* local_mr,
                                           void* remote_buffer, uint32_t rkey,
                                           uint64_t comp_add, uint64_t swap) {
  static uint32_t wr_id = 1;
  ibv_sge sge{.addr = reinterpret_cast<uint64_t>(local_buffer),
              .length = 8,
              .lkey = local_mr->lkey};
  ibv_send_wr comp_swap = CreateCompSwapWr(wr_id++, &sge, /*num_sge=*/1,
                                           remote_buffer, rkey, comp_add, swap);
  PostSend(qp, comp_swap);
  ASSIGN_OR_RETURN(ibv_wc completion, WaitForCompletion(qp->send_cq));
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  EXPECT_EQ(completion.wr_id, comp_swap.wr_id);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_COMP_SWAP, completion.opcode);
  }
  return completion.status;
}

absl::StatusOr<ibv_wc_status> LocalInvalidateSync(ibv_qp* qp, uint32_t rkey) {
  static uint32_t wr_id = 1;
  ibv_send_wr invalidate = CreateLocalInvalidateWr(wr_id++, rkey);
  PostSend(qp, invalidate);
  ASSIGN_OR_RETURN(ibv_wc completion, WaitForCompletion(qp->send_cq));
  EXPECT_EQ(completion.wr_id, invalidate.wr_id);
  EXPECT_EQ(completion.qp_num, qp->qp_num);
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(completion.opcode, IBV_WC_LOCAL_INV);
  }
  return completion.status;
}

absl::StatusOr<std::pair<ibv_wc_status, ibv_wc_status>> SendRecvSync(
    ibv_qp* src_qp, ibv_qp* dst_qp, absl::Span<uint8_t> src_buffer,
    ibv_mr* src_mr, absl::Span<uint8_t> dst_buffer, ibv_mr* dst_mr) {
  ibv_sge dst_sge = CreateSge(dst_buffer, dst_mr);
  ibv_recv_wr recv = CreateRecvWr(/*wr_id=*/0, &dst_sge, /*num_sge=*/1);
  PostRecv(dst_qp, recv);

  ibv_sge src_sge = CreateSge(src_buffer, src_mr);
  ibv_send_wr send = CreateSendWr(/*wr_id=*/1, &src_sge, /*num_sge=*/1);
  PostSend(src_qp, send);

  ASSIGN_OR_RETURN(ibv_wc src_completion, WaitForCompletion(src_qp->send_cq));
  if (src_completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_SEND, src_completion.opcode);
  }
  ASSIGN_OR_RETURN(ibv_wc dst_completion, WaitForCompletion(dst_qp->recv_cq));
  if (dst_completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_RECV, dst_completion.opcode);
  }
  return std::make_pair(src_completion.status, dst_completion.status);
}

absl::StatusOr<ibv_context*> OpenUntrackedDevice(
    const std::string device_name) {
  ibv_device** devices = nullptr;
  absl::Cleanup free_list = [&devices]() {
    if (devices) {
      ibv_free_device_list(devices);
    }
  };
  int num_devices = 0;
  devices = ibv_get_device_list(&num_devices);
  if (num_devices <= 0 || !devices) {
    return absl::InternalError("No devices found.");
  }

  ibv_device* device = nullptr;
  bool device_selected = false;
  if (device_name.empty()) {
    device = devices[0];
    LOG(INFO) << "Select devices[0] (" << device->name << ").";
    device_selected = true;
  }
  for (int i = 0; i < num_devices; ++i) {
    if (device_name == devices[i]->name) {
      LOG(INFO) << "Select device " << device_name << ".";
      device = devices[i];
      device_selected = true;
    }
  }

  if (!device_selected) {
    LOG(INFO) << "Available devices for --device_name flag";
    for (int i = 0; i < num_devices; i++) {
      LOG(INFO) << devices[i]->name;
    }
    return absl::InternalError("RDMA device " + device_name + " not found.");
  }
  if (!device) {
    return absl::InternalError("Selected device is nullptr.");
  }

  ibv_context* context = ibv_open_device(device);
  if (!context) {
    return absl::InternalError("Failed to open device.");
  }

  return context;
}

}  // namespace verbs_util
}  // namespace rdma_unit_test
