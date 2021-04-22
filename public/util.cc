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

#include "public/util.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <resolv.h>
#include <stdint.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"


#include "infiniband/verbs.h"
#include "public/flags.h"

namespace rdma_unit_test {
namespace verbs_util {
namespace {

static constexpr std::array<std::pair<ibv_mtu, uint32_t>, 6> ibv_mtu_map = {{
    {IBV_MTU_256, 256},
    {IBV_MTU_512, 512},
    {IBV_MTU_1024, 1024},
    {IBV_MTU_2048, 2048},
    {IBV_MTU_4096, 4096},
}};

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

}  // namespace

LocalVerbsAddress::LocalVerbsAddress() {
}

AddressHandleAttributes::AddressHandleAttributes(
    const verbs_util::LocalVerbsAddress& verbs_address) {
  // Set base ibv_ah parameters as the source of truth.  Then
  // copy those into custom transport settings when requested.
  ibv_ah_attr_.sl = 0;
  ibv_ah_attr_.is_global = 1;
  ibv_ah_attr_.port_num = verbs_address.port();
  ibv_ah_attr_.grh.dgid = verbs_address.gid();
  ibv_ah_attr_.grh.flow_label = 0;
  ibv_ah_attr_.grh.sgid_index = verbs_address.gid_index();
  ibv_ah_attr_.grh.hop_limit = 10;
  ibv_ah_attr_.grh.traffic_class = 0;

}

// Only allow return of ibv_ah_attr if not in GOOGLE3 build.
ibv_ah_attr AddressHandleAttributes::GetAttributes() const {
  return ibv_ah_attr_;
}


std::vector<std::string> GetInterfaces() {
  std::vector<std::string> interfaces;
  struct ifaddrs* ifaddrs;
  if (getifaddrs(&ifaddrs) < 0) return interfaces;

  for (struct ifaddrs* ifa = ifaddrs; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr && (ifa->ifa_addr->sa_family == AF_INET ||
                          ifa->ifa_addr->sa_family == AF_INET6)) {
      interfaces.push_back(std::string(ifa->ifa_name));
      VLOG(1) << absl::StrFormat("GetInterfaces - %s", interfaces.back());
    }
  }
  freeifaddrs(ifaddrs);
  return interfaces;
}

std::array<uint8_t, ETH_ALEN> GetEthernetAddress(std::string_view interface) {
  int fd;
  struct ifreq ifr;
  std::array<uint8_t, ETH_ALEN> ethernet_address;
  fd = socket(AF_UNIX, SOCK_DGRAM, 0);
  CHECK_NE(fd, -1);  // Crash ok
  ifr.ifr_addr.sa_family = AF_INET;
  strncpy(ifr.ifr_name, interface.data(), IFNAMSIZ - 1);
  int result = ioctl(fd, SIOCGIFHWADDR, &ifr);
  CHECK_NE(result, -1);  // Crash ok
  close(fd);
  VLOG(1) << absl::StrFormat(
      "GetEthernetAddress: if(%s) eth(%x:%x:%x:%x:%x:%x)", interface,
      ifr.ifr_hwaddr.sa_data[0], ifr.ifr_hwaddr.sa_data[1],
      ifr.ifr_hwaddr.sa_data[2], ifr.ifr_hwaddr.sa_data[3],
      ifr.ifr_hwaddr.sa_data[4], ifr.ifr_hwaddr.sa_data[5]);
  std::copy(std::begin(ifr.ifr_hwaddr.sa_data),
            std::end(ifr.ifr_hwaddr.sa_data), std::begin(ethernet_address));

  return ethernet_address;
}

ibv_mtu ToVerbsMtu(uint64_t mtu) {
  for (auto [mtu_enum, value] : ibv_mtu_map) {
    if (mtu == value) return mtu_enum;
  }
  LOG(INFO) << "MTU value " << mtu
            << " not supported. Using default value of 1024.";
  return IBV_MTU_1024;
}

uint64_t VerbsMtuToValue(ibv_mtu mtu) {
  for (auto [mtu_enum, value] : ibv_mtu_map) {
    if (mtu == mtu_enum) return value;
  }
  LOG(FATAL) << "illegal mtu size " << static_cast<uint64_t>(mtu);  // Crash ok
  return IBV_MTU_1024;
}

std::string GidToString(const ibv_gid& gid) {
  return absl::StrFormat("GID: %x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x",
                         gid.raw[0], gid.raw[1], gid.raw[2], gid.raw[3],
                         gid.raw[4], gid.raw[5], gid.raw[6], gid.raw[7],
                         gid.raw[8], gid.raw[9], gid.raw[10], gid.raw[11],
                         gid.raw[12], gid.raw[13], gid.raw[14], gid.raw[15]);
}

absl::StatusOr<std::vector<LocalVerbsAddress>> EnumeratePortsForContext(
    ibv_context* context) {
  std::vector<LocalVerbsAddress> result;
  bool no_ipv6_for_gid = absl::GetFlag(FLAGS_no_ipv6_for_gid);
  LOG(INFO) << "Enumerating Ports for " << context
            << "no_ipv6: " << no_ipv6_for_gid;
  ibv_device_attr dev_attr = {};
  int query_result = ibv_query_device(context, &dev_attr);
  if (query_result != 0) {
    return absl::InternalError("Failed to query device ports.");
  }

  // libibverbs port numbers start at 1.
  for (int port_idx = 1; port_idx <= dev_attr.phys_port_cnt; ++port_idx) {
    ibv_port_attr port_attr = {};
    query_result = ibv_query_port(context, port_idx, &port_attr);
    if (query_result != 0) {
      return absl::InternalError("Failed to query port attributes.");
    }
    if (port_attr.state != IBV_PORT_ACTIVE) {
      continue;
    }
    for (int gid_idx = 0; gid_idx < port_attr.gid_tbl_len; ++gid_idx) {
      ibv_gid gid = {};
      query_result = ibv_query_gid(context, port_idx, gid_idx, &gid);
      if (query_result != 0) {
        return absl::InternalError("Failed to query gid.");
      }
      auto ip_type = GetIpAddressType(gid);
      if (ip_type == -1) {
        continue;
      }
      if (no_ipv6_for_gid && (ip_type == AF_INET6)) {
        continue;
      }

      // Check the MTU size of the port against the max mtu attribute if the
      // value is other than 0. 0 means it is unset.
      if (port_attr.active_mtu && result.empty() &&
          (absl::GetFlag(FLAGS_verbs_mtu) >
           VerbsMtuToValue(port_attr.active_mtu))) {
        LOG(FATAL) << "--verbs_mtu exceeds active port limit of "  // Crash ok
                   << VerbsMtuToValue(port_attr.active_mtu);
      }
      VLOG(2) << "Adding: " << GidToString(gid);
      LocalVerbsAddress match;
      match.set_port(port_idx);
      match.set_gid(gid);
      match.set_gid_index(gid_idx);
      result.push_back(match);
    }
  }
  CHECK(!result.empty()) << "No active ports detected.";  // Crash ok
  return result;
}


ibv_qp_state GetQpState(ibv_qp* qp) {
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  int result = ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr);
  DCHECK_EQ(0, result);
  return attr.qp_state;
}

ibv_sge CreateSge(absl::Span<uint8_t> buffer, ibv_mr* mr) {
  ibv_sge sge;
  sge.addr = reinterpret_cast<uint64_t>(buffer.data());
  sge.length = buffer.length();
  sge.lkey = mr->lkey;
  return sge;
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

absl::Status BindType1Mw(ibv_mw* mw, ibv_qp* qp, ibv_mw_bind bind_arg,
                         ibv_cq* cq) {
  int result = ibv_bind_mw(qp, mw, &bind_arg);
  if (result != 0) {
    return absl::InternalError(absl::StrCat("Library call returned ", result));
  }
  auto completion_result = verbs_util::WaitForCompletion(cq);
  if (!completion_result.ok()) return completion_result.status();
  ibv_wc completion = completion_result.value();
  if (IBV_WC_SUCCESS != completion.status) {
    return absl::InternalError(
        absl::StrCat("Completion error code ", completion.status));
  }
  if (IBV_WC_BIND_MW != completion.opcode) {
    return absl::InternalError(
        absl::StrCat("Wrong completion type ", completion.opcode));
  }
  return absl::OkStatus();
}

ibv_send_wr CreateInvalidateWr(uint64_t wr_id, uint32_t rkey) {
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

absl::StatusOr<ibv_wc> WaitForCompletion(ibv_cq* cq, absl::Duration timeout) {
  ibv_wc result;
  absl::Time stop = absl::Now() + timeout;
  int count = ibv_poll_cq(cq, 1, &result);
  while (count == 0 && absl::Now() < stop) {
    absl::SleepFor(absl::Milliseconds(10));
    count = ibv_poll_cq(cq, 1, &result);
  }
  if (count > 0) {
    return result;
  }
  return absl::InternalError("Timeout while waiting for a completion");
}

void PrintCompletion(const ibv_wc& completion) {
  LOG(INFO) << "  status = " << completion.status;
  LOG(INFO) << "  vendor_err = " << completion.vendor_err;
  LOG(INFO) << "  wr_id = " << completion.wr_id;
  LOG(INFO) << "  opcode = " << completion.opcode;
  LOG(INFO) << "  qp_num = " << completion.qp_num;
}

ibv_wc_status BindType1MwSync(ibv_qp* qp, ibv_mw* mw,
                              absl::Span<uint8_t> buffer, ibv_mr* mr,
                              int access) {
  ibv_mw_bind bind = CreateType1MwBind(/*wr_id=*/1, buffer, mr, access);
  PostType1Bind(qp, mw, bind);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_BIND_MW, completion.opcode);
  }
  return completion.status;
}

ibv_wc_status BindType2MwSync(ibv_qp* qp, ibv_mw* mw,
                              absl::Span<uint8_t> buffer, uint32_t rkey,
                              ibv_mr* mr, int access) {
  ibv_send_wr bind =
      CreateType2BindWr(/*wr_id=*/1, mw, buffer, rkey, mr, access);
  PostSend(qp, bind);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_BIND_MW, completion.opcode);
    bind.bind_mw.mw->rkey = bind.bind_mw.rkey;
  }
  return completion.status;
}

ibv_wc_status ReadSync(ibv_qp* qp, absl::Span<uint8_t> local_buffer,
                       ibv_mr* local_mr, void* remote_buffer, uint32_t rkey) {
  ibv_sge sge = CreateSge(local_buffer, local_mr);
  ibv_send_wr read =
      CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1, remote_buffer, rkey);
  PostSend(qp, read);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  }
  return completion.status;
}

ibv_wc_status WriteSync(ibv_qp* qp, absl::Span<uint8_t> local_buffer,
                        ibv_mr* local_mr, void* remote_buffer, uint32_t rkey) {
  ibv_sge sge = CreateSge(local_buffer, local_mr);
  ibv_send_wr read =
      CreateWriteWr(/*wr_id=*/1, &sge, /*num_sge=*/1, remote_buffer, rkey);
  PostSend(qp, read);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  }
  return completion.status;
}

ibv_wc_status FetchAddSync(ibv_qp* qp, void* local_buffer, ibv_mr* local_mr,
                           void* remote_buffer, uint32_t rkey,
                           uint64_t comp_add) {
  ibv_sge sge;
  sge.addr = reinterpret_cast<uint64_t>(local_buffer);
  sge.length = 8;
  sge.lkey = local_mr->lkey;
  ibv_send_wr read = CreateFetchAddWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                      remote_buffer, rkey, comp_add);
  PostSend(qp, read);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_FETCH_ADD, completion.opcode);
  }
  return completion.status;
}

ibv_wc_status CompSwapSync(ibv_qp* qp, void* local_buffer, ibv_mr* local_mr,
                           void* remote_buffer, uint32_t rkey,
                           uint64_t comp_add, uint64_t swap) {
  ibv_sge sge;
  sge.addr = reinterpret_cast<uint64_t>(local_buffer);
  sge.length = 8;
  sge.lkey = local_mr->lkey;
  ibv_send_wr read = CreateCompSwapWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                      remote_buffer, rkey, comp_add, swap);
  PostSend(qp, read);
  ibv_wc completion = WaitForCompletion(qp->send_cq).value();
  if (completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_COMP_SWAP, completion.opcode);
  }
  return completion.status;
}

std::pair<ibv_wc_status, ibv_wc_status> SendRecvSync(
    ibv_qp* src_qp, ibv_qp* dst_qp, absl::Span<uint8_t> src_buffer,
    ibv_mr* src_mr, absl::Span<uint8_t> dst_buffer, ibv_mr* dst_mr) {
  ibv_sge dst_sge = CreateSge(dst_buffer, dst_mr);
  ibv_recv_wr recv = CreateRecvWr(/*wr_id=*/0, &dst_sge, /*num_sge=*/1);
  PostRecv(dst_qp, recv);

  ibv_sge src_sge = CreateSge(src_buffer, src_mr);
  ibv_send_wr send = CreateSendWr(/*wr_id=*/1, &src_sge, /*num_sge=*/1);
  PostSend(src_qp, send);

  ibv_wc src_completion = WaitForCompletion(src_qp->send_cq).value();
  if (src_completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_SEND, src_completion.opcode);
  }
  ibv_wc dst_completion = WaitForCompletion(dst_qp->recv_cq).value();
  if (dst_completion.status == IBV_WC_SUCCESS) {
    EXPECT_EQ(IBV_WC_RECV, dst_completion.opcode);
  }
  return std::make_pair(src_completion.status, dst_completion.status);
}

absl::StatusOr<ibv_context*> OpenUntrackedDevice(
    const std::string device_name) {
  ibv_device** devices = nullptr;
  auto free_list = absl::MakeCleanup([&devices]() {
    if (devices) {
      ibv_free_device_list(devices);
    }
  });
  int num_devices = 0;
  devices = ibv_get_device_list(&num_devices);
  if (num_devices <= 0 || !devices) {
    return absl::InternalError("No devices found.");
  }

  ibv_device* device = nullptr;
  bool device_selected = false;
  if (device_name.empty()) {
    LOG(INFO) << "Select devices[0] (" << device_name << ").";
    device = devices[0];
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
