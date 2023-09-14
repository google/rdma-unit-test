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

#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <sched.h>
#include <sys/socket.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "internal/handle_garble.h"
#include "internal/verbs_attribute.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/loopback_fixture.h"

namespace rdma_unit_test {
namespace {

// TODO(author1): UD send to invalid dst IP (via AH).
// TODO(author1): UD with GRH split across many SGE.
// TODO(author1): Send between RC and UD.
// TODO(author1): UD Send with first SGE with invalid lkey.

using ::testing::AnyOf;
using ::testing::Each;
using ::testing::NotNull;

class LoopbackUdQpTest : public LoopbackFixture {
 public:
  static constexpr int kQKey = 200;
  static constexpr char kLocalBufferContent = 'a';
  static constexpr char kRemoteBufferContent = 'b';

 protected:
  absl::StatusOr<std::pair<Client, Client>> CreateUdClientsPair(
      size_t pages = 1) {
    ASSIGN_OR_RETURN(Client local, CreateClient(IBV_QPT_UD, pages));
    std::fill_n(local.buffer.data(), local.buffer.size(), kLocalBufferContent);
    ASSIGN_OR_RETURN(Client remote, CreateClient(IBV_QPT_UD, pages));
    std::fill_n(remote.buffer.data(), remote.buffer.size(),
                kRemoteBufferContent);
    RETURN_IF_ERROR(ibv_.ModifyUdQpResetToRts(local.qp, kQKey));
    RETURN_IF_ERROR(ibv_.ModifyUdQpResetToRts(remote.qp, kQKey));
    return std::make_pair(local, remote);
  }

  // In ROCE2.0, the GRH is replaced by IP headers.
  // ipv4: the first 20 bytes of the GRH buffer is undefined and last 20 bytes
  // are the ipv4 headers.
  // ipv6: the whole GRH is replaced by an ipv6 header.
  iphdr ExtractIp4Header(void* buffer) {
    // TODO(author2): Verify checksum.
    iphdr iphdr;
    memcpy(&iphdr, static_cast<uint8_t*>(buffer) + 20, sizeof(iphdr));
    return iphdr;
  }
};

TEST_F(LoopbackUdQpTest, Send) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.byte_len, sizeof(ibv_grh) + kPayloadLength);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  absl::Span<uint8_t> recv_payload =
      remote.buffer.subspan(sizeof(ibv_grh), kPayloadLength);
  EXPECT_THAT(recv_payload, Each(kLocalBufferContent));
}

TEST_F(LoopbackUdQpTest, SendLargerThanMtu) {
  constexpr size_t kPages = 20;
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair(kPages));
  const int kPayloadLength =
      verbs_util::VerbsMtuToInt(local.port_attr.attr.active_mtu) + 10;
  ASSERT_GT(kPages * kPageSize, kPayloadLength);

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_LEN_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackUdQpTest, SendRnr) {
  static constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_GENERAL_ERR));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

/* According to ROCE v2 Annex A17.9.2, when setting traffic class,
   it should be correctly reflected in GRH, and should be the same on both
   ends. */
TEST_F(LoopbackUdQpTest, SendTrafficClass) {
  constexpr int kPayloadLength = 1000;
  constexpr uint8_t traffic_class = 0xff;
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  // Set with customized traffic class.
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid,
                             AhAttribute().set_traffic_class(traffic_class));
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc send_completion,
                       verbs_util::WaitForCompletion(local.cq));
  ASSERT_OK_AND_ASSIGN(ibv_wc recv_completion,
                       verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(send_completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(recv_completion.status, IBV_WC_SUCCESS);
  int ip_family = verbs_util::GetIpAddressType(local.port_attr.gid);
  ASSERT_NE(ip_family, -1);
  // On RoCE 2.0, the GRH (global routing header) is replaced by the IP header.
  if (ip_family == AF_INET) {
    // The ipv4 header is located at the lower bytes bits of the GRH. The higher
    // 20 bytes are undefined.
    // According to IPV4 header format and ROCEV2 Annex A17.4.5.2
    // Last 2 bits might be used for ECN
    iphdr ipv4_hdr = ExtractIp4Header(remote.buffer.data());
    uint8_t actual_traffic_class = ipv4_hdr.tos;
    EXPECT_EQ(actual_traffic_class & 0xfc, traffic_class & 0xfc);
    return;
  }
  // According to IPV6 header format and ROCEV2 Annex A17.4.5.2
  // Last 2 bits might be used for ECN
  ibv_grh* grh = reinterpret_cast<ibv_grh*>(remote.buffer.data());
  // 4 bits version, 8 bits traffic class, 20 bits flow label.
  uint32_t version_tclass_flow = ntohl(grh->version_tclass_flow);
  uint8_t actual_traffic_class = version_tclass_flow >> 20 & 0xfc;
  EXPECT_EQ(actual_traffic_class & 0xfc, traffic_class & 0xfc);
}

TEST_F(LoopbackUdQpTest, SendWithTooSmallRecv) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  const uint32_t recv_length = kPayloadLength + sizeof(ibv_grh) - 100;
  ibv_sge rsge =
      verbs_util::CreateSge(remote.buffer.subspan(0, recv_length), remote.mr);
  // Receive buffer is too small for the message length.
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge =
      verbs_util::CreateSge(local.buffer.subspan(0, kPayloadLength), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote.cq));
}

TEST_F(LoopbackUdQpTest, SendInvalidAh) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  HandleGarble garble(ah->handle);
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  if (completion.status == IBV_WC_SUCCESS) {
    // Some provider will ignore the handle of ibv_ah and only examine its
    // PD and ibv_grh.
    EXPECT_EQ(completion.opcode, IBV_WC_SEND);
    EXPECT_EQ(completion.qp_num, local.qp->qp_num);
    EXPECT_EQ(completion.wr_id, 1);
    ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
    EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS));
    EXPECT_EQ(IBV_WC_RECV, completion.opcode);
    EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
    EXPECT_EQ(completion.wr_id, 0);
    absl::Span<uint8_t> recv_payload =
        remote.buffer.subspan(sizeof(ibv_grh), kPayloadLength);
    EXPECT_THAT(recv_payload, Each(kLocalBufferContent));
  } else {
    // Otherwise packet won't be sent.
    EXPECT_EQ(completion.status, IBV_WC_LOC_QP_OP_ERR);
    EXPECT_EQ(completion.qp_num, local.qp->qp_num);
    EXPECT_EQ(completion.wr_id, 1);

    EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote.cq));
    absl::Span<uint8_t> recv_payload =
        remote.buffer.subspan(sizeof(ibv_grh), kPayloadLength);
    EXPECT_THAT(recv_payload, Each(kRemoteBufferContent));
  }
}

TEST_F(LoopbackUdQpTest, SendInvalidQpn) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = 0xDEADBEEF;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote.cq));
  absl::Span<uint8_t> recv_payload =
      remote.buffer.subspan(sizeof(ibv_grh), kPayloadLength);
  EXPECT_THAT(recv_payload, Each(kRemoteBufferContent));
}

TEST_F(LoopbackUdQpTest, SendInvalidQKey) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  // MSB of remote_qkey must be unset for SR's QKey to be effective.
  send.wr.ud.remote_qkey = 0xDEADBEEF & 0x7FFFFFF;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_GENERAL_ERR));
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote.cq));
  absl::Span<uint8_t> recv_payload =
      remote.buffer.subspan(sizeof(ibv_grh), kPayloadLength);
  EXPECT_THAT(recv_payload, Each(kRemoteBufferContent));
}

// Read not supported on UD.
TEST_F(LoopbackUdQpTest, Read) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  read.wr.ud.ah = ah;
  read.wr.ud.remote_qkey = kQKey;
  read.wr.ud.remote_qpn = remote.qp->qp_num;
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_QP_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

// Tests polling multiple CQE in a single call.
TEST_F(LoopbackUdQpTest, PollMultipleCqe) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  static constexpr int kNumCompletions = 5;
  static constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.

  ibv_sge recv_sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  recv_sge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, /*num_sge=*/1);
  for (int i = 0; i < kNumCompletions; ++i) {
    verbs_util::PostRecv(remote.qp, recv);
  }
  ibv_sge send_sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  send_sge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateLoopbackAh(local.pd, remote.port_attr);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  for (int i = 0; i < kNumCompletions; ++i) {
    verbs_util::PostSend(local.qp, send);
  }

  // Wait for recv completions.
  for (int i = 0; i < kNumCompletions; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(remote.cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion.opcode, IBV_WC_RECV);
    EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  }
  int count = 0;
  ibv_wc result[kNumCompletions + 1];
  // // This is inherantly racy, just because recv posted doesn't mean send
  // completion is there yet.  Allow up to 1 second for completions to arrive.
  absl::Time stop = absl::Now() + absl::Seconds(1);
  while (count != kNumCompletions && absl::Now() < stop) {
    count += ibv_poll_cq(local.cq, kNumCompletions + 1, &result[count]);
  }
  ASSERT_EQ(kNumCompletions, count)
      << "Missing completions see comment above about potential race.";
  // Spot check last completion.
  ibv_wc& completion = result[kNumCompletions - 1];
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(send.wr_id, completion.wr_id);
}

// Write not supported on Ud.
TEST_F(LoopbackUdQpTest, Write) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  write.wr.ud.ah = ah;
  write.wr.ud.remote_qkey = kQKey;
  write.wr.ud.remote_qpn = remote.qp->qp_num;
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_QP_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

// FetchAndAdd not supported on UD.
TEST_F(LoopbackUdQpTest, FetchAdd) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  *reinterpret_cast<uint64_t*>(local.buffer.data()) = 1;
  *reinterpret_cast<uint64_t*>(remote.buffer.data()) = 2;
  // The local SGE will be used to store the value before the update.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.subspan(0, 8), local.mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey,
      0);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  fetch_add.wr.ud.ah = ah;
  fetch_add.wr.ud.remote_qkey = kQKey;
  fetch_add.wr.ud.remote_qpn = remote.qp->qp_num;
  verbs_util::PostSend(local.qp, fetch_add);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_QP_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

// CompareAndSwap not supported on UD.
TEST_F(LoopbackUdQpTest, CompareSwap) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  *reinterpret_cast<uint64_t*>(local.buffer.data()) = 1;
  *reinterpret_cast<uint64_t*>(remote.buffer.data()) = 2;
  ibv_sge sge = verbs_util::CreateSge(local.buffer.subspan(0, 8), local.mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey,
      2, 3);
  ibv_ah* ah = ibv_.CreateAh(local.pd, local.port_attr.port,
                             local.port_attr.gid_index, remote.port_attr.gid);
  ASSERT_THAT(ah, NotNull());
  cmp_swp.wr.ud.ah = ah;
  cmp_swp.wr.ud.remote_qkey = kQKey;
  cmp_swp.wr.ud.remote_qpn = remote.qp->qp_num;
  verbs_util::PostSend(local.qp, cmp_swp);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_QP_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

class AdvancedLoopbackTest : public RdmaVerbsFixture {
 public:
  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    PortAttribute port_attr;
    ibv_pd* pd;
    ibv_mr* mr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(/*pages=*/1);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    return setup;
  }
};

// TODO(author2): Use LoopbackFixture and CreateClient.
TEST_F(AdvancedLoopbackTest, RcSendToUd) {
  constexpr size_t kPayloadLength = 1000;
  constexpr int kQKey = 200;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* local_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(local_cq, NotNull());
  ibv_cq* remote_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(remote_cq, NotNull());
  ibv_qp* local_qp = ibv_.CreateQp(setup.pd, local_cq, IBV_QPT_RC);
  ASSERT_THAT(local_qp, NotNull());
  ibv_qp* remote_qp = ibv_.CreateQp(setup.pd, remote_cq, IBV_QPT_UD);
  ASSERT_THAT(remote_qp, NotNull());
  ASSERT_OK(ibv_.ModifyRcQpResetToRts(
      local_qp, setup.port_attr, setup.port_attr.gid, remote_qp->qp_num,
      QpAttribute().set_timeout(absl::Seconds(1))));
  ASSERT_OK(ibv_.ModifyUdQpResetToRts(remote_qp, kQKey));

  ibv_sge rsge = verbs_util::CreateSge(
      setup.buffer.span().subspan(0, kPayloadLength + sizeof(ibv_grh)),
      setup.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote_qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(
      setup.buffer.span().subspan(0, kPayloadLength), setup.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/0, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local_cq));
  EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);
  EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote_cq));
}

TEST_F(AdvancedLoopbackTest, UdSendToRc) {
  constexpr size_t kPayloadLength = 1000;
  constexpr int kQKey = 200;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* local_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(local_cq, NotNull());
  ibv_cq* remote_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(remote_cq, NotNull());
  ibv_qp* local_qp = ibv_.CreateQp(setup.pd, local_cq, IBV_QPT_UD);
  ASSERT_THAT(local_qp, NotNull());
  ibv_qp* remote_qp = ibv_.CreateQp(setup.pd, remote_cq, IBV_QPT_RC);
  ASSERT_THAT(remote_qp, NotNull());
  ASSERT_OK(ibv_.ModifyUdQpResetToRts(local_qp, kQKey));
  ASSERT_OK(ibv_.ModifyRcQpResetToRts(
      remote_qp, setup.port_attr, setup.port_attr.gid, local_qp->qp_num,
      QpAttribute().set_timeout(absl::Seconds(1))));
  ibv_ah* ah = ibv_.CreateLoopbackAh(setup.pd, setup.port_attr);
  ASSERT_THAT(ah, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(
      setup.buffer.span().subspan(0, kPayloadLength + sizeof(ibv_grh)),
      setup.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote_qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(
      setup.buffer.span().subspan(0, kPayloadLength), setup.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/0, &lsge, /*num_sge=*/1);
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote_qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local_cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote_cq));
}

}  // namespace
}  // namespace rdma_unit_test
