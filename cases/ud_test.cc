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

#include <sched.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/loopback_fixture.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {
namespace {

// TODO(author1): UD send with invalid AH.
// TODO(author1): UD send with invalid qpn.
// TODO(author1): UD send with invalid qkey.
// TODO(author1): UD send to invalid dst IP (via AH).
// TODO(author1): UD with GRH split across many SGE.
// TODO(author1): Send between RC and UD.
// TODO(author1): UD Send with first SGE with invalid lkey.
// TODO(author1): UD Recv with insufficient buffer size.
// TODO(author1): UD send larger than MTU.

using ::testing::AnyOf;
using ::testing::Each;
using ::testing::NotNull;

class LoopbackUdQpTest : public LoopbackFixture {
 public:
  static constexpr int kQKey = 200;
  static constexpr char kLocalBufferContent = 'a';
  static constexpr char kRemoteBufferContent = 'b';

 protected:
  absl::StatusOr<std::pair<Client, Client>> CreateUdClientsPair() {
    ASSIGN_OR_RETURN(Client local, CreateClient(IBV_QPT_UD));
    std::fill_n(local.buffer.data(), local.buffer.size(), kLocalBufferContent);
    ASSIGN_OR_RETURN(Client remote, CreateClient(IBV_QPT_UD));
    std::fill_n(remote.buffer.data(), remote.buffer.size(),
                kRemoteBufferContent);
    RETURN_IF_ERROR(ibv_.SetUpUdQp(local.qp, local.port_gid, kQKey));
    RETURN_IF_ERROR(ibv_.SetUpUdQp(remote.qp, remote.port_gid, kQKey));
    return std::make_pair(local, remote);
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
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  auto recv_payload = remote.buffer.span();
  recv_payload = recv_payload.subspan(sizeof(ibv_grh), kPayloadLength);
  EXPECT_THAT(recv_payload, Each(kLocalBufferContent));
}

TEST_F(LoopbackUdQpTest, SendRnr) {
  static constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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
}

TEST_F(LoopbackUdQpTest, SendWithTooSmallRecv) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  const uint32_t recv_length = local.buffer.span().size() / 2;
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = recv_length + sizeof(ibv_grh);
  // Recv buffer is to small to fit the whole buffer.
  rsge.length -= 1;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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
  ibv_ah dummy_ah{.context = nullptr, .pd = nullptr, .handle = 1234};
  send.wr.ud.ah = &dummy_ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  // Several acceptable outcomes posting a send WR to a UD QP with invalid AH:
  // 1. IBTA spec indicates the NIC should return an immediate error. But this
  // is not following by many of the Mellanox NICs.
  // 2. Instead, Mellanox will return a IBV_WC_SUCCESS on completion.
  // 3. Based on description on both IBTA spec and Mellanox RDMA manual,
  // IBV_WC_LOC_QP_OP_ERR also matches the semantic for any invalidity of AH.
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_SUCCESS, IBV_WC_LOC_QP_OP_ERR));
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
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
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
  ASSERT_THAT(ah, NotNull());
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num + 10;  // Make invalid.
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_TRUE(verbs_util::ExpectNoCompletion(remote.cq));
}

// Read not supported on UD.
TEST_F(LoopbackUdQpTest, Read) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateUdClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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

  // This is inherantly racy, just because recv posted doesn't mean send
  // completion is there yet.
  absl::SleepFor(absl::Milliseconds(20));
  ibv_wc result[kNumCompletions + 1];
  int count = ibv_poll_cq(local.cq, kNumCompletions + 1, result);
  EXPECT_EQ(kNumCompletions, count)
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
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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
  ibv_ah* ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
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

class AdvancedLoopbackTest : public BasicFixture {
 public:
  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
    ibv_mr* mr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(/*pages=*/1);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
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

TEST_F(AdvancedLoopbackTest, RcSendUdRecvFails) {
  constexpr size_t kPayloadLength = 1000;
  constexpr int kQKey = 200;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* local_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(local_cq, NotNull());
  ibv_cq* remote_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(remote_cq, NotNull());
  ibv_qp* local_qp = ibv_.CreateQp(
      setup.pd, local_cq, local_cq, /*srq=*/nullptr, verbs_util::kDefaultMaxWr,
      verbs_util::kDefaultMaxWr, IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(local_qp, NotNull());
  ibv_qp* remote_qp =
      ibv_.CreateQp(setup.pd, remote_cq, remote_cq, /*srq=*/nullptr,
                    verbs_util::kDefaultMaxWr, verbs_util::kDefaultMaxWr,
                    IBV_QPT_UD, /*sig_all=*/0);
  ASSERT_THAT(remote_qp, NotNull());
  ASSERT_OK(ibv_.SetUpRcQp(local_qp, setup.port_gid, setup.port_gid.gid,
                           remote_qp->qp_num));
  ASSERT_OK(ibv_.SetUpUdQp(remote_qp, setup.port_gid, kQKey));

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

TEST_F(AdvancedLoopbackTest, UdSendToRcFails) {
  constexpr size_t kPayloadLength = 1000;
  constexpr int kQKey = 200;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* local_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(local_cq, NotNull());
  ibv_cq* remote_cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(remote_cq, NotNull());
  ibv_qp* local_qp = ibv_.CreateQp(
      setup.pd, local_cq, local_cq, /*srq=*/nullptr, verbs_util::kDefaultMaxWr,
      verbs_util::kDefaultMaxWr, IBV_QPT_UD, /*sig_all=*/0);
  ASSERT_THAT(local_qp, NotNull());
  ibv_qp* remote_qp =
      ibv_.CreateQp(setup.pd, remote_cq, remote_cq, /*srq=*/nullptr,
                    verbs_util::kDefaultMaxWr, verbs_util::kDefaultMaxWr,
                    IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(remote_qp, NotNull());
  ASSERT_OK(ibv_.SetUpUdQp(local_qp, setup.port_gid, kQKey));
  ASSERT_OK(ibv_.SetUpRcQp(remote_qp, setup.port_gid, setup.port_gid.gid,
                           local_qp->qp_num));
  ibv_ah* ah = ibv_.CreateAh(setup.pd, setup.port_gid.gid);
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
