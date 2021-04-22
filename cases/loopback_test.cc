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
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/status_matchers.h"
#include "public/introspection.h"
#include "public/rdma-memblock.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

// TODO(author1): UD send with invalid AH.
// TODO(author1): UD send with invalid qpn.
// TODO(author1): UD send with invalid qkey.
// TODO(author1): UD send to invalid dst IP (via AH).
// TODO(author1): UD with GRH split across many SGE.
// TODO(author1): Send between RC and UD.
// TODO(author1): UD Send with first SGE with invalid lkey.
// TODO(author1): UD Recv with insufficient buffer size.
// TODO(author1): UD send larger than MTU.
// TODO(author1): Add tests stressing SGEs.
// TODO(author1): Send with insufficient recv buffering (RC and UD).

class LoopbackTest : public BasicFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr int kQueueSize = 200;
  static constexpr int kQKey = 200;

  struct Client {
    ibv_context* context = nullptr;
    verbs_util::LocalVerbsAddress address;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_qp* qp = nullptr;
    // UD based tests need a RC QP in order to bind Type1 MW.
    ibv_qp* control_qp = nullptr;
    // AH pointing to the other client.
    ibv_ah* other_ah = nullptr;
    ibv_mr* mr = nullptr;
    ibv_mw* type1_mw = nullptr;
    ibv_mw* type2_mw = nullptr;
    RdmaMemBlock buffer;
    ibv_mr* atomic_mr = nullptr;
    RdmaMemBlock atomic_buffer;
  };

  // Interface to lookup the type of QP under test.
  virtual ibv_qp_type QpType() const = 0;

  // Interface to connection a pair of QP's.  his method is overridden by
  // LoopbackRcQpTest and LoopbackUdQpTest to interconnect the 'local' and
  // 'remote' QP's.
  virtual absl::Status SetUpInterconnection(Client& local, Client& remote) = 0;

  absl::StatusOr<Client> CreateClient(uint8_t buf_content = '-') {
    Client client;
    client.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    memset(client.buffer.data(), buf_content, client.buffer.size());
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    client.context = context_or.value();
    client.address = ibv_.GetContextAddressInfo(client.context);
    client.pd = ibv_.AllocPd(client.context);
    if (!client.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    client.cq = ibv_.CreateCq(client.context);
    if (!client.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    client.qp = ibv_.CreateQp(client.pd, client.cq, client.cq, nullptr,
                              kQueueSize, kQueueSize, QpType(), /*sig_all=*/0);
    if (!client.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    client.control_qp =
        ibv_.CreateQp(client.pd, client.cq, client.cq, nullptr, kQueueSize,
                      kQueueSize, IBV_QPT_RC, /*sig_all=*/0);
    if (!client.control_qp) {
      return absl::InternalError("Failed to create qp.");
    }
    ibv_.SetUpSelfConnectedRcQp(client.control_qp, client.address);
    // memory setup.
    client.mr = ibv_.RegMr(client.pd, client.buffer);
    if (!client.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    client.type1_mw = ibv_.AllocMw(client.pd, IBV_MW_TYPE_1);
    if (!client.type1_mw) {
      return absl::InternalError("Failed to allocate type1 mw.");
    }
    if (Introspection().SupportsType2()) {
      client.type2_mw = ibv_.AllocMw(client.pd, IBV_MW_TYPE_2);
      if (!client.type2_mw) {
        return absl::InternalError("Failed to open device.");
      }
    }
    return client;
  }

  // Populates |client|.atomic_buffer and |client|.atomic_mr and fills the
  // resulting buffer with |content| repeated.
  absl::Status InitializeAtomicBuffer(Client& client, uint64_t content) {
    DCHECK_EQ(client.atomic_buffer.size(), 0UL)
        << "Atomic buffer already initialized";
    client.atomic_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    DCHECK_EQ(reinterpret_cast<uintptr_t>(client.atomic_buffer.data()) % 8, 0);
    for (size_t i = 0; i < client.atomic_buffer.size(); i += sizeof(content)) {
      *reinterpret_cast<uint64_t*>(client.atomic_buffer.data() + i) = content;
    }
    DCHECK(!client.atomic_mr);
    client.atomic_mr = ibv_.RegMr(client.pd, client.atomic_buffer);
    if (!client.mr) {
      return absl::InternalError("Failed to register atomic_mr.");
    }
    return absl::OkStatus();
  }

  absl::Status BindMws(Client& client, uint32_t type2_rkey) {
    DCHECK(client.control_qp);
    DCHECK(client.type1_mw);
    DCHECK(client.mr);
    DCHECK_EQ(IBV_QPT_RC, client.control_qp->qp_type);
    DCHECK_EQ(IBV_QPS_RTS, verbs_util::GetQpState(client.control_qp));
    DCHECK_EQ(IBV_QPS_RTS, verbs_util::GetQpState(client.qp));
    ibv_wc_status status = verbs_util::BindType1MwSync(
        client.control_qp, client.type1_mw, client.buffer.span(), client.mr);
    if (status != IBV_WC_SUCCESS) {
      return absl::InternalError(
          absl::StrCat("Failed to bind type 1 mw.(", status, ")."));
    }
    if (Introspection().SupportsType2()) {
      ibv_wc_status status_type2 = verbs_util::BindType2MwSync(
          client.qp, client.type2_mw, client.buffer.span(), type2_rkey,
          client.mr);
      if (status_type2 != IBV_WC_SUCCESS) {
        return absl::InternalError(
            absl::StrCat("Failed to bind type 2 mw.(", status_type2, ")."));
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::pair<Client, Client>> CreateConnectedClientsPair() {
    auto local_or = CreateClient(/*buf_content=*/'a');
    if (!local_or.ok()) {
      return local_or.status();
    }
    Client local = local_or.value();
    auto remote_or = CreateClient(/*buf_content=*/'b');
    if (!remote_or.ok()) {
      return remote_or.status();
    }
    Client remote = remote_or.value();
    absl::Status status = SetUpInterconnection(local, remote);
    if (!status.ok()) {
      return status;
    }
    status = BindMws(local, /*type2_rkey=*/1024);
    if (!status.ok()) {
      return status;
    }
    status = BindMws(remote, /*type2_rkey=*/2028);
    if (!status.ok()) {
      return status;
    }
    return std::make_pair(local, remote);
  }
};

class LoopbackRcQpTest : public LoopbackTest {
 public:
  void SetUp() override {
    if (!Introspection().SupportsRcQp()) {
      GTEST_SKIP() << "Nic does not support RC QP";
    }
  }

 protected:
  ibv_qp_type QpType() const override { return IBV_QPT_RC; }

  absl::Status SetUpInterconnection(Client& local, Client& remote) {
    ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, remote.address);
    return absl::OkStatus();
  }
};

class LoopbackUdQpTest : public LoopbackTest {
 public:
  void SetUp() override {
    if (!Introspection().SupportsUdQp()) {
      GTEST_SKIP() << "Nic does not support UD QP";
    }
  }

 protected:
  ibv_qp_type QpType() const override { return IBV_QPT_UD; }

  absl::Status SetUpInterconnection(Client& local, Client& remote) {
    // Set up UD connection.
    absl::Status status = ibv_.SetUpUdQp(local.qp, local.address, kQKey);
    if (!status.ok()) {
      return status;
    }
    status = ibv_.SetUpUdQp(remote.qp, remote.address, kQKey);
    if (!status.ok()) {
      return status;
    }
    local.other_ah = ibv_.CreateAh(local.pd);
    if (!local.other_ah) {
      return absl::InternalError("Failed to create local ah.");
    }
    remote.other_ah = ibv_.CreateAh(remote.pd);
    if (!remote.other_ah) {
      return absl::InternalError("Failed to create remote ah.");
    }
    return absl::OkStatus();
  }
};

TEST_F(LoopbackRcQpTest, Send) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();

  ibv_sge sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_EQ(0, completion.wc_flags);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, UnsignaledSend) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.send_flags = send.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_EQ(0, completion.wc_flags);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));

  EXPECT_EQ(0, ibv_poll_cq(local.cq, 1, &completion));
}

TEST_F(LoopbackRcQpTest, SendZeroSize) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, nullptr, /*num_sge=*/0);
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));
}

// Send a 64MB chunk from local to remote
TEST_F(LoopbackRcQpTest, SendLargeChunk) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  // prepare buffer
  RdmaMemBlock send_buf = ibv_.AllocBuffer(/*pages=*/16);
  memset(send_buf.data(), 'a', send_buf.size());
  RdmaMemBlock recv_buf = ibv_.AllocBuffer(/*pages=*/16);
  memset(recv_buf.data(), 'b', recv_buf.size());
  ibv_mr* send_mr = ibv_.RegMr(local.pd, send_buf);
  ibv_mr* recv_mr = ibv_.RegMr(remote.pd, recv_buf);

  ibv_sge rsge = verbs_util::CreateSge(recv_buf.span(), recv_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(send_buf.span(), send_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_THAT(recv_buf.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, SendInlineData) {
  const size_t kSendSize = verbs_util::kDefaultMaxInlineSize;
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_GE(remote.buffer.size(), kSendSize) << "receiver buffer too small";
  // a vector which is not registered to pd or mr
  auto data_src = std::make_unique<std::vector<uint8_t>>(kSendSize);
  std::fill(data_src->begin(), data_src->end(), 'c');

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge;
  lsge.addr = reinterpret_cast<uint64_t>(data_src->data());
  lsge.length = kSendSize;
  lsge.lkey = 0xDEADBEEF;  // random bad keys
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.send_flags |= IBV_SEND_INLINE;
  verbs_util::PostSend(local.qp, send);
  (*data_src)[0] = 'a';  // source can be modified immediately
  data_src.reset();  // delete the source buffer immediately after post_send()

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data(), kSendSize),
              ::testing::Each('c'));
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data() + kSendSize,
                             remote.buffer.data() + remote.buffer.size()),
              ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, SendImmData) {
  // The immediate data should be in network byte order according to the type
  // But we are just lazy here and assume that the two sides have the same
  // endianness.
  const uint32_t kImm = 0xBADDCAFE;
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_IMM;
  send.imm_data = kImm;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_NE(0, completion.wc_flags & IBV_WC_WITH_IMM);
  EXPECT_EQ(kImm, completion.imm_data);

  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, SendWithInvalidate) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_INV;
  send.invalidate_rkey = remote.type2_mw->rkey;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_EQ(0, completion.wc_flags);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));

  // Check that rkey is invalid.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);
  completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(2, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, SendWithInvalidateNoBuffer) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_INV;
  send.invalidate_rkey = remote.type2_mw->rkey;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_RNR_RETRY_EXC_ERR, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);

  // Check that rkey is valid.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);
  completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(2, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, SendWithInvalidateBadRkey) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_INV;
  // Invalid rkey.
  send.invalidate_rkey = (remote.type2_mw->rkey + 10) * 5;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_EQ(0, completion.wc_flags);

}

TEST_F(LoopbackRcQpTest, SendWithInvalidateType1Rkey) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_INV;
  // Type1 rkey.
  send.invalidate_rkey = remote.type1_mw->rkey;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_EQ(0, completion.wc_flags);

}

// Send with Invalidate targeting another QPs MW.
TEST_F(LoopbackRcQpTest, SendWithInvalidateWrongQp) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_qp* qp = ibv_.CreateQp(local.pd, local.cq, local.cq, nullptr, kQueueSize,
                             kQueueSize, QpType(), /*sig_all=*/0);
  ASSERT_NE(nullptr, qp);
  ibv_.SetUpSelfConnectedRcQp(qp, local.address);

  ibv_sge rsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_INV;
  send.invalidate_rkey = local.type2_mw->rkey;
  verbs_util::PostSend(qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_TRUE(completion.opcode == IBV_WC_SEND ||
              completion.opcode == IBV_WC_RECV);
  EXPECT_EQ(qp->qp_num, completion.qp_num);
  EXPECT_TRUE(completion.wr_id == 0 || completion.wr_id == 1);
  completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_TRUE(completion.opcode == IBV_WC_SEND ||
              completion.opcode == IBV_WC_RECV);
  EXPECT_EQ(qp->qp_num, completion.qp_num);
  EXPECT_TRUE(completion.wr_id == 0 || completion.wr_id == 1);

}

TEST_F(LoopbackRcQpTest, SendWithTooSmallRecv) {
  // Recv buffer is to small to fit the whole buffer.
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  const uint32_t kRecvLength = local.buffer.span().size() - 1;
  sge.length = kRecvLength;
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  ASSERT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);
  ASSERT_EQ(local.qp->qp_num, completion.qp_num);
  ASSERT_EQ(1, completion.wr_id);
  ASSERT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(local.qp));
  ASSERT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(remote.qp));
}

TEST_F(LoopbackRcQpTest, BadRecvAddr) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  --rsge.addr;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, RecvOnDeregisteredRegion) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ASSERT_EQ(0, ibv_.DeallocMw(remote.type1_mw));
  // If the NIC supports type 2 windows both windows must be deallocated.
  if (Introspection().SupportsType2()) {
    ASSERT_EQ(0, ibv_.DeallocMw(remote.type2_mw));
  }
  ASSERT_EQ(0, ibv_.DeregMr(remote.mr));

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  // The op code isn't set by the hardware in the error case.
  EXPECT_EQ(IBV_WC_REM_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));

  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(local.qp));
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(remote.qp));
}

TEST_F(LoopbackRcQpTest, BadRecvLength) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ++rsge.length;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  ibv_wc completion2 = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(remote.qp->qp_num, completion2.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(0, completion2.wr_id);
  if (!Introspection().ShouldDeviateForCurrentTest() &&
      Introspection().CorrectlyReportsInvalidRecvLengthErrors()) {
    EXPECT_EQ(IBV_WC_REM_OP_ERR, completion.status);
    EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion2.status);
  } else {
    EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
    EXPECT_EQ(IBV_WC_SEND, completion.opcode);
    EXPECT_EQ(IBV_WC_SUCCESS, completion2.status);
    EXPECT_EQ(IBV_WC_RECV, completion2.opcode);
  }
}

TEST_F(LoopbackRcQpTest, BadRecvLkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  // Change lkey to be invalid.
  rsge.lkey = (rsge.lkey + 10) * 5;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, SendInvalidLkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  // Existing hardware does not set opcode on error.
  EXPECT_EQ(1, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, UnsignaledSendError) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  send.send_flags = send.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, send);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  // Existing hardware does not set opcode on error.
  EXPECT_EQ(1, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, BasicRead) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, UnsignaledRead) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  read.send_flags = read.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, read);
  read.send_flags = read.send_flags | IBV_SEND_SIGNALED;
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(2, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, QpSigAll) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_qp* qp = ibv_.CreateQp(local.pd, local.cq, local.cq, nullptr, kQueueSize,
                             kQueueSize, QpType(), /*sig_all=*/1);
  ASSERT_NE(nullptr, qp);
  ibv_.SetUpSelfConnectedRcQp(qp, local.address);
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, local.buffer.data(), local.mr->rkey);
  read.send_flags = read.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(qp->qp_num, completion.qp_num);
}

TEST_F(LoopbackRcQpTest, Type1MWRead) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type1_mw->rkey);
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, Type2MWRead) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  if (!Introspection().SupportsType2()) {
    GTEST_SKIP() << "Needs type 2 MW.";
  }
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, Type1MWUnbind) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);

  // Rebind to get a new rkey.
  uint32_t original_rkey = remote.type1_mw->rkey;
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::BindType1MwSync(remote.control_qp, remote.type1_mw,
                                        remote.buffer.span(), remote.mr));
  uint32_t new_rkey = remote.type1_mw->rkey;
  EXPECT_NE(original_rkey, new_rkey);

  // Issue a read with the new rkey.
  ibv_send_wr read = verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                              remote.buffer.data(), new_rkey);
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('b'));

  // Issue a read with the old rkey.
  read.wr.rdma.rkey = original_rkey;
  verbs_util::PostSend(local.qp, read);
  completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
}

TEST_F(LoopbackRcQpTest, ReadInvalidLkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Verify buffer is unchanged.
  EXPECT_THAT(local.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, UnsignaledReadError) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  read.send_flags = read.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, ReadInvalidRkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Change rkey to be invalid.
  read.wr.rdma.rkey = (read.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Verify buffer is unchanged.
  EXPECT_THAT(local.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, ReadInvalidRKeyAndInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Invalidate the lkey.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Invalidate the rkey too.
  read.wr.rdma.rkey = (read.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, BasicWrite) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, UnsignaledWrite) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.send_flags = write.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, write);
  write.wr_id = 2;
  write.send_flags = write.send_flags | IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(2, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, WriteInlineData) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  const size_t kWriteSize = verbs_util::kDefaultMaxInlineSize;
  ASSERT_GE(remote.buffer.size(), kWriteSize) << "receiver buffer too small";
  // a vector which is not registered to pd or mr
  auto data_src = std::make_unique<std::vector<uint8_t>>(kWriteSize);
  std::fill(data_src->begin(), data_src->end(), 'c');

  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  sge.addr = reinterpret_cast<uint64_t>(data_src->data());
  sge.length = kWriteSize;
  sge.lkey = 0xDEADBEEF;  // random bad keys
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.send_flags |= IBV_SEND_INLINE;
  verbs_util::PostSend(local.qp, write);
  (*data_src)[0] = 'a';  // source can be modified immediately
  data_src.reset();  // src buffer can be deleted immediately after post_send()

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data(), kWriteSize),
              ::testing::Each('c'));
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data() + kWriteSize,
                             remote.buffer.data() + remote.buffer.size()),
              ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteImmData) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  const uint32_t kImm = 0xBADDCAFE;

  // Create a dummy buffer which shouldn't be touched.
  const unsigned int kDummyBufSize = 100;
  RdmaMemBlock dummy_buf =
      ibv_.AllocAlignedBuffer(/*pages=*/1).subblock(0, kDummyBufSize);
  ASSERT_EQ(kDummyBufSize, dummy_buf.size());
  memset(dummy_buf.data(), 'd', dummy_buf.size());
  ibv_mr* dummy_mr = ibv_.RegMr(remote.pd, dummy_buf);

  // WRITE_WITH_IMM requires a RR. Use the dummy buf for sg_list.
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.addr = reinterpret_cast<uint64_t>(dummy_buf.data());
  rsge.length = dummy_buf.size();
  rsge.lkey = dummy_mr->lkey;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  // Post write to remote.buffer.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  write.imm_data = kImm;
  verbs_util::PostSend(local.qp, write);

  // Verify WRITE completion.
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);

  // Verify RECV completion.
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV_RDMA_WITH_IMM, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  EXPECT_NE(0, completion.wc_flags & IBV_WC_WITH_IMM);
  EXPECT_EQ(kImm, completion.imm_data);

  // Verify that data written to the correct buffer.
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));
  EXPECT_THAT(dummy_buf.span(), ::testing::Each('d'));
}

TEST_F(LoopbackRcQpTest, WriteImmDataRnR) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  const uint32_t kImm = 0xBADDCAFE;

  // Post write to remote.buffer.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  write.imm_data = kImm;
  verbs_util::PostSend(local.qp, write);

  // Verify WRITE completion.
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_RNR_RETRY_EXC_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, Type1MWWrite) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write =
      verbs_util::CreateWriteWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                remote.buffer.data(), remote.type1_mw->rkey);
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, Type2MWWrite) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  if (!Introspection().SupportsType2()) {
    GTEST_SKIP() << "Needs type 2 MW.";
  }
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write =
      verbs_util::CreateWriteWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                remote.buffer.data(), remote.type2_mw->rkey);
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('a'));
}

TEST_F(LoopbackRcQpTest, WriteInvalidLkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Verify buffer is unchanged.
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteInvalidRkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Change rkey to be invalid.
  write.wr.rdma.rkey = (write.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Verify buffer is unchanged.
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, UnsignaledWriteInvalidRkey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Change rkey to be invalid.
  write.wr.rdma.rkey = (write.wr.rdma.rkey + 10) * 5;
  write.send_flags = write.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(IBV_WC_RDMA_WRITE, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Verify buffer is unchanged.
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteInvalidRKeyAndInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Invalidate the lkey.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Also invalidate the rkey.
  write.wr.rdma.rkey = (write.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  // On a write the local key is checked first.
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, FetchAddNoOp) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  // The local SGE will be used to store the value before the update.
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 0);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);

  // The remote should remain b/c we added 0.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  // The local buffer should be the same as the remote.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, FetchAddSmallSge) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  // The local SGE will be used to store the value before the update.
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 0);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  } else {
    EXPECT_THAT(completion.status,
                testing::AnyOf(IBV_WC_LOC_LEN_ERR, IBV_WC_REM_ACCESS_ERR));
  }
}

TEST_F(LoopbackRcQpTest, FetchAddLargeSge) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  // The local SGE will be used to store the value before the update.
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 9;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 0);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  } else {
    EXPECT_THAT(completion.status,
                testing::AnyOf(IBV_WC_LOC_LEN_ERR, IBV_WC_REM_ACCESS_ERR));
  }
}

TEST_F(LoopbackRcQpTest, FetchAddSplitSgl) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  static constexpr uint64_t kSrcContent = 0xAAAAAAAAAAAAAAAA;
  ASSERT_OK(InitializeAtomicBuffer(local, kSrcContent));
  static constexpr uint64_t kDstContent = 2;
  ASSERT_OK(InitializeAtomicBuffer(remote, kDstContent));
  // The local SGE will be used to store the value before the update.
  // Going to read 0x2 from the remote and split it into 2 to store on the local
  // side:
  // 0x00 --------
  // 0x10 xxxx----
  // 0x20 xxxx----
  // 0x30 --------
  ibv_sge sgl[2];
  sgl[0] =
      verbs_util::CreateSge(local.atomic_buffer.subspan(8, 4), local.atomic_mr);
  sgl[1] = verbs_util::CreateSge(local.atomic_buffer.subspan(16, 4),
                                 local.atomic_mr);
  static constexpr uint64_t kIncrementAmount = 15;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, sgl, /*num_sge=*/2, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, kIncrementAmount);
  ibv_send_wr* bad_wr = nullptr;
  int result =
      ibv_post_send(local.qp, const_cast<ibv_send_wr*>(&fetch_add), &bad_wr);

  // Some adapters do not allow 2 SG entries
  if (result) return;
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  // Check destination.
  uint64_t value = *reinterpret_cast<uint64_t*>(remote.atomic_buffer.data());
  EXPECT_EQ(value, kDstContent + kIncrementAmount);
  // Check source.
  value = *reinterpret_cast<uint64_t*>(local.atomic_buffer.data());
  EXPECT_EQ(value, kSrcContent);
  value = *reinterpret_cast<uint64_t*>(local.atomic_buffer.data() + 8);
  uint64_t fetched = kDstContent;
  uint64_t expected = kSrcContent;
  memcpy(&expected, &fetched, 4);
  EXPECT_EQ(value, expected);
  value = *reinterpret_cast<uint64_t*>(local.atomic_buffer.data() + 16);
  expected = kSrcContent;
  memcpy(&expected, reinterpret_cast<uint8_t*>(&fetched) + 4, 4);
  EXPECT_EQ(value, expected);
  value = *reinterpret_cast<uint64_t*>(local.atomic_buffer.data() + 24);
  EXPECT_EQ(value, kSrcContent);
}

TEST_F(LoopbackRcQpTest, UnsignaledFetchAdd) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  // The local SGE will be used to store the value before the update.
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 10);
  fetch_add.send_flags = fetch_add.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_send_wr fetch_add2 = verbs_util::CreateFetchAddWr(
      /*wr_id=*/2, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add2);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(2, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  // Remote = 2 (orig) + 10 + 1 = 14.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 13);
  // Local = 2 (orig) + 10 = 12.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 12);
}

TEST_F(LoopbackRcQpTest, FetchAddIncrementBy1) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  // The local SGE will be used to store the value before the update.
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey,
      /*compare_add=*/1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);

  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should be the same as the remote before update.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

// This tests increments by a value that is larger than 32 bits.
TEST_F(LoopbackRcQpTest, FetchAddLargeIncrement) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 68719476736);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);

  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())),
            68719476738);
  // The local buffer should be the same as the remote before the add.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, FetchAddUnaligned) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));

  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // The atomic_buffer is always an index in the first half of the 16 byte
  // vector so we can increment by 1 to get an unaligned address with enough
  // space.
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);

  // The buffers should be unmodified.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // There may be a more standard way to corrupt such a key.
  sge.lkey = sge.lkey * 13 + 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);

  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
}

TEST_F(LoopbackRcQpTest, UnsignaledFetchAddError) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // There may be a more standard way to corrupt such a key.
  sge.lkey = sge.lkey * 13 + 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1);
  fetch_add.send_flags = fetch_add.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidRKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // There may be a more standard way to corrupt such a key.
  uint32_t corrupted_rkey = remote.atomic_mr->rkey * 13 + 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      corrupted_rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidLKeyAndInvalidRKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // Corrupt the rkey.
  uint32_t corrupted_rkey = remote.atomic_mr->rkey * 13 + 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      corrupted_rkey, 1);
  // Also corrupt the lkey.
  sge.lkey = sge.lkey * 13 + 7;
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddUnalignedInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // There may be a more standard way to corrupt such a key.
  sge.lkey = sge.lkey * 13 + 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Our implementation checks the key first. The hardware may check the
  // alignment first.
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddUnalignedInvalidRKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // There may be a more standard way to corrupt such a key.
  uint32_t corrupted_rkey = remote.atomic_mr->rkey * 13 + 7;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      corrupted_rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  // Our implementation will check the key first. The hardware may or may not
  // behave the same way.
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidSize) {
  if (!Introspection().CorrectlyReportsInvalidSizeErrors()) GTEST_SKIP();
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 9;

  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_LOC_LEN_ERR, completion.status);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpNotEqualNoSwap) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);

  // The remote buffer should not have changed because the compare value != 2.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  // The local buffer should have had the remote value written back even though
  // the comparison wasn't true.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpEqualWithSwap) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);

  // The remote buffer should get updated.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should have had the remote value written back.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, UnsignaledCmpAndSwp) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  cmp_swp.send_flags = cmp_swp.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, cmp_swp);
  cmp_swp = verbs_util::CreateCompSwapWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                         remote.atomic_buffer.data(),
                                         remote.atomic_mr->rkey, 3, 2);
  cmp_swp.wr_id = 2;
  verbs_util::PostSend(local.qp, cmp_swp);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(2, completion.wr_id);
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);

  // The remote buffer should get updated.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  // The local buffer should have had the remote value written back.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 3);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // Corrupt the lkey.
  sge.lkey = sge.lkey * 13 + 7;
  ASSERT_EQ(sge.length, 8U);
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);

  // The remote buffer should be updated because the lkey is not eagerly
  // checked.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should not be updated because of the invalid lkey.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, UnsignaledCmpAndSwpError) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // Corrupt the lkey.
  sge.lkey = sge.lkey * 13 + 7;
  ASSERT_EQ(sge.length, 8U);
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  cmp_swp.send_flags = cmp_swp.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);

  // The remote buffer should be updated because the lkey is not eagerly
  // checked.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should not be updated because of the invalid lkey.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpInvalidRKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // Corrupt the lkey.
  ASSERT_EQ(sge.length, 8U);
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey + 7 * 10, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);

  // The remote buffer should not have changed because it will be caught by the
  // invalid rkey.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpInvalidRKeyAndInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  sge.lkey = sge.lkey * 7 + 10;
  ASSERT_EQ(sge.length, 8U);
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey + 7 * 10, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, completion.status);

  // The buffers shouldn't change because the rkey will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpUnaligned) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);

  // No buffers should change because the alignment will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpUnalignedInvalidRKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // The data gets moved to an invalid location and the rkey is corrupted
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey * 10 + 3, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);

  // No buffers should change because the alignment will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpUnalignedInvalidLKey) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  // Corrupt the lkey.
  sge.lkey = sge.lkey * 10 + 7;
  ASSERT_EQ(sge.length, 8U);
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, completion.status);

  // No buffers should change because the alignment will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CmpAndSwpInvalidSize) {
  if (!Introspection().CorrectlyReportsInvalidSizeErrors()) GTEST_SKIP();
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  sge.length = 9;

  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_EQ(IBV_WC_LOC_LEN_ERR, completion.status);

  // The buffers should not be updated.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, SgePointerChase) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  {
    // This scope causes the SGE to be cleaned up.
    ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
    ibv_send_wr read =
        verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                 remote.buffer.data(), remote.mr->rkey);
    verbs_util::PostSend(local.qp, read);
  }
  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RDMA_READ, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('b'));
}

// Should be qp-fatal under custom transport and roce meaning that the queue
// pair will be transitioned to the error state.
TEST_F(LoopbackRcQpTest, RemoteFatalError) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  sge.length = 8;
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);

  verbs_util::PostSend(local.qp, cmp_swp);

  ibv_wc cmp_and_swp_completion =
      verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, cmp_and_swp_completion.qp_num);
  EXPECT_EQ(1, cmp_and_swp_completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, cmp_and_swp_completion.status);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);

  // Both should have transitioned now that the completion has been received.
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(local.qp));
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(remote.qp));

  // Reset the buffer values.
  memset(local.buffer.data(), 'a', local.buffer.size());
  memset(remote.buffer.data(), 'b', remote.buffer.size());

  // Create a second WR that should return an error.
  ibv_sge write_sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write =
      verbs_util::CreateWriteWr(/*wr_id=*/2, &write_sg, /*num_sge=*/1,
                                remote.buffer.data(), remote.mr->rkey);
  write.wr_id = 2;
  verbs_util::PostSend(local.qp, write);

  ibv_wc write_completion = verbs_util::WaitForCompletion(local.cq).value();
  // This write should not have landed.
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, write_completion.status);
  EXPECT_EQ(local.qp->qp_num, write_completion.qp_num);
  EXPECT_EQ(2, write_completion.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));

  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(local.qp));
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(remote.qp));

  ibv_wc result;
  EXPECT_EQ(ibv_poll_cq(local.cq, 1, &result), 0);
  EXPECT_EQ(ibv_poll_cq(remote.cq, 1, &result), 0);
}

TEST_F(LoopbackRcQpTest, QueryQpInitialState) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  ASSERT_EQ(ibv_query_qp(local.qp, &attr, IBV_QP_STATE, &init_attr), 0);
  EXPECT_EQ(attr.qp_state, IBV_QPS_RTS);

  ASSERT_EQ(ibv_query_qp(remote.qp, &attr, IBV_QP_STATE, &init_attr), 0);
  EXPECT_EQ(attr.qp_state, IBV_QPS_RTS);
}

TEST_F(LoopbackRcQpTest, RequestOnFailedQp) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  // The invalid lkey will fail the local qp.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_PROT_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  EXPECT_THAT(local.buffer.span(), ::testing::Each('a'));

  // Send a request from the remote to the local failed qp.
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_send_wr read2 = verbs_util::CreateReadWr(
      /*wr_id=*/1, &rsge, /*num_sge=*/1, local.buffer.data(), local.mr->rkey);
  verbs_util::PostSend(remote.qp, read2);

  ibv_wc completion2 =
      verbs_util::WaitForCompletion(remote.cq,
                                    verbs_util::kDefaultErrorCompletionTimeout)
          .value();
  EXPECT_EQ(IBV_WC_RETRY_EXC_ERR, completion2.status);
  EXPECT_EQ(remote.qp->qp_num, completion2.qp_num);
  EXPECT_EQ(1, completion2.wr_id);
  EXPECT_THAT(remote.buffer.span(), ::testing::Each('b'));
}

TEST_F(LoopbackRcQpTest, FullSubmissionQueue) {
  static_assert(
      kQueueSize % 2 == 0,
      "Queue size must be even for this test to hit queue boundaries");
  static constexpr int kBatchSize = kQueueSize / 2;
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();

  // Basic Read.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Submit a batch at a time.
  ibv_send_wr submissions[kBatchSize];
  for (int i = 0; i < kBatchSize; ++i) {
    memcpy(&submissions[i], &read, sizeof(ibv_send_wr));
    submissions[i].next = &submissions[i + 1];
  }
  submissions[kBatchSize - 1].next = nullptr;

  // At this value we should issue more work.
  static constexpr int kTargetOutstanding = kQueueSize - kBatchSize;
  ibv_wc completions[kBatchSize];
  int outstanding = 0;
  int total = 0;
  // Issue 20 batches of work.
  while (total < kBatchSize * 20) {
    if (outstanding <= kTargetOutstanding) {
      verbs_util::PostSend(local.qp, submissions[0]);
      outstanding += kBatchSize;
    }
    // Wait a little.
    sched_yield();
    absl::SleepFor(absl::Milliseconds(20));
    // Poll completions
    int count = ibv_poll_cq(local.cq, kBatchSize, completions);
    total += count;
    outstanding -= count;
  }

  // Wait for outstanding to avoid any leaks...
  while (outstanding > 0) {
    // Wait a little.
    sched_yield();
    absl::SleepFor(absl::Milliseconds(20));
    // Poll completions
    int count = ibv_poll_cq(local.cq, kBatchSize, completions);
    outstanding -= count;
  }
}

TEST_F(LoopbackRcQpTest, PostToRecvQueueInErrorState) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge cs_sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  cs_sge.length = 8;
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &cs_sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);

  ibv_wc cmp_and_swp_completion =
      verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, cmp_and_swp_completion.qp_num);
  EXPECT_EQ(1, cmp_and_swp_completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, cmp_and_swp_completion.status);

  // Both should have transitioned now that the completion has been received.
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(local.qp));
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(remote.qp));

  // Post a send and a recv WR to local and remote QP.
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(send.wr_id, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, completion.status);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(recv.wr_id, completion.wr_id);
}

TEST_F(LoopbackRcQpTest, ProcessRecvQueueInErrorState) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge cs_sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  cs_sge.length = 8;
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_and_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &cs_sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_and_swp);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr2 = nullptr;
  ASSERT_EQ(0, ibv_post_recv(remote.qp, &recv, &bad_wr2));

  ibv_wc cmp_and_swp_completion =
      verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(local.qp->qp_num, cmp_and_swp_completion.qp_num);
  EXPECT_EQ(1, cmp_and_swp_completion.wr_id);
  EXPECT_EQ(IBV_WC_REM_INV_REQ_ERR, cmp_and_swp_completion.status);

  // Both should have transitioned now that the completion has been received.
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(local.qp));
  EXPECT_EQ(IBV_QPS_ERR, verbs_util::GetQpState(remote.qp));

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(send.wr_id, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, completion.status);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(recv.wr_id, completion.wr_id);
}

TEST_F(LoopbackUdQpTest, Send) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  rsge.length = kPayloadLength + sizeof(ibv_grh);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.wr.ud.ah = local.other_ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
  completion = verbs_util::WaitForCompletion(remote.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_RECV, completion.opcode);
  EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
  EXPECT_EQ(0, completion.wr_id);
  auto recv_payload = remote.buffer.span();
  recv_payload = recv_payload.subspan(sizeof(ibv_grh), kPayloadLength);
  EXPECT_THAT(recv_payload, ::testing::Each('a'));
}

TEST_F(LoopbackUdQpTest, SendRnr) {
  static constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.wr.ud.ah = local.other_ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

TEST_F(LoopbackUdQpTest, SendWithTooSmallRecv) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
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
  send.wr.ud.ah = local.other_ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(local.qp, send);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

// Read not supported on UD.
TEST_F(LoopbackUdQpTest, Read) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  read.wr.ud.ah = local.other_ah;
  read.wr.ud.remote_qkey = kQKey;
  read.wr.ud.remote_qpn = remote.qp->qp_num;
  verbs_util::PostSend(local.qp, read);

  ibv_wc completion =
      verbs_util::WaitForCompletion(local.cq, absl::Seconds(1)).value();
  EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

// Tests polling multiple CQE in a single call.
TEST_F(LoopbackUdQpTest, PollMultipleCqe) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
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
  send.wr.ud.ah = local.other_ah;
  send.wr.ud.remote_qpn = remote.qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  for (int i = 0; i < kNumCompletions; ++i) {
    verbs_util::PostSend(local.qp, send);
  }

  // Wait for recv completions.
  for (int i = 0; i < kNumCompletions; ++i) {
    ibv_wc completion = verbs_util::WaitForCompletion(remote.cq).value();
    EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
    EXPECT_EQ(IBV_WC_RECV, completion.opcode);
    EXPECT_EQ(remote.qp->qp_num, completion.qp_num);
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
  EXPECT_EQ(IBV_WC_SUCCESS, completion.status);
  EXPECT_EQ(IBV_WC_SEND, completion.opcode);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(send.wr_id, completion.wr_id);
}

// Write not supported on Ud.
TEST_F(LoopbackUdQpTest, Write) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.wr.ud.ah = local.other_ah;
  write.wr.ud.remote_qkey = kQKey;
  write.wr.ud.remote_qpn = remote.qp->qp_num;
  verbs_util::PostSend(local.qp, write);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

// FetchAndAdd not supported on UD.
TEST_F(LoopbackUdQpTest, FetchAdd) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  // The local SGE will be used to store the value before the update.
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 0);
  verbs_util::PostSend(local.qp, fetch_add);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

// CompareAndSwap not supported on UD.
TEST_F(LoopbackUdQpTest, CmpAndSwp) {
  auto client_pair_or = CreateConnectedClientsPair();
  ASSERT_OK(client_pair_or);
  auto [local, remote] = client_pair_or.value();
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);

  ibv_wc completion = verbs_util::WaitForCompletion(local.cq).value();
  EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, completion.status);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(1, completion.wr_id);
}

}  // namespace rdma_unit_test
