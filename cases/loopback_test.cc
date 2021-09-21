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
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
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
// TODO(author1): Add tests stressing SGEs.
// TODO(author1): Send with insufficient recv buffering (RC and UD).

using ::testing::AnyOf;
using ::testing::Each;
using ::testing::Eq;
using ::testing::NotNull;

class LoopbackTest : public BasicFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr int kQueueSize = 200;
  static constexpr int kQKey = 200;

  struct Client {
    ibv_context* context = nullptr;
    verbs_util::PortGid port_gid;
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
    ASSIGN_OR_RETURN(client.context, ibv_.OpenDevice());
    client.port_gid = ibv_.GetLocalPortGid(client.context);
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
    ibv_.SetUpSelfConnectedRcQp(client.control_qp, client.port_gid);
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
    DCHECK_EQ(reinterpret_cast<uintptr_t>(client.atomic_buffer.data()) % 8,
              0UL);
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
    DCHECK_EQ(client.control_qp->qp_type, IBV_QPT_RC);
    DCHECK_EQ(verbs_util::GetQpState(client.control_qp), IBV_QPS_RTS);
    DCHECK_EQ(verbs_util::GetQpState(client.qp), IBV_QPS_RTS);
    ASSIGN_OR_RETURN(
        ibv_wc_status status,
        verbs_util::BindType1MwSync(client.control_qp, client.type1_mw,
                                    client.buffer.span(), client.mr));
    if (status != IBV_WC_SUCCESS) {
      return absl::InternalError(
          absl::StrCat("Failed to bind type 1 mw.(", status, ")."));
    }
    if (Introspection().SupportsType2()) {
      ASSIGN_OR_RETURN(ibv_wc_status status_type2,
                       verbs_util::BindType2MwSync(client.qp, client.type2_mw,
                                                   client.buffer.span(),
                                                   type2_rkey, client.mr));
      if (status_type2 != IBV_WC_SUCCESS) {
        return absl::InternalError(
            absl::StrCat("Failed to bind type 2 mw.(", status_type2, ")."));
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::pair<Client, Client>> CreateConnectedClientsPair() {
    ASSIGN_OR_RETURN(Client local, CreateClient(/*buf_content=*/'a'));
    ASSIGN_OR_RETURN(Client remote, CreateClient(/*buf_content=*/'b'));
    RETURN_IF_ERROR(SetUpInterconnection(local, remote));
    RETURN_IF_ERROR(BindMws(local, /*type2_rkey=*/1024));
    RETURN_IF_ERROR(BindMws(remote, /*type2_rkey=*/2028));
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
    ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, remote.port_gid);
    return absl::OkStatus();
  }

  static bool TransitionQpToErrorState(ibv_qp* qp) {
    ibv_qp_attr attr;
    attr.qp_state = IBV_QPS_ERR;
    ibv_qp_attr_mask attr_mask = IBV_QP_STATE;
    return (ibv_modify_qp(qp, &attr, attr_mask) == 0);
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
    RETURN_IF_ERROR(ibv_.SetUpUdQp(local.qp, local.port_gid, kQKey));
    RETURN_IF_ERROR(ibv_.SetUpUdQp(remote.qp, remote.port_gid, kQKey));
    local.other_ah = ibv_.CreateAh(local.pd, remote.port_gid.gid);
    if (!local.other_ah) {
      return absl::InternalError("Failed to create local ah.");
    }
    remote.other_ah = ibv_.CreateAh(remote.pd, local.port_gid.gid);
    if (!remote.other_ah) {
      return absl::InternalError("Failed to create remote ah.");
    }
    return absl::OkStatus();
  }
};

TEST_F(LoopbackRcQpTest, Send) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_sge sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
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
  EXPECT_EQ(completion.wc_flags, 0);
  EXPECT_THAT(remote.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, SendEmptySgl) {
  if (!Introspection().AllowsEmptySgl()) {
    GTEST_SKIP() << "Device does not allow empty SGL.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, nullptr, /*num_sge=*/0);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, nullptr, /*num_sge=*/0);
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
  EXPECT_EQ(completion.wc_flags, 0);
}

TEST_F(LoopbackRcQpTest, UnsignaledSend) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.send_flags = send.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_EQ(completion.wc_flags, 0);
  EXPECT_THAT(remote.buffer.span(), Each('a'));

  EXPECT_EQ(ibv_poll_cq(local.cq, 1, &completion), 0);
}

TEST_F(LoopbackRcQpTest, SendZeroSize) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, nullptr, /*num_sge=*/0);
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
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

// Send a 64MB chunk from local to remote
TEST_F(LoopbackRcQpTest, SendLargeChunk) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  // prepare buffer
  RdmaMemBlock send_buf = ibv_.AllocBuffer(/*pages=*/16);
  memset(send_buf.data(), 'a', send_buf.size());
  RdmaMemBlock recv_buf = ibv_.AllocBuffer(/*pages=*/16);
  memset(recv_buf.data(), 'b', recv_buf.size());
  ibv_mr* send_mr = ibv_.RegMr(local.pd, send_buf);
  ibv_mr* recv_mr = ibv_.RegMr(remote.pd, recv_buf);
  ASSERT_THAT(send_mr, NotNull());
  ASSERT_THAT(recv_mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(recv_buf.span(), recv_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(send_buf.span(), send_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(recv_buf.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, SendInlineData) {
  const size_t kSendSize = verbs_util::kDefaultMaxInlineSize;
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data(), kSendSize), Each('c'));
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data() + kSendSize,
                             remote.buffer.data() + remote.buffer.size()),
              Each('b'));
}

TEST_F(LoopbackRcQpTest, SendImmData) {
  // The immediate data should be in network byte order according to the type
  // But we are just lazy here and assume that the two sides have the same
  // endianness.
  const uint32_t kImm = 0xBADDCAFE;
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  EXPECT_NE(completion.wc_flags & IBV_WC_WITH_IMM, 0);
  EXPECT_EQ(kImm, completion.imm_data);

  EXPECT_THAT(remote.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, SendWithInvalidate) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  EXPECT_EQ(completion.wc_flags, IBV_WC_WITH_INV);
  EXPECT_EQ(remote.type2_mw->rkey, completion.invalidated_rkey);

  EXPECT_THAT(remote.buffer.span(), Each('a'));

  // Check that rkey is invalid.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
}

TEST_F(LoopbackRcQpTest, SendWithInvalidateEmptySgl) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  if (!Introspection().AllowsEmptySgl()) {
    GTEST_SKIP() << "NIC does not allow empty SGL.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, nullptr, /*num_sge=*/0);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, nullptr, /*num_sge=*/0);
  send.opcode = IBV_WR_SEND_WITH_INV;
  send.invalidate_rkey = remote.type2_mw->rkey;
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
  EXPECT_EQ(completion.wc_flags, IBV_WC_WITH_INV);
  EXPECT_EQ(remote.type2_mw->rkey, completion.invalidated_rkey);

  // Check that rkey is invalid.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
}

TEST_F(LoopbackRcQpTest, SendWithInvalidateNoBuffer) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.opcode = IBV_WR_SEND_WITH_INV;
  send.invalidate_rkey = remote.type2_mw->rkey;
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_RNR_RETRY_EXC_ERR);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);

  // Check that rkey is valid.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
}

TEST_F(LoopbackRcQpTest, SendWithInvalidateBadRkey) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_EQ(completion.wc_flags, 0);

}

TEST_F(LoopbackRcQpTest, SendWithInvalidateType1Rkey) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_EQ(completion.wc_flags, 0);

}

// Send with Invalidate targeting another QPs MW.
TEST_F(LoopbackRcQpTest, SendWithInvalidateWrongQp) {
  if (!Introspection().SupportsType2() ||
      !Introspection().SupportsRcSendWithInvalidate()) {
    GTEST_SKIP() << "Needs type 2 MW and SendWithInvalidate.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_qp* qp = ibv_.CreateQp(local.pd, local.cq, local.cq, nullptr, kQueueSize,
                             kQueueSize, QpType(), /*sig_all=*/0);
  ASSERT_THAT(qp, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp, local.port_gid);

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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_TRUE(completion.opcode == IBV_WC_SEND ||
              completion.opcode == IBV_WC_RECV);
  EXPECT_EQ(qp->qp_num, completion.qp_num);
  EXPECT_TRUE(completion.wr_id == 0 || completion.wr_id == 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_TRUE(completion.opcode == IBV_WC_SEND ||
              completion.opcode == IBV_WC_RECV);
  EXPECT_EQ(qp->qp_num, completion.qp_num);
  EXPECT_TRUE(completion.wr_id == 0 || completion.wr_id == 1);

}

TEST_F(LoopbackRcQpTest, SendWithTooSmallRecv) {
  // Recv buffer is to small to fit the whole buffer.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  const uint32_t kRecvLength = local.buffer.span().size() - 1;
  sge.length = kRecvLength;
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);
  EXPECT_EQ(local.qp->qp_num, completion.qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(verbs_util::GetQpState(local.qp), IBV_QPS_ERR);
  EXPECT_EQ(verbs_util::GetQpState(remote.qp), IBV_QPS_ERR);
}

TEST_F(LoopbackRcQpTest, SendRnr) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_RNR_RETRY_EXC_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackRcQpTest, SendRnrInfinteRetries) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(Client local, CreateClient());
  ASSERT_OK_AND_ASSIGN(Client remote, CreateClient());
  // Manually set up QPs with custom attributes.
  ibv_qp_attr custom_attr{
      .rnr_retry = 7,
  };
  int mask = IBV_QP_RNR_RETRY;
  ASSERT_OK(ibv_.SetQpInit(local.qp, local.port_gid.port));
  ASSERT_OK(ibv_.SetQpInit(remote.qp, remote.port_gid.port));
  ASSERT_OK(ibv_.SetQpRtr(local.qp, local.port_gid, remote.port_gid.gid,
                          remote.qp->qp_num));
  ASSERT_OK(ibv_.SetQpRtr(remote.qp, remote.port_gid, local.port_gid.gid,
                          local.qp->qp_num));
  ASSERT_OK(ibv_.SetQpRts(local.qp, custom_attr, mask));
  ASSERT_OK(ibv_.SetQpRts(remote.qp, custom_attr, mask));

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  auto result = verbs_util::WaitForCompletion(local.cq, absl::Seconds(10));
  EXPECT_EQ(absl::StatusCode::kInternal, result.status().code());
  EXPECT_EQ(result.status().message(),
            "Timeout while waiting for a completion");
}

TEST_F(LoopbackRcQpTest, BadRecvAddr) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  --rsge.addr;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
}

TEST_F(LoopbackRcQpTest, RecvOnDeregisteredRegion) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ASSERT_EQ(ibv_.DeallocMw(remote.type1_mw), 0);
  // If the NIC supports type 2 windows both windows must be deallocated.
  if (Introspection().SupportsType2()) {
    ASSERT_EQ(ibv_.DeallocMw(remote.type2_mw), 0);
  }
  ASSERT_EQ(ibv_.DeregMr(remote.mr), 0);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  // The op code isn't set by the hardware in the error case.
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(remote.buffer.span(), Each('b'));

  EXPECT_EQ(verbs_util::GetQpState(local.qp), IBV_QPS_ERR);
  EXPECT_EQ(verbs_util::GetQpState(remote.qp), IBV_QPS_ERR);
}

TEST_F(LoopbackRcQpTest, BadRecvLength) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ++rsge.length;
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  ASSERT_OK_AND_ASSIGN(ibv_wc completion2,
                       verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(remote.qp->qp_num, completion2.qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion2.wr_id, 0);
  if (!Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
    EXPECT_EQ(completion2.status, IBV_WC_LOC_PROT_ERR);
  } else {
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion.opcode, IBV_WC_SEND);
    EXPECT_EQ(completion2.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion2.opcode, IBV_WC_RECV);
  }
}

TEST_F(LoopbackRcQpTest, BadRecvLkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
}

TEST_F(LoopbackRcQpTest, SendInvalidLkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  // Existing hardware does not set opcode on error.
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackRcQpTest, UnsignaledSendError) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  send.send_flags = send.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  // Existing hardware does not set opcode on error.
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackRcQpTest, SendRemoteQpInErrorStateRecvWqeAfterTransition) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));

  ibv_sge sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  if (!Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);
  }
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_WR_FLUSH_ERR);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, SendRemoteQpInErrorStateRecvWqeBeforeTransition) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_sge sge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_THAT(completion.status,
              AnyOf(IBV_WC_WR_FLUSH_ERR, IBV_WC_RETRY_EXC_ERR));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(IBV_WC_WR_FLUSH_ERR, completion.status);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, SendRemoteQpInErrorStateNoRecvWqe) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));
  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_THAT(completion.status,
              AnyOf(IBV_WC_WR_FLUSH_ERR, IBV_WC_RETRY_EXC_ERR));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackRcQpTest, BasicRead) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, UnsignaledRead) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  read.send_flags = read.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, read);
  read.send_flags = read.send_flags | IBV_SEND_SIGNALED;
  read.wr_id = 2;
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
}

TEST_F(LoopbackRcQpTest, QpSigAll) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_qp* qp = ibv_.CreateQp(local.pd, local.cq, local.cq, nullptr, kQueueSize,
                             kQueueSize, QpType(), /*sig_all=*/1);
  ASSERT_THAT(qp, NotNull());
  ibv_.SetUpSelfConnectedRcQp(qp, local.port_gid);
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, local.buffer.data(), local.mr->rkey);
  read.send_flags = read.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(qp->qp_num, completion.qp_num);
}

TEST_F(LoopbackRcQpTest, Type1MWRead) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type1_mw->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, Type2MWRead) {
  if (!Introspection().SupportsType2()) {
    GTEST_SKIP() << "Needs type 2 MW.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type2_mw->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, Type1MWUnbind) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);

  // Rebind to get a new rkey.
  uint32_t original_rkey = remote.type1_mw->rkey;
  ASSERT_THAT(verbs_util::BindType1MwSync(remote.control_qp, remote.type1_mw,
                                          remote.buffer.span(), remote.mr),
              IsOkAndHolds(IBV_WC_SUCCESS));
  EXPECT_NE(original_rkey, remote.type1_mw->rkey);

  // Issue a read with the new rkey.
  ibv_send_wr read =
      verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                               remote.buffer.data(), remote.type1_mw->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('b'));

  // Issue a read with the old rkey.
  read.wr.rdma.rkey = original_rkey;
  verbs_util::PostSend(local.qp, read);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(LoopbackRcQpTest, ReadInvalidLkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Verify buffer is unchanged.
  EXPECT_THAT(local.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, UnsignaledReadError) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  read.send_flags = read.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackRcQpTest, ReadInvalidRkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Change rkey to be invalid.
  read.wr.rdma.rkey = (read.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Verify buffer is unchanged.
  EXPECT_THAT(local.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, ReadInvalidRKeyAndInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Invalidate the lkey.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Invalidate the rkey too.
  read.wr.rdma.rkey = (read.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, ReadRemoteQpInErrorState) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, BasicWrite) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(remote.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, UnsignaledWrite) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.send_flags = write.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, write);
  write.wr_id = 2;
  write.send_flags = write.send_flags | IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
}

TEST_F(LoopbackRcQpTest, WriteInlineData) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data(), kWriteSize), Each('c'));
  EXPECT_THAT(absl::MakeSpan(remote.buffer.data() + kWriteSize,
                             remote.buffer.data() + remote.buffer.size()),
              Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteImmData) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);

  // Verify RECV completion.
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV_RDMA_WITH_IMM);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_NE(completion.wc_flags & IBV_WC_WITH_IMM, 0);
  EXPECT_EQ(kImm, completion.imm_data);

  // Verify that data written to the correct buffer.
  EXPECT_THAT(remote.buffer.span(), Each('a'));
  EXPECT_THAT(dummy_buf.span(), Each('d'));
}

TEST_F(LoopbackRcQpTest, WriteImmDataRnR) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  const uint32_t kImm = 0xBADDCAFE;

  // Post write to remote.buffer.
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  write.imm_data = kImm;
  verbs_util::PostSend(local.qp, write);

  // Verify WRITE completion.
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_RNR_RETRY_EXC_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackRcQpTest, Type1MWWrite) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write =
      verbs_util::CreateWriteWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                remote.buffer.data(), remote.type1_mw->rkey);
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(remote.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, Type2MWWrite) {
  if (!Introspection().SupportsType2()) {
    GTEST_SKIP() << "Needs type 2 MW.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write =
      verbs_util::CreateWriteWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                remote.buffer.data(), remote.type2_mw->rkey);
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(remote.buffer.span(), Each('a'));
}

TEST_F(LoopbackRcQpTest, WriteInvalidLkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Change lkey to be invalid.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Verify buffer is unchanged.
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteInvalidRkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Change rkey to be invalid.
  write.wr.rdma.rkey = (write.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Verify buffer is unchanged.
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, UnsignaledWriteInvalidRkey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Change rkey to be invalid.
  write.wr.rdma.rkey = (write.wr.rdma.rkey + 10) * 5;
  write.send_flags = write.send_flags & ~IBV_SEND_SIGNALED;
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  // Existing hardware does not set this on error.
  // EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Verify buffer is unchanged.
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteInvalidRKeyAndInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  // Invalidate the lkey.
  sge.lkey = (sge.lkey + 10) * 5;
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  // Also invalidate the rkey.
  write.wr.rdma.rkey = (write.wr.rdma.rkey + 10) * 5;
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  // On a write the local key is checked first.
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, WriteRemoteQpInErrorState) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, write);

  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(remote.buffer.span(), Each('b'));
}

TEST_F(LoopbackRcQpTest, FetchAddNoOp) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);

  // The remote should remain b/c we added 0.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  // The local buffer should be the same as the remote.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, FetchAddSmallSge) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  } else {
    EXPECT_THAT(completion.status,
                AnyOf(IBV_WC_LOC_LEN_ERR, IBV_WC_REM_ACCESS_ERR));
  }
}

TEST_F(LoopbackRcQpTest, FetchAddLargeSge) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  } else {
    EXPECT_THAT(completion.status,
                AnyOf(IBV_WC_LOC_LEN_ERR, IBV_WC_REM_ACCESS_ERR));
  }
}

// Note: Multiple SGEs for Atomics are specifically not supported by the IBTA
// spec, but some Mellanox NICs and its successors choose to
// support it.
TEST_F(LoopbackRcQpTest, FetchAddSplitSgl) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  // Some NICs do not allow 2 SG entries
  if (result) return;

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
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
  } else {
    EXPECT_NE(completion.status, IBV_WC_SUCCESS);
  }
}

TEST_F(LoopbackRcQpTest, UnsignaledFetchAdd) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  // Remote = 2 (orig) + 10 + 1 = 14.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 13);
  // Local = 2 (orig) + 10 = 12.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 12);
}

TEST_F(LoopbackRcQpTest, FetchAddIncrementBy1) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);

  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should be the same as the remote before update.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

// This tests increments by a value that is larger than 32 bits.
TEST_F(LoopbackRcQpTest, FetchAddLargeIncrement) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 68719476736);
  verbs_util::PostSend(local.qp, fetch_add);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);

  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())),
            68719476738);
  // The local buffer should be the same as the remote before the add.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, FetchAddUnaligned) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // The buffers should be unmodified.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);

  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
}

TEST_F(LoopbackRcQpTest, UnsignaledFetchAddError) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidRKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidLKeyAndInvalidRKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddUnalignedInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Our implementation checks the key first. The hardware may check the
  // alignment first.
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddUnalignedInvalidRKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  // Our implementation will check the key first. The hardware may or may not
  // behave the same way.
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddInvalidSize) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 9), local.atomic_mr);

  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1);
  verbs_util::PostSend(local.qp, fetch_add);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_LOC_LEN_ERR);

  // The buffers should not have changed.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, FetchAddRemoteQpInErrorState) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));

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
  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);

  // The local buffer should remain the same.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
  // The remote buffer should remain the same.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, CompareSwapNotEqualNoSwap) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 1, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);

  // The remote buffer should not have changed because the compare value != 2.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  // The local buffer should have had the remote value written back even though
  // the comparison wasn't true.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, CompareSwapEqualWithSwap) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);

  // The remote buffer should get updated.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should have had the remote value written back.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, UnsignaledCompareSwap) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);

  // The remote buffer should get updated.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  // The local buffer should have had the remote value written back.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 3);
}

TEST_F(LoopbackRcQpTest, CompareSwapInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);

  // The remote buffer should be updated because the lkey is not eagerly
  // checked.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should not be updated because of the invalid lkey.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, UnsignaledCompareSwapError) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);

  // The remote buffer should be updated because the lkey is not eagerly
  // checked.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 3);
  // The local buffer should not be updated because of the invalid lkey.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapInvalidRKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);

  // The remote buffer should not have changed because it will be caught by the
  // invalid rkey.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapInvalidRKeyAndInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);

  // The buffers shouldn't change because the rkey will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapUnaligned) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // No buffers should change because the alignment will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapUnalignedInvalidRKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // No buffers should change because the alignment will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapUnalignedInvalidLKey) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // No buffers should change because the alignment will get caught.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapInvalidSize) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 9), local.atomic_mr);

  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_LOC_LEN_ERR);

  // The buffers should not be updated.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
}

TEST_F(LoopbackRcQpTest, CompareSwapRemoteQpInErrorState) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_TRUE(TransitionQpToErrorState(remote.qp));
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);
  if (Introspection().ShouldDeviateForCurrentTest("NoCompletion")) {
    return;
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);

  // The local buffer should remain the same.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);
  // The remote buffer should remain the same.
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
}

TEST_F(LoopbackRcQpTest, SgePointerChase) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  {
    // This scope causes the SGE to be cleaned up.
    ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
    ibv_send_wr read =
        verbs_util::CreateReadWr(/*wr_id=*/1, &sge, /*num_sge=*/1,
                                 remote.buffer.data(), remote.mr->rkey);
    verbs_util::PostSend(local.qp, read);
  }
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RDMA_READ);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_THAT(local.buffer.span(), Each('b'));
}

// Should be qp-fatal under custom transport and roce meaning that the queue
// pair will be transitioned to the error state.
TEST_F(LoopbackRcQpTest, RemoteFatalError) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  sge.length = 8;
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);

  verbs_util::PostSend(local.qp, cmp_swp);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(remote.atomic_buffer.data())), 2);
  EXPECT_EQ(*(reinterpret_cast<uint64_t*>(local.atomic_buffer.data())), 1);

  // Both should have transitioned now that the completion has been received.
  EXPECT_EQ(verbs_util::GetQpState(local.qp), IBV_QPS_ERR);
  EXPECT_EQ(verbs_util::GetQpState(remote.qp), IBV_QPS_ERR);

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

  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  // This write should not have landed.
  EXPECT_EQ(completion.status, IBV_WC_WR_FLUSH_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 2);
  EXPECT_THAT(remote.buffer.span(), Each('b'));

  EXPECT_EQ(verbs_util::GetQpState(local.qp), IBV_QPS_ERR);
  EXPECT_EQ(verbs_util::GetQpState(remote.qp), IBV_QPS_ERR);

  ibv_wc result;
  EXPECT_EQ(ibv_poll_cq(local.cq, 1, &result), 0);
  EXPECT_EQ(ibv_poll_cq(remote.cq, 1, &result), 0);
}

TEST_F(LoopbackRcQpTest, QueryQpInitialState) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  ASSERT_EQ(ibv_query_qp(local.qp, &attr, IBV_QP_STATE, &init_attr), 0);
  EXPECT_EQ(attr.qp_state, IBV_QPS_RTS);

  ASSERT_EQ(ibv_query_qp(remote.qp, &attr, IBV_QP_STATE, &init_attr), 0);
  EXPECT_EQ(attr.qp_state, IBV_QPS_RTS);
}

TEST_F(LoopbackRcQpTest, FullSubmissionQueue) {
  static_assert(
      kQueueSize % 2 == 0,
      "Queue size must be even for this test to hit queue boundaries");
  static constexpr int kBatchSize = kQueueSize / 2;
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

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
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge cs_sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  cs_sge.length = 8;
  // The data gets moved to an invalid location.
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &cs_sge, /*num_sge=*/1, remote.atomic_buffer.data() + 1,
      remote.atomic_mr->rkey, 2, 3);
  verbs_util::PostSend(local.qp, cmp_swp);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.status, IBV_WC_REM_INV_REQ_ERR);

  // Both should have transitioned now that the completion has been received.
  EXPECT_EQ(verbs_util::GetQpState(local.qp), IBV_QPS_ERR);
  EXPECT_EQ(verbs_util::GetQpState(remote.qp), IBV_QPS_ERR);

  // Post a send and a recv WR to local and remote QP.
  ibv_sge rsge = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_WR_FLUSH_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(send.wr_id, completion.wr_id);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_WR_FLUSH_ERR);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(recv.wr_id, completion.wr_id);
}

TEST_F(LoopbackUdQpTest, Send) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

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
  EXPECT_THAT(recv_payload, Each('a'));
}

TEST_F(LoopbackUdQpTest, SendRnr) {
  static constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  ibv_sge lsge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  lsge.length = kPayloadLength;
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  send.wr.ud.ah = local.other_ah;
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
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

TEST_F(LoopbackUdQpTest, SendInvalidAh) {
  constexpr int kPayloadLength = 1000;  // Sub-MTU length for UD.
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

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

// Read not supported on UD.
TEST_F(LoopbackUdQpTest, Read) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  read.wr.ud.ah = local.other_ah;
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
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  write.wr.ud.ah = local.other_ah;
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
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_QP_OP_ERR);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 1);
}

// CompareAndSwap not supported on UD.
TEST_F(LoopbackUdQpTest, CompareSwap) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_OK(InitializeAtomicBuffer(local, /*content=*/1));
  ASSERT_OK(InitializeAtomicBuffer(remote, /*content=*/2));
  ibv_sge sge =
      verbs_util::CreateSge(local.atomic_buffer.subspan(0, 8), local.atomic_mr);
  sge.length = 8;
  ibv_send_wr cmp_swp = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, remote.atomic_buffer.data(),
      remote.atomic_mr->rkey, 2, 3);
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                              local_cq, absl::Seconds(10)));
  EXPECT_EQ(completion.status, IBV_WC_RETRY_EXC_ERR);
  EXPECT_THAT(
      verbs_util::WaitForCompletion(remote_cq, absl::Seconds(10)).status(),
      StatusIs(absl::StatusCode::kInternal,
               "Timeout while waiting for a completion"));
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
  ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                              local_cq, absl::Seconds(10)));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_THAT(
      verbs_util::WaitForCompletion(remote_cq, absl::Seconds(10)).status(),
      StatusIs(absl::StatusCode::kInternal,
               "Timeout while waiting for a completion"));
}

}  // namespace
}  // namespace rdma_unit_test
