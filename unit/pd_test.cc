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

#include <errno.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "internal/handle_garble.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/loopback_fixture.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::IsNull;
using ::testing::NotNull;

class PdTest : public RdmaVerbsFixture {
 protected:
  static constexpr size_t kBufferMemoryPages = 1;
};

TEST_F(PdTest, AllocPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_pd* pd = ibv_alloc_pd(context);
  ASSERT_THAT(pd, NotNull());
  EXPECT_EQ(pd->context, context);
  EXPECT_EQ(ibv_dealloc_pd(pd), 0);
}

TEST_F(PdTest, DeleteInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_pd* pd = ibv_.AllocPd(context);
  HandleGarble garble(pd->handle);
  EXPECT_EQ(ibv_dealloc_pd(pd), ENOENT);
}

TEST_F(PdTest, AllocQpWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_THAT(cq, NotNull());
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  HandleGarble garble(pd->handle);
  EXPECT_THAT(ibv_.CreateQp(pd, cq), IsNull());
}

TEST_F(PdTest, AllocMrWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  HandleGarble garble(pd->handle);
  EXPECT_THAT(ibv_.RegMr(pd, buffer), IsNull());
}

TEST_F(PdTest, AllocMwWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  HandleGarble garble(pd->handle);
  EXPECT_THAT(ibv_.AllocMw(pd, IBV_MW_TYPE_1), IsNull());
}

class PdBindTest : public LoopbackFixture,
                   public ::testing::WithParamInterface<ibv_mw_type> {
 public:
  void SetUp() override {
    if (GetParam() == IBV_MW_TYPE_1 && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "NIC does not support type 1.";
    }
    if (GetParam() == IBV_MW_TYPE_2 && !Introspection().SupportsType2()) {
      GTEST_SKIP() << "NIC does not support type 2.";
    }
  }

 protected:
  static constexpr uint32_t kType2RKey = 1024;
  static constexpr uint32_t kClientMemoryPages = 1;
};

TEST_P(PdBindTest, MwOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ibv_pd* alternate_pd = ibv_.AllocPd(setup.context);
  ASSERT_THAT(alternate_pd, NotNull());
  ibv_mw* mw = ibv_.AllocMw(alternate_pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  if (GetParam() == IBV_MW_TYPE_1) {
    // Some clients do client side validation on type 1. First check
    // succcess/failure of the bind and if successful than check for completion.
    ibv_mw_bind bind_args = verbs_util::CreateType1MwBindWr(
        /*wr_id=*/1, setup.buffer.span(), setup.mr,
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);
    int result = ibv_bind_mw(setup.remote_qp, mw, &bind_args);
    EXPECT_THAT(result, AnyOf(0, EPERM));
    if (result == 0) {
      ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                  setup.remote_qp->send_cq));
      if (completion.status == IBV_WC_SUCCESS) {
        EXPECT_EQ(completion.opcode, IBV_WC_BIND_MW);
      }
    }

  } else {
    EXPECT_THAT(
        verbs_util::ExecuteType2MwBind(setup.remote_qp, mw, setup.buffer.span(),
                                       kType2RKey, setup.mr),
        IsOkAndHolds(IBV_WC_MW_BIND_ERR));
  }
}

TEST_P(PdBindTest, MrOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ibv_pd* alternate_pd = ibv_.AllocPd(setup.context);
  ASSERT_THAT(alternate_pd, NotNull());
  ibv_mr* mr = ibv_.RegMr(alternate_pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(alternate_pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  if (GetParam() == IBV_MW_TYPE_1) {
    // Some clients do client side validation on type 1. First check
    // succcess/failure of the bind and if successful than check for completion.
    ibv_mw_bind bind_args = verbs_util::CreateType1MwBindWr(
        /*wr_id=*/1, setup.buffer.span(), mr,
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);
    int result = ibv_bind_mw(setup.remote_qp, mw, &bind_args);
    EXPECT_THAT(result, AnyOf(0, EPERM));
    if (result == 0) {
      ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                  setup.remote_qp->send_cq));
      if (completion.status == IBV_WC_SUCCESS) {
        EXPECT_EQ(completion.opcode, IBV_WC_BIND_MW);
      }
    }
  } else {
    ASSERT_OK_AND_ASSIGN(
        ibv_wc_status status,
        verbs_util::ExecuteType2MwBind(setup.remote_qp, mw, setup.buffer.span(),
                                       kType2RKey, mr));
    EXPECT_EQ(status, IBV_WC_MW_BIND_ERR);
  }
}

TEST_P(PdBindTest, MrMwOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ibv_pd* alternate_pd = ibv_.AllocPd(setup.context);
  ASSERT_THAT(alternate_pd, NotNull());
  ibv_mr* mr = ibv_.RegMr(alternate_pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(alternate_pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  if (GetParam() == IBV_MW_TYPE_1) {
    EXPECT_THAT(verbs_util::ExecuteType1MwBind(setup.remote_qp, mw,
                                               setup.buffer.span(), mr),
                IsOkAndHolds(IBV_WC_MW_BIND_ERR));
  } else {
    EXPECT_THAT(verbs_util::ExecuteType2MwBind(
                    setup.remote_qp, mw, setup.buffer.span(), kType2RKey, mr),
                IsOkAndHolds(IBV_WC_MW_BIND_ERR));
  }
}

INSTANTIATE_TEST_SUITE_P(PdBindTestCase, PdBindTest,
                         ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2));

class PdRcLoopbackMrTest : public RdmaVerbsFixture {
 protected:
  static constexpr uint32_t kClientMemoryPages = 1;
  static constexpr uint64_t kCompareAdd = 1;
  static constexpr uint64_t kSwap = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    PortAttribute port_attr;
    ibv_cq* local_cq;
    ibv_cq* remote_cq;
    ibv_pd* qp_pd;
    ibv_pd* other_pd;
    ibv_qp* local_qp;
    ibv_qp* remote_qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.local_cq = ibv_.CreateCq(setup.context);
    if (!setup.local_cq) {
      return absl::InternalError("Failed to create local cq.");
    }
    setup.remote_cq = ibv_.CreateCq(setup.context);
    if (!setup.remote_cq) {
      return absl::InternalError("Failed to create remote cq.");
    }
    setup.qp_pd = ibv_.AllocPd(setup.context);
    if (!setup.qp_pd) {
      return absl::InternalError("Failed to the qp's pd.");
    }
    setup.other_pd = ibv_.AllocPd(setup.context);
    if (!setup.other_pd) {
      return absl::InternalError("Failed to another pd.");
    }
    setup.local_qp = ibv_.CreateQp(setup.qp_pd, setup.local_cq);
    if (!setup.local_qp) {
      return absl::InternalError("Failed to create local qp.");
    }
    setup.remote_qp = ibv_.CreateQp(setup.qp_pd, setup.remote_cq);
    if (!setup.remote_qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    RETURN_IF_ERROR(ibv_.SetUpLoopbackRcQps(setup.local_qp, setup.remote_qp,
                                            setup.port_attr));
    return setup;
  }
};

TEST_F(PdRcLoopbackMrTest, SendMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), remote_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);

  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=1*/ 1);
  verbs_util::PostSend(setup.local_qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, SendMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), remote_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);

  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=1*/ 1);
  verbs_util::PostSend(setup.local_qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.remote_cq));
  EXPECT_THAT(completion.status,
              AnyOf(IBV_WC_LOC_PROT_ERR, IBV_WC_LOC_QP_OP_ERR));
  ASSERT_OK_AND_ASSIGN(completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicReadMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicReadMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_EQ(completion.status, expected);
}

TEST_F(PdRcLoopbackMrTest, BasicWriteMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicWriteMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey);
  verbs_util::PostSend(setup.local_qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicFetchAddMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicFetchAddMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);

  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, expected);
}

TEST_F(PdRcLoopbackMrTest, BasicCompSwapMrOtherPdLocal) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

TEST_F(PdRcLoopbackMrTest, BasicCompSwapMrOtherPdRemote) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* local_mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(local_mr, NotNull());
  ibv_mr* remote_mr = ibv_.RegMr(setup.other_pd, setup.buffer);
  ASSERT_THAT(remote_mr, NotNull());

  ibv_sge sg = verbs_util::CreateSge(setup.buffer.span(), local_mr);
  sg.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, setup.buffer.data(), remote_mr->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);

  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  EXPECT_EQ(completion.status, expected);
}

class PdUdLoopbackTest : public RdmaVerbsFixture {
 public:
  void SetUp() override {
    if (!Introspection().SupportsUdQp()) {
      GTEST_SKIP() << "Nic does not support UD QP";
    }
  }

 protected:
  static constexpr uint32_t kClientMemoryPages = 1;
  static constexpr int kQKey = 200;
  static constexpr size_t kPayloadSize = 1000;  // Sub-MTU size for UD.

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    PortAttribute port_attr;
    ibv_cq* local_cq;
    ibv_cq* remote_cq;
    ibv_pd* qp_pd;
    ibv_pd* other_pd;
    ibv_qp* local_qp;
    ibv_qp* remote_qp;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kClientMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.local_cq = ibv_.CreateCq(setup.context);
    if (!setup.local_cq) {
      return absl::InternalError("Failed to create local cq.");
    }
    setup.remote_cq = ibv_.CreateCq(setup.context);
    if (!setup.remote_cq) {
      return absl::InternalError("Failed to create remote cq.");
    }
    setup.qp_pd = ibv_.AllocPd(setup.context);
    if (!setup.qp_pd) {
      return absl::InternalError("Failed to allocate the qp's pd.");
    }
    setup.other_pd = ibv_.AllocPd(setup.context);
    if (!setup.other_pd) {
      return absl::InternalError("Failed to allocate another pd.");
    }
    setup.local_qp = ibv_.CreateQp(setup.qp_pd, setup.local_cq, IBV_QPT_UD);
    if (!setup.local_qp) {
      return absl::InternalError("Failed to create local qp.");
    }
    absl::Status status = ibv_.ModifyUdQpResetToRts(setup.local_qp, kQKey);
    if (!status.ok()) {
      return absl::InternalError("Failed to set up local ud qp.");
    }
    setup.remote_qp = ibv_.CreateQp(setup.qp_pd, setup.remote_cq, IBV_QPT_UD);
    if (!setup.remote_qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    status = ibv_.ModifyUdQpResetToRts(setup.remote_qp, kQKey);
    if (!status.ok()) {
      return absl::InternalError("Failed to set up remote ud qp.");
    }
    return setup;
  }
};

TEST_F(PdUdLoopbackTest, SendAhOnOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateLoopbackAh(setup.other_pd, setup.port_attr);
  ASSERT_THAT(ah, NotNull());
  ibv_mr* mr = ibv_.RegMr(setup.qp_pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_sge ssge =
      verbs_util::CreateSge(setup.buffer.subspan(0, kPayloadSize), mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  send.wr.ud.ah = ah;
  send.wr.ud.remote_qpn = setup.remote_qp->qp_num;
  send.wr.ud.remote_qkey = kQKey;
  verbs_util::PostSend(setup.local_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.local_cq));
  // It should return IBV_WC_LOC_QP_OP_ERR, see InfiniBand™ Architecture
  // Specification, Volume 1, P466, C10-10. But some NIC will return
  // IBV_WC_SUCCESS and behave as if the packet is lost.
  EXPECT_THAT(completion.status, AnyOf(IBV_WC_LOC_QP_OP_ERR, IBV_WC_SUCCESS));
}

class PdType1MwTest : public LoopbackFixture {
 public:
  void SetUp() override {
    if (!Introspection().SupportsType1()) {
      GTEST_SKIP() << "Nic does not support MW";
    }
  }

  absl::StatusOr<ibv_mw*> CreateMwWithAlternatePd(BasicSetup& setup) {
    ibv_pd* alternate_pd = ibv_.AllocPd(setup.context);
    if (alternate_pd == nullptr) {
      return absl::InternalError("Fail to allocate pd.");
    }
    ibv_mr* mr = ibv_.RegMr(alternate_pd, setup.buffer);
    if (mr == nullptr) {
      return absl::InternalError("Fail to register mr.");
    }
    ibv_mw* mw = ibv_.AllocMw(alternate_pd, IBV_MW_TYPE_1);
    if (mw == nullptr) {
      return absl::InternalError("Failed to allocate mw.");
    }
    ibv_qp* local_qp = ibv_.CreateQp(alternate_pd, setup.cq);
    if (local_qp == nullptr) {
      return absl::InternalError("Cannot create qp.");
    }
    ibv_qp* remote_qp = ibv_.CreateQp(alternate_pd, setup.cq);
    if (remote_qp == nullptr) {
      return absl::InternalError("Cannot create qp.");
    }
    RETURN_IF_ERROR(
        ibv_.SetUpLoopbackRcQps(local_qp, remote_qp, setup.port_attr));
    absl::StatusOr<ibv_wc_status> result =
        verbs_util::ExecuteType1MwBind(remote_qp, mw, setup.buffer.span(), mr);
    if (!result.ok()) {
      return result.status();
    }
    if (result.value() != IBV_WC_SUCCESS) {
      return absl::InternalError(absl::StrCat("Failed to bind memory windows (",
                                              result.value(), ")."));
    }
    return mw;
  }

 protected:
  static constexpr size_t kClientMemoryPages = 1;
  static constexpr uint64_t kCompareAdd = 1;
  static constexpr uint64_t kSwap = 1;
};

TEST_F(PdType1MwTest, ReadMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ASSERT_OK_AND_ASSIGN(ibv_mw * mw, CreateMwWithAlternatePd(setup));

  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.local_qp, read);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_EQ(completion.status, expected);
}

TEST_F(PdType1MwTest, WriteMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ASSERT_OK_AND_ASSIGN(ibv_mw * mw, CreateMwWithAlternatePd(setup));

  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), mw->rkey);
  verbs_util::PostSend(setup.local_qp, write);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
}

TEST_F(PdType1MwTest, FetchAddMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ASSERT_OK_AND_ASSIGN(ibv_mw * mw, CreateMwWithAlternatePd(setup));
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  sge.length = 8;
  ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), mw->rkey,
      kCompareAdd);
  verbs_util::PostSend(setup.local_qp, fetch_add);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  EXPECT_EQ(completion.status, expected);
}

TEST_F(PdType1MwTest, CompSwapMwOtherPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kClientMemoryPages));
  ASSERT_OK_AND_ASSIGN(ibv_mw * mw, CreateMwWithAlternatePd(setup));

  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  sge.length = 8;
  ibv_send_wr comp_swap = verbs_util::CreateCompSwapWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), mw->rkey,
      kCompareAdd, kSwap);
  verbs_util::PostSend(setup.local_qp, comp_swap);
  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, expected);
}

class PdSrqTest : public RdmaVerbsFixture {
 protected:
  static constexpr size_t kBufferMemoryPages = 1;

  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context = nullptr;
    PortAttribute port_attr;
    ibv_cq* cq = nullptr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    return setup;
  }
};

TEST_F(PdSrqTest, CreateSrq) {
  // Fun fact: SRQ can be of different QP with its associated QP(s).
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_THAT(srq, NotNull());
  ibv_qp* qp = ibv_.CreateQp(pd2, setup.cq, setup.cq, srq);
  EXPECT_THAT(qp, NotNull());
}

// When Pd of receive MR matches SRQ but not the receive QP.
TEST_F(PdSrqTest, SrqRecvMrSrqMatch) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_THAT(srq, NotNull());
  ibv_qp* local_qp = ibv_.CreateQp(pd2, setup.cq);
  ASSERT_THAT(local_qp, NotNull());
  ibv_qp* remote_qp = ibv_.CreateQp(pd2, setup.cq, setup.cq, srq);
  ASSERT_THAT(remote_qp, NotNull());
  ASSERT_OK(ibv_.SetUpLoopbackRcQps(local_qp, remote_qp, setup.port_attr));
  ibv_mr* mr_recv = ibv_.RegMr(pd1, setup.buffer);
  ASSERT_THAT(mr_recv, NotNull());
  ibv_mr* mr_send = ibv_.RegMr(pd2, setup.buffer);
  ASSERT_THAT(mr_send, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), mr_recv);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr_send);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(local_qp, send);

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  }
}

// When Pd of receive MR matches the QP but not the SRQ.
TEST_F(PdSrqTest, SrqRecvMrSrqMismatch) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* send_cq = ibv_.CreateCq(setup.context);
  ibv_cq* recv_cq = ibv_.CreateCq(setup.context);
  ibv_pd* pd1 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_pd* pd2 = ibv_.AllocPd(setup.context);
  ASSERT_THAT(pd1, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd1);
  ASSERT_THAT(srq, NotNull());
  ibv_qp* recv_qp = ibv_.CreateQp(pd2, recv_cq, recv_cq, srq);
  ASSERT_THAT(recv_qp, NotNull());
  ibv_qp* send_qp = ibv_.CreateQp(pd2, send_cq);
  ASSERT_THAT(send_qp, NotNull());
  ASSERT_OK(ibv_.SetUpLoopbackRcQps(send_qp, recv_qp, setup.port_attr));
  ibv_mr* mr = ibv_.RegMr(pd2, setup.buffer);
  ASSERT_THAT(mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(send_qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(send_cq));
  EXPECT_EQ(completion.status, IBV_WC_REM_OP_ERR);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(recv_cq));
  EXPECT_EQ(completion.status, IBV_WC_LOC_PROT_ERR);
}

// TODO(author1): Create Max

}  // namespace rdma_unit_test
