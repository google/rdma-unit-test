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

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "internal/handle_garble.h"
#include "internal/verbs_attribute.h"
#include "public/introspection.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

using ::testing::Each;
using ::testing::IsNull;
using ::testing::Ne;
using ::testing::NotNull;

class SrqTest : public RdmaVerbsFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr char kSendContent = 'a';
  static constexpr char kRecvContent = 'b';

  struct BasicSetup {
    RdmaMemBlock send_buffer;
    RdmaMemBlock recv_buffer;
    ibv_context* context = nullptr;
    PortAttribute port_attr;
    ibv_pd* pd = nullptr;
    ibv_mr* send_mr = nullptr;
    ibv_mr* recv_mr = nullptr;
    ibv_cq* send_cq = nullptr;
    ibv_cq* recv_cq = nullptr;
    ibv_srq_init_attr srq_init_attr;
    ibv_srq* srq = nullptr;
    ibv_qp* send_qp = nullptr;
    ibv_qp* recv_qp = nullptr;
  };

  // `max_outstanding` is the total number of outstanding ops in the test and
  // determines the size of QPs, CQs and SRQs
  absl::StatusOr<BasicSetup> CreateBasicSetup(uint32_t max_outstanding = 200) {
    BasicSetup setup;
    setup.send_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.send_buffer.data(),
              setup.send_buffer.data() + setup.send_buffer.size(),
              kSendContent);
    setup.recv_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.recv_buffer.data(),
              setup.recv_buffer.data() + setup.send_buffer.size(),
              kRecvContent);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.send_mr = ibv_.RegMr(setup.pd, setup.send_buffer);
    if (!setup.send_mr) {
      return absl::InternalError("Failed to register send mr.");
    }
    setup.recv_mr = ibv_.RegMr(setup.pd, setup.recv_buffer);
    if (!setup.recv_mr) {
      return absl::InternalError("Failed to register recv mr.");
    }
    setup.send_cq = ibv_.CreateCq(setup.context, max_outstanding + 10);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context, max_outstanding + 10);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    setup.srq_init_attr = ibv_srq_init_attr{
        .attr = ibv_srq_attr{.max_wr = max_outstanding, .max_sge = 1}};
    setup.srq = ibv_.CreateSrq(setup.pd, setup.srq_init_attr);
    if (!setup.srq) {
      return absl::InternalError("Failed to create srq.");
    }
    setup.send_qp = ibv_.CreateQp(setup.pd, setup.send_cq, IBV_QPT_RC,
                                  QpInitAttribute()
                                      .set_max_send_wr(max_outstanding)
                                      .set_max_recv_wr(max_outstanding));
    if (!setup.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    setup.recv_qp = ibv_.CreateQp(setup.pd, setup.send_cq, setup.recv_cq,
                                  setup.srq, IBV_QPT_RC,
                                  QpInitAttribute()
                                      .set_max_send_wr(max_outstanding)
                                      .set_max_recv_wr(max_outstanding));
    if (!setup.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    RETURN_IF_ERROR(
        ibv_.SetUpLoopbackRcQps(setup.send_qp, setup.recv_qp, setup.port_attr));
    return setup;
  }
};

TEST_F(SrqTest, Create) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ASSERT_THAT(context, NotNull());
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  ibv_srq_init_attr attr;
  attr.attr = verbs_util::DefaultSrqAttr();
  ibv_srq* srq = ibv_create_srq(pd, &attr);
  ASSERT_THAT(srq, NotNull());
  EXPECT_EQ(ibv_destroy_srq(srq), 0);
}

TEST_F(SrqTest, DestroyInvalidSrq) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ASSERT_THAT(context, NotNull());
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  ibv_srq_init_attr attr{.attr = verbs_util::DefaultSrqAttr()};
  ibv_srq* srq = ibv_.CreateSrq(pd, attr);
  ASSERT_THAT(srq, NotNull());
  HandleGarble garble(srq->handle);
  EXPECT_EQ(ibv_destroy_srq(srq), ENOENT);
}

TEST_F(SrqTest, CreateWithInvalidPd) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ASSERT_THAT(context, NotNull());
  ibv_pd* pd = ibv_.AllocPd(context);
  HandleGarble garble(pd->handle);
  ibv_srq_init_attr attr{.attr = verbs_util::DefaultSrqAttr()};
  ASSERT_THAT(ibv_create_srq(pd, &attr), IsNull());
}

TEST_F(SrqTest, CreateQpWithSrq) {
  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());
  ASSERT_THAT(context, NotNull());
  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  ibv_srq* srq = ibv_.CreateSrq(pd);
  ASSERT_THAT(srq, NotNull());
  ibv_cq* cq = ibv_.CreateCq(context);
  ASSERT_THAT(cq, NotNull());
  ibv_qp* qp = ibv_.CreateQp(pd, cq, cq, srq);
  ASSERT_THAT(qp, NotNull());
  EXPECT_EQ(qp->srq, srq);
}

TEST_F(SrqTest, PostWrongApi) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_recv(setup.recv_qp, &recv, &bad_wr), Ne(0));
}

TEST_F(SrqTest, MaxWr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  uint32_t max_wr = setup.srq_init_attr.attr.max_wr;

  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(max_wr, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, recv_wrs.data(), &bad_wr), 0);
}

TEST_F(SrqTest, ExceedMaxWr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  uint32_t num_wrs = setup.srq_init_attr.attr.max_wr + 1;

  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(num_wrs, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, recv_wrs.data(), &bad_wr), Ne(0));
}

TEST_F(SrqTest, MaxSge) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  uint32_t max_sge = setup.srq_init_attr.attr.max_sge;

  ASSERT_LT(max_sge, setup.recv_buffer.size());
  std::vector<ibv_sge> sges(max_sge);
  for (size_t i = 0; i < sges.size(); ++i) {
    sges[i] =
        verbs_util::CreateSge(setup.recv_buffer.subspan(i, 1), setup.recv_mr);
  }

  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, sges.data(),
                                              /*num_sge=*/sges.size());
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv, &bad_wr), 0);
}

TEST_F(SrqTest, ExceedMaxSge) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  uint32_t max_sge = setup.srq_init_attr.attr.max_sge;

  ASSERT_LT(max_sge, setup.recv_buffer.size());
  std::vector<ibv_sge> sges(max_sge + 1);
  for (size_t i = 0; i < sges.size(); ++i) {
    sges[i] =
        verbs_util::CreateSge(setup.recv_buffer.subspan(i, 1), setup.recv_mr);
  }

  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, sges.data(),
                                              /*num_sge=*/sges.size());
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv, &bad_wr), Ne(0));
  EXPECT_EQ(bad_wr, &recv);
}

TEST_F(SrqTest, DeviceCap) {
  const ibv_device_attr& device_attr = Introspection().device_attr();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(
      ibv_.CreateSrq(setup.pd, device_attr.max_srq_wr, device_attr.max_srq_sge),
      NotNull());
}

TEST_F(SrqTest, ExceedDeviceMaxWr) {
  const ibv_device_attr& device_attr = Introspection().device_attr();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(
      ibv_.CreateSrq(setup.pd, device_attr.max_srq_wr + 1, /*max_sge=*/1),
      IsNull());
}

TEST_F(SrqTest, ExceedDeviceMaxSge) {
  const ibv_device_attr& device_attr = Introspection().device_attr();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(
      ibv_.CreateSrq(setup.pd, /*max_wr=*/5, device_attr.max_srq_sge + 1),
      IsNull());
}

TEST_F(SrqTest, ModifyMaxWr) {
  if (!Introspection().CheckCapability(IBV_DEVICE_SRQ_RESIZE)) {
    GTEST_SKIP() << "Device does not support SRQ resizing.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_srq_attr attr;
  ASSERT_EQ(ibv_query_srq(setup.srq, &attr), 0);
  uint32_t max_wr = attr.max_wr;

  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  std::vector<ibv_recv_wr> recv_wrs(max_wr, recv);
  for (size_t i = 0; i < recv_wrs.size() - 1; ++i) {
    recv_wrs[i].next = &recv_wrs[i + 1];
    recv_wrs[i + 1].wr_id = i + 1;
  }
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, recv_wrs.data(), &bad_wr), 0);
  // Post another RR to SRQ.
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv, &bad_wr), Ne(0));
  // Increment SRQ capacity and post again.
  attr.max_wr = attr.max_wr + 10;
  ASSERT_EQ(ibv_modify_srq(setup.srq, &attr, IBV_SRQ_MAX_WR), 0);
  EXPECT_EQ(ibv_post_srq_recv(setup.srq, &recv, &bad_wr), 0);
}

TEST_F(SrqTest, ExceedMaxWrInfinitChain) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sge, /*num_sge=*/1);
  recv.next = &recv;
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_srq_recv(setup.srq, &recv, &bad_wr), Ne(0));
  EXPECT_EQ(bad_wr, &recv);
}

TEST_F(SrqTest, Send) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge rsge = verbs_util::CreateSge(setup.recv_buffer.span(), setup.recv_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostSrqRecv(setup.srq, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.send_buffer.span(), setup.send_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(setup.send_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.send_cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.wr_id, 1);
  ASSERT_OK_AND_ASSIGN(completion,
                       verbs_util::WaitForCompletion(setup.recv_cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(setup.recv_buffer.span(), Each(kSendContent));
}

TEST_F(SrqTest, SendRnr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_sge sge = verbs_util::CreateSge(setup.send_buffer.span(), setup.send_mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  verbs_util::PostSend(setup.send_qp, send);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.send_cq));
  EXPECT_EQ(completion.status, IBV_WC_RNR_RETRY_EXC_ERR);
  EXPECT_THAT(setup.recv_buffer.span(), Each(kRecvContent));
}

class SrqRcMultiplexTest : public SrqTest {
 protected:
  struct MultiplexSetup {
    BasicSetup basic_setup;
    std::vector<ibv_qp*> send_qps;
    std::vector<ibv_qp*> recv_qps;
  };

  // Compared to BasicSetup, this creates multiple RC QP pairs with all receive
  // QPs muxed.
  absl::StatusOr<MultiplexSetup> CreateMultiplexSetup(int qp_count,
                                                      int ops_per_queue) {
    MultiplexSetup setup;
    ASSIGN_OR_RETURN(setup.basic_setup,
                     CreateBasicSetup(qp_count * ops_per_queue));
    setup.send_qps.resize(qp_count, nullptr);
    setup.recv_qps.resize(qp_count, nullptr);
    for (int i = 0; i < qp_count; ++i) {
      ibv_qp* send_qp =
          ibv_.CreateQp(setup.basic_setup.pd, setup.basic_setup.send_cq);
      if (!send_qp) {
        return absl::InternalError("Failed to create send qp.");
      }
      ibv_qp* recv_qp =
          ibv_.CreateQp(setup.basic_setup.pd, setup.basic_setup.send_cq,
                        setup.basic_setup.recv_cq, setup.basic_setup.srq);
      if (!recv_qp) {
        return absl::InternalError("Failed to create recv qp.");
      }
      RETURN_IF_ERROR(ibv_.SetUpLoopbackRcQps(send_qp, recv_qp,
                                              setup.basic_setup.port_attr));
      setup.send_qps[i] = send_qp;
      setup.recv_qps[i] = recv_qp;
    }
    return setup;
  }
};

TEST_F(SrqRcMultiplexTest, Send) {
  constexpr uint32_t kQueueCount = 10;
  constexpr uint32_t kOpsPerQueue = 8;
  constexpr uint32_t kOpSize = 8;
  static_assert(
      kQueueCount * kOpsPerQueue * kOpSize <= kBufferMemoryPages * kPageSize,
      "Buffer too small to hold seperate buffers for each op.");
  ASSERT_OK_AND_ASSIGN(MultiplexSetup setup,
                       CreateMultiplexSetup(kQueueCount, kOpsPerQueue));

  for (int i = 0; i < kQueueCount; ++i) {
    for (int j = 0; j < kOpsPerQueue; ++j) {
      uint32_t wr_id = i * kOpsPerQueue + j;
      uint64_t offset = wr_id * kOpSize;
      ibv_sge sge = verbs_util::CreateSge(
          setup.basic_setup.recv_buffer.subspan(offset, kOpSize),
          setup.basic_setup.recv_mr);
      ibv_recv_wr recv = verbs_util::CreateRecvWr(wr_id, &sge, /*num_sge=*/1);
      verbs_util::PostSrqRecv(setup.basic_setup.srq, recv);
    }
  }

  for (int i = 0; i < kQueueCount; ++i) {
    for (int j = 0; j < kOpsPerQueue; ++j) {
      uint32_t wr_id = i * kOpsPerQueue + j;
      uint64_t offset = wr_id * kOpSize;
      ibv_sge sge = verbs_util::CreateSge(
          setup.basic_setup.send_buffer.subspan(offset, kOpSize),
          setup.basic_setup.send_mr);
      ibv_send_wr send = verbs_util::CreateSendWr(wr_id, &sge, /*num_sge=*/1);
      verbs_util::PostSend(setup.send_qps[i], send);
    }
  }

  // Count the number of recv entries each QP get.
  absl::flat_hash_map<uint32_t, uint32_t> recv_count;
  for (ibv_qp* qp : setup.recv_qps) {
    recv_count[qp->qp_num] = 0;
  }
  // Verify RECVs.
  uint32_t total_ops_count = kQueueCount * kOpsPerQueue;
  for (int i = 0; i < total_ops_count; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                setup.basic_setup.recv_cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion.opcode, IBV_WC_RECV);
    ASSERT_TRUE(recv_count.contains(completion.qp_num));
    ++recv_count.at(completion.qp_num);
  }
  EXPECT_THAT(recv_count, testing::SizeIs(kQueueCount));
  EXPECT_THAT(recv_count, testing::Each(testing::Pair(
                              testing::_, testing::Eq(kOpsPerQueue))));

  EXPECT_THAT(
      setup.basic_setup.recv_buffer.subspan(0, total_ops_count * kOpSize),
      testing::Each(kSendContent));

  for (int i = 0; i < total_ops_count; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                setup.basic_setup.send_cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  }
}

TEST_F(SrqRcMultiplexTest, WriteWithImmediate) {
  constexpr uint32_t kQueueCount = 10;
  constexpr uint32_t kOpsPerQueue = 8;
  constexpr uint32_t kOpSize = 8;
  static_assert(
      kQueueCount * kOpsPerQueue * kOpSize <= kBufferMemoryPages * kPageSize,
      "Buffer too small to hold seperate buffers for each op.");
  ASSERT_OK_AND_ASSIGN(MultiplexSetup setup,
                       CreateMultiplexSetup(kQueueCount, kOpsPerQueue));

  for (int i = 0; i < kQueueCount; ++i) {
    for (int j = 0; j < kOpsPerQueue; ++j) {
      uint32_t wr_id = i * kOpsPerQueue + j;
      ibv_recv_wr recv =
          verbs_util::CreateRecvWr(wr_id, /*sge=*/nullptr, /*num_sge=*/0);
      verbs_util::PostSrqRecv(setup.basic_setup.srq, recv);
    }
  }

  for (int i = 0; i < kQueueCount; ++i) {
    for (int j = 0; j < kOpsPerQueue; ++j) {
      uint32_t wr_id = i * kOpsPerQueue + j;
      uint64_t offset = wr_id * kOpSize;
      ibv_sge sge = verbs_util::CreateSge(
          setup.basic_setup.send_buffer.subspan(offset, kOpSize),
          setup.basic_setup.send_mr);
      ibv_send_wr write = verbs_util::CreateWriteWr(
          wr_id, &sge, /*num_sge=*/1,
          setup.basic_setup.recv_buffer.data() + offset,
          setup.basic_setup.recv_mr->rkey);
      write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
      // Use immediate data to store the buffer offset.
      write.imm_data = offset;
      verbs_util::PostSend(setup.send_qps[i], write);
    }
  }

  // count the number of recv entries each QP get.
  absl::flat_hash_map<uint32_t, uint32_t> recv_count;
  for (ibv_qp* qp : setup.recv_qps) {
    recv_count[qp->qp_num] = 0;
  }
  // Verify RECVs.
  uint32_t total_ops_count = kQueueCount * kOpsPerQueue;
  for (int i = 0; i < total_ops_count; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                setup.basic_setup.recv_cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion.opcode, IBV_WC_RECV_RDMA_WITH_IMM);
    uint64_t offset = completion.imm_data;
    EXPECT_THAT(setup.basic_setup.recv_buffer.subspan(offset, kOpSize),
                testing::Each(kSendContent));
    ++recv_count.at(completion.qp_num);
  }
  EXPECT_THAT(recv_count, testing::SizeIs(kQueueCount));
  EXPECT_THAT(recv_count, testing::Each(testing::Pair(
                              testing::_, testing::Eq(kOpsPerQueue))));

  EXPECT_THAT(
      setup.basic_setup.recv_buffer.subspan(0, total_ops_count * kOpSize),
      testing::Each(kSendContent));

  for (int i = 0; i < total_ops_count; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion, verbs_util::WaitForCompletion(
                                                setup.basic_setup.send_cq));
    EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
    EXPECT_EQ(completion.opcode, IBV_WC_RDMA_WRITE);
  }
}

// TODO(author2): Reuse SRQ multiplex test and use seperate QP in each
// thread.
class SrqMultiThreadTest : public SrqTest {
 public:
  // Multithreaded tests uses [queue_capacity] to reserve enough space in all
  // queues, i.e. CQ, QP and SRQ.
  absl::StatusOr<BasicSetup> CreateBasicSetup(uint32_t queue_capacity) {
    BasicSetup setup;
    setup.send_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.send_buffer.data(),
              setup.send_buffer.data() + setup.send_buffer.size(),
              kSendContent);
    setup.recv_buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    std::fill(setup.recv_buffer.data(),
              setup.recv_buffer.data() + setup.send_buffer.size(),
              kRecvContent);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.send_mr = ibv_.RegMr(setup.pd, setup.send_buffer);
    if (!setup.send_mr) {
      return absl::InternalError("Failed to register send mr.");
    }
    setup.recv_mr = ibv_.RegMr(setup.pd, setup.recv_buffer);
    if (!setup.recv_mr) {
      return absl::InternalError("Failed to register recv mr.");
    }
    setup.send_cq = ibv_.CreateCq(setup.context, queue_capacity);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context, queue_capacity);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    setup.srq = ibv_.CreateSrq(setup.pd, queue_capacity);
    if (!setup.srq) {
      return absl::InternalError("Failed to create srq.");
    }
    setup.send_qp = ibv_.CreateQp(setup.pd, setup.send_cq, setup.recv_cq,
                                  setup.srq, IBV_QPT_RC,
                                  QpInitAttribute()
                                      .set_max_send_wr(queue_capacity)
                                      .set_max_recv_wr(queue_capacity));
    if (!setup.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    setup.recv_qp = ibv_.CreateQp(setup.pd, setup.send_cq, setup.recv_cq,
                                  setup.srq, IBV_QPT_RC,
                                  QpInitAttribute()
                                      .set_max_send_wr(queue_capacity)
                                      .set_max_recv_wr(queue_capacity));
    if (!setup.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    RETURN_IF_ERROR(
        ibv_.SetUpLoopbackRcQps(setup.send_qp, setup.recv_qp, setup.port_attr));
    return setup;
  }
};

TEST_F(SrqMultiThreadTest, MultiThreadedSrqLoopback) {
  // concurrecy parameters
  static constexpr int kThreadCount = 50;
  static constexpr int kWrPerThread = 20;
  static constexpr int kTotalWr = kThreadCount * kWrPerThread;
  static constexpr uint32_t kRequestMaxWr = kTotalWr + 10;
  static_assert(kTotalWr <= kBufferMemoryPages * kPageSize,
                "Buffer too small for one byte send. Reduce total WRs or "
                "increase buffer size.");
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup(kRequestMaxWr));

  // Concurrently post buffers into the SRQ using multiple threads
  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; thread_id++) {
    threads.push_back(std::thread([thread_id, &setup]() {
      for (int i = 0; i < kWrPerThread; i++) {
        uint32_t wr_id = thread_id * kWrPerThread + i;
        ibv_sge sge = verbs_util::CreateSge(
            setup.recv_buffer.subspan(/*offset=*/wr_id, 1), setup.recv_mr);
        ibv_recv_wr recv = verbs_util::CreateRecvWr(wr_id, &sge, /*num_sge=*/1);
        verbs_util::PostSrqRecv(setup.srq, recv);
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // Post sends (1 more than expected).
  for (int i = 0; i <= kTotalWr; i++) {
    ibv_sge sge =
        verbs_util::CreateSge(setup.send_buffer.subspan(i, 1), setup.send_mr);
    ibv_send_wr send =
        verbs_util::CreateSendWr(/*wr_id=*/kTotalWr + i, &sge, /*num_sge=*/1);
    verbs_util::PostSend(setup.send_qp, send);
  }
  // Poll send completions.
  for (int i = 0; i < kTotalWr; i++) {
    ASSERT_OK_AND_ASSIGN(ibv_wc wc,
                         verbs_util::WaitForCompletion(setup.send_cq));
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS);
  }
  // The last send_wr should get an RnR.
  ASSERT_OK_AND_ASSIGN(ibv_wc wc, verbs_util::WaitForCompletion(setup.send_cq));
  EXPECT_EQ(wc.status, IBV_WC_RNR_RETRY_EXC_ERR) << "too many rr in srq";

  std::vector<bool> succeeded(kTotalWr, false);
  for (int i = 0; i < kTotalWr; i++) {
    ASSERT_OK_AND_ASSIGN(ibv_wc wc,
                         verbs_util::WaitForCompletion(setup.recv_cq));
    EXPECT_FALSE(succeeded[wc.wr_id]);
    succeeded[wc.wr_id] = wc.status == IBV_WC_SUCCESS;
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS)
        << "WR # " << wc.wr_id << " failed with status " << wc.status << ".";
  }
  for (int i = 0; i < kTotalWr; ++i) {
    EXPECT_TRUE(succeeded[i]) << "WR # " << i << " not succeeded.";
  }
  EXPECT_THAT(setup.recv_buffer.subspan(0, kTotalWr), Each(kSendContent));
}

}  // namespace rdma_unit_test
