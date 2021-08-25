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

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>

#include <array>
#include <cstdint>
#include <thread>  // NOLINT
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::IsNull;
using ::testing::NotNull;

class QpTest : public BasicFixture {
 public:
  int InitRcQP(ibv_qp* qp) const {
    ibv_qp_attr mod_init = CreateBasicRcQpInitAttr(qp);
    return ibv_modify_qp(qp, &mod_init, kRcQpInitMask);
  }

  int InitUdQP(ibv_qp* qp, uint32_t qkey) const {
    verbs_util::PortGid port_gid = ibv_.GetLocalPortGid(qp->context);
    ibv_qp_attr mod_init = {};
    mod_init.qp_state = IBV_QPS_INIT;
    mod_init.pkey_index = 0;
    mod_init.port_num = port_gid.port;
    mod_init.qkey = qkey;
    constexpr int kInitMask =
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY;
    return ibv_modify_qp(qp, &mod_init, kInitMask);
  }

  static ibv_send_wr DummySend() {
    ibv_send_wr send;
    send.wr_id = 1;
    send.next = nullptr;
    send.sg_list = nullptr;
    send.num_sge = 0;
    send.opcode = IBV_WR_SEND;
    send.send_flags = IBV_SEND_SIGNALED;
    return send;
  }

 protected:
  static constexpr int kRcQpInitMask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  static constexpr int kRcQpRtrMask =
      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
      IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  static constexpr int kRcQpRtsMask =
      IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
      IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

  static constexpr ibv_qp_cap kBasicQpCap = {
      .max_send_wr = 10,
      .max_recv_wr = 1,
      .max_send_sge = 1,
      .max_recv_sge = 1,
      .max_inline_data = verbs_util::kDefaultMaxInlineSize};

  struct BasicSetup {
    ibv_context* context;
    ibv_pd* pd;
    ibv_cq* cq;
    ibv_qp_init_attr basic_attr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.basic_attr = CreateBasicInitAttr(setup.cq);
    return setup;
  }

  ibv_qp_init_attr CreateBasicInitAttr(ibv_cq* cq) const {
    // Set up a sensible default attr.
    ibv_qp_init_attr basic_attr{};
    basic_attr.send_cq = cq;
    basic_attr.recv_cq = cq;
    basic_attr.srq = nullptr;
    basic_attr.cap = kBasicQpCap;
    basic_attr.qp_type = IBV_QPT_RC;
    basic_attr.sq_sig_all = 0;
    return basic_attr;
  }

  ibv_qp_attr CreateBasicRcQpInitAttr(ibv_qp* qp) const {
    static constexpr int kRemoteAccessAll = IBV_ACCESS_REMOTE_WRITE |
                                            IBV_ACCESS_REMOTE_READ |
                                            IBV_ACCESS_REMOTE_ATOMIC;
    verbs_util::PortGid port_gid = ibv_.GetLocalPortGid(qp->context);
    ibv_qp_attr mod_init = {};
    mod_init.qp_state = IBV_QPS_INIT;
    mod_init.pkey_index = 0;
    mod_init.port_num = port_gid.port;
    mod_init.qp_access_flags = kRemoteAccessAll;
    return mod_init;
  }

  ibv_qp_attr CreateBasicRcQpRtrAttr(ibv_qp* qp) const {
    verbs_util::PortGid port_gid = ibv_.GetLocalPortGid(qp->context);
    ibv_qp_attr mod_rtr = {};
    mod_rtr.qp_state = IBV_QPS_RTR;
    // Small enough MTU that should be supported everywhere.
    mod_rtr.path_mtu = IBV_MTU_1024;
    mod_rtr.dest_qp_num = qp->qp_num;
    mod_rtr.rq_psn = 24;
    mod_rtr.max_dest_rd_atomic = 10;
    mod_rtr.min_rnr_timer = 26;  // 82us delay
    mod_rtr.ah_attr.grh.dgid = port_gid.gid;
    mod_rtr.ah_attr.grh.sgid_index = port_gid.gid_index;
    mod_rtr.ah_attr.grh.hop_limit = 127;
    mod_rtr.ah_attr.is_global = 1;
    mod_rtr.ah_attr.sl = 5;
    mod_rtr.ah_attr.port_num = 1;
    return mod_rtr;
  }

  ibv_qp_attr CreateBasicRcQpRtsAttr() const {
    ibv_qp_attr mod_rts = {};
    mod_rts.qp_state = IBV_QPS_RTS;
    mod_rts.sq_psn = 1225;
    mod_rts.timeout = 17;  // ~500 ms
    mod_rts.retry_cnt = 7;
    mod_rts.rnr_retry = 5;
    mod_rts.max_rd_atomic = 5;
    return mod_rts;
  }

  // Takes a bit-mask of arbitrary qp attributes and returns a vector with
  // each attribute that is set or'd with IBV_QP_STATE so that it can be
  // directly used in a ibv_modify_qp invocation.
  std::vector<int> CreateMaskCombinations(int required_attributes) {
    std::vector<int> masks;
    // Ensure the IBV_QP_STATE bit is set.  This will result in the first
    // vector element being no attributes set.
    required_attributes |= IBV_QP_STATE;
    // Traverse the bitmask from bit0..binN and find each set bit which
    // corresponds to an attribute that is set and add it the vector.
    for (unsigned int bit = 0; bit < 8 * sizeof(required_attributes); ++bit) {
      if ((1 << bit) & required_attributes)
        masks.push_back(1 << bit | IBV_QP_STATE);
    }
    return masks;
  }
};

TEST_F(QpTest, Create) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_.CreateQp(setup.pd, setup.basic_attr), NotNull());
}

TEST_F(QpTest, CreateWithInlineMax) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  setup.basic_attr.cap.max_inline_data = verbs_util::kDefaultMaxInlineSize;
  EXPECT_THAT(ibv_.CreateQp(setup.pd, setup.basic_attr), NotNull());
}

TEST_F(QpTest, CreateWithInvalidSendCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, NotNull());
  static_assert(sizeof(ibv_cq) < sizeof(ibv_qp), "Unsafe cast below");
  ibv_cq* fake_cq = reinterpret_cast<ibv_cq*>(qp);
  EXPECT_THAT(
      ibv_.CreateQp(setup.pd, fake_cq /*send_cq*/, setup.cq /*recv_cq*/,
                    nullptr /*srq*/, 1 /*max_send_wqr*/, 1 /*max_recv_rw*/,
                    IBV_QPT_RC /*qp_type*/, true /*sig_all*/),
      IsNull());
}

TEST_F(QpTest, CreateWithInvalidRecvCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, NotNull());
  static_assert(sizeof(ibv_cq) < sizeof(ibv_qp), "Unsafe cast below");
  ibv_cq* fake_cq = reinterpret_cast<ibv_cq*>(qp);
  EXPECT_THAT(
      ibv_.CreateQp(setup.pd, setup.cq /*send_cq*/, fake_cq /*recv_cq*/,
                    nullptr /*srq*/, 1 /*max_send_wqr*/, 1 /*max_recv_rw*/,
                    IBV_QPT_RC /*qp_type*/, true /*sig_all*/),
      IsNull());
}

TEST_F(QpTest, CreateWithInvalidSrq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, NotNull());
  static_assert(sizeof(ibv_srq) < sizeof(ibv_qp), "Unsafe cast below");
  ibv_srq* fake_srq = reinterpret_cast<ibv_srq*>(qp);
  EXPECT_THAT(
      ibv_.CreateQp(setup.pd, setup.cq /*send_cq*/, setup.cq /*recv_cq*/,
                    fake_srq /*srq*/, 1 /*max_send_wqr*/, 1 /*max_recv_rw*/,
                    IBV_QPT_RC /*qp_type*/, true /*sig_all*/),
      IsNull());
}

TEST_F(QpTest, UnknownType) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp_init_attr attr = setup.basic_attr;
  attr.qp_type = static_cast<ibv_qp_type>(0xf0);
  ibv_qp* qp = ibv_.CreateQp(setup.pd, attr);
  if (!Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_THAT(qp, IsNull());
  }
}

TEST_F(QpTest, CreateUd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp_init_attr attr = setup.basic_attr;
  attr.qp_type = IBV_QPT_UD;
  EXPECT_THAT(ibv_.CreateQp(setup.pd, attr), NotNull());
}

TEST_F(QpTest, ZeroSendWr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  setup.basic_attr.cap.max_send_wr = 0;
  EXPECT_THAT(ibv_.CreateQp(setup.pd, setup.basic_attr), NotNull());
}

TEST_F(QpTest, OverflowSendWr) {
  if (!Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr uint32_t kTargetQueueSize = 300;
  setup.basic_attr.cap.max_send_wr = kTargetQueueSize;
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  // mlx treats max_send_wr as a recommendation use the actual value, but make
  // sure we got at least the amount requested.
  LOG(INFO) << "size " << setup.basic_attr.cap.max_send_wr;
  ASSERT_GE(setup.basic_attr.cap.max_send_wr, kTargetQueueSize);
  uint64_t queue_size = setup.basic_attr.cap.max_send_wr;

  // Issue enough commands to overflow the queue.
  ibv_send_wr wqe = DummySend();
  std::vector<ibv_send_wr> wqes(queue_size + 1, wqe);
  for (unsigned int i = 0; i < wqes.size() - 1; ++i) {
    wqes[i].wr_id = wqe.wr_id + i;
    wqes[i].next = &wqes[i + 1];
  }
  ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_send(qp, wqes.data(), &bad_wr), ENOMEM);
  EXPECT_EQ(bad_wr, &wqes[queue_size]);
}

TEST_F(QpTest, QueueRefs) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.cq);
  ASSERT_THAT(qp, NotNull());
  // Rejected because QP holds ref.
  EXPECT_EQ(ibv_destroy_cq(setup.cq), EBUSY);
}

TEST_F(QpTest, ExceedsDeviceCap) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "Some NICs allow QP creation over device cap.";
  }
  // Some devices will fail the qp creation. Other will just adjust the QP
  // capacity accordingly.
  const ibv_device_attr& device_attr = Introspection().device_attr();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());

  setup.basic_attr.cap = kBasicQpCap;
  setup.basic_attr.cap.max_send_wr = device_attr.max_qp_wr + 1;
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, IsNull());

  setup.basic_attr.cap = kBasicQpCap;
  setup.basic_attr.cap.max_recv_wr = device_attr.max_qp_wr + 1;
  qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, IsNull());

  setup.basic_attr.cap = kBasicQpCap;
  setup.basic_attr.cap.max_send_sge = device_attr.max_sge + 1;
  qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, IsNull());

  setup.basic_attr.cap = kBasicQpCap;
  setup.basic_attr.cap.max_recv_sge = device_attr.max_sge + 1;
  qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  EXPECT_THAT(qp, IsNull());
}

TEST_F(QpTest, ExceedsMaxQp) {
  if (Introspection().ShouldDeviateForCurrentTest()) GTEST_SKIP();
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  const ibv_device_attr& device_attr = Introspection().device_attr();
  for (int qpn = 0; qpn < device_attr.max_qp; ++qpn) {
    ASSERT_THAT(ibv_.CreateQp(setup.pd, setup.basic_attr), NotNull());
  }
  // Create one more..
  EXPECT_EQ(ibv_.CreateQp(setup.pd, setup.basic_attr), nullptr);
}

TEST_F(QpTest, ThreadedCreate) {
  static constexpr int kThreadCount = 5;
  static constexpr int kQpsPerThread = 50;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());

  std::array<std::array<ibv_qp*, kQpsPerThread>, kThreadCount> qps;
  qps = {{{nullptr}}};
  auto create_qps = [this, &setup, &qps](int thread_id) {
    // No QPs can share the same position in the array, so no need for thread
    // synchronization.
    for (int i = 0; i < kQpsPerThread; ++i) {
      ibv_qp_init_attr basic_attr = CreateBasicInitAttr(setup.cq);
      qps[thread_id][i] = ibv_.CreateQp(setup.pd, basic_attr);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(create_qps, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_qps : qps) {
    for (const auto& qp : thread_qps) {
      EXPECT_THAT(qp, NotNull());
    }
  }
}

TEST_F(QpTest, ThreadedCreateAndDestroy) {
  static constexpr int kThreadCount = 5;
  static constexpr int kQpsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());

  // Initialize to 1 since we are expecting the values to be 0 after destroying
  // QPs.
  std::array<std::array<int, kQpsPerThread>, kThreadCount> destroy_results;
  destroy_results = {{{1}}};
  auto create_destroy_qps = [this, &setup, &destroy_results](int thread_id) {
    for (int i = 0; i < kQpsPerThread; ++i) {
      ibv_qp_init_attr basic_attr = CreateBasicInitAttr(setup.cq);
      ibv_qp* qp = ibv_.CreateQp(setup.pd, basic_attr);
      ASSERT_THAT(qp, NotNull());
      destroy_results[thread_id][i] = ibv_.DestroyQp(qp);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(create_destroy_qps, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_results : destroy_results) {
    for (const auto& destroy_result : thread_results) {
      EXPECT_EQ(destroy_result, 0);
    }
  }
}

// TODO(author1): Cross context objects.
// TODO(author1): List of WQEs which is too large.
// TODO(author1): Single WQEs which is too large.
// TODO(author1): List of WQEs which partially fits (make sure to check
// where it stops)

TEST_F(QpTest, Modify) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  // Reset -> Init.
  ASSERT_EQ(InitRcQP(qp), 0);
  // Init -> Ready to receive.
  ibv_qp_attr mod_rtr = CreateBasicRcQpRtrAttr(qp);
  ASSERT_EQ(ibv_modify_qp(qp, &mod_rtr, kRcQpRtrMask), 0);
  // Ready to receive -> Ready to send.
  ibv_qp_attr mod_rts = CreateBasicRcQpRtsAttr();
  EXPECT_EQ(ibv_modify_qp(qp, &mod_rts, kRcQpRtsMask), 0);
}

TEST_F(QpTest, ModifyInvalidResetToRtrTransition) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  ibv_qp_attr mod_rtr = CreateBasicRcQpRtrAttr(qp);
  EXPECT_NE(ibv_modify_qp(qp, &mod_rtr, kRcQpRtrMask), 0);
}

TEST_F(QpTest, ModifyInvalidInitToRtsTransition) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());
  // Reset -> Init.
  ASSERT_EQ(InitRcQP(qp), 0);
  // Init -> Ready to send.
  ibv_qp_attr mod_rts = CreateBasicRcQpRtsAttr();
  EXPECT_NE(ibv_modify_qp(qp, &mod_rts, kRcQpRtsMask), 0);
}

TEST_F(QpTest, ModifyInvalidRtsToRtrTransition) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());
  // Reset -> Init.
  ASSERT_EQ(InitRcQP(qp), 0);
  // Init -> Ready to receive.
  ibv_qp_attr mod_rtr = CreateBasicRcQpRtrAttr(qp);
  ASSERT_EQ(ibv_modify_qp(qp, &mod_rtr, kRcQpRtrMask), 0);
  // Ready to receive -> Ready to send.
  ibv_qp_attr mod_rts = CreateBasicRcQpRtsAttr();
  ASSERT_EQ(ibv_modify_qp(qp, &mod_rts, kRcQpRtsMask), 0);
  // Ready to send -> Ready to receive.
  EXPECT_NE(ibv_modify_qp(qp, &mod_rtr, kRcQpRtrMask), 0);
}

TEST_F(QpTest, ModifyBadResetToInitStateTransition) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  ibv_qp_attr mod_init = CreateBasicRcQpInitAttr(qp);
  for (auto mask : CreateMaskCombinations(kRcQpInitMask)) {
    EXPECT_NE(ibv_modify_qp(qp, &mod_init, mask), 0);
  }
  EXPECT_EQ(ibv_modify_qp(qp, &mod_init, kRcQpInitMask), 0);
}

TEST_F(QpTest, ModifyBadInitToRtrStateTransition) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  // Reset -> Init.
  ASSERT_EQ(InitRcQP(qp), 0);
  // Init -> Ready to receive
  ibv_qp_attr mod_rtr = CreateBasicRcQpRtrAttr(qp);
  for (auto mask : CreateMaskCombinations(kRcQpRtrMask)) {
    EXPECT_NE(ibv_modify_qp(qp, &mod_rtr, mask), 0);
  }
  EXPECT_EQ(ibv_modify_qp(qp, &mod_rtr, kRcQpRtrMask), 0);
}

TEST_F(QpTest, ModifyBadRtrToRtsStateTransition) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  // Reset -> Init.
  ASSERT_EQ(InitRcQP(qp), 0);
  // Init -> Ready to receive.
  ibv_qp_attr mod_rtr = CreateBasicRcQpRtrAttr(qp);
  ASSERT_EQ(ibv_modify_qp(qp, &mod_rtr, kRcQpRtrMask), 0);

  // Ready to receive -> Ready to send.
  ibv_qp_attr mod_rts = CreateBasicRcQpRtsAttr();
  for (auto mask : CreateMaskCombinations(kRcQpRtsMask)) {
    int result = ibv_modify_qp(qp, &mod_rts, mask);
    EXPECT_NE(result, 0);
  }
  EXPECT_EQ(ibv_modify_qp(qp, &mod_rts, kRcQpRtsMask), 0);
}

TEST_F(QpTest, UdModify) {
  if (!Introspection().SupportsUdQp()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp_init_attr attr = setup.basic_attr;
  attr.qp_type = IBV_QPT_UD;
  ibv_qp* qp = ibv_.CreateQp(setup.pd, attr);
  ASSERT_THAT(qp, NotNull());

  // Reset -> Init.
  ASSERT_EQ(InitUdQP(qp, /*qkey=*/17), 0);
  // Init -> Ready to receive.
  ibv_qp_attr mod_rtr = {};
  mod_rtr.qp_state = IBV_QPS_RTR;
  constexpr int kRtrMask = IBV_QP_STATE;
  ASSERT_EQ(ibv_modify_qp(qp, &mod_rtr, kRtrMask), 0);
  // Ready to receive -> Ready to send.
  ibv_qp_attr mod_rts = {};
  mod_rts.qp_state = IBV_QPS_RTS;
  mod_rts.sq_psn = 1225;
  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_SQ_PSN;
  ASSERT_EQ(ibv_modify_qp(qp, &mod_rts, kRtsMask), 0);
}

TEST_F(QpTest, DeallocPdWithOutstandingQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());
  EXPECT_EQ(ibv_.DeallocPd(setup.pd), EBUSY);
}

TEST_F(QpTest, ModifyInvalidQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  // Modify on invalid qp.
  ibv_qp original = *qp;
  qp->handle = setup.cq->handle;
  ibv_qp_attr mod_init = CreateBasicRcQpInitAttr(qp);
  EXPECT_THAT(ibv_modify_qp(qp, &mod_init, kRcQpInitMask),
              AnyOf(ENOENT, EPERM, EINVAL));
  *qp = original;
}

TEST_F(QpTest, QueryInvalidQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_.CreateQp(setup.pd, setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  // Reset -> Init.
  ASSERT_EQ(InitRcQP(qp), 0);
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  ASSERT_EQ(ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr), 0);
  EXPECT_EQ(attr.qp_state, IBV_QPS_INIT);

  // Query on invalid qp.
  ibv_qp original = *qp;
  qp->handle = setup.cq->handle;
  EXPECT_THAT(ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr),
              AnyOf(EFAULT, ENOENT, EINVAL));
  *qp = original;
}

TEST_F(QpTest, DestroyInvalidQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_qp* qp = ibv_create_qp(setup.pd, &setup.basic_attr);
  ASSERT_THAT(qp, NotNull());

  // Destroy on invalid qp. Only modify the handle in this instance as the
  // underlying ibv_qp is a cast of an internal structure.
  uint32_t original_handle = qp->handle;
  qp->handle = setup.cq->handle;
  int result = ibv_destroy_qp(qp);
  EXPECT_THAT(result, AnyOf(ENOENT, EPERM, EINVAL));
  qp->handle = original_handle;
  EXPECT_EQ(ibv_destroy_qp(qp), 0);
}

// This is a series of test to test whether a QP correctly responds to a posted
// WR when QP is in different state. The expectation is set based on the IBTA
// spec, but some low level drivers will deviate from the spec for better
// performance. See specific testcases for explanation.
class QpStateTest : public BasicFixture {
 protected:
  struct BasicSetup {
    ibv_context* context = nullptr;
    verbs_util::PortGid port_gid;
    ibv_cq* cq = nullptr;
    ibv_pd* pd = nullptr;
    ibv_qp* qp = nullptr;
    ibv_qp* remote_qp = nullptr;
    RdmaMemBlock buffer;
    ibv_mr* mr = nullptr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.qp = ibv_.CreateQp(setup.pd, setup.cq);
    if (!setup.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    setup.remote_qp = ibv_.CreateQp(setup.pd, setup.cq);
    if (!setup.remote_qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    setup.buffer = ibv_.AllocBuffer(/*pages=*/1);
    setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    return setup;
  }
};

// IBTA specs says we posting to a QP in RESET, INIT or RTR state should be
// be returned with immediate error (see IBTA v1, chapter 10.8.2, c10-96).
// But a vast majority of the low-level driver deviate from this rule for better
// performance. Hence here we expect the post to be successful.
TEST_F(QpStateTest, PostSendReset) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RESET);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mr->rkey);
  ibv_send_wr* bad_wr = nullptr;
  // This deviates from IBTA spec, see comment above the test.
  EXPECT_THAT(ibv_post_send(setup.qp, &read, &bad_wr), 0);
}

TEST_F(QpStateTest, PostRecvReset) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RESET);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_THAT(ibv_post_recv(setup.qp, &recv, &bad_wr), 0);
}

TEST_F(QpStateTest, PostSendInit) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK(ibv_.SetQpInit(setup.qp, setup.port_gid.port));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_INIT);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mr->rkey);
  ibv_send_wr* bad_wr = nullptr;
  // This deviates from IBTA spec, see comment at QpStateTest.PostSendReset
  // for details.
  EXPECT_THAT(ibv_post_send(setup.qp, &read, &bad_wr), 0);
}

// We are allowed to post to recv queue in INIT or RTR state, but the WR will
// not get processed.
TEST_F(QpStateTest, PostRecvInit) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK(ibv_.SetQpInit(setup.qp, setup.port_gid.port));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_INIT);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr = nullptr;
  ASSERT_EQ(ibv_post_recv(setup.qp, &recv, &bad_wr), 0);
  absl::SleepFor(absl::Seconds(1));
  ibv_wc completion;
  EXPECT_EQ(ibv_poll_cq(setup.cq, 1, &completion), 0);
}

TEST_F(QpStateTest, PostSendRtr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK(ibv_.SetQpInit(setup.qp, setup.port_gid.port));
  ASSERT_OK(ibv_.SetQpRtr(setup.qp, setup.port_gid, setup.port_gid.gid,
                          setup.qp->qp_num));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RTR);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sge, /*num_sge=*/1, setup.buffer.data(), setup.mr->rkey);
  ibv_send_wr* bad_wr = nullptr;
  // This deviates from IBTA spec, see comment at QpStateTest.PostSendReset
  // for details.
  EXPECT_THAT(ibv_post_send(setup.qp, &read, &bad_wr), 0);
}

TEST_F(QpStateTest, PostRecvRtr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK(ibv_.SetQpInit(setup.qp, setup.port_gid.port));
  ASSERT_OK(ibv_.SetQpRtr(setup.qp, setup.port_gid, setup.port_gid.gid,
                          setup.qp->qp_num));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RTR);
  ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/1, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(ibv_post_recv(setup.qp, &recv, &bad_wr), 0);
  absl::SleepFor(absl::Seconds(1));
  ibv_wc completion;
  EXPECT_EQ(ibv_poll_cq(setup.cq, 1, &completion), 0);
}

TEST_F(QpStateTest, PostSendErr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RTS);
  ASSERT_THAT(verbs_util::FetchAddSync(setup.qp, setup.buffer.data(), setup.mr,
                                       setup.buffer.data() + 1, setup.mr->rkey,
                                       /*comp_add=*/1),
              IsOkAndHolds(IBV_WC_REM_INV_REQ_ERR));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_ERR);

  ASSERT_OK_AND_ASSIGN(
      ibv_wc_status status,
      verbs_util::ReadSync(setup.qp, setup.buffer.span(), setup.mr,
                           setup.buffer.data(), setup.mr->rkey));
  EXPECT_EQ(status, IBV_WC_WR_FLUSH_ERR);
}

TEST_F(QpStateTest, PostRecvErr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RTS);
  ASSERT_THAT(verbs_util::FetchAddSync(setup.qp, setup.buffer.data(), setup.mr,
                                       setup.buffer.data() + 1, setup.mr->rkey,
                                       /*comp_add=*/1),
              IsOkAndHolds(IBV_WC_REM_INV_REQ_ERR));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_ERR);
  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/2, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.qp, recv);
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(setup.cq));
  EXPECT_EQ(completion.status, IBV_WC_WR_FLUSH_ERR);
}

TEST_F(QpStateTest, RtsSendToRtr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_OK(ibv_.SetQpInit(setup.qp, setup.port_gid.port));
  ASSERT_OK(ibv_.SetQpRtr(setup.qp, setup.port_gid, setup.port_gid.gid,
                          setup.remote_qp->qp_num));
  ASSERT_OK(ibv_.SetQpRts(setup.qp));
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_RTS);
  ASSERT_OK(ibv_.SetQpInit(setup.remote_qp, setup.port_gid.port));
  ASSERT_OK(ibv_.SetQpRtr(setup.remote_qp, setup.port_gid, setup.port_gid.gid,
                          setup.qp->qp_num));
  ASSERT_EQ(verbs_util::GetQpState(setup.remote_qp), IBV_QPS_RTR);

  ibv_sge rsge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/1, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(setup.remote_qp, recv);
  ibv_sge ssge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/0, &ssge, /*num_sge=*/1);
  verbs_util::PostSend(setup.qp, send);

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.cq));
    ASSERT_EQ(completion.status, IBV_WC_SUCCESS);
    if (completion.qp_num == setup.remote_qp->qp_num) {
      EXPECT_EQ(completion.opcode, IBV_WC_RECV);
      EXPECT_EQ(completion.wr_id, 1);
    } else {
      EXPECT_EQ(completion.opcode, IBV_WC_SEND);
      EXPECT_EQ(completion.wr_id, 0);
    }
  }
}

TEST_F(QpStateTest, ModRtsToError) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
  ibv_qp_attr attr;
  attr.qp_state = IBV_QPS_ERR;
  ibv_qp_attr_mask attr_mask = IBV_QP_STATE;
  ASSERT_EQ(ibv_modify_qp(setup.qp, &attr, attr_mask), 0);
  ASSERT_EQ(verbs_util::GetQpState(setup.qp), IBV_QPS_ERR);
}

// TODO(author1): Test larger MTU than the device allows

}  // namespace rdma_unit_test
