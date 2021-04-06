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

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/status_matchers.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

class CompChannelTest : public BasicFixture {
 protected:
  static constexpr int kNotifyAny = 0;
  static constexpr int kNotifySolicited = 1;
  static constexpr uint32_t kRKey = 17;
  static constexpr uint32_t kCqMaxWr = 10;

  struct BasicSetup {
    struct QpEnd {
      ibv_comp_channel* channel;
      ibv_cq* cq;
      ibv_qp* qp;
    };
    ibv_context* context;
    ibv_pd* pd;
    RdmaMemBlock buffer;
    ibv_mr* mr;
    ibv_sge sge;
    QpEnd local;
    QpEnd remote;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.buffer = ibv_.AllocBuffer(/*pages=*/1);
    setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    setup.sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
    setup.local.channel = ibv_.CreateChannel(setup.context);
    if (!setup.local.channel) {
      return absl::InternalError("Failed to create local comm channel.");
    }
    setup.remote.channel = ibv_.CreateChannel(setup.context);
    if (!setup.remote.channel) {
      return absl::InternalError("Failed to create remote comm channel.");
    }
    setup.local.cq =
        ibv_.CreateCq(setup.context, kCqMaxWr, setup.local.channel);
    if (!setup.local.cq) {
      return absl::InternalError("Failed to create local cq.");
    }
    setup.remote.cq =
        ibv_.CreateCq(setup.context, kCqMaxWr, setup.remote.channel);
    if (!setup.remote.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.local.qp = ibv_.CreateQp(setup.pd, setup.local.cq);
    if (!setup.local.qp) {
      return absl::InternalError("Failed to create local qp.");
    }
    setup.remote.qp = ibv_.CreateQp(setup.pd, setup.remote.cq);
    if (!setup.remote.qp) {
      return absl::InternalError("Failed to create remote qp.");
    }
    ibv_.SetUpLoopbackRcQps(setup.local.qp, setup.remote.qp,
                            ibv_.GetContextAddressInfo(setup.context));
    return setup;
  }

  void DoAtomic(BasicSetup& setup, ibv_qp* qp) {
    DCHECK_GT(setup.sge.length, 32UL);
    ibv_sge sg = setup.sge;
    sg.addr += sg.addr % 8;
    sg.length = 8;
    // Offset target from source enough to ensure no overlap after alignment
    // fixes.
    const int kSpacing = 16;
    uint8_t* target = setup.buffer.data() + kSpacing;
    target += reinterpret_cast<uint64_t>(target) % 8;
    ibv_send_wr fetch_add = verbs_util::CreateFetchAddWr(
        /*wr_id=*/1, &sg, /*num_sge=*/1, target, setup.mr->rkey, 0);
    ibv_send_wr* bad_wr = nullptr;
    int result = ibv_post_send(qp, &fetch_add, &bad_wr);
    CHECK_EQ(result, 0);
  }

  void DoWrite(BasicSetup& setup, ibv_qp* qp) {
    ibv_send_wr write =
        verbs_util::CreateWriteWr(/*wr_id=*/1, &setup.sge, /*num_sge=*/1,
                                  setup.buffer.data(), setup.mr->rkey);
    ibv_send_wr* bad_wr = nullptr;
    int result = ibv_post_send(qp, &write, &bad_wr);
    CHECK_EQ(result, 0);
  }

  void DoSend(BasicSetup& setup, ibv_qp* qp, bool solicited) {
    ibv_send_wr send =
        verbs_util::CreateSendWr(/*wr_id=*/1, &setup.sge, /*num_sge=*/1);
    send.send_flags = solicited ? IBV_SEND_SOLICITED : 0;
    send.send_flags |= IBV_SEND_SIGNALED;
    verbs_util::PostSend(qp, send);
  }

  void DoRecv(BasicSetup& setup, ibv_qp* qp) {
    ibv_recv_wr recv =
        verbs_util::CreateRecvWr(/*wr_id=*/1, &setup.sge, /*num_sge=*/1);
    verbs_util::PostRecv(qp, recv);
  }

  static void CheckEvent(ibv_comp_channel* channel, ibv_cq* expected_cq) {
    ibv_cq* cq;
    void* cq_context;
    int result = ibv_get_cq_event(channel, &cq, &cq_context);
    ASSERT_EQ(0, result);
    ASSERT_EQ(cq, expected_cq);
    ASSERT_EQ(cq->context, expected_cq->context);
  }

  static void CheckSend(ibv_cq* cq) {
    ibv_wc completion = verbs_util::WaitForCompletion(cq).value();
    ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
    ASSERT_EQ(IBV_WC_SEND, completion.opcode);
  }

  static void CheckRecv(ibv_cq* cq) {
    ibv_wc completion = verbs_util::WaitForCompletion(cq).value();
    ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
    ASSERT_EQ(IBV_WC_RECV, completion.opcode);
  }

  static bool IsReady(ibv_comp_channel* channel) {
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(channel->fd, &fds);
    timeval no_block = {.tv_sec = 0, .tv_usec = 0};
    int result = select(FD_SETSIZE, &fds, nullptr, nullptr, &no_block);
    CHECK_GE(result, 0);
    return result == 1;
  }
};

TEST_F(CompChannelTest, CreateDestroy) {
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_comp_channel* channel = ibv_create_comp_channel(context);
  ASSERT_NE(nullptr, channel);
  ASSERT_EQ(0, ibv_destroy_comp_channel(channel));
}

TEST_F(CompChannelTest, DestroyChannelWithCqRef) {
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_comp_channel* channel = ibv_create_comp_channel(context);
  ASSERT_NE(nullptr, channel);
  ibv_cq* cq = ibv_create_cq(context, 10, nullptr, channel, 0);
  ASSERT_NE(nullptr, cq);
  // Expected failure due to outstanding ref from CQ.
  ASSERT_EQ(EBUSY, ibv_destroy_comp_channel(channel));

  ASSERT_EQ(0, ibv_destroy_cq(cq));
  ASSERT_EQ(0, ibv_destroy_comp_channel(channel));
}

TEST_F(CompChannelTest, RequestNoificationOnCqWithoutCompChannel) {
  ibv_context* context = ibv_.OpenDevice().value();
  ibv_cq* cq = ibv_create_cq(context, 10, nullptr, nullptr, 0);
  ASSERT_NE(nullptr, cq);
  int result = ibv_req_notify_cq(cq, kNotifyAny);
  if (Introspection().CorrectlyReportsCompChannelErrors()) {
    ASSERT_NE(result, 0);
  }
  ASSERT_EQ(0, ibv_destroy_cq(cq));
}

TEST_F(CompChannelTest, Atomic) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.local.channel));
  DoAtomic(setup, setup.local.qp);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.local.cq).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_TRUE(IsReady(setup.local.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.local.channel, setup.local.cq));
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, Bind) {
  if (!Introspection().SupportsType2()) {
    GTEST_SKIP() << "Nic does not support Type2 MW";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, IBV_MW_TYPE_2);
  ASSERT_NE(mw, nullptr);
  ASSERT_EQ(IBV_WC_SUCCESS, verbs_util::BindType2MwSync(
                                setup.local.qp, mw, setup.buffer.span(), kRKey,
                                setup.mr, IBV_ACCESS_REMOTE_READ));
  ASSERT_TRUE(IsReady(setup.local.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.local.channel, setup.local.cq));
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, Write) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.local.channel));
  DoWrite(setup, setup.local.qp);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.local.cq).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_TRUE(IsReady(setup.local.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.local.channel, setup.local.cq));
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, RecvSolicitedNofityAny) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.remote.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.remote.channel));
  DoRecv(setup, setup.remote.qp);
  DoSend(setup, setup.local.qp, /*solicited=*/true);
  ASSERT_NO_FATAL_FAILURE(CheckSend(setup.local.cq));
  ASSERT_NO_FATAL_FAILURE(CheckRecv(setup.remote.cq));
  ASSERT_FALSE(IsReady(setup.local.channel));
  ASSERT_TRUE(IsReady(setup.remote.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.remote.channel, setup.remote.cq));
  ibv_ack_cq_events(setup.remote.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, RecvSolicitedNofitySolicited) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.remote.cq, kNotifySolicited);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.remote.channel));
  DoRecv(setup, setup.remote.qp);
  DoSend(setup, setup.local.qp, /*solicited=*/true);
  ASSERT_NO_FATAL_FAILURE(CheckSend(setup.local.cq));
  ASSERT_NO_FATAL_FAILURE(CheckRecv(setup.remote.cq));
  ASSERT_FALSE(IsReady(setup.local.channel));
  ASSERT_TRUE(IsReady(setup.remote.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.remote.channel, setup.remote.cq));
  ibv_ack_cq_events(setup.remote.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, RecvUnsolicitedNofityAny) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.remote.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.remote.channel));
  DoRecv(setup, setup.remote.qp);
  DoSend(setup, setup.local.qp, /*solicited=*/false);
  ASSERT_NO_FATAL_FAILURE(CheckSend(setup.local.cq));
  ASSERT_NO_FATAL_FAILURE(CheckRecv(setup.remote.cq));
  ASSERT_FALSE(IsReady(setup.local.channel));
  ASSERT_TRUE(IsReady(setup.remote.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.remote.channel, setup.remote.cq));
  ibv_ack_cq_events(setup.remote.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, RecvUnsolicitedNofitySolicited) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.remote.cq, kNotifySolicited);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.remote.channel));
  DoRecv(setup, setup.remote.qp);
  DoSend(setup, setup.local.qp, /*solicited=*/false);
  ASSERT_NO_FATAL_FAILURE(CheckSend(setup.local.cq));
  ASSERT_NO_FATAL_FAILURE(CheckRecv(setup.remote.cq));
  ASSERT_FALSE(IsReady(setup.local.channel));
  ASSERT_FALSE(IsReady(setup.remote.channel));
}

TEST_F(CompChannelTest, AcknowledgeWithoutOutstanding) {
  if (!Introspection().CorrectlyReportsCompChannelErrors()) {
    GTEST_SKIP() << "transport hangs when acknowledging too many.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_comp_channel* channel = ibv_.CreateChannel(setup.context);
  ASSERT_NE(nullptr, channel);
  ibv_cq* cq = ibv_.CreateCq(setup.context, kCqMaxWr, channel);
  ibv_ack_cq_events(cq, 1);
}

TEST_F(CompChannelTest, AcknowledgeTooMany) {
  if (!Introspection().CorrectlyReportsCompChannelErrors()) {
    GTEST_SKIP() << "transport hangs when acknowledging too many.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.local.channel));
  DoWrite(setup, setup.local.qp);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.local.cq).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_TRUE(IsReady(setup.local.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.local.channel, setup.local.cq));
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/10);
}

TEST_F(CompChannelTest, DeleteWithUnacked) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.local.channel));
  DoWrite(setup, setup.local.qp);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.local.cq).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_TRUE(IsReady(setup.local.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.local.channel, setup.local.cq));
  ASSERT_NE(0, ibv_destroy_cq(setup.local.cq));
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, SameQueueMultipleOutstanding) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.local.channel));
  DoWrite(setup, setup.local.qp);
  ibv_wc completion = verbs_util::WaitForCompletion(setup.local.cq).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  // Queue up a second event before processing the first.
  result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  DoWrite(setup, setup.local.qp);
  completion = verbs_util::WaitForCompletion(setup.local.cq).value();
  ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  ASSERT_TRUE(IsReady(setup.local.channel));
  // Hardware collapses events into 1.
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.local.channel, setup.local.cq));
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, MuxOntoSingleChannel) {
  static constexpr int kNumberOfPairs = 20;
  struct QpPair {
    ibv_cq* cq1;
    ibv_cq* cq2;
    ibv_qp* qp1;
    ibv_qp* qp2;
  };
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  std::vector<QpPair> qps;
  ibv_comp_channel* channel = ibv_.CreateChannel(setup.context);
  ASSERT_NE(nullptr, channel);  // Crash ok
  for (int i = 0; i < kNumberOfPairs; ++i) {
    QpPair new_pair;
    new_pair.cq1 = ibv_.CreateCq(setup.context, kCqMaxWr, channel);
    new_pair.cq2 = ibv_.CreateCq(setup.context, kCqMaxWr, channel);
    new_pair.qp1 = ibv_.CreateQp(setup.pd, new_pair.cq1);
    new_pair.qp2 = ibv_.CreateQp(setup.pd, new_pair.cq2);
    ibv_.SetUpLoopbackRcQps(new_pair.qp1, new_pair.qp2,
                            ibv_.GetContextAddressInfo(setup.context));
    qps.push_back(new_pair);
  }
  for (auto& pair : qps) {
    int result = ibv_req_notify_cq(pair.cq1, kNotifyAny);
    ASSERT_EQ(result, 0);
    DoWrite(setup, pair.qp1);
  }
  for (auto& pair : qps) {
    ibv_wc completion = verbs_util::WaitForCompletion(pair.cq1).value();
    ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  }
  ASSERT_TRUE(IsReady(channel));
  for (int i = 0; i < kNumberOfPairs; ++i) {
    ibv_cq* cq;
    void* cq_context;
    int result = ibv_get_cq_event(channel, &cq, &cq_context);
    ASSERT_EQ(0, result);
    bool found = false;
    for (const auto& pair : qps) {
      if (pair.cq1 == cq) {
        ASSERT_EQ(pair.cq1->cq_context, cq_context);
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found);
    ibv_ack_cq_events(cq, /*nevents=*/1);
  }
}

TEST_F(CompChannelTest, ManyOutstanding) {
  static constexpr int kTargetOutstanding = 1000;
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_FALSE(IsReady(setup.local.channel));
  for (int i = 0; i < kTargetOutstanding; ++i) {
    int result = ibv_req_notify_cq(setup.local.cq, kNotifyAny);
    ASSERT_EQ(result, 0);
    DoWrite(setup, setup.local.qp);
    ibv_wc completion = verbs_util::WaitForCompletion(setup.local.cq).value();
    ASSERT_EQ(IBV_WC_SUCCESS, completion.status);
  }
  ASSERT_TRUE(IsReady(setup.local.channel));
  // Hardware collapses events into 1.
  CheckEvent(setup.local.channel, setup.local.cq);
  ibv_ack_cq_events(setup.local.cq, /*nevents=*/1);
}

TEST_F(CompChannelTest, DowngradeRequest) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.remote.cq, kNotifyAny);
  ASSERT_EQ(result, 0);
  // Change to only notifying on solicited. Which is ignored.
  result = ibv_req_notify_cq(setup.remote.cq, kNotifySolicited);
  ASSERT_EQ(result, 0);
  ASSERT_FALSE(IsReady(setup.remote.channel));
  DoRecv(setup, setup.remote.qp);
  DoSend(setup, setup.local.qp, /*solicited=*/false);
  ASSERT_NO_FATAL_FAILURE(CheckSend(setup.local.cq));
  ASSERT_NO_FATAL_FAILURE(CheckRecv(setup.remote.cq));
  ASSERT_FALSE(IsReady(setup.local.channel));
  ASSERT_TRUE(IsReady(setup.remote.channel));
}

TEST_F(CompChannelTest, UpgradeRequest) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  int result = ibv_req_notify_cq(setup.remote.cq, kNotifySolicited);
  ASSERT_EQ(result, 0);
  // Change to notifying on Any.
  result = ibv_req_notify_cq(setup.remote.cq, kNotifyAny);
  ASSERT_FALSE(IsReady(setup.remote.channel));
  DoRecv(setup, setup.remote.qp);
  DoSend(setup, setup.local.qp, /*solicited=*/false);
  ASSERT_NO_FATAL_FAILURE(CheckSend(setup.local.cq));
  ASSERT_NO_FATAL_FAILURE(CheckRecv(setup.remote.cq));
  ASSERT_FALSE(IsReady(setup.local.channel));
  ASSERT_TRUE(IsReady(setup.remote.channel));
  ASSERT_NO_FATAL_FAILURE(CheckEvent(setup.remote.channel, setup.remote.cq));
  ASSERT_NO_FATAL_FAILURE(ibv_ack_cq_events(setup.remote.cq, /*nevents=*/1));
}

}  // namespace rdma_unit_test
