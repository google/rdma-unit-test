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

#include <stddef.h>
#include <string.h>

#include <cerrno>
#include <cstdint>
#include <string>
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

using ::testing::IsNull;
using ::testing::NotNull;

class CqExTest : public BasicFixture {
 protected:
  struct BasicSetup {
    ibv_context* context;
    ibv_pd* pd;
    ibv_comp_channel* channel;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.channel = ibv_.CreateChannel(setup.context);
    if (!setup.channel) {
      return absl::InternalError("Failed to create channel.");
    }
    return setup;
  }
};

TEST_F(CqExTest, Basic) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, ZeroCqe) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 0};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, IsNull());
}

TEST_F(CqExTest, MaxCqe) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe)};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, AboveMaxCqe) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe) + 1};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  EXPECT_THAT(cq, IsNull());
}

TEST_F(CqExTest, WithChannel) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10, .channel = setup.channel};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, ShareChannel) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10, .channel = setup.channel};
  ibv_cq_ex* cq1 = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq1, NotNull());
  ibv_cq_ex* cq2 = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq2, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq1)), 0);
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq2)), 0);
}

TEST_F(CqExTest, ZeroCompVector) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, LargeCompVector) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{.cqe = 10,
                              .comp_vector = static_cast<uint32_t>(
                                  setup.context->num_comp_vectors - 1)};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, NotNull());
  ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
}

TEST_F(CqExTest, AboveMaxCompVector) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq_init_attr_ex cq_attr{
      .cqe = 10,
      .comp_vector = static_cast<uint32_t>(setup.context->num_comp_vectors)};
  ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
  ASSERT_THAT(cq, IsNull());
}

class CqExOpTest : public BasicFixture {
 protected:
  static constexpr int kBufferSize = 16 * 1024 * 1024;  // 16 MB
  static constexpr uint8_t kSrcContent = 1;
  static constexpr uint8_t kDstContent = 0;

  struct QpInfo {
    ibv_qp* qp;
    uint64_t next_send_wr_id = 0;
    uint64_t next_recv_wr_id = 0;
    // Each Qp write/recv to a separate remote buffer and each op should write
    // to one distinct byte of memory. Therefore the size of the buffer
    // should be at least as large as the number of ops.
    absl::Span<uint8_t> dst_buffer;
  };

  struct WcInfo {
    uint64_t wr_id;
    ibv_wc_status status;
    uint32_t qp_num;
    uint64_t timestamp_hcaclock = 0;
    uint64_t timestamp_wallclock = 0;
  };

  struct BasicSetup {
    // Each index of src_memblock contains the index value.
    RdmaMemBlock src_memblock;
    // Initially initialized to 0. Each QP targets its wrs to write to the index
    // of dst_memblock which corresponds to the QP.
    RdmaMemBlock dst_memblock;
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
    ibv_mr* src_mr;
    ibv_mr* dst_mr;
  };

  enum class WorkType : uint8_t { kSend, kWrite };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.src_memblock = ibv_.AllocAlignedBufferByBytes(kBufferSize);
    std::fill_n(setup.src_memblock.data(), setup.src_memblock.size(),
                kSrcContent);
    setup.dst_memblock = ibv_.AllocAlignedBufferByBytes(kBufferSize);
    std::fill_n(setup.dst_memblock.data(), setup.dst_memblock.size(),
                kDstContent);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }

    setup.src_mr = ibv_.RegMr(setup.pd, setup.src_memblock);
    if (!setup.src_mr) {
      return absl::InternalError("Failed to register source mr.");
    }
    memset(setup.dst_memblock.data(), 0, setup.dst_memblock.size());
    setup.dst_mr = ibv_.RegMr(setup.pd, setup.dst_memblock);
    if (!setup.dst_mr) {
      return absl::InternalError("Failed to register destination mr.");
    }

    // Set the supported timestamp types.
    support_completion_timestamp_hcaclock_ =
        verbs_util::CheckExtendedCompletionHasCapability(
            setup.context, IBV_WC_EX_WITH_COMPLETION_TIMESTAMP);
    support_completion_timestamp_wallclock_ =
        verbs_util::CheckExtendedCompletionHasCapability(
            setup.context, IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK);
    return setup;
  }

  // Posts send WQE.
  int QueueSend(BasicSetup& setup, QpInfo& qp) {
    return QueueWork(setup, qp, WorkType::kSend);
  }

  // Posts write WQE.
  int QueueWrite(BasicSetup& setup, QpInfo& qp) {
    return QueueWork(setup, qp, WorkType::kWrite);
  }

  // Posts recv WQE.
  int QueueRecv(BasicSetup& setup, QpInfo& qp) {
    uint32_t wr_id = qp.next_recv_wr_id++;
    DCHECK_LT(wr_id, setup.dst_memblock.size());
    auto dst_buffer =
        qp.dst_buffer.subspan(wr_id, 1);  // use wr_id as index to the buffer.
    ibv_sge sge = verbs_util::CreateSge(dst_buffer, setup.dst_mr);
    ibv_recv_wr wqe = verbs_util::CreateRecvWr(wr_id, &sge, /*num_sge=*/1);
    ibv_recv_wr* bad_wr;
    return ibv_post_recv(qp.qp, &wqe, &bad_wr);
  }

  // Post a Send or Write WR to a QP. The WR uses a 1-byte buffer at byte 1,
  // then byte 2, and so on.
  int QueueWork(BasicSetup& setup, QpInfo& qp, WorkType work_type) {
    uint64_t wr_id = qp.next_send_wr_id++;
    CHECK_LT(wr_id, setup.src_memblock.size());
    auto src_buffer = setup.src_memblock.subspan(
        wr_id, 1);  // use wr_id as index to the buffer.
    ibv_sge sge = verbs_util::CreateSge(src_buffer, setup.src_mr);
    ibv_send_wr wqe;
    switch (work_type) {
      case WorkType::kSend: {
        wqe = verbs_util::CreateSendWr(wr_id, &sge, /*num_sge=*/1);
        break;
      }
      case WorkType::kWrite: {
        CHECK_LT(wr_id, setup.dst_memblock.size());
        auto dst_buffer = qp.dst_buffer.subspan(
            wr_id, 1);  // use wr_id as index to the buffer.
        wqe = verbs_util::CreateWriteWr(wr_id, &sge, /*num_sge=*/1,
                                        dst_buffer.data(), setup.dst_mr->rkey);
        break;
      }
    }
    ibv_send_wr* bad_wr;
    return ibv_post_send(qp.qp, &wqe, &bad_wr);
  }

  uint64_t GetWcFlags() {
    uint64_t wc_flags = IBV_WC_EX_WITH_QP_NUM;
    if (support_completion_timestamp_hcaclock_) {
      wc_flags |= IBV_WC_EX_WITH_COMPLETION_TIMESTAMP;
    }
    if (support_completion_timestamp_wallclock_) {
      wc_flags |= IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK;
    }
    return wc_flags;
  }

  WcInfo GetWcInfo(ibv_cq_ex* cq) {
    WcInfo wc{.wr_id = cq->wr_id,
              .status = cq->status,
              .qp_num = ibv_wc_read_qp_num(cq)};
    if (support_completion_timestamp_hcaclock_) {
      wc.timestamp_hcaclock = ibv_wc_read_completion_ts(cq);
    }
    if (support_completion_timestamp_wallclock_) {
      wc.timestamp_wallclock = ibv_wc_read_completion_wallclock_ns(cq);
    }
    return wc;
  }

  // Whether WCs support HCA clock timestamps or not.
  bool support_completion_timestamp_hcaclock_ = false;
  // Whether WCs support wallclock timestamps or not.
  bool support_completion_timestamp_wallclock_ = false;
};

TEST_F(CqExOpTest, BasicPollSendCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kMaxQpWr = 1;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  QpInfo qp;
  qp.qp = ibv_.CreateQp(setup.pd, ibv_cq_ex_to_cq(send_cq),
                        ibv_cq_ex_to_cq(recv_cq), nullptr, kMaxQpWr, kMaxQpWr,
                        IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(qp.qp, NotNull()) << "Failed to create qp - " << errno;
  qp.dst_buffer = setup.dst_memblock.subspan(kMaxQpWr, kMaxQpWr);
  ibv_.SetUpSelfConnectedRcQp(qp.qp, setup.port_gid);
  QueueWrite(setup, qp);

  // Wait for completion and verify timestamp.
  ASSERT_OK(verbs_util::WaitForPollingExtendedCompletion(send_cq));
  ASSERT_EQ(send_cq->status, IBV_WC_SUCCESS);
  if (support_completion_timestamp_hcaclock_) {
    EXPECT_GT(ibv_wc_read_completion_ts(send_cq), 0);
  }
  if (support_completion_timestamp_wallclock_) {
    EXPECT_GT(ibv_wc_read_completion_wallclock_ns(send_cq), 0);
  }
  ibv_end_poll(send_cq);
}

TEST_F(CqExOpTest, BasicPollRecvCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kMaxQpWr = 1;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  QpInfo qp;
  qp.qp = ibv_.CreateQp(setup.pd, ibv_cq_ex_to_cq(send_cq),
                        ibv_cq_ex_to_cq(recv_cq), nullptr, kMaxQpWr, kMaxQpWr,
                        IBV_QPT_RC, /*sig_all=*/0);
  ASSERT_THAT(qp.qp, NotNull()) << "Failed to create qp - " << errno;
  qp.dst_buffer = setup.dst_memblock.subspan(kMaxQpWr, kMaxQpWr);
  ibv_.SetUpSelfConnectedRcQp(qp.qp, setup.port_gid);
  QueueRecv(setup, qp);
  QueueSend(setup, qp);

  // Wait for completion and verify timestamp.
  ASSERT_OK(verbs_util::WaitForPollingExtendedCompletion(recv_cq));
  ASSERT_EQ(recv_cq->status, IBV_WC_SUCCESS);
  if (support_completion_timestamp_hcaclock_) {
    EXPECT_GT(ibv_wc_read_completion_ts(recv_cq), 0);
  }
  if (support_completion_timestamp_wallclock_) {
    EXPECT_GT(ibv_wc_read_completion_wallclock_ns(recv_cq), 0);
  }
  ibv_end_poll(recv_cq);
}

class CqExBatchOpTest : public CqExOpTest {
 protected:
  // Create |count| qps.
  std::vector<QpInfo> CreateTestQps(BasicSetup& setup, ibv_cq* send_cq,
                                    ibv_cq* recv_cq, size_t max_qp_wr,
                                    int count) {
    CHECK_LE(max_qp_wr * count, setup.dst_memblock.size())
        << "Not enough space on destination buffer for all QPs.";
    std::vector<QpInfo> qps;
    for (int i = 0; i < count; ++i) {
      QpInfo qp;
      qp.qp = ibv_.CreateQp(setup.pd, send_cq, recv_cq, nullptr, max_qp_wr,
                            max_qp_wr, IBV_QPT_RC,
                            /*sig_all=*/0);
      CHECK(qp.qp) << "Failed to create qp - " << errno;
      ibv_.SetUpSelfConnectedRcQp(qp.qp, setup.port_gid);
      qp.dst_buffer = setup.dst_memblock.subspan(i * max_qp_wr, max_qp_wr);
      qps.push_back(qp);
    }
    return qps;
  }

  void WaitForAndVerifyCompletions(ibv_cq_ex* cq, int count) {
    ASSERT_OK(verbs_util::WaitForPollingExtendedCompletion(cq));
    WcInfo wc = GetWcInfo(cq);
    ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
    if (support_completion_timestamp_hcaclock_) {
      EXPECT_GT(wc.timestamp_hcaclock, 0);
    }
    if (support_completion_timestamp_wallclock_) {
      EXPECT_GT(wc.timestamp_wallclock, 0);
    }

    // Maps qp_num to the next wr_id.
    absl::flat_hash_map<uint32_t, uint64_t> expected_wr_id;
    expected_wr_id.insert({wc.qp_num, wc.wr_id + 1});
    uint64_t last_timestamp_hcaclock = 0;
    uint64_t last_timestamp_wallclock = 0;
    for (int i = 1; i < count; ++i) {
      ASSERT_OK(verbs_util::WaitForNextExtendedCompletion(cq));
      WcInfo wc = GetWcInfo(cq);
      ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
      auto iter = expected_wr_id.find(wc.qp_num);
      if (iter == expected_wr_id.end()) {
        expected_wr_id.insert({wc.qp_num, wc.wr_id + 1});
      } else {
        ASSERT_EQ(wc.wr_id, iter->second++);
      }
      EXPECT_GE(wc.timestamp_hcaclock, last_timestamp_hcaclock);
      EXPECT_GE(wc.timestamp_wallclock, last_timestamp_wallclock);
      last_timestamp_hcaclock = wc.timestamp_hcaclock;
      last_timestamp_wallclock = wc.timestamp_wallclock;
    }
    ibv_end_poll(cq);
  }
};

TEST_F(CqExBatchOpTest, PollSendCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kCqSize = 60;
  static constexpr int kQpCount = 20;
  static constexpr int kWritesPerQp = kCqSize / kQpCount;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  std::vector<QpInfo> qps =
      CreateTestQps(setup, ibv_cq_ex_to_cq(send_cq), ibv_cq_ex_to_cq(recv_cq),
                    kWritesPerQp, kQpCount);

  for (auto& qp : qps) {
    for (int i = 0; i < kWritesPerQp; ++i) {
      QueueWrite(setup, qp);
    }
  }
  WaitForAndVerifyCompletions(send_cq, kWritesPerQp * kQpCount);
}

TEST_F(CqExBatchOpTest, PollRecvCq) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  static constexpr int kCqSize = 60;
  static constexpr int kQpCount = 20;
  static constexpr int kSendsPerQp = kCqSize / kQpCount;
  ibv_cq_init_attr_ex cq_attr = {
      .cqe = static_cast<uint32_t>(Introspection().device_attr().max_cqe),
      .wc_flags = GetWcFlags()};
  ibv_cq_ex* send_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  ibv_cq_ex* recv_cq = ibv_.CreateCqEx(setup.context, cq_attr);
  std::vector<QpInfo> qps =
      CreateTestQps(setup, ibv_cq_ex_to_cq(send_cq), ibv_cq_ex_to_cq(recv_cq),
                    kSendsPerQp, kQpCount);

  for (auto& qp : qps) {
    for (int i = 0; i < kSendsPerQp; ++i) {
      QueueRecv(setup, qp);
    }
  }
  for (auto& qp : qps) {
    for (int i = 0; i < kSendsPerQp; ++i) {
      QueueSend(setup, qp);
    }
  }
  WaitForAndVerifyCompletions(recv_cq, kSendsPerQp);
}

}  // namespace rdma_unit_test
