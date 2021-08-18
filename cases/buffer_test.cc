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
#include <sched.h>
#include <stdlib.h>
#include <string.h>

#include <cstdint>
#include <optional>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/status_matchers.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::NotNull;

// For testing the functions of verbs on different buffer memory layouts, e.g.
// MW memory out of bounds, 0 length buffers for MW/MRs, etc.
class BufferTest : public BasicFixture {
 public:
  static constexpr int kBufferMemoryPage = 6;
  static constexpr uint64_t kMrMemoryOffset = verbs_util::kPageSize;
  static constexpr uint64_t kMrMemoryLength = 4 * verbs_util::kPageSize;
  static_assert(kBufferMemoryPage * verbs_util::kPageSize >
                    kMrMemoryOffset + kMrMemoryLength,
                "MR buffer exceeds the memory limits. Consider increasing "
                "kBufferMemoryPage.");

  BufferTest() = default;
  ~BufferTest() override = default;

 protected:
  // Struct containing some basic objects shared by all buffer tests.
  struct BasicSetup {
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
    ibv_mr* mr;
    ibv_cq* cq;
    ibv_qp* qp;
    RdmaMemBlock buffer;
    RdmaMemBlock mr_buffer;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPage);
    setup.mr_buffer = setup.buffer.subblock(kMrMemoryOffset, kMrMemoryLength);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.mr = ibv_.RegMr(setup.pd, setup.mr_buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.qp = ibv_.CreateQp(setup.pd, setup.cq);
    if (!setup.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.port_gid);
    return setup;
  }
};

TEST_F(BufferTest, ReadMrExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = verbs_util::kPageSize;
  ASSERT_LE(kExceedLength, kMrMemoryOffset);
  absl::Span<uint8_t> remote_buffer =
      setup.buffer.subspan(kMrMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              AnyOf(IBV_WC_WR_FLUSH_ERR, IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferTest, ReadMrExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = verbs_util::kPageSize;
  ASSERT_LE(kExceedLength, kBufferMemoryPage * verbs_util::kPageSize -
                               kMrMemoryOffset - kMrMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(
      kMrMemoryOffset + kMrMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              AnyOf(IBV_WC_WR_FLUSH_ERR, IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferTest, RegisterZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, buffer);
  EXPECT_THAT(mr, NotNull());
}

TEST_F(BufferTest, SendZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> send_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = setup.mr_buffer.subspan(0, 0);
  ibv_sge send_sge = verbs_util::CreateSge(send_buffer, setup.mr);
  ibv_sge recv_sge = verbs_util::CreateSge(recv_buffer, setup.mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, 1);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, 1);
  verbs_util::PostRecv(setup.qp, recv);
  verbs_util::PostSend(setup.qp, send);

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.cq));
    if (completion.wr_id == 0) {  // Receive
      EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
      EXPECT_EQ(completion.opcode, IBV_WC_RECV);
    } else if (completion.wr_id == 1) {  // Send
      EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
      EXPECT_EQ(completion.opcode, IBV_WC_SEND);
    } else {
      EXPECT_TRUE(false) << "Unregonized wr_id " << completion.wr_id;
    }
  }
}

TEST_F(BufferTest, SendZeroByteFromZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, mr_buffer);
  ASSERT_THAT(mr, NotNull());
  absl::Span<uint8_t> send_buffer = mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = mr_buffer.subspan(0, 0);
  ibv_sge send_sge = verbs_util::CreateSge(send_buffer, setup.mr);
  ibv_sge recv_sge = verbs_util::CreateSge(recv_buffer, setup.mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, 1);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, 1);
  verbs_util::PostRecv(setup.qp, recv);
  verbs_util::PostSend(setup.qp, send);

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.cq));
    if (completion.wr_id == 0) {  // Receive
      EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
      EXPECT_EQ(completion.opcode, IBV_WC_RECV);
    } else if (completion.wr_id == 1) {  // Send
      EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
      EXPECT_EQ(completion.opcode, IBV_WC_SEND);
    } else {
      EXPECT_TRUE(false);
    }
  }
}

TEST_F(BufferTest, SendZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> send_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = setup.buffer.subspan(0, 0);
  ibv_sge send_sge = verbs_util::CreateSge(send_buffer, setup.mr);
  ibv_sge recv_sge = verbs_util::CreateSge(recv_buffer, setup.mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, 1);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, 1);
  verbs_util::PostRecv(setup.qp, recv);
  verbs_util::PostSend(setup.qp, send);

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(setup.cq));
    if (completion.wr_id == 0) {  // Receive
      EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
      EXPECT_EQ(completion.opcode, IBV_WC_RECV);
    } else if (completion.wr_id == 1) {  // Send
      EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
      EXPECT_EQ(completion.opcode, IBV_WC_SEND);
    } else {
      EXPECT_TRUE(false);
    }
  }
}

TEST_F(BufferTest, BasicReadZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicReadZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicWriteZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicWriteZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicReadZeroByteFromZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicReadZeroByteOutsideZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicWriteZeroByteToZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferTest, BasicWriteZeroByteOutsideZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferTest, ZeroByteReadInvalidRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(
      verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                           remote_buffer.data(), (setup.mr->rkey + 10) * 10),
      IsOkAndHolds(
          Introspection().ShouldDeviateForCurrentTest("no error")
              ? IBV_WC_SUCCESS
              : (Introspection().ShouldDeviateForCurrentTest("local error")
                     ? IBV_WC_LOC_QP_OP_ERR
                     : IBV_WC_REM_ACCESS_ERR)));
}

TEST_F(BufferTest, ZeroByteWriteInvalidRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(
      verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                            remote_buffer.data(), (setup.mr->rkey + 10) * 10),
      IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                       ? IBV_WC_SUCCESS
                       : IBV_WC_REM_ACCESS_ERR));
}

class BufferMwTest : public BufferTest,
                     public ::testing::WithParamInterface<ibv_mw_type> {
 public:
  void SetUp() override {
    if (GetParam() == IBV_MW_TYPE_1 && !Introspection().SupportsType1()) {
      GTEST_SKIP() << "Nic does not support Type1 MW";
    }
    if (GetParam() == IBV_MW_TYPE_2 && !Introspection().SupportsType2()) {
      GTEST_SKIP() << "Nic does not support Type2 MW";
    }
  }

 protected:
  absl::StatusOr<ibv_wc_status> DoBind(const BasicSetup& setup, ibv_mw* mw,
                                       absl::Span<uint8_t> buffer) {
    static uint32_t rkey = 17;
    // Do Bind.
    switch (GetParam()) {
      case IBV_MW_TYPE_1: {
        return verbs_util::BindType1MwSync(setup.qp, mw, buffer, setup.mr);
      } break;
      case IBV_MW_TYPE_2: {
        return verbs_util::BindType2MwSync(setup.qp, mw, buffer, ++rkey,
                                           setup.mr);
      } break;
      default:
        CHECK(false) << "Unknown param.";
    }
  }
};

INSTANTIATE_TEST_SUITE_P(BufferMwTestCase, BufferMwTest,
                         ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2));

TEST_P(BufferMwTest, ExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  ASSERT_LE(kExceedLength, kMrMemoryOffset);
  absl::Span<uint8_t> invalid_buffer = setup.buffer.subspan(
      kMrMemoryOffset - kExceedLength, kMrMemoryLength + kExceedLength);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  EXPECT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, invalid_buffer),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_SUCCESS
                               : IBV_WC_MW_BIND_ERR));
}

TEST_P(BufferMwTest, ExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  ASSERT_LE(kExceedLength, kBufferMemoryPage * verbs_util::kPageSize -
                               kMrMemoryOffset - kMrMemoryLength);
  absl::Span<uint8_t> invalid_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kMrMemoryLength + kExceedLength);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, invalid_buffer),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_SUCCESS
                               : IBV_WC_MW_BIND_ERR));
}

TEST_P(BufferMwTest, ReadExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = verbs_util::kPageSize;
  ASSERT_LE(kExceedLength, kMwMemoryOffset);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_P(BufferMwTest, ReadExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  uint64_t kExceedLength = 1;
  uint64_t kReadBufferLength = verbs_util::kPageSize;
  ASSERT_LE(kExceedLength, kMrMemoryLength - kMwMemoryOffset - kMwMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset + kMwMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_P(BufferMwTest, CreateZeroByteFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> buffer = setup.mr_buffer.subspan(0, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, buffer), IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, CreateZeroByteRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> buffer =
      setup.buffer.subspan(kMrMemoryOffset + kMrMemoryLength, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, buffer), IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, CreateZeroByteOutsideRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kDistance = 1;
  absl::Span<uint8_t> buffer =
      setup.buffer.subspan(kMrMemoryOffset + kMrMemoryLength + kDistance, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, buffer), IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, CreateZeroByteOutsideFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kDistance = 1;
  absl::Span<uint8_t> buffer =
      setup.buffer.subspan(kMrMemoryOffset - kDistance, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, buffer), IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, CreateZeroByteInZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, buffer);
  ASSERT_THAT(mr, NotNull());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, buffer.span()), IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, CreateZeroByteOutsideZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, mr_buffer);
  ASSERT_THAT(mr, NotNull());
  absl::Span<uint8_t> mw_buffer = setup.buffer.subspan(kMrMemoryOffset - 1, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, ReadZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, ReadZeroByteOutsideMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, WriteZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, WriteZeroByteOutsideMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, MwReadZeroByteFromZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, ReadZeroByteOutsideZeroByteMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(Introspection().ShouldDeviateForCurrentTest()
                               ? IBV_WC_LOC_QP_OP_ERR
                               : IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, WriteZeroByteToZeroByteMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, WriteZeroByteOutsideZeroByteMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

}  // namespace rdma_unit_test
