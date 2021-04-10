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
#include "cases/status_matchers.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

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
    verbs_util::LocalVerbsAddress address;
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
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.address = ibv_.GetContextAddressInfo(setup.context);
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
    ibv_.SetUpSelfConnectedRcQp(setup.qp, setup.address);
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
              testing::AnyOf(IBV_WC_WR_FLUSH_ERR, IBV_WC_REM_ACCESS_ERR));
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
              testing::AnyOf(IBV_WC_WR_FLUSH_ERR, IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferTest, RegisterZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, buffer);
  EXPECT_NE(nullptr, mr);
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
    ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
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
  ASSERT_NE(nullptr, mr);
  absl::Span<uint8_t> send_buffer = mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = mr_buffer.subspan(0, 0);
  ibv_sge send_sge = verbs_util::CreateSge(send_buffer, setup.mr);
  ibv_sge recv_sge = verbs_util::CreateSge(recv_buffer, setup.mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, 1);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, 1);
  verbs_util::PostRecv(setup.qp, recv);
  verbs_util::PostSend(setup.qp, send);

  for (int i = 0; i < 2; ++i) {
    ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
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
  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> send_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = setup.buffer.subspan(0, 0);
  ibv_sge send_sge = verbs_util::CreateSge(send_buffer, setup.mr);
  ibv_sge recv_sge = verbs_util::CreateSge(recv_buffer, setup.mr);
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, 1);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, 1);
  verbs_util::PostRecv(setup.qp, recv);
  verbs_util::PostSend(setup.qp, send);

  for (int i = 0; i < 2; ++i) {
    ibv_wc completion = verbs_util::WaitForCompletion(setup.cq).value();
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
  ibv_wc_status result = verbs_util::ReadSync(
      setup.qp, local_buffer, setup.mr, remote_buffer.data(), setup.mr->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_F(BufferTest, BasicReadZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  ibv_wc_status result = verbs_util::ReadSync(
      setup.qp, local_buffer, setup.mr, remote_buffer.data(), setup.mr->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_F(BufferTest, BasicWriteZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), setup.mr->rkey));
}

TEST_F(BufferTest, BasicWriteZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), setup.mr->rkey));
}

TEST_F(BufferTest, BasicReadZeroByteFromZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  CHECK(zerobyte_mr);
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  ibv_wc_status result =
      verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                           remote_buffer.data(), zerobyte_mr->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_F(BufferTest, BasicReadZeroByteOutsideZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  CHECK(zerobyte_mr);
  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  ibv_wc_status result =
      verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                           remote_buffer.data(), zerobyte_mr->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_F(BufferTest, BasicWriteZeroByteToZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  CHECK(zerobyte_mr);
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), zerobyte_mr->rkey));
}

TEST_F(BufferTest, BasicWriteZeroByteOutsideZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  CHECK(zerobyte_mr);
  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), zerobyte_mr->rkey));
}

TEST_F(BufferTest, ZeroByteReadInvalidRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  ibv_wc_status result =
      verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                           remote_buffer.data(), (setup.mr->rkey + 10) * 10);
  if (!Introspection().CorrectlyReportsInvalidRemoteKeyErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_F(BufferTest, ZeroByteWriteInvalidRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  ibv_wc_status result =
      verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                            remote_buffer.data(), (setup.mr->rkey + 10) * 10);

  if (Introspection().CorrectlyReportsMemoryRegionErrors() &&
      Introspection().CorrectlyReportsInvalidRemoteKeyErrors()) {
    EXPECT_EQ(IBV_WC_REM_ACCESS_ERR, result);
  } else {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  }
}

class BufferMWTest : public BufferTest,
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
  ibv_wc_status DoBind(const BasicSetup& setup, ibv_mw* mw,
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
INSTANTIATE_TEST_SUITE_P(BufferMWTestCase, BufferMWTest,
                         ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2));

TEST_P(BufferMWTest, ExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  ASSERT_LE(kExceedLength, kMrMemoryOffset);
  absl::Span<uint8_t> invalid_buffer = setup.buffer.subspan(
      kMrMemoryOffset - kExceedLength, kMrMemoryLength + kExceedLength);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  EXPECT_NE(mw, nullptr);
  const ibv_wc_status kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? IBV_WC_MW_BIND_ERR
                                                           : IBV_WC_SUCCESS;
  EXPECT_EQ(kExpected, DoBind(setup, mw, invalid_buffer));
}

TEST_P(BufferMWTest, ExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  ASSERT_LE(kExceedLength, kBufferMemoryPage * verbs_util::kPageSize -
                               kMrMemoryOffset - kMrMemoryLength);
  absl::Span<uint8_t> invalid_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kMrMemoryLength + kExceedLength);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  const ibv_wc_status kExpected =
      Introspection().CorrectlyReportsMemoryWindowErrors() ? IBV_WC_MW_BIND_ERR
                                                           : IBV_WC_SUCCESS;
  EXPECT_EQ(kExpected, DoBind(setup, mw, invalid_buffer));
}

TEST_P(BufferMWTest, ReadExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = verbs_util::kPageSize;
  DCHECK_LE(kExceedLength, kMwMemoryOffset);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR,
            verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                 remote_buffer.data(), mw->rkey));
}

TEST_P(BufferMWTest, ReadExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  uint64_t kExceedLength = 1;
  uint64_t kReadBufferLength = verbs_util::kPageSize;
  DCHECK_LE(kExceedLength, kMrMemoryLength - kMwMemoryOffset - kMwMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset + kMwMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_EQ(IBV_WC_REM_ACCESS_ERR,
            verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                 remote_buffer.data(), mw->rkey));
}

TEST_P(BufferMWTest, CreateZeroByteFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> buffer = setup.mr_buffer.subspan(0, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  EXPECT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, buffer));
}

TEST_P(BufferMWTest, CreateZeroByteRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> buffer =
      setup.buffer.subspan(kMrMemoryOffset + kMrMemoryLength, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  EXPECT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, buffer));
}

TEST_P(BufferMWTest, CreateZeroByteOutsideRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kDistance = 1;
  absl::Span<uint8_t> buffer =
      setup.buffer.subspan(kMrMemoryOffset + kMrMemoryLength + kDistance, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  EXPECT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, buffer));
}

TEST_P(BufferMWTest, CreateZeroByteOutsideFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr int kDistance = 1;
  absl::Span<uint8_t> buffer =
      setup.buffer.subspan(kMrMemoryOffset - kDistance, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  EXPECT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, buffer));
}

TEST_P(BufferMWTest, CreateZeroByteInZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, buffer);
  ASSERT_NE(nullptr, mr);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  EXPECT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, buffer.span()));
}

TEST_P(BufferMWTest, CreateZeroByteOutsideZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, mr_buffer);
  ASSERT_NE(nullptr, mr);
  absl::Span<uint8_t> mw_buffer = setup.buffer.subspan(kMrMemoryOffset - 1, 0);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  EXPECT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));
}

TEST_P(BufferMWTest, ReadZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  ibv_wc_status result = verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                              remote_buffer.data(), mw->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_P(BufferMWTest, ReadZeroByteOutsideMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  ibv_wc_status result = verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                              remote_buffer.data(), mw->rkey);

  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_P(BufferMWTest, WriteZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), mw->rkey));
}

TEST_P(BufferMWTest, WriteZeroByteOutsideMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * verbs_util::kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), mw->rkey));
}

TEST_P(BufferMWTest, MwReadZeroByteFromZeroByteMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  ibv_wc_status result = verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                              remote_buffer.data(), mw->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_P(BufferMWTest, ReadZeroByteOutsideZeroByteMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  ibv_wc_status result = verbs_util::ReadSync(setup.qp, local_buffer, setup.mr,
                                              remote_buffer.data(), mw->rkey);
  if (Introspection().CorrectlyReportsMemoryRegionErrors()) {
    EXPECT_EQ(IBV_WC_SUCCESS, result);
  } else {
    EXPECT_EQ(IBV_WC_LOC_QP_OP_ERR, result);
  }
}

TEST_P(BufferMWTest, WriteZeroByteToZeroByteMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), mw->rkey));
}

TEST_P(BufferMWTest, WriteZeroByteOutsideZeroByteMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_NE(mw, nullptr);
  constexpr uint64_t kMwMemoryOffset = verbs_util::kPageSize;
  constexpr uint64_t kMwMemoryLength = 0;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_EQ(IBV_WC_SUCCESS, DoBind(setup, mw, mw_buffer));

  ASSERT_LT(0, kMrMemoryOffset);
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_EQ(IBV_WC_SUCCESS,
            verbs_util::WriteSync(setup.qp, local_buffer, setup.mr,
                                  remote_buffer.data(), mw->rkey));
}

}  // namespace rdma_unit_test
