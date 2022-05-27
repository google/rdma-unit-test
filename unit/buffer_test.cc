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

#include <cstdint>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "public/introspection.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::NotNull;
using ::testing::Pair;

// For testing the functions of verbs on different buffer memory layouts, e.g.
// MW memory out of bounds, 0 length buffers for MW/MRs, etc.
class BufferTest : public RdmaVerbsFixture {
 public:
  // Memory layout
  //                  buffer
  // |----------------------------------------|
  // | MR offset |----------------------|
  //                MR (length)
  static constexpr int kBufferMemoryPage = 6;
  static constexpr uint64_t kMrMemoryOffset = kPageSize;
  static constexpr uint64_t kMrMemoryLength = 4 * kPageSize;
  static_assert(kBufferMemoryPage * kPageSize >
                    kMrMemoryOffset + kMrMemoryLength,
                "MR buffer exceeds the memory limits. Consider increasing "
                "kBufferMemoryPage.");

  BufferTest() = default;
  ~BufferTest() override = default;

 protected:
  // Struct containing some basic objects shared by all buffer tests.
  struct BasicSetup {
    ibv_context* context;
    PortAttribute port_attr;
    ibv_pd* pd;
    RdmaMemBlock buffer;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPage);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    return setup;
  }
};

TEST_F(BufferTest, RegisterZeroByteMr) {
  if (!Introspection().SupportsZeroLengthMr()) {
    GTEST_SKIP() << "NIC does not support zero length mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, buffer);
  EXPECT_THAT(mr, NotNull());
}

class BufferMrTest : public BufferTest {
 protected:
  struct BasicSetup {
    ibv_context* context;
    PortAttribute port_attr;
    ibv_pd* pd;
    ibv_mr* mr;
    ibv_cq* send_cq;
    ibv_cq* recv_cq;
    ibv_qp* send_qp;
    ibv_qp* recv_qp;
    RdmaMemBlock buffer;
    RdmaMemBlock mr_buffer;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPage);
    setup.mr_buffer = setup.buffer.subblock(kMrMemoryOffset, kMrMemoryLength);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.mr = ibv_.RegMr(setup.pd, setup.mr_buffer);
    if (!setup.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    setup.send_cq = ibv_.CreateCq(setup.context);
    if (!setup.send_cq) {
      return absl::InternalError("Failed to create send cq.");
    }
    setup.send_qp = ibv_.CreateQp(setup.pd, setup.send_cq);
    if (!setup.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    setup.recv_cq = ibv_.CreateCq(setup.context);
    if (!setup.recv_cq) {
      return absl::InternalError("Failed to create recv cq.");
    }
    setup.recv_qp = ibv_.CreateQp(setup.pd, setup.recv_cq);
    if (!setup.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    RETURN_IF_ERROR(
        ibv_.SetUpLoopbackRcQps(setup.send_qp, setup.recv_qp, setup.port_attr));
    return setup;
  }
};

TEST_F(BufferMrTest, SendZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> send_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(
      verbs_util::SendRecvSync(setup.send_qp, setup.recv_qp, send_buffer,
                               setup.mr, recv_buffer, setup.mr),
      IsOkAndHolds(Pair(IBV_WC_SUCCESS, IBV_WC_SUCCESS)));
}

TEST_F(BufferMrTest, SendZeroByteFromZeroByteMr) {
  if (!Introspection().SupportsZeroLengthMr()) {
    GTEST_SKIP() << "NIC does not support zero length mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* mr = ibv_.RegMr(setup.pd, mr_buffer);
  ASSERT_THAT(mr, NotNull());
  absl::Span<uint8_t> send_buffer = mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = mr_buffer.subspan(0, 0);
  ASSERT_THAT(
      verbs_util::SendRecvSync(setup.send_qp, setup.recv_qp, send_buffer,
                               setup.mr, recv_buffer, setup.mr),
      IsOkAndHolds(Pair(IBV_WC_SUCCESS, IBV_WC_SUCCESS)));
}

TEST_F(BufferMrTest, SendZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> send_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> recv_buffer = setup.buffer.subspan(0, 0);
  ASSERT_THAT(
      verbs_util::SendRecvSync(setup.send_qp, setup.recv_qp, send_buffer,
                               setup.mr, recv_buffer, setup.mr),
      IsOkAndHolds(Pair(IBV_WC_SUCCESS, IBV_WC_SUCCESS)));
}

TEST_F(BufferMrTest, ReadExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength, kMrMemoryOffset);
  absl::Span<uint8_t> remote_buffer =
      setup.buffer.subspan(kMrMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferMrTest, ReadExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength,
            kBufferMemoryPage * kPageSize - kMrMemoryOffset - kMrMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(
      kMrMemoryOffset + kMrMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferMrTest, ReadZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, ReadZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, ReadZeroByteFromZeroByteMr) {
  if (!Introspection().SupportsZeroLengthMr()) {
    GTEST_SKIP() << "NIC does not support zero length mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, ReadZeroByteOutsideZeroByteMr) {
  if (!Introspection().SupportsZeroLengthMr()) {
    GTEST_SKIP() << "NIC does not support zero length mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, ReadZeroByteInvalidRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  ASSERT_OK_AND_ASSIGN(
      ibv_wc_status result,
      verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                           remote_buffer.data(), (setup.mr->rkey + 10) * 10));
    EXPECT_THAT(result, AnyOf(IBV_WC_SUCCESS, IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferMrTest, WriteExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength, kMrMemoryOffset);
  absl::Span<uint8_t> remote_buffer =
      setup.buffer.subspan(kMrMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kReadBufferLength);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferMrTest, WriteExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength,
            kBufferMemoryPage * kPageSize - kMrMemoryOffset - kMrMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(
      kMrMemoryOffset + kMrMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kReadBufferLength);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(BufferMrTest, WriteZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, WriteZeroByteOutsideMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), setup.mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, WriteZeroByteToZeroByteMr) {
  if (!Introspection().SupportsZeroLengthMr()) {
    GTEST_SKIP() << "NIC does not support zero length mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  absl::Span<uint8_t> local_buffer = zerobyte_mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = zerobyte_mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, WriteZeroByteOutsideZeroByteMr) {
  if (!Introspection().SupportsZeroLengthMr()) {
    GTEST_SKIP() << "NIC does not support zero length mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  RdmaMemBlock zerobyte_mr_buffer = setup.buffer.subblock(kMrMemoryOffset, 0);
  ibv_mr* zerobyte_mr = ibv_.RegMr(setup.pd, zerobyte_mr_buffer);
  ASSERT_THAT(zerobyte_mr, NotNull());
  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), zerobyte_mr->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_F(BufferMrTest, WriteZeroByteInvalidRKey) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  ASSERT_OK_AND_ASSIGN(
      ibv_wc_status result,
      verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                            remote_buffer.data(), (setup.mr->rkey + 10) * 10));
    EXPECT_THAT(result, AnyOf(IBV_WC_SUCCESS, IBV_WC_REM_ACCESS_ERR));
}

class BufferMwTest : public BufferMrTest,
                     public ::testing::WithParamInterface<ibv_mw_type> {
 public:
  void SetUp() override {
    BasicFixture::SetUp();
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
        return verbs_util::BindType1MwSync(setup.recv_qp, mw, buffer, setup.mr);
      } break;
      case IBV_MW_TYPE_2: {
        return verbs_util::BindType2MwSync(setup.recv_qp, mw, buffer, ++rkey,
                                           setup.mr);
      } break;
      default:
        CHECK(false) << "Unknown param.";
    }
  }
};

INSTANTIATE_TEST_SUITE_P(BufferMwTestCase, BufferMwTest,
                         ::testing::Values(IBV_MW_TYPE_1, IBV_MW_TYPE_2));

TEST_P(BufferMwTest, BindExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  ASSERT_LE(kExceedLength, kMrMemoryOffset);
  absl::Span<uint8_t> invalid_buffer = setup.buffer.subspan(
      kMrMemoryOffset - kExceedLength, kMrMemoryLength + kExceedLength);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  EXPECT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, invalid_buffer),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_P(BufferMwTest, BindExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  constexpr uint64_t kExceedLength = 1;
  ASSERT_LE(kExceedLength,
            kBufferMemoryPage * kPageSize - kMrMemoryOffset - kMrMemoryLength);
  absl::Span<uint8_t> invalid_buffer =
      setup.buffer.subspan(kMrMemoryOffset, kMrMemoryLength + kExceedLength);
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  EXPECT_THAT(DoBind(setup, mw, invalid_buffer),
              IsOkAndHolds(IBV_WC_MW_BIND_ERR));
}

TEST_P(BufferMwTest, ReadExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength, kMwMemoryOffset);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_P(BufferMwTest, ReadExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  uint64_t kExceedLength = 1;
  uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength, kMrMemoryLength - kMwMemoryOffset - kMwMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset + kMwMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_P(BufferMwTest, ReadZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, ReadZeroByteOutsideMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::ReadSync(setup.send_qp, local_buffer, setup.mr,
                                   remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, WriteExceedFront) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  constexpr uint64_t kExceedLength = 1;
  constexpr uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength, kMwMemoryOffset);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset - kExceedLength, kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_P(BufferMwTest, WriteExceedRear) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  uint64_t kExceedLength = 1;
  uint64_t kReadBufferLength = kPageSize;
  ASSERT_LE(kExceedLength, kMrMemoryLength - kMwMemoryOffset - kMwMemoryLength);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(
      kMwMemoryOffset + kMwMemoryLength - kReadBufferLength + kExceedLength,
      kReadBufferLength);
  absl::Span<uint8_t> local_buffer =
      setup.mr_buffer.subspan(0, kReadBufferLength);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_REM_ACCESS_ERR));
}

TEST_P(BufferMwTest, WriteZeroByte) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  absl::Span<uint8_t> local_buffer = mw_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = mw_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

TEST_P(BufferMwTest, WriteZeroByteOutsideMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mw* mw = ibv_.AllocMw(setup.pd, GetParam());
  ASSERT_THAT(mw, NotNull());
  constexpr uint64_t kMwMemoryOffset = kPageSize;
  constexpr uint64_t kMwMemoryLength = 2 * kPageSize;
  absl::Span<uint8_t> mw_buffer =
      setup.mr_buffer.subspan(kMwMemoryOffset, kMwMemoryLength);
  ASSERT_THAT(DoBind(setup, mw, mw_buffer), IsOkAndHolds(IBV_WC_SUCCESS));

  ASSERT_GT(kMrMemoryOffset, 0);
  absl::Span<uint8_t> local_buffer = setup.mr_buffer.subspan(0, 0);
  absl::Span<uint8_t> remote_buffer = setup.mr_buffer.subspan(0, 0);
  EXPECT_THAT(verbs_util::WriteSync(setup.send_qp, local_buffer, setup.mr,
                                    remote_buffer.data(), mw->rkey),
              IsOkAndHolds(IBV_WC_SUCCESS));
}

}  // namespace rdma_unit_test
