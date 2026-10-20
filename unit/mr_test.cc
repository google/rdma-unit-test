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
#include <sys/mman.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <ios>
#include <thread>  // NOLINT
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/notification.h"
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

using ::testing::IsNull;
using ::testing::NotNull;

class MrTest : public RdmaVerbsFixture {
 public:
  static constexpr int kBufferMemoryPages = 4;

 protected:
  struct BasicSetup {
    RdmaMemBlock buffer;
    ibv_context* context;
    ibv_pd* pd;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    setup.buffer = ibv_.AllocBuffer(kBufferMemoryPages);
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    return setup;
  }
};

TEST_F(MrTest, RegisterMemory) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  EXPECT_EQ(mr->pd, setup.pd);
  EXPECT_EQ(mr->addr, setup.buffer.data());
  EXPECT_EQ(mr->length, setup.buffer.size());
  EXPECT_EQ(ibv_.DeregMr(mr), 0);
}

TEST_F(MrTest, DeregInvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  EXPECT_THAT(mr, NotNull());
  HandleGarble garble(mr->handle);
  EXPECT_EQ(ibv_dereg_mr(mr), ENOENT);
  EXPECT_EQ(errno, ENOENT);
}

TEST_F(MrTest, RemoteWriteWithoutLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_.RegMr(setup.pd, setup.buffer,
                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ),
              IsNull());
}

TEST_F(MrTest, RemoteAtomicWithoutLocalWrite) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_.RegMr(setup.pd, setup.buffer,
                         IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC),
              IsNull());
}

// Test using ibv_rereg_mr to associate the MR with another PD.
TEST_F(MrTest, ReregMrChangePd) {
  if (!Introspection().SupportsReRegMr()) {
    GTEST_SKIP() << "Nic does not support rereg_mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());

  ibv_pd* pd = ibv_.AllocPd(setup.context);
  ASSERT_EQ(ibv_.ReregMr(mr, IBV_REREG_MR_CHANGE_PD, pd, nullptr, 0), 0);
  EXPECT_EQ(mr->pd, pd);
}

// Change the MR's permission to a invalid one (remote write without local
// write)
TEST_F(MrTest, ReregMrInvalidPermission) {
  if (!Introspection().SupportsReRegMr()) {
    GTEST_SKIP() << "Nic does not support rereg_mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());

  ASSERT_EQ(ibv_.ReregMr(mr, IBV_REREG_MR_CHANGE_ACCESS, nullptr, nullptr,
                         IBV_ACCESS_REMOTE_WRITE),
            IBV_REREG_MR_ERR_CMD);
}

// Invalid buffer: valid address but has a length of 0.
TEST_F(MrTest, ReregMrInvalidBuffer) {
  if (!Introspection().SupportsReRegMr()) {
    GTEST_SKIP() << "Nic does not support rereg_mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());

  RdmaMemBlock subblock = setup.buffer.subblock(0, 0);
  ASSERT_EQ(
      ibv_.ReregMr(mr, IBV_REREG_MR_CHANGE_TRANSLATION, nullptr, &subblock, 0),
      IBV_REREG_MR_ERR_INPUT);
}

// TODO(author1): Threaded rkey user (IBV_WC_REM_ACCESS_ERR)

class MrLoopbackTest : public LoopbackFixture {
 protected:
  absl::StatusOr<std::pair<Client, Client>> CreateConnectedClientsPair() {
    ASSIGN_OR_RETURN(Client local, CreateClient(IBV_QPT_RC));
    ASSIGN_OR_RETURN(Client remote, CreateClient(IBV_QPT_RC));
    RETURN_IF_ERROR(
        ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, local.port_attr));
    return std::make_pair(local, remote);
  }

  void StressDereg(const Client& local, const ibv_send_wr& wqe,
                   std::function<void()> dereg,
                   std::vector<uint64_t>* results) {
    // Submit in batches of 10.
    static constexpr int kBatchSize = 10;
    uint32_t wr_id = 1;
    std::vector<ibv_send_wr> wqes(kBatchSize, wqe);
    for (size_t i = 0; i < wqes.size() - 1; ++i) {
      wqes[i].next = &wqes[i + 1];
    }
    for (auto& wqe : wqes) {
      wqe.wr_id = wr_id++;
    }

    *results = std::vector<uint64_t>(IBV_WC_GENERAL_ERR + 1, 0);
    absl::Notification cancel_notification;
    absl::Notification running_notification;
    std::thread another_thread([&local, &wqes, &cancel_notification,
                                &running_notification, results]() {
      // Set target outstanding to the default CQ size.
      constexpr int kTargetOutstanding = verbs_util::kDefaultMaxWr;
      int outstanding = 0;
      int total_results = 0;
      bool done = false;
      while (!done || outstanding > 0) {
        // Submit work.
        if (!done) {
          if (outstanding + wqes.size() < kTargetOutstanding) {
            done = cancel_notification.HasBeenNotified();
            ibv_send_wr* bad_wr = nullptr;
            EXPECT_EQ(ibv_post_send(local.qp, wqes.data(), &bad_wr), 0);
            outstanding += wqes.size();
            for (auto& wqe : wqes) {
              wqe.wr_id += kBatchSize;
            }
          }
        }
        // Prioritize reaping completions by pulling 50 per batch vs. the
        // submission batch of 10.
        constexpr int kCompletions = 50;
        ibv_wc completions[kCompletions];
        int count = ibv_poll_cq(local.cq, kCompletions, completions);
        for (int i = 0; i < count; ++i) {
          uint64_t& count = (*results)[completions[i].status];
          ++count;
        }
        total_results += count;
        // Wait for a good number before triggering.
        if (total_results > 2 * kTargetOutstanding &&
            !running_notification.HasBeenNotified()) {
          running_notification.Notify();
        }
        outstanding -= count;
      }
    });
    running_notification.WaitForNotification();
    LOG(INFO) << "Removing memory.";
    dereg();
    cancel_notification.Notify();
    another_thread.join();
  }
};

TEST_F(MrLoopbackTest, OutstandingRecv) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sg = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sg, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ASSERT_EQ(ibv_.DeregMr(remote.mr), 0);

  ibv_wc completions[10];
  int count = ibv_poll_cq(local.cq, sizeof(completions), completions);
  EXPECT_EQ(count, 0);
}

TEST_F(MrLoopbackTest, ReregMrChangeAccess) {
  if (!Introspection().SupportsReRegMr()) {
    GTEST_SKIP() << "Nic does not support rereg_mr.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ASSERT_EQ(
      ibv_.ReregMr(remote.mr, IBV_REREG_MR_CHANGE_ACCESS, nullptr, nullptr,
                   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE),
      0);

  // Attempt to do a remote_read to the remote side.
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/0, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  enum ibv_wc_status expected = Introspection().GeneratesRetryExcOnConnTimeout()
                                    ? IBV_WC_RETRY_EXC_ERR
                                    : IBV_WC_REM_ACCESS_ERR;
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  // The remote_read request should fail since the remote side does not have
  // permission for remote_read.
  EXPECT_EQ(completion.status, expected);
  EXPECT_EQ(completion.wr_id, 0);
}

TEST_F(MrLoopbackTest, ReregMrChangeAddress) {
  if (!Introspection().SupportsReRegMr()) {
    GTEST_SKIP() << "Nic does not support rereg_mr.";
  }
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());

  RdmaMemBlock new_buffer = ibv_.AllocBuffer(MrTest::kBufferMemoryPages);
  RdmaMemBlock new_local_buffer = ibv_.AllocBuffer(MrTest::kBufferMemoryPages);

  ASSERT_EQ(ibv_.ReregMr(remote.mr, IBV_REREG_MR_CHANGE_TRANSLATION, nullptr,
                         &new_buffer, 0),
            0);

  ASSERT_EQ(ibv_.ReregMr(local.mr, IBV_REREG_MR_CHANGE_TRANSLATION, nullptr,
                         &new_local_buffer, 0),
            0);

  constexpr uint64_t kTestData = 0xDDDDDDDDDDDDDDDD;
  *reinterpret_cast<uint64_t*>(new_local_buffer.data()) = kTestData;

  // 1. Test Remote Write
  ibv_sge sg_write = verbs_util::CreateSge(
      new_local_buffer.span().subspan(0, sizeof(kTestData)), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg_write, /*num_sge=*/1, new_buffer.data(),
      remote.mr->rkey);
  verbs_util::PostSend(local.qp, write);

  ASSERT_OK_AND_ASSIGN(ibv_wc write_completion,
                       verbs_util::WaitForCompletion(local.cq));
  ASSERT_EQ(write_completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(*reinterpret_cast<uint64_t*>(new_buffer.data()), kTestData);

  // 2. Test Remote Read
  ibv_sge sg_read = verbs_util::CreateSge(
      new_local_buffer.span().subspan(sizeof(kTestData), sizeof(kTestData)),
      local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/2, &sg_read, /*num_sge=*/1, new_buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc read_completion,
                       verbs_util::WaitForCompletion(local.cq));
  ASSERT_EQ(read_completion.status, IBV_WC_SUCCESS);
  ASSERT_EQ(
      *reinterpret_cast<uint64_t*>(new_local_buffer.data() + sizeof(kTestData)),
      kTestData);
}

TEST_F(MrLoopbackTest, OutstandingRead) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);

  std::vector<uint64_t> results;
  auto deregister = [this, &local = local, &remote = remote]() {
    EXPECT_EQ(ibv_.DeregMr(local.mr), 0);
    std::atomic<uint64_t>* data =
        reinterpret_cast<std::atomic<uint64_t>*>(remote.buffer.data());
    // Store new secret data into deregistered buffer.
    data->store(0xaabbccdd11223344);
  };

  uint64_t* remote_data = reinterpret_cast<uint64_t*>(remote.buffer.data());
  *remote_data = 0x55667788aabbccdd;
  StressDereg(local, read, deregister, &results);

  // Make sure we weren't able to pick up the secret data.
  uint64_t* local_data = reinterpret_cast<uint64_t*>(local.buffer.data());
  EXPECT_NE(local_data[0], 0xaabbccdd11223344);
  // It also shouldn't be corrupted.
  EXPECT_EQ(local_data[0], 0x55667788aabbccdd);

  // TODO(author1): Update to expect IBV_WC_WR_FLUSH_ERR when QP
  // cancellation is implemented.
  EXPECT_GT(results[IBV_WC_SUCCESS], 0);
  EXPECT_GT(results[IBV_WC_LOC_PROT_ERR], 0);
}

TEST_F(MrLoopbackTest, LargeMrWritesLandInCorrectBlock) {
  constexpr size_t kHalfMiB = 512ull << 10;
  constexpr size_t kTwoMiB = 2ull << 20;
  constexpr size_t kTwoGiB = 2ull << 30;
  constexpr size_t kLength = 5ull << 29;  // 2.5 GiB (> 2 GiB is required)
  constexpr size_t kMarkerOffset = 64;    // never on a block boundary
  constexpr uint64_t kTag = 0xA5A5000000000000ull;
#ifndef MADV_COLLAPSE
  constexpr int kMadvCollapse = 25;
#else
  constexpr int kMadvCollapse = MADV_COLLAPSE;
#endif

  ASSERT_OK_AND_ASSIGN(Client local, CreateClient());
  ASSERT_OK_AND_ASSIGN(Client remote, CreateClient());
  ASSERT_OK(ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, local.port_attr));
  //  Allocate with slack, then carve out a 2 MiB-aligned window.
  RdmaMemBlock raw = ibv_.AllocAlignedBufferByBytes(kLength + kTwoMiB);
  ASSERT_THAT(raw.data(), NotNull());
  const uintptr_t base = reinterpret_cast<uintptr_t>(raw.data());
  const size_t off = (kTwoMiB - (base % kTwoMiB)) % kTwoMiB;
  RdmaMemBlock buf = raw.subblock(off, kLength);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf.data()) % kTwoMiB, 0u);
  memset(buf.data(), 0, buf.size());

  // Fault in a single 2 MiB THP, verify/collapse it with MADV_COLLAPSE, and
  // mremap its middle 1 MiB across the 2 GiB boundary.
  void* thp_raw = mmap(nullptr, 2 * kTwoMiB, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(thp_raw, MAP_FAILED);
  const uintptr_t thp_addr = reinterpret_cast<uintptr_t>(thp_raw);
  uint8_t* thp = reinterpret_cast<uint8_t*>(
      thp_addr + ((kTwoMiB - (thp_addr % kTwoMiB)) % kTwoMiB));
  madvise(thp, kTwoMiB, MADV_HUGEPAGE);
  memset(thp, 0, kTwoMiB);
  if (madvise(thp, kTwoMiB, kMadvCollapse) != 0) {
    LOG(INFO) << "MADV_COLLAPSE failed (" << strerror(errno)
              << "); continuing with MADV_HUGEPAGE mapping";
  }
  ASSERT_NE(
      mremap(thp + kHalfMiB, 2 * kHalfMiB, 2 * kHalfMiB,
             MREMAP_MAYMOVE | MREMAP_FIXED, buf.data() + kTwoGiB - kHalfMiB),
      MAP_FAILED)
      << strerror(errno);

  ibv_mr* mr = ibv_.RegMr(remote.pd, buf);
  ASSERT_EQ(munmap(thp_raw, 2 * kTwoMiB), 0);
  ASSERT_THAT(mr, NotNull());

  // RDMA-WRITE a unique marker into every 2 MiB block.
  const size_t nblocks = kLength / kTwoMiB;
  uint64_t* src = reinterpret_cast<uint64_t*>(local.buffer.data());
  ibv_sge sge = verbs_util::CreateSge(local.buffer.subspan(0, 8), local.mr);
  for (size_t k = 0; k < nblocks; ++k) {
    *src = kTag | k;
    ibv_send_wr wr = verbs_util::CreateWriteWr(
        /*wr_id=*/k, &sge, /*num_sge=*/1,
        buf.data() + k * kTwoMiB + kMarkerOffset, mr->rkey);
    verbs_util::PostSend(local.qp, wr);
    ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                         verbs_util::WaitForCompletion(local.cq));
    ASSERT_EQ(completion.status, IBV_WC_SUCCESS) << "block " << k;
  }

  size_t bad = 0;
  for (size_t k = 0; k < nblocks; ++k) {
    uint64_t got;
    memcpy(&got, buf.data() + k * kTwoMiB + kMarkerOffset, sizeof(got));
    if (got != (kTag | k)) {
      if (bad < 8) {
        ADD_FAILURE() << "block " << k << " (offset 0x" << std::hex
                      << k * kTwoMiB << ") holds 0x" << got
                      << ", expected marker of block " << std::dec << k;
      }
      ++bad;
    }
  }
  EXPECT_EQ(bad, 0u) << bad << "/" << nblocks << " blocks mismatched";
}

TEST_F(MrLoopbackTest, OutstandingWrite) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);

  std::vector<uint64_t> results;
  auto deregister = [this, &local = local]() {
    ASSERT_EQ(ibv_.DeregMr(local.mr), 0);
    std::atomic<uint64_t>* data =
        reinterpret_cast<std::atomic<uint64_t>*>(local.buffer.data());
    // Store new secret data into deregistered buffer.
    data->store(0xaabbccdd11223344);
  };

  uint64_t* local_data = reinterpret_cast<uint64_t*>(local.buffer.data());
  *local_data = 0x55667788aabbccdd;
  StressDereg(local, write, deregister, &results);

  // Make sure we weren't able to pick up the secret data.
  uint64_t* remote_data = reinterpret_cast<uint64_t*>(remote.buffer.data());
  EXPECT_NE(remote_data[0], 0xaabbccdd11223344);
  // It also shouldn't be corrupted.
  EXPECT_EQ(remote_data[0], 0x55667788aabbccdd);

  EXPECT_GT(results[IBV_WC_SUCCESS], 0);
  // Not checking for IBV_WC_LOC_PROT_ERR since all ops might have already sent
  // their data and be in flight. Test still functions to detect msan errors and
  // other crashes.
}

}  // namespace rdma_unit_test
