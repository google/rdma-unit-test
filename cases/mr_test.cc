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
#include <string.h>
#include <sys/mman.h>

#include <exception>
#include <functional>
#include <thread>  // NOLINT
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/notification.h"
#include "absl/types/span.h"

#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

using ::testing::AnyOf;
using ::testing::IsNull;
using ::testing::NotNull;

class MrTest : public BasicFixture {
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
  EXPECT_THAT(mr, NotNull());
}

TEST_F(MrTest, ThreadedReg) {
  static constexpr int kThreadCount = 5;
  static constexpr int kMrsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());

  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  std::array<std::array<ibv_mr*, kMrsPerThread>, kThreadCount> mrs;
  mrs = {{{nullptr}}};
  auto reg_mrs = [this, &pd, &buffer, &mrs](int thread_id) {
    // No MRs can share the same position in the array, so no need for thread
    // synchronization.
    for (int i = 0; i < kMrsPerThread; ++i) {
      mrs[thread_id][i] = ibv_.RegMr(pd, buffer);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(reg_mrs, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_mrs : mrs) {
    for (const auto& mr : thread_mrs) {
      EXPECT_THAT(mr, NotNull());
    }
  }
}

TEST_F(MrTest, ThreadedRegAndDereg) {
  static constexpr int kThreadCount = 5;
  static constexpr int kMrsPerThread = 50;

  ASSERT_OK_AND_ASSIGN(ibv_context * context, ibv_.OpenDevice());

  ibv_pd* pd = ibv_.AllocPd(context);
  ASSERT_THAT(pd, NotNull());
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  // Initialize to 1 since we are expecting the values to be 0 after
  // deregistering MRs.
  std::array<std::array<int, kMrsPerThread>, kThreadCount> dereg_results;
  std::fill(dereg_results.front().begin(), dereg_results.back().end(), 1);
  auto reg_dereg_mrs = [this, &pd, &buffer, &dereg_results](int thread_id) {
    for (int i = 0; i < kMrsPerThread; ++i) {
      ibv_mr* mr = ibv_.RegMr(pd, buffer);
      ASSERT_THAT(mr, NotNull());
      dereg_results[thread_id][i] = ibv_.DeregMr(mr);
    }
  };

  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    threads.push_back(std::thread(reg_dereg_mrs, thread_id));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& thread_results : dereg_results) {
    for (const auto& dereg_result : thread_results) {
      EXPECT_EQ(dereg_result, 0);
    }
  }
}


// Check with a pointer in the correct range.
TEST_F(MrTest, DeregInvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  ASSERT_THAT(cq, NotNull());
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_mr) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_mr* fake_mr = reinterpret_cast<ibv_mr*>(cq);
  fake_mr->context = setup.context;
  fake_mr->handle = original.handle;
  EXPECT_THAT(ibv_dereg_mr(fake_mr), AnyOf(EINVAL, ENOENT));
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

// Cannot have remote write without local write.
TEST_F(MrTest, InvalidPermissions) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_.RegMr(setup.pd, setup.buffer,
                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ),
              IsNull());
}

// Cannot have remote atomic without local write.
TEST_F(MrTest, InvalidPermissions2) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  EXPECT_THAT(ibv_.RegMr(setup.pd, setup.buffer,
                         IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC),
              IsNull());
}

TEST_F(MrTest, DestroyPdWithOutstandingMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ASSERT_THAT(ibv_.RegMr(setup.pd, setup.buffer), NotNull());
  EXPECT_EQ(ibv_.DeallocPd(setup.pd), EBUSY);
}

// TODO(author1): Create Many/Max

// Test using ibv_rereg_mr to associate the MR with another buffer.
TEST_F(MrTest, ReregMrChangeAddress) {
  if (!Introspection().SupportsReRegMr()) {
    GTEST_SKIP() << "Nic does not support rereg_mr.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(mr, NotNull());
  RdmaMemBlock buffer = ibv_.AllocBuffer(kBufferMemoryPages);
  ASSERT_EQ(ibv_rereg_mr(mr, IBV_REREG_MR_CHANGE_TRANSLATION, nullptr,
                         buffer.data(), buffer.size(), 0),
            0);
  EXPECT_EQ(mr->addr, buffer.data());
  EXPECT_EQ(mr->length, buffer.size());
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
  ASSERT_EQ(ibv_rereg_mr(mr, IBV_REREG_MR_CHANGE_PD, pd, nullptr, 0, 0), 0);
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

  ASSERT_EQ(ibv_rereg_mr(mr, IBV_REREG_MR_CHANGE_ACCESS, nullptr, nullptr, 0,
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

  ASSERT_EQ(ibv_rereg_mr(mr, IBV_REREG_MR_CHANGE_TRANSLATION, nullptr,
                         setup.buffer.data(), 0, 0),
            IBV_REREG_MR_ERR_INPUT);
}

// TODO(author1): Threaded rkey user (IBV_WC_REM_ACCESS_ERR)

class MrLoopbackTest : public BasicFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr int kQueueSize = 200;
  static constexpr int kQKey = 200;

  struct Client {
    ibv_context* context = nullptr;
    verbs_util::PortGid port_gid;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    // RC qp.
    ibv_qp* qp = nullptr;
    // AH pointing to the other client.
    ibv_ah* other_ah = nullptr;
    ibv_mr* mr = nullptr;
    RdmaMemBlock buffer;
  };

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
    client.qp =
        ibv_.CreateQp(client.pd, client.cq, client.cq, nullptr, kQueueSize,
                      kQueueSize, IBV_QPT_RC, /*sig_all=*/0);
    if (!client.qp) {
      return absl::InternalError("Failed to create qp.");
    }
    client.mr = ibv_.RegMr(client.pd, client.buffer);
    if (!client.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    return client;
  }

  absl::StatusOr<std::pair<Client, Client>> CreateConnectedClientsPair() {
    ASSIGN_OR_RETURN(Client local, CreateClient(/*buf_content=*/'a'));
    ASSIGN_OR_RETURN(Client remote, CreateClient(/*buf_content=*/'b'));
    ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, local.port_gid);
    return std::make_pair(local, remote);
  }

  void StressDereg(const Client& local, const ibv_send_wr& wqe,
                   std::function<void()> dereg,
                   std::vector<uint64_t>* results) {
    // Submit in batches of 10.
    std::vector<ibv_send_wr> wqes(10, wqe);
    for (size_t i = 0; i < wqes.size() - 1; ++i) {
      wqes[i].next = &wqes[i + 1];
    }

    // Indicates that the client filled the outstanding queue.
    bool saturation = false;
    *results = std::vector<uint64_t>(IBV_WC_GENERAL_ERR + 1, 0);
    absl::Notification cancel_notification;
    absl::Notification running_notification;
    std::thread another_thread([&local, &wqes, &cancel_notification,
                                &running_notification, &saturation, results]() {
      // Must have enough outstanding to saturate the QP, but not too many
      // outstanding to overflow the default CQ size.
      constexpr int kTargetOutstanding = 100;
      int outstanding = 0;
      int total_results = 0;
      while (!cancel_notification.HasBeenNotified() || outstanding > 0) {
        // Submit work.
        if (!cancel_notification.HasBeenNotified()) {
          if (outstanding + wqes.size() < kTargetOutstanding) {
            ibv_send_wr* bad_wr = nullptr;
            EXPECT_EQ(ibv_post_send(local.qp, wqes.data(), &bad_wr), 0);
            outstanding += wqes.size();
          } else {
            saturation = true;
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
    EXPECT_TRUE(saturation);
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
      ibv_rereg_mr(remote.mr, IBV_REREG_MR_CHANGE_ACCESS, nullptr, nullptr, 0,
                   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE),
      0);

  // Attempt to do a remote_read to the remote side.
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/0, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);
  verbs_util::PostSend(local.qp, read);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  // The remote_read request should fail since the remote side does not have
  // permission for remote_read.
  EXPECT_EQ(completion.status, IBV_WC_REM_ACCESS_ERR);
  EXPECT_EQ(completion.wr_id, 0);
}

TEST_F(MrLoopbackTest, OutstandingRead) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);

  std::vector<uint64_t> results;
  auto deregister = [this, &local = local]() {
    EXPECT_EQ(ibv_.DeregMr(local.mr), 0);
  };
  StressDereg(local, read, deregister, &results);

  // TODO(author1): Update to expect IBV_WC_WR_FLUSH_ERR when QP
  // cancellation is implemented.
  EXPECT_GT(results[IBV_WC_SUCCESS], 0);
  EXPECT_GT(results[IBV_WC_LOC_PROT_ERR], 0);
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
  };
  StressDereg(local, write, deregister, &results);

  EXPECT_GT(results[IBV_WC_SUCCESS], 0);
  // Not checking for IBV_WC_LOC_PROT_ERR since all ops might have already sent
  // their data and be in flight. Test still functions to detect msan errors and
  // other crashes.
}

}  // namespace rdma_unit_test
