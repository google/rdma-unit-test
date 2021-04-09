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
#include "cases/status_matchers.h"
#include "public/rdma-memblock.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

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
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
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
  EXPECT_NE(nullptr, mr);
}


// Check with a pointer in the correct range.
TEST_F(MrTest, DeregInvalidMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_cq* cq = ibv_.CreateCq(setup.context);
  ASSERT_NE(nullptr, cq);
  ibv_cq original;
  // Save original so we can restore for cleanup.
  memcpy(&original, cq, sizeof(original));
  static_assert(sizeof(ibv_mr) < sizeof(ibv_cq), "Unsafe cast below");
  ibv_mr* fake_mr = reinterpret_cast<ibv_mr*>(cq);
  fake_mr->context = setup.context;
  fake_mr->handle = original.handle;
  EXPECT_EQ(EINVAL, ibv_dereg_mr(fake_mr));
  // Restore original.
  memcpy(cq, &original, sizeof(original));
}

// Cannot have remote write without local write.
TEST_F(MrTest, InvalidPermissions) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer,
                          IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  EXPECT_EQ(nullptr, mr);
}

// Cannot have remote atomic without local write.
TEST_F(MrTest, InvalidPermissions2) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer,
                          IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
  EXPECT_EQ(nullptr, mr);
}

TEST_F(MrTest, DestroyPdWithOutstandingMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_mr* mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_NE(nullptr, mr);

  EXPECT_EQ(EBUSY, ibv_.DeallocPd(setup.pd));
}

// TODO(author1): Create Many/Max
// TODO(author1): Threaded MR creation/closure
// TODO(author1): Threaded rkey user (IBV_WC_REM_ACCESS_ERR)

class MRLoopbackTest : public BasicFixture {
 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr int kQueueSize = 200;
  static constexpr int kQKey = 200;

  struct Client {
    ibv_context* context = nullptr;
    verbs_util::LocalVerbsAddress address;
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
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    client.context = context_or.value();
    client.address = ibv_.GetContextAddressInfo(client.context);
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
    auto local_or = CreateClient(/*buf_content=*/'a');
    if (!local_or.ok()) {
      return local_or.status();
    }
    Client local = local_or.value();
    auto remote_or = CreateClient(/*buf_content=*/'b');
    if (!remote_or.ok()) {
      return remote_or.status();
    }
    Client remote = remote_or.value();
    ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, local.address);
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
            EXPECT_EQ(0, ibv_post_send(local.qp, wqes.data(), &bad_wr));
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

TEST_F(MRLoopbackTest, OutstandingRecv) {
  auto clients_or = CreateConnectedClientsPair();
  ASSERT_OK(clients_or);
  auto [local, remote] = clients_or.value();
  ibv_sge sg = verbs_util::CreateSge(remote.buffer.span(), remote.mr);
  ibv_recv_wr recv = verbs_util::CreateRecvWr(/*wr_id=*/0, &sg, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  ASSERT_EQ(0, ibv_.DeregMr(remote.mr));

  ibv_wc completions[10];
  int count = ibv_poll_cq(local.cq, sizeof(completions), completions);
  EXPECT_EQ(0, count);
}

TEST_F(MRLoopbackTest, OutstandingRead) {
  auto clients_or = CreateConnectedClientsPair();
  ASSERT_OK(clients_or);
  auto [local, remote] = clients_or.value();
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr read = verbs_util::CreateReadWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);

  std::vector<uint64_t> results;
  auto deregister = [this, &local = local]() {
    EXPECT_EQ(0, ibv_.DeregMr(local.mr));
  };
  StressDereg(local, read, deregister, &results);

  // TODO(author1): Update to expect IBV_WC_WR_FLUSH_ERR when QP
  // cancellation is implemented.
  EXPECT_GT(results[IBV_WC_SUCCESS], 0);
  EXPECT_GT(results[IBV_WC_LOC_PROT_ERR], 0);
}

TEST_F(MRLoopbackTest, OutstandingWrite) {
  auto clients_or = CreateConnectedClientsPair();
  ASSERT_OK(clients_or);
  auto [local, remote] = clients_or.value();
  ibv_sge sg = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr write = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &sg, /*num_sge=*/1, remote.buffer.data(), remote.mr->rkey);

  std::vector<uint64_t> results;
  auto deregister = [this, &local = local]() {
    ASSERT_EQ(0, ibv_.DeregMr(local.mr));
  };
  StressDereg(local, write, deregister, &results);

  EXPECT_GT(results[IBV_WC_SUCCESS], 0);
  // Not checking for IBV_WC_LOC_PROT_ERR since all ops might have already sent
  // their data and be in flight. Test still functions to detect msan errors and
  // other crashes.
}

}  // namespace rdma_unit_test
