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

#include <mntent.h>
#include <paths.h>
#include <stdio.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "public/introspection.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/loopback_fixture.h"

namespace rdma_unit_test {
namespace {

using ::testing::Each;
using ::testing::NotNull;

const int kNumHugepages = 512;

class HugePageTest : public LoopbackFixture {
 public:
  void SetUp() override {
    if (!HugepageEnabled()) {
      GTEST_SKIP() << "Hugepages not available";
    }
    if (!Introspection().SupportsRcQp()) {
      GTEST_SKIP() << "Nic does not support RC QP";
    }
  }

 protected:
  static bool HugepageEnabled() {
    std::ifstream procfs("/proc/sys/vm/nr_hugepages");
    if (procfs.fail()) {
      return false;
    }
    std::string line;
    std::getline(procfs, line, '\0');
    int nr_hugepages;
    CHECK(absl::SimpleAtoi(line, &nr_hugepages));
    return nr_hugepages >= kNumHugepages;
  }
};

// Send a 1GB chunk from local to remote
TEST_F(HugePageTest, SendLargeChunk) {
  ASSERT_OK_AND_ASSIGN(Client local, CreateClient());
  ASSERT_OK_AND_ASSIGN(Client remote, CreateClient());
  ASSERT_OK(ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, local.port_attr));

  // prepare buffer
  RdmaMemBlock send_buf = ibv_.AllocHugepageBuffer(kNumHugepages);
  memset(send_buf.data(), 'a', send_buf.size());
  RdmaMemBlock recv_buf = ibv_.AllocHugepageBuffer(kNumHugepages);
  memset(recv_buf.data(), 'b', recv_buf.size());
  ibv_mr* send_mr = ibv_.RegMr(local.pd, send_buf);
  ibv_mr* recv_mr = ibv_.RegMr(remote.pd, recv_buf);
  ASSERT_THAT(send_mr, NotNull());
  ASSERT_THAT(recv_mr, NotNull());

  ibv_sge rsge = verbs_util::CreateSge(recv_buf.span(), recv_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsge, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);

  ibv_sge lsge = verbs_util::CreateSge(send_buf.span(), send_mr);
  ibv_send_wr send =
      verbs_util::CreateSendWr(/*wr_id=*/1, &lsge, /*num_sge=*/1);
  verbs_util::PostSend(local.qp, send);

  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(recv_buf.span(), Each('a'));
}

// Send with mutiple SGEs
TEST_F(HugePageTest, SendMultipleSge) {
  ASSERT_OK_AND_ASSIGN(Client local, CreateClient());
  ASSERT_OK_AND_ASSIGN(Client remote, CreateClient());
  ASSERT_OK(ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, local.port_attr));
  // prepare buffer
  RdmaMemBlock send_buf = ibv_.AllocHugepageBuffer(/*pages=*/512);
  memset(send_buf.data(), 'a', send_buf.size());
  RdmaMemBlock recv_buf = ibv_.AllocHugepageBuffer(/*pages=*/512);
  memset(recv_buf.data(), 'b', recv_buf.size());
  ibv_mr* send_mr = ibv_.RegMr(local.pd, send_buf);
  ibv_mr* recv_mr = ibv_.RegMr(remote.pd, recv_buf);
  ASSERT_THAT(send_mr, NotNull());
  ASSERT_THAT(recv_mr, NotNull());
  ibv_sge lsgl[4];
  ibv_sge rsgl;
  rsgl = verbs_util::CreateSge(recv_buf.span(), recv_mr);
  ibv_recv_wr recv =
      verbs_util::CreateRecvWr(/*wr_id=*/0, &rsgl, /*num_sge=*/1);
  verbs_util::PostRecv(remote.qp, recv);
  // Divide the send buffer evenly among 4 sges.
  for (int i = 0; i < 4; i++) {
    lsgl[i] = verbs_util::CreateSge(
        send_buf.subspan(i * 128 * kHugepageSize, 128 * kHugepageSize),
        send_mr);
  }
  ibv_send_wr send = verbs_util::CreateSendWr(/*wr_id=*/1, lsgl, /*num_sge=*/4);
  ibv_send_wr* bad_wr = nullptr;
  int result =
      ibv_post_send(local.qp, const_cast<ibv_send_wr*>(&send), &bad_wr);
  if (result) return;
  ASSERT_OK_AND_ASSIGN(ibv_wc completion,
                       verbs_util::WaitForCompletion(local.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_SEND);
  EXPECT_EQ(completion.wr_id, 1);
  EXPECT_EQ(completion.qp_num, local.qp->qp_num);
  ASSERT_OK_AND_ASSIGN(completion, verbs_util::WaitForCompletion(remote.cq));
  EXPECT_EQ(completion.status, IBV_WC_SUCCESS);
  EXPECT_EQ(completion.opcode, IBV_WC_RECV);
  EXPECT_EQ(completion.qp_num, remote.qp->qp_num);
  EXPECT_EQ(completion.wr_id, 0);
  EXPECT_THAT(recv_buf.span(), Each('a'));
}

}  // namespace
}  // namespace rdma_unit_test
