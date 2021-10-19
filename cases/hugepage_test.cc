#include <mntent.h>
#include <paths.h>
#include <stdio.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/introspection.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {
namespace {

using ::testing::Each;
using ::testing::NotNull;

class HugePageTest : public BasicFixture {
 public:
  void SetUp() override {
    if (!HugepageEnabled()) {
      GTEST_SKIP() << "hugetlbfs is not mounted";
    }
    if (!Introspection().SupportsRcQp()) {
      GTEST_SKIP() << "Nic does not support RC QP";
    }
  }

 protected:
  static constexpr int kBufferMemoryPages = 1;
  static constexpr int kQueueSize = 200;

  struct Client {
    ibv_context* context = nullptr;
    verbs_util::PortGid port_gid;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_qp* qp = nullptr;
    // UD based tests need a RC QP in order to bind Type1 MW.
    ibv_qp* control_qp = nullptr;
    // AH pointing to the other client.
    ibv_ah* other_ah = nullptr;
    ibv_mr* mr = nullptr;
    ibv_mw* type1_mw = nullptr;
    ibv_mw* type2_mw = nullptr;
    RdmaMemBlock buffer;
    ibv_mr* atomic_mr = nullptr;
    RdmaMemBlock atomic_buffer;
  };

  absl::StatusOr<Client> CreateClient(uint8_t buf_content = '-') {
    Client client;
    client.buffer = ibv_.AllocHugepageBuffer(kBufferMemoryPages);
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
    client.control_qp =
        ibv_.CreateQp(client.pd, client.cq, client.cq, nullptr, kQueueSize,
                      kQueueSize, IBV_QPT_RC, /*sig_all=*/0);
    if (!client.control_qp) {
      return absl::InternalError("Failed to create qp.");
    }
    ibv_.SetUpSelfConnectedRcQp(client.control_qp, client.port_gid);
    // memory setup.
    client.mr = ibv_.RegMr(client.pd, client.buffer);
    if (!client.mr) {
      return absl::InternalError("Failed to register mr.");
    }
    client.type1_mw = ibv_.AllocMw(client.pd, IBV_MW_TYPE_1);
    if (!client.type1_mw) {
      return absl::InternalError("Failed to allocate type1 mw.");
    }
    if (Introspection().SupportsType2()) {
      client.type2_mw = ibv_.AllocMw(client.pd, IBV_MW_TYPE_2);
      if (!client.type2_mw) {
        return absl::InternalError("Failed to open device.");
      }
    }
    return client;
  }

  absl::Status BindMws(Client& client, uint32_t type2_rkey) {
    DCHECK(client.control_qp);
    DCHECK(client.type1_mw);
    DCHECK(client.mr);
    DCHECK_EQ(client.control_qp->qp_type, IBV_QPT_RC);
    DCHECK_EQ(verbs_util::GetQpState(client.control_qp), IBV_QPS_RTS);
    DCHECK_EQ(verbs_util::GetQpState(client.qp), IBV_QPS_RTS);
    ASSIGN_OR_RETURN(
        ibv_wc_status status,
        verbs_util::BindType1MwSync(client.control_qp, client.type1_mw,
                                    client.buffer.span(), client.mr));
    if (status != IBV_WC_SUCCESS) {
      return absl::InternalError(
          absl::StrCat("Failed to bind type 1 mw.(", status, ")."));
    }
    if (Introspection().SupportsType2()) {
      ASSIGN_OR_RETURN(ibv_wc_status status_type2,
                       verbs_util::BindType2MwSync(client.qp, client.type2_mw,
                                                   client.buffer.span(),
                                                   type2_rkey, client.mr));
      if (status_type2 != IBV_WC_SUCCESS) {
        return absl::InternalError(
            absl::StrCat("Failed to bind type 2 mw.(", status_type2, ")."));
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::pair<Client, Client>> CreateConnectedClientsPair() {
    ASSIGN_OR_RETURN(Client local, CreateClient(/*buf_content=*/'a'));
    ASSIGN_OR_RETURN(Client remote, CreateClient(/*buf_content=*/'b'));
    RETURN_IF_ERROR(SetUpInterconnection(local, remote));
    RETURN_IF_ERROR(BindMws(local, /*type2_rkey=*/1024));
    RETURN_IF_ERROR(BindMws(remote, /*type2_rkey=*/2028));
    return std::make_pair(local, remote);
  }

  absl::Status SetUpInterconnection(Client& local, Client& remote) {
    ibv_.SetUpLoopbackRcQps(local.qp, remote.qp, remote.port_gid);
    return absl::OkStatus();
  }

  static bool HugepageEnabled() {
    struct mntent* mnt;
    FILE* f;
    f = setmntent(_PATH_MOUNTED, "r");
    bool hugetlbfs_mounted = false;
    while ((mnt = getmntent(f))) {
      if (!strcmp(mnt->mnt_type, "hugetlbfs")) {
        hugetlbfs_mounted = true;
        break;
      }
    }
    endmntent(f);
    return hugetlbfs_mounted;
  }
};

// Send a 1GB chunk from local to remote
TEST_F(HugePageTest, SendLargeChunk) {
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
  // prepare buffer
  RdmaMemBlock send_buf = ibv_.AllocHugepageBuffer(/*pages=*/512);
  memset(send_buf.data(), 'a', send_buf.size());
  RdmaMemBlock recv_buf = ibv_.AllocHugepageBuffer(/*pages=*/512);
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
  Client local, remote;
  ASSERT_OK_AND_ASSIGN(std::tie(local, remote), CreateConnectedClientsPair());
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
