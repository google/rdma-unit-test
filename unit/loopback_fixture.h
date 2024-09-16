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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_UNIT_LOOPBACK_FIXTURE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_UNIT_LOOPBACK_FIXTURE_H_

#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/rdma_memblock.h"
#include "public/verbs_helper_suite.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

class LoopbackFixture : public RdmaVerbsFixture {
 protected:
  struct Client {
    ibv_context* context = nullptr;
    PortAttribute port_attr;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_qp* qp = nullptr;
    RdmaMemBlock buffer;
    ibv_mr* mr = nullptr;
  };

  struct BasicSetup {
    ibv_context* context = nullptr;
    PortAttribute port_attr;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_qp* local_qp = nullptr;
    ibv_qp* remote_qp = nullptr;
    RdmaMemBlock buffer;
    ibv_mr* mr = nullptr;
  };

  // Performs one of the four (READ, WRITE, FETCH_ADD, COMP_SWAP) RDMA ops.
  absl::StatusOr<ibv_wc_status> ExecuteRdmaOp(Client& local, Client& remote,
                                              ibv_wr_opcode op_code);

  // Create a client given a specific QP type, memory buffer size and content.
  absl::StatusOr<Client> CreateClient(
      ibv_qp_type qp_type = IBV_QPT_RC, int pages = 1,
      QpInitAttribute qp_init_attr = QpInitAttribute());

  // Creates a BasicSetup struct. The object contains basic ibverbs objects that
  // facilitate a loopback connecction that sends traffic.
  absl::StatusOr<BasicSetup> CreateBasicSetup(int pages = 1);
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_UNIT_LOOPBACK_FIXTURE_H_
