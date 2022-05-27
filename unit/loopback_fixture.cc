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

#include "unit/loopback_fixture.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

absl::StatusOr<ibv_wc_status> LoopbackFixture::ExecuteRdmaOp(
    Client& local, Client& remote, ibv_wr_opcode op_code) {
  ibv_sge sge = verbs_util::CreateSge(local.buffer.span(), local.mr);
  ibv_send_wr wr;
  wr.next = nullptr;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.wr_id = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.opcode = op_code;
  switch (op_code) {
    case IBV_WR_RDMA_READ:
    case IBV_WR_RDMA_WRITE: {
      wr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote.buffer.data());
      wr.wr.rdma.rkey = remote.mr->rkey;
      break;
    }
    case IBV_WR_ATOMIC_FETCH_AND_ADD:
    case IBV_WR_ATOMIC_CMP_AND_SWP: {
      // TODO(author2): We should not need to set this. Most NIC ignore this.
      wr.sg_list->length = 8;
      wr.wr.atomic.remote_addr =
          reinterpret_cast<uint64_t>(remote.buffer.data());
      wr.wr.atomic.rkey = remote.mr->rkey;
      wr.wr.atomic.compare_add = 1;
      wr.wr.atomic.swap = 1;
      break;
    }
    default:
      return absl::InternalError(
          absl::StrCat("OpCode ", op_code, " not supported."));
  }
  verbs_util::PostSend(local.qp, wr);
  ASSIGN_OR_RETURN(ibv_wc completion, verbs_util::WaitForCompletion(local.cq));
  CHECK_EQ(completion.wr_id, 1ul);                // Crash ok
  CHECK_EQ(completion.qp_num, local.qp->qp_num);  // Crash ok
  return completion.status;
}

absl::StatusOr<LoopbackFixture::Client> LoopbackFixture::CreateClient(
    ibv_qp_type qp_type, int pages, QpInitAttribute qp_init_attr) {
  Client client;
  client.buffer = ibv_.AllocBuffer(pages);
  std::fill_n(client.buffer.data(), client.buffer.size(), '-');
  ASSIGN_OR_RETURN(client.context, ibv_.OpenDevice());
  client.port_attr = ibv_.GetPortAttribute(client.context);
  client.pd = ibv_.AllocPd(client.context);
  if (!client.pd) {
    return absl::InternalError("Failed to allocate pd.");
  }
  client.cq = ibv_.CreateCq(client.context);
  if (!client.cq) {
    return absl::InternalError("Failed to create cq.");
  }
  client.qp = ibv_.CreateQp(client.pd, client.cq, qp_type, qp_init_attr);
  if (!client.qp) {
    return absl::InternalError("Failed to create qp.");
  }
  // memory setup.
  client.mr = ibv_.RegMr(client.pd, client.buffer);
  if (!client.mr) {
    return absl::InternalError("Failed to register mr.");
  }
  return client;
}

absl::StatusOr<LoopbackFixture::BasicSetup> LoopbackFixture::CreateBasicSetup(
    int pages) {
  BasicSetup setup;
  setup.buffer = ibv_.AllocBuffer(pages);
  std::fill_n(setup.buffer.data(), setup.buffer.size(), '-');
  ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
  setup.port_attr = ibv_.GetPortAttribute(setup.context);
  setup.pd = ibv_.AllocPd(setup.context);
  if (!setup.pd) {
    return absl::InternalError("Failed to allocate pd.");
  }
  setup.cq = ibv_.CreateCq(setup.context);
  if (!setup.cq) {
    return absl::InternalError("Failed to create cq.");
  }
  ibv_qp_init_attr attr =
      QpInitAttribute().GetAttribute(setup.cq, setup.cq, IBV_QPT_RC);
  setup.local_qp = ibv_.CreateQp(setup.pd, attr);
  if (!setup.local_qp) {
    return absl::InternalError("Failed to create local qp.");
  }
  setup.remote_qp = ibv_.CreateQp(setup.pd, attr);
  if (!setup.remote_qp) {
    return absl::InternalError("Failed to create remote qp.");
  }
  RETURN_IF_ERROR(ibv_.SetUpLoopbackRcQps(setup.local_qp, setup.remote_qp,
                                          setup.port_attr));
  setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
  if (!setup.mr) {
    return absl::InternalError("Failed to register mr.");
  }
  return setup;
}

}  // namespace rdma_unit_test
