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

#include "impl/verbs_backend.h"

#include "gmock/gmock.h"
#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "public/util.h"

namespace rdma_unit_test {

void VerbsBackend::SetUpSelfConnectedRcQp(
    ibv_qp* qp, const verbs_util::VerbsAddress& address) {
  absl::Status result = SetUpRcQp(qp, address, qp, address);
  CHECK(result.ok());  // Crash ok
}

void VerbsBackend::SetUpLoopbackRcQps(
    ibv_qp* qp1, ibv_qp* qp2, const verbs_util::VerbsAddress& local_address) {
  absl::Status result = SetUpRcQp(qp1, local_address, qp2, local_address);
  CHECK(result.ok());  // Crash ok
  result = SetUpRcQp(qp2, local_address, qp1, local_address);
  CHECK(result.ok());  // Crash ok
}

absl::Status VerbsBackend::SetUpUdQp(ibv_qp* qp,
                                     verbs_util::VerbsAddress address,
                                     uint32_t qkey) {
  ibv_qp_attr mod_init = {};
  mod_init.qp_state = IBV_QPS_INIT;
  mod_init.pkey_index = 0;
  mod_init.port_num = address.port();
  mod_init.qkey = qkey;
  constexpr int kInitMask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY;
  if (ibv_modify_qp(qp, &mod_init, kInitMask) != 0) {
    // TODO(author1): Go through and return errno for all of these
    return absl::InternalError("Modify Qp (init) failed.");
  }

  // Ready to receive.
  ibv_qp_attr mod_rtr = {};
  mod_rtr.qp_state = IBV_QPS_RTR;
  constexpr int kRtrMask = IBV_QP_STATE;
  if (ibv_modify_qp(qp, &mod_rtr, kRtrMask) != 0) {
    return absl::InternalError("Modify QP (RtR) failed.");
  }

  // Ready to send.
  ibv_qp_attr mod_rts = {};
  mod_rts.qp_state = IBV_QPS_RTS;
  mod_rts.sq_psn =
      1225;  // TODO(author1): Eventually randomize for reality.
  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_SQ_PSN;
  if (ibv_modify_qp(qp, &mod_rts, kRtsMask) != 0) {
    return absl::InternalError("Modify Qp (RtS) failed.");
  }
  return absl::OkStatus();
}

}  // namespace rdma_unit_test
