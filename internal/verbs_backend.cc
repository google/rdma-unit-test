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

#include "internal/verbs_backend.h"

#include <cstdint>
#include <string>

#include "glog/logging.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

absl::Status VerbsBackend::SetUpRcQp(ibv_qp* qp,
                                     const verbs_util::PortGid& local,
                                     ibv_gid remote_gid, uint32_t remote_qpn) {
  RETURN_IF_ERROR(SetQpInit(qp, local.port));
  RETURN_IF_ERROR(SetQpRtr(qp, local, remote_gid, remote_qpn));
  return SetQpRts(qp);
}

void VerbsBackend::SetUpSelfConnectedRcQp(ibv_qp* qp,
                                          const verbs_util::PortGid& local) {
  absl::Status result = SetUpRcQp(qp, local, local.gid, qp->qp_num);
  CHECK(result.ok());  // Crash ok
}

void VerbsBackend::SetUpLoopbackRcQps(ibv_qp* qp1, ibv_qp* qp2,
                                      const verbs_util::PortGid& local) {
  absl::Status result = SetUpRcQp(qp1, local, local.gid, qp2->qp_num);
  CHECK(result.ok());  // Crash ok
  result = SetUpRcQp(qp2, local, local.gid, qp1->qp_num);
  CHECK(result.ok());  // Crash ok
}

absl::Status VerbsBackend::SetUpUdQp(ibv_qp* qp, verbs_util::PortGid local,
                                     uint32_t qkey) {
  ibv_qp_attr mod_init = {};
  mod_init.qp_state = IBV_QPS_INIT;
  mod_init.pkey_index = 0;
  mod_init.port_num = local.port;
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

absl::Status VerbsBackend::SetQpInit(ibv_qp* qp, uint8_t port) {
  ibv_qp_attr mod_init = {};
  mod_init.qp_state = IBV_QPS_INIT;
  mod_init.pkey_index = 0;
  mod_init.port_num = port;
  constexpr int kRemoteAccessAll =
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND;
  mod_init.qp_access_flags = kRemoteAccessAll;
  constexpr int kInitMask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  int result_code = ibv_modify_qp(qp, &mod_init, kInitMask);
  if (result_code) {
    return absl::InternalError(
        absl::StrCat("Modify QP (Init) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

absl::Status VerbsBackend::SetQpRts(ibv_qp* qp, ibv_qp_attr attr, int mask) {
  ibv_qp_attr mod_rts = {};
  mod_rts.qp_state = IBV_QPS_RTS;
  mod_rts.sq_psn = mask & IBV_QP_SQ_PSN ? attr.sq_psn : 1225;
  mod_rts.timeout = mask & IBV_QP_TIMEOUT ? attr.timeout : 17;  // ~500 ms
  mod_rts.retry_cnt = mask & IBV_QP_RETRY_CNT ? attr.retry_cnt : 5;
  mod_rts.rnr_retry = mask & IBV_QP_RNR_RETRY ? attr.rnr_retry : 5;
  mod_rts.max_rd_atomic =
      mask & IBV_QP_MAX_QP_RD_ATOMIC ? attr.max_rd_atomic : 5;

  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                           IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                           IBV_QP_MAX_QP_RD_ATOMIC;
  int result_code = ibv_modify_qp(qp, &mod_rts, kRtsMask);
  if (result_code != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (Rts) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

absl::Status VerbsBackend::SetQpError(ibv_qp* qp) {
  ibv_qp_attr modify_error = {};
  modify_error.qp_state = IBV_QPS_ERR;
  constexpr int kEerorMask = IBV_QP_STATE;
  int result_code = ibv_modify_qp(qp, &modify_error, kEerorMask);
  if (result_code != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (Error) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

}  // namespace rdma_unit_test
