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

#include "impl/roce_backend.h"

#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/util.h"

namespace rdma_unit_test {

absl::Status RoceBackend::SetUpRcQp(
    ibv_qp* local_qp, const verbs_util::VerbsAddress& local_address,
    ibv_qp* remote_qp, const verbs_util::VerbsAddress& remote_address) {
  ibv_mtu mtu = verbs_util::ToVerbsMtu(absl::GetFlag(FLAGS_verbs_mtu));
  // Init.
  ibv_qp_attr mod_init = {};
  mod_init.qp_state = IBV_QPS_INIT;
  mod_init.pkey_index = 0;
  mod_init.port_num = local_address.port();
  constexpr int kRemoteAccessAll =
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND;
  mod_init.qp_access_flags = kRemoteAccessAll;
  constexpr int kInitMask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  if (ibv_modify_qp(local_qp, &mod_init, kInitMask) != 0) {
    // TODO(author1): Go through and return errno for all of these
    return absl::InternalError("Modify Qp (init) failed.");
  }

  // Ready to receive.
  ibv_qp_attr mod_rtr = {};
  mod_rtr.qp_state = IBV_QPS_RTR;
  mod_rtr.path_mtu = verbs_util::ToVerbsMtu(mtu);
  mod_rtr.dest_qp_num = remote_qp->qp_num;
  static unsigned int psn = 1225;
  mod_rtr.rq_psn = psn;
  // 1225;  // TODO(author1): Eventually randomize for reality.
  mod_rtr.max_dest_rd_atomic = 10;
  mod_rtr.min_rnr_timer = 26;  // 82us delay
  mod_rtr.ah_attr.grh.dgid = remote_address.gid();
  mod_rtr.ah_attr.grh.flow_label = 0;
  mod_rtr.ah_attr.grh.sgid_index = local_address.gid_index();
  mod_rtr.ah_attr.grh.hop_limit = 127;
  mod_rtr.ah_attr.is_global = 1;
  mod_rtr.ah_attr.sl = 5;
  mod_rtr.ah_attr.port_num = local_address.port();
  constexpr int kRtrMask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                           IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                           IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  int result_code = ibv_modify_qp(local_qp, &mod_rtr, kRtrMask);
  if (result_code != 0) {
    LOG(INFO) << "ibv_modify_qp failed: " << result_code;
    return absl::InternalError("Modify QP (RtR) failed.");
  }

  // Ready to send.
  ibv_qp_attr mod_rts = {};
  mod_rts.qp_state = IBV_QPS_RTS;
  mod_rts.sq_psn =
      1225;              // TODO(author1): Eventually randomize for reality.
  mod_rts.timeout = 17;  // ~500 ms
  mod_rts.retry_cnt = 7;
  mod_rts.rnr_retry = 5;
  mod_rts.max_rd_atomic = 5;

  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                           IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                           IBV_QP_MAX_QP_RD_ATOMIC;
  result_code = ibv_modify_qp(local_qp, &mod_rts, kRtsMask);
  if (result_code != 0) {
    LOG(INFO) << "ibv_modify_qp failed: " << result_code;
    return absl::InternalError("Modify Qp (RtS) failed.");
  }
  return absl::OkStatus();
}

}  // namespace rdma_unit_test
