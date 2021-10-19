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

#include "internal/roce_backend.h"

#include <cstdint>
#include <string>

#include "glog/logging.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

absl::Status RoceBackend::SetQpRtr(ibv_qp* qp, const verbs_util::PortGid& local,
                                   ibv_gid remote_gid, uint32_t remote_qpn) {
  ibv_qp_attr mod_rtr = {};
  mod_rtr.qp_state = IBV_QPS_RTR;
  mod_rtr.path_mtu = verbs_util::ToVerbsMtu(absl::GetFlag(FLAGS_verbs_mtu));
  mod_rtr.dest_qp_num = remote_qpn;
  static unsigned int psn = 1225;
  mod_rtr.rq_psn = psn;
  // 1225;  // TODO(author1): Eventually randomize for reality.
  mod_rtr.max_dest_rd_atomic = 10;
  mod_rtr.min_rnr_timer = 26;  // 82us delay
  mod_rtr.ah_attr.grh.dgid = remote_gid;
  mod_rtr.ah_attr.grh.flow_label = 0;
  mod_rtr.ah_attr.grh.sgid_index = local.gid_index;
  mod_rtr.ah_attr.grh.hop_limit = 127;
  mod_rtr.ah_attr.is_global = 1;
  mod_rtr.ah_attr.sl = 5;
  mod_rtr.ah_attr.port_num = local.port;
  constexpr int kRtrMask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                           IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                           IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  int result_code = ibv_modify_qp(qp, &mod_rtr, kRtrMask);
  if (result_code != 0) {
    LOG(INFO) << "ibv_modify_qp failed: " << result_code;
    return absl::InternalError(
        absl::StrCat("Modify QP (RtR) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

}  // namespace rdma_unit_test
