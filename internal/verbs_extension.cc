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

#include "internal/verbs_extension.h"

#include "glog/logging.h"

namespace rdma_unit_test {

ibv_mr* VerbsExtension::RegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                              int access) {
  return ibv_reg_mr(pd, memblock.data(), memblock.size(), access);
}

int VerbsExtension::ReregMr(ibv_mr* mr, int flags, ibv_pd* pd,
                            const RdmaMemBlock* memblock, int access) {
  if (memblock) {
    return ibv_rereg_mr(mr, flags, pd, memblock->data(), memblock->size(),
                        access);
  }
  return ibv_rereg_mr(mr, flags, pd, /*addr=*/nullptr, /*length=*/0, access);
}

ibv_ah* VerbsExtension::CreateAh(ibv_pd* pd, verbs_util::PortGid local,
                                 ibv_gid remote_gid) {
  ibv_ah_attr attr = verbs_util::CreateAhAttr(local, remote_gid);
  return ibv_create_ah(pd, &attr);
}

ibv_qp* VerbsExtension::CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr) {
  return ibv_create_qp(pd, &basic_attr);
}

absl::Status VerbsExtension::SetQpRtr(ibv_qp* qp,
                                      const verbs_util::PortGid& local,
                                      ibv_gid remote_gid, uint32_t remote_qpn) {
  ibv_qp_attr mod_rtr =
      verbs_util::CreateBasicQpAttrRtr(local, remote_gid, remote_qpn);
  int result_code = ibv_modify_qp(qp, &mod_rtr, verbs_util::kQpAttrRtrMask);
  if (result_code != 0) {
    LOG(INFO) << "ibv_modify_qp failed: " << result_code;
    return absl::InternalError(
        absl::StrCat("Modify QP (RtR) failed (", result_code, ")."));
  }
  return absl::OkStatus();
}

}  // namespace rdma_unit_test
