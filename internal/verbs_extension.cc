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

#include <utility>

#include "glog/logging.h"
#include "infiniband/verbs.h"

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

ibv_ah* VerbsExtension::CreateAh(ibv_pd* pd, ibv_ah_attr& ah_attr) {
  return ibv_create_ah(pd, &ah_attr);
}

ibv_qp* VerbsExtension::CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr) {
  return ibv_create_qp(pd, &basic_attr);
}

int VerbsExtension::ModifyRcQpInitToRtr(ibv_qp* qp, ibv_qp_attr& qp_attr,
                                        int qp_attr_mask) {
  return ibv_modify_qp(qp, &qp_attr, qp_attr_mask);
}

}  // namespace rdma_unit_test
