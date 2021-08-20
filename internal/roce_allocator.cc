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

#include "internal/roce_allocator.h"

#include "infiniband/verbs.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

ibv_mr* RoceAllocator::RegMrInternal(ibv_pd* pd, const RdmaMemBlock& memblock,
                                     int access) {
  return ibv_reg_mr(pd, memblock.data(), memblock.size(), access);
}

ibv_ah* RoceAllocator::CreateAhInternal(ibv_pd* pd, ibv_gid remote_gid) {
  verbs_util::PortGid local = GetLocalPortGid(pd->context);
  ibv_ah_attr attr = verbs_util::CreateAhAttr(local, remote_gid);
  return ibv_create_ah(pd, &attr);
}

ibv_qp* RoceAllocator::CreateQpInternal(ibv_pd* pd,
                                        ibv_qp_init_attr& basic_attr) {
  return ibv_create_qp(pd, &basic_attr);
}

}  // namespace rdma_unit_test
