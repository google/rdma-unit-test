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

#include "impl/roce_allocator.h"

#include "infiniband/verbs.h"
#include "public/util.h"

namespace rdma_unit_test {

ibv_mr* RoceAllocator::RegMrInternal(ibv_pd* pd, const RdmaMemBlock& memblock,
                                     int access) {
  return ibv_reg_mr(pd, memblock.data(), memblock.size(), access);
}

ibv_ah* RoceAllocator::CreateAhInternal(ibv_pd* pd) {
  verbs_util::LocalEndpointAttr local = GetLocalEndpointAttr(pd->context);
  verbs_util::AddressHandleAttr attr(local, /*remote_gid=*/local.gid());
  ibv_ah_attr ibv_attr = attr.GetAttributes();
  return ibv_create_ah(pd, &ibv_attr);
}

ibv_qp* RoceAllocator::CreateQpInternal(ibv_pd* pd,
                                        ibv_qp_init_attr& basic_attr) {
  return ibv_create_qp(pd, &basic_attr);
}

}  // namespace rdma_unit_test
