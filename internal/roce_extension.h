/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_ROCE_EXTENSION_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_ROCE_EXTENSION_H_

#include "infiniband/verbs.h"
#include "internal/verbs_extension_interface.h"
#include "public/rdma_memblock.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

class RoceExtension : public VerbsExtensionInterface {
 public:
  RoceExtension() = default;
  // Movable but not copyable.
  RoceExtension(RoceExtension&& allocator) = default;
  RoceExtension& operator=(RoceExtension&& allocator) = default;
  RoceExtension(const RoceExtension& allocator) = delete;
  RoceExtension& operator=(const RoceExtension& allocator) = delete;
  ~RoceExtension() override = default;

  ibv_mr* RegMr(ibv_pd* pd, const RdmaMemBlock& memblock, int access) override;
  ibv_ah* CreateAh(ibv_pd* pd, verbs_util::PortGid local,
                   ibv_gid remote_gid) override;
  ibv_qp* CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr) override;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_ROCE_EXTENSION_H_
