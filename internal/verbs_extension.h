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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_EXTENSION_INTERFACE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_EXTENSION_INTERFACE_H_

#include <cstdint>

#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "public/rdma_memblock.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// This class contains libibverbs functions that has custom extensions. User
// should route their ibv_* function call for those functions via this class so
// the collection of tests supports all extensions simultaneously.
class VerbsExtension {
 public:
  VerbsExtension() = default;
  // Movable but not copyable.
  VerbsExtension(VerbsExtension&& allocator) = default;
  VerbsExtension& operator=(VerbsExtension&& allocator) = default;
  VerbsExtension(const VerbsExtension& allocator) = delete;
  VerbsExtension& operator=(const VerbsExtension& allocator) = delete;
  virtual ~VerbsExtension() = default;

  // Registers a memory region. Calls ibv_reg_mr on default.
  virtual ibv_mr* RegMr(ibv_pd* pd, const RdmaMemBlock& memblock, int access);

  // Reregisters a memory region. Calls ibv_rereg_mr on default.
  virtual int ReregMr(ibv_mr* mr, int flags, ibv_pd* pd,
                      const RdmaMemBlock* memblock, int access);

  // Creates a address handle. Calls ibv_create_ah on default.
  virtual ibv_ah* CreateAh(ibv_pd* pd, verbs_util::PortGid local,
                           ibv_gid remote_gid, uint8_t traffic_class = 0);

  // Create a queue pair. Calls ibv_create_qp on default.
  virtual ibv_qp* CreateQp(ibv_pd* pd, ibv_qp_init_attr& basic_attr);
  // Modify the QP to RTR(ready to receive) state.
  virtual absl::Status SetQpRtr(ibv_qp* qp, const verbs_util::PortGid& local,
                                ibv_gid remote_gid, uint32_t remote_qpn);
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_EXTENSION_INTERFACE_H_
