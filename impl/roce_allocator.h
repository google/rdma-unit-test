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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_ROCE_ALLOCATOR_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_ROCE_ALLOCATOR_H_

#include "infiniband/verbs.h"
#include "impl/verbs_allocator.h"

namespace rdma_unit_test {

class RoceAllocator : public VerbsAllocator {
 public:
  RoceAllocator() = default;
  RoceAllocator(const RoceAllocator&& allocator) = delete;
  RoceAllocator& operator=(const RoceAllocator&& allocator) = delete;
  RoceAllocator(const RoceAllocator& allocator) = delete;
  RoceAllocator& operator=(const RoceAllocator& allocator) = delete;
  ~RoceAllocator() override = default;

 private:
  ibv_mr* RegMrInternal(ibv_pd* pd, const RdmaMemBlock& memblock,
                        int access) override;
  ibv_ah* CreateAhInternal(ibv_pd* pd) override;
  ibv_qp* CreateQpInternal(ibv_pd* pd, ibv_qp_init_attr& basic_attr) override;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_ROCE_ALLOCATOR_H_
