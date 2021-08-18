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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_ROCE_BACKEND_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_ROCE_BACKEND_H_

#include <cstdint>

#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "impl/verbs_backend.h"
#include "public/util.h"

namespace rdma_unit_test {

class RoceBackend : public VerbsBackend {
 public:
  RoceBackend() = default;
  // Movable but not copyable.
  RoceBackend(RoceBackend&& backend) = default;
  RoceBackend& operator=(RoceBackend&& backend) = default;
  RoceBackend(const RoceBackend& backend) = delete;
  RoceBackend& operator=(const RoceBackend& backend) = delete;
  ~RoceBackend() override = default;

  absl::Status SetQpRtr(ibv_qp* qp, const verbs_util::PortGid& local,
                        ibv_gid remote_gid, uint32_t remote_qpn) override;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_ROCE_BACKEND_H_
