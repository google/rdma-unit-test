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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_BACKEND_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_BACKEND_H_

#include <cstdint>

#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// This is an abstract class that hides transport-specific details for
// setting up a qp connection. It has one virtual function SetUpRcQp for
// setting up a local qp to be able to talk to a rmeote qp, and two other
// functions SetUpSelfConnectedRcQp and SetUpLoopbackRcQps to handle
// the most frequent usecases: setting up a qp to talk to itself or setting
// up two qps to talk to each other via loopback.
class VerbsBackend {
 public:
  VerbsBackend() = default;
  // Movable but not copyable.
  VerbsBackend(VerbsBackend&& transport) = default;
  VerbsBackend& operator=(VerbsBackend&& transport) = default;
  VerbsBackend(const VerbsBackend& transport) = delete;
  VerbsBackend& operator=(const VerbsBackend& transport) = delete;
  virtual ~VerbsBackend() = default;

  // Sets up a reliable connection queue pair to RTS (ready to send).
  absl::Status SetUpRcQp(ibv_qp* qp, const verbs_util::PortGid& local,
                         ibv_gid remote_gid, uint32_t remote_qpn);

  // Set up a QP that is connected to itself. Succeed or crash.
  void SetUpSelfConnectedRcQp(ibv_qp* qp, const verbs_util::PortGid& local);

  // Set up a pair of interconnected RC QPs on loopback port. Both QPs share the
  // same NIC and thus same verbs_util::VerbsAddress.
  void SetUpLoopbackRcQps(ibv_qp* qp1, ibv_qp* qp2,
                          const verbs_util::PortGid& local);

  // Sets up a unreliable datagram queue pair to RTS (ready to send).
  absl::Status SetUpUdQp(ibv_qp* qp, verbs_util::PortGid local, uint32_t qkey);

  // Modify the QP to Init state.
  absl::Status SetQpInit(ibv_qp* qp, uint8_t port);

  // Modify the QP to RTR(ready to receive) state.
  virtual absl::Status SetQpRtr(ibv_qp* qp, const verbs_util::PortGid& local,
                                ibv_gid remote_gid, uint32_t remote_qpn) = 0;

  // Modify the QP to RTS(ready to send) state.
  absl::Status SetQpRts(ibv_qp* qp);

  // Modify the QP to RTS(ready to send) state, also setting a set of optional
  // attributes: sq_psn, timeout, retry_cnt, rnr_retry, max_rd_atomic.
  // Any other attributes will be ignored.
  absl::Status SetQpRts(ibv_qp* qp, ibv_qp_attr optional_attr, int mask);

  // Modify the Qp to ERROR state.
  absl::Status SetQpError(ibv_qp* qp);
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_BACKEND_H_
