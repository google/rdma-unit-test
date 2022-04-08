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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_RDMA_STRESS_FIXTURE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_RDMA_STRESS_FIXTURE_H_

#include <cstdint>
#include <memory>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "infiniband/verbs.h"
#include "public/basic_fixture.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "traffic/client.h"
#include "traffic/latency_measurement.h"
#include "traffic/transport_validation.h"

namespace rdma_unit_test {

// Parent fixture for all RDMA datapath tests. Provides functions to set up
// queue pairs acrosss multiple clients, execute ops, poll completions and
// verify statistics.
class RdmaStressFixture : public BasicFixture {
 public:
  static constexpr uint64_t kAtomicWordSize = 8;

  RdmaStressFixture();
  ~RdmaStressFixture() override = default;

  // Creates a one way connection from local qp to remote qp. A separate
  // call to this function is require if you need to setup connection in the
  // reverse direction.
  absl::Status SetUpRcClientsQPs(Client* local, uint32_t local_qp_id,
                                 Client* remote, uint32_t remote_qp_id);

  // Creates number of qps_per_client RC qps for each client and connects
  // pairs across the two clients.
  void CreateSetUpRcQps(Client& initiator, Client& target,
                        uint16_t qps_per_client);

  // Halt execution of ops by:
  // 1. Dumps the pending ops.
  // 2. Check whether all async events have completed.
  void HaltExecution(Client& initiator);

  // Best effort attempt to poll async event queue and log results to stderr.
  // Polls the event queue once in non-blocking mode (the fixture's constructor
  // sets up the non-blocking mode) and acknowledges the events polled. Returns
  // OkStatus if no events polled, returns Internal error if an event is polled
  // and acknowledged, returns error if it fails on poll or event calls.
  absl::Status PollAndAckAsyncEvents();

  // Configures latencies measurement parameters for the given RDMA operations.
  void ConfigureLatencyMeasurements(OpTypes op_type);

  // Collects latencies statistics for a a given client.
  void CollectClientLatencyStats(const Client& client);

  // Makes sure that the latency measurements in each stats set are within
  // a certain percentage of one another.
  void CheckLatencies();

  // Logs the qp state for the initiator qps.
  void DumpState(Client& initiator);

  // Creates and returns a new PD.
  ibv_pd* NewPd();

  ibv_context* context() const { return context_; }

  verbs_util::PortGid port_gid() const { return port_gid_; }

 protected:
  ibv_context* context_ = nullptr;
  verbs_util::PortGid port_gid_;
  std::unique_ptr<TransportValidation> validation_ = nullptr;
  std::unique_ptr<LatencyMeasurement> latency_measure_;
  VerbsHelperSuite ibv_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_RDMA_STRESS_FIXTURE_H_
