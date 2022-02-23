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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_LATENCY_MEASUREMENT_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_LATENCY_MEASUREMENT_H_

#include "traffic/client.h"
#include "traffic/op_types.h"

namespace rdma_unit_test {

// Interface for conducting latency measurement and verification. The interface
// is implemented with different transport specific functions.
class LatencyMeasurement {
 public:
  virtual ~LatencyMeasurement() = default;
  // Sets the primary and secondary latency measurement type for given
  // operations.
  virtual void ConfigureLatencyMeasurements(OpTypes op_type) {}

  // Collect latency measurement for Client QPs.
  virtual void CollectClientLatencyStats(const Client& client) {}

  // Verify latency statistics.
  virtual void CheckLatencies() {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_LATENCY_MEASUREMENT_H_
