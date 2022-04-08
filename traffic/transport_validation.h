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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_TRANSPORT_VALIDATION_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_TRANSPORT_VALIDATION_H_

#include "absl/status/status.h"

namespace rdma_unit_test {

// This class abstracts the validation of counters and statistics at the start
// and end of a test. This can be used to sanity check global counters, make
// sure all resources are cleaned up correctly and record telemetry statistics.
class TransportValidation {
 public:
  virtual ~TransportValidation() = default;
  // Runs at the beginning of the test.
  virtual absl::Status PreTestValidation() { return absl::OkStatus(); }
  // Runs at the end of the test.
  virtual absl::Status PostTestValidation() { return absl::OkStatus(); }
  // Used to capture the intermediate details. We separate "dumping debug state"
  // from post-test validation.
  virtual absl::Status TransportSnapshot() { return absl::OkStatus(); }
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_TRANSPORT_VALIDATION_H_
