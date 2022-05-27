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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_ACTION_WEIGHTS_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_ACTION_WEIGHTS_H_

#include "random_walk/internal/random_walk_config.pb.h"

namespace rdma_unit_test {
namespace random_walk {

// Returns an ActionWeights that is uniform across all control path actions.
// This includes the creation and destrouctions of QP, AH, CQ, PD.
ActionWeights UniformControlPathActions();

// Returns an ActionWeights that focus mainly on RDMA.
ActionWeights SimpleRdmaActions();

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_ACTION_WEIGHTS_H_
