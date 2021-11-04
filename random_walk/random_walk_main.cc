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

// Initialize absl::Flags before initializing/running unit tests.

#include <cstdint>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/debugging/failure_signal_handler.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/time/time.h"
#include "internal/introspection_mlx4.h"
#include "internal/introspection_mlx5.h"
#include "internal/introspection_rxe.h"
#include "public/introspection.h"
#include "random_walk/flags.h"
#include "random_walk/internal/multi_node_orchestrator.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/single_node_orchestrator.h"

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  absl::ParseCommandLine(argc, argv);
  absl::FailureSignalHandlerOptions options;
  absl::InstallFailureSignalHandler(options);

  // Register supported NIC's
  rdma_unit_test::IntrospectionMlx4::Register();
  rdma_unit_test::IntrospectionMlx5::Register();
  rdma_unit_test::IntrospectionRxe::Register();

  rdma_unit_test::random_walk::ActionWeights weights;
  if (!rdma_unit_test::Introspection().SupportsType2()) {
    weights.set_allocate_type_2_mw(0);
    weights.set_bind_type_2_mw(0);
    weights.set_deallocate_type_2_mw(0);
  }
  int clients = absl::GetFlag(FLAGS_clients);
  int duration = absl::GetFlag(FLAGS_duration);
  bool multinode = absl::GetFlag(FLAGS_multinode);
  if (multinode) {
    rdma_unit_test::random_walk::MultiNodeOrchestrator orchestrator(clients,
                                                                    weights);
    orchestrator.RunClients(absl::Seconds(duration));
  } else {
    rdma_unit_test::random_walk::SingleNodeOrchestrator orchestrator(clients,
                                                                     weights);
    orchestrator.RunClients(absl::Seconds(duration));
  }

  return 0;
}
