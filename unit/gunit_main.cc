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

#include "gtest/gtest.h"
#include "absl/debugging/failure_signal_handler.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "internal/introspection_irdma.h"
#include "internal/introspection_mlx4.h"
#include "internal/introspection_mlx5.h"
#include "internal/introspection_rxe.h"

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  absl::InitializeLog();
  absl::FailureSignalHandlerOptions options;
  absl::InstallFailureSignalHandler(options);
  absl::ParseCommandLine(argc, argv);

  // Register supported NIC's
  rdma_unit_test::IntrospectionIrdma::Register();
  rdma_unit_test::IntrospectionMlx4::Register();
  rdma_unit_test::IntrospectionMlx5::Register();
  rdma_unit_test::IntrospectionRxe::Register();

  return RUN_ALL_TESTS();
}
