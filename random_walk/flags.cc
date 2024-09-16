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

#include "absl/flags/flag.h"
#include "absl/strings/string_view.h"

ABSL_FLAG(int, duration, 20,
          "Duration of the random walk test measured in seconds.");

ABSL_FLAG(int, clients, 2, "The number of random walk clients in the test.");

ABSL_FLAG(bool, multinode, false,
          "If enabled, run the random walk test in multinode mode where "
          "out-of-band communication will be done using gRPC.");
