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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_TYPES_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_TYPES_H_

#include <string>

#include "absl/strings/string_view.h"

namespace rdma_unit_test {

enum class OpTypes {
  kWrite,
  kRead,
  kSend,
  kRecv,
  kFetchAdd,
  kCompSwap,
  kInvalid
};

bool AbslParseFlag(absl::string_view text, OpTypes* out, std::string* error);

std::string AbslUnparseFlag(OpTypes op_type);

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_TYPES_H_
