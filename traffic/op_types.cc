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

#include "traffic/op_types.h"

#include <string>

namespace rdma_unit_test {

bool AbslParseFlag(absl::string_view text, OpTypes* out, std::string* error) {
  if (text == "read") {
    *out = OpTypes::kRead;
    return true;
  } else if (text == "write") {
    *out = OpTypes::kWrite;
    return true;
  } else if (text == "recv") {
    *out = OpTypes::kRecv;
    return true;
  } else if (text == "send") {
    *out = OpTypes::kSend;
    return true;
  } else if (text == "comp_swap") {
    *out = OpTypes::kCompSwap;
    return true;
  } else if (text == "fetch_add") {
    *out = OpTypes::kFetchAdd;
    return true;
  }
  *out = OpTypes::kInvalid;
  return true;
}

std::string AbslUnparseFlag(OpTypes op_type) {
  switch (op_type) {
    case OpTypes::kRead:
      return "read";
    case OpTypes::kWrite:
      return "write";
    case OpTypes::kRecv:
      return "recv";
    case OpTypes::kSend:
      return "send";
    case OpTypes::kCompSwap:
      return "comp_swap";
    case OpTypes::kFetchAdd:
      return "fetch_add";
    case OpTypes::kInvalid:
      return "invalid";
    default:
      return "unknown";
  }
}

}  // namespace rdma_unit_test
