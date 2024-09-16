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

#include "traffic/test_op.h"

#include <cstdint>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "traffic/op_types.h"

namespace rdma_unit_test {

struct HexFormatter {
  void operator()(std::string* out, uint8_t c) const {
    absl::StrAppend(out, absl::AlphaNum(absl::Hex(c, absl::kZeroPad2)));
  }
};

std::string TestOp::ToString(OpTypes op_type) {
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
    default:
      return "invalid";
  }
}

std::string TestOp::SrcBuffer() const {
  if (!src_addr || length == 0) {
    return "Empty source buffer";
  }
  return absl::StrJoin(
      absl::MakeSpan(reinterpret_cast<char*>(src_addr), length), "",
      HexFormatter());
}

std::string TestOp::DestBuffer() const {
  if (!dest_addr || length == 0) {
    return "Empty destination buffer";
  }
  return absl::StrJoin(
      absl::MakeSpan(reinterpret_cast<char*>(dest_addr), length), "",
      HexFormatter());
}

}  // namespace rdma_unit_test
