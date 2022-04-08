/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "random_walk/internal/types.h"

#include <cstdint>

#include <magic_enum.hpp>

namespace rdma_unit_test {
namespace random_walk {

uint64_t EncodeAction(uint32_t wr_id, Action action) {
  return static_cast<uint64_t>(wr_id) | (static_cast<uint64_t>(action) << 32);
}

Action DecodeAction(uint64_t wr_id) { return static_cast<Action>(wr_id >> 32); }

}  // namespace random_walk
}  // namespace rdma_unit_test
