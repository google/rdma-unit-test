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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_HANDLE_GARBLE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_HANDLE_GARBLE_H_

#include <cstdint>

namespace rdma_unit_test {

// A RAII class that garbles a handle (32 bits unsigned integer) of various
// libibverbs resources such as AH, MW, QP, etc. When destroyed, the old value
// will be restored. This is specifically used when the resource is allocated
// using VerbsHelperSuite which features automatic destruction. When the
// resource is manually destroyed (e.g. ibv_destroy_cq), one should not use
// this class.
class HandleGarble {
 public:
  HandleGarble() = delete;
  HandleGarble(uint32_t& handle);
  // Movable but not copyable.
  HandleGarble(HandleGarble&& garble) = default;
  HandleGarble& operator=(HandleGarble&& garble) = default;
  HandleGarble(const HandleGarble& garble) = delete;
  HandleGarble& operator=(const HandleGarble& arble) = delete;
  ~HandleGarble();

 private:
  const uint32_t original_handle_;
  uint32_t& handle_ptr_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_HANDLE_GARBLE_H_
