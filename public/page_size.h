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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_PAGE_SIZE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_PAGE_SIZE_H_

#include <cstddef>

namespace rdma_unit_test {

// Please use the kPageSize expression exported by this header instead of
// calling sysconf(_SC_PAGE_SIZE). Should the values
// differ on some execution environment, one should change the definition here.

#if defined(__x86_64__) || defined(__i386__) || \
    (defined(__riscv) && __riscv_xlen == 64)
constexpr size_t kPageShift = 12;          // 4 KB
constexpr size_t kHugepageShift = 21;      // 2 MB
#elif defined(__powerpc64__)
constexpr size_t kPageShift = 16;      // 64 KiB
constexpr size_t kHugepageShift = 24;  // 16 MiB
#elif defined(__aarch64__)
constexpr size_t kPageShift = 12;      // 4 KiB
constexpr size_t kHugepageShift = 21;  // 2 MiB
#else
#error "No page size defined for this architecture"
#endif

constexpr size_t kPageSize = 1ll << kPageShift;
constexpr size_t kPageMask = ~(kPageSize - 1);
constexpr size_t kHugepageSize = 1ll << kHugepageShift;
constexpr size_t kHugepageMask = ~(kHugepageSize - 1);

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_PAGE_SIZE_H_
