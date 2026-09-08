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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_GPU_RDMA_MEMBLOCK_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_GPU_RDMA_MEMBLOCK_H_

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>

#include "absl/types/span.h"
#include "public/cuda_helper.h"
#include "public/rdma_memblock.h"

namespace rdma_unit_test {
namespace cuda {

// GPU memory allocation
class GpuRdmaMemBlock : public RdmaMemBlock {
 public:
  GpuRdmaMemBlock() = default;
  // Creates a new memory region with the specified alignment and length in
  // elements. The underlying allocation will be extended to a page size
  // boundary.
  explicit GpuRdmaMemBlock(size_t length,
                           const cuda::GpuDeviceInfo* gpu_device_info);
  // Allow copy constructor, the underlying filememblock is a shared pointer.
  GpuRdmaMemBlock(const GpuRdmaMemBlock&) = default;
  GpuRdmaMemBlock& operator=(const GpuRdmaMemBlock&) = default;

  ~GpuRdmaMemBlock() = default;

 private:
  static std::shared_ptr<MemBlock> CreateGpuMemblock(
      size_t size, const cuda::GpuDeviceInfo& gpu_device_info);

  // No ownership, it represents the context of the GPU device.
  const cuda::GpuDeviceInfo* gpu_device_info_ = nullptr;
};
std::ostream& operator<<(std::ostream& os, const GpuRdmaMemBlock& block);

}  // namespace cuda
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_GPU_RDMA_MEMBLOCK_H_
