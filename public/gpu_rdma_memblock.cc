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

#include "public/gpu_rdma_memblock.h"

#include <fcntl.h>
#include <linux/memfd.h>
#include <sys/mman.h>
#include <syscall.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "public/cuda_helper.h"

namespace rdma_unit_test {
namespace cuda {

GpuRdmaMemBlock::GpuRdmaMemBlock(size_t length,
                                 const cuda::GpuDeviceInfo* gpu_device_info)
    : gpu_device_info_(gpu_device_info) {
  CHECK_NE(gpu_device_info_, nullptr);  // Crash ok
  memblock_ = CreateGpuMemblock(length, *gpu_device_info);
  span_ = memblock_->buffer;
}

// We also allow crash for GPU memory allocation failures, similar to host
// memory allocation, with one caveat of GPU context/device deconstruction.
std::shared_ptr<GpuRdmaMemBlock::MemBlock> GpuRdmaMemBlock::CreateGpuMemblock(
    size_t size, const cuda::GpuDeviceInfo& gpu_device_info) {
  auto gpu_ptr = cuda::AllocateGpuMemory(size, gpu_device_info);
  CHECK_OK(gpu_ptr);                   // Crash ok
  CHECK_NE(gpu_ptr.value(), nullptr);  // Crash ok
  auto dma_buf_fd = cuda::GetGpuMemoryDmaBufFd(*gpu_ptr, size, gpu_device_info);
  CHECK_OK(dma_buf_fd);             // Crash ok
  CHECK_GE(dma_buf_fd.value(), 0);  // Crash ok

  return std::shared_ptr<MemBlock>(
      new MemBlock{
          .fd = *dma_buf_fd,
          .buffer = absl::MakeSpan(reinterpret_cast<uint8_t*>(*gpu_ptr), size),
          .memory_type = MemoryType::kGpu},
      [gpu_device_info](MemBlock* memblock) {
        CHECK_OK(cuda::FreeGpuMemory(memblock->buffer.data(),  // Crash OK
                                     gpu_device_info));
        delete memblock;
      });
}

std::ostream& operator<<(std::ostream& os, const GpuRdmaMemBlock& block) {
  return os << absl::StrFormat("GpuRdmaMemBlock(%d, %x,%d)", block.GetFd(),
                               reinterpret_cast<uint64_t>(block.data()),
                               block.size());
}

}  // namespace cuda
}  // namespace rdma_unit_test
