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

#include "public/rdma-memblock.h"

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <syscall.h>
#include <unistd.h>

#include <algorithm>
#include <memory>

#include "glog/logging.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "public/util.h"

namespace rdma_unit_test {

static inline int memfd_create(const char* name, unsigned int flags) {
  return syscall(SYS_memfd_create, name, flags);
}

RdmaMemBlock::RdmaMemBlock(size_t length, size_t alignment) {
  offset_ = 0;
  size_t pad = (alignment == verbs_util::kPageSize) ? 0 : alignment;
  size_t alloc_size = length + pad;
  memblock_ = Create(alloc_size);
  uint8_t* buffer = memblock_->buffer.data() + pad;
  span_ = absl::MakeSpan(buffer, length);
  VLOG(1) << absl::StrCat("created new memblock: ", " alloc_size=", alloc_size,
                          " length=", length, " alignment=", alignment,
                          " base=")
          << std::hex << reinterpret_cast<uint64_t>(buffer);
}

RdmaMemBlock::RdmaMemBlock(const RdmaMemBlock& base, size_t offset,
                           size_t size) {
  // The caller can provide a size of 0 to test memory region registration
  VLOG(1) << absl::StrCat("creating sub memblock offset=", offset,
                          " size=", size);

  offset_ = base.span_.data() - base.memblock_->buffer.data() + offset;
  CHECK_LE(offset_ + size, base.memblock_->buffer.size());  // Crash ok
  uint8_t* new_base = base.memblock_->buffer.data() + offset_;
  span_ = absl::Span<uint8_t>(new_base, size);
  memblock_ = base.memblock_;
}

RdmaMemBlock RdmaMemBlock::subblock(size_t offset, size_t size) const {
  return RdmaMemBlock(*this, offset, size);
}

std::shared_ptr<RdmaMemBlock::MemBlock> RdmaMemBlock::Create(size_t size) {
  // First create the memory file file descriptor.
  int fd = memfd_create("memfd", 0);
  CHECK_GT(fd, 0);  // Crash ok

  // Allocate space in 2MB chunks to reduce the number EINTR attempts.
  size_t remaining = size;
  constexpr size_t kChunkSize = 2 * 1024 * 1024;
  constexpr int kMaximumFallocateEintrAttempts = 3;
  while (remaining > 0) {
    const off_t offset = size - remaining;
    const off_t length = std::min<off_t>(remaining, kChunkSize);
    int attempts = 0;
    int result;
    do {
      result = fallocate(fd, /* mode */ 0, offset, length);
    } while (result == -1 && errno == EINTR &&
             ++attempts < kMaximumFallocateEintrAttempts);
    CHECK_EQ(result, 0);  // Crash ok
    remaining -= length;
  }

  // Now create the virtual address space for the memory block.
  void* address = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_LOCKED, fd, /* offset */ 0);
  CHECK_NE(address, (void*)-1);  // Crash ok
  return std::shared_ptr<MemBlock>(
      new MemBlock{.fd = fd,
                   .buffer = absl::Span<uint8_t>(
                       reinterpret_cast<uint8_t*>(address), size)},
      MemBlockDeleter);
}

void RdmaMemBlock::MemBlockDeleter(MemBlock* memblock) {
  if (!memblock->buffer.empty()) {
    int result = munmap(memblock->buffer.data(), memblock->buffer.size());
    CHECK_EQ(result, 0);  // Crash ok
  }
  int result = close(memblock->fd);
  CHECK_EQ(result, 0);  // Crash ok
  delete memblock;
}

std::ostream& operator<<(std::ostream& os, const RdmaMemBlock& block) {
  return os << absl::StrFormat("RdmaMemBlock(%d, %x,%d)", block.GetFd(),
                               reinterpret_cast<uint64_t>(block.data()),
                               block.size());
}

}  // namespace rdma_unit_test
