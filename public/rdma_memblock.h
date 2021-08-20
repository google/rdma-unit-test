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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_RDMA_MEMBLOCK_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_RDMA_MEMBLOCK_H_

#include <stddef.h>

#include <cstdint>
#include <iosfwd>
#include <memory>

#include "absl/types/span.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// Provides memory allocation for rdma unit tests where the underlying
// allocation is file backed shared memory regions to support test providers
// that require shared memory for the memory region registration.
// This class supports the concept of both aligned and unaligned allocation
// relative to page size. Given that file backed shared memory is allocated
// on page boundaries it may not satisfy the caller's intent of supplying
// an alignment other than a page.  If an alignment is provided that is not
// a page boundary the length will be rounded up to the length plus the page
// size and alignment. The buffer presented will occur at the proper alignment.
class RdmaMemBlock {
 public:
  RdmaMemBlock() = default;
  // Creates a new memory region with the specified alignment and length in
  // elements. The underlying allocation will be extended to a page size
  // boundary.
  explicit RdmaMemBlock(size_t length,
                        size_t alignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  // Allow copy constructor, the underlying filememblock is a shared pointer.
  RdmaMemBlock(const RdmaMemBlock&) = default;
  RdmaMemBlock& operator=(const RdmaMemBlock&) = default;
  ~RdmaMemBlock() = default;

  // Returns the backing file descriptor. Ownership is not transferred to the
  // caller.
  int GetFd() const { return memblock_->fd; }

  // Returns the offset into the base fd for this buffer.
  size_t GetOffset() { return offset_; }

  // Returns the pointer to the start of the buffer.
  uint8_t* data() const { return span_.data(); }

  // Returns the size.
  size_t size() const { return span_.size(); }

  // Returns a span from the underlying memory.
  template <typename ValueT = uint8_t>
  absl::Span<ValueT> span() const {
    return absl::MakeSpan(reinterpret_cast<ValueT*>(span_.data()),
                          span_.size() / sizeof(ValueT));
  }

  // Returns a subspan from the underlying memory. The behaviour is undefined if
  // offset or count exceed the underlying buffer.
  absl::Span<uint8_t> subspan(size_t offset) const {
    return span_.subspan(offset);
  }
  absl::Span<uint8_t> subspan(size_t offset, size_t size) const {
    return span_.subspan(offset, size);
  }

  // Creates a new FileBackedMemBlock at the offset reusing the original
  // file backed memory.
  RdmaMemBlock subblock(size_t offset, size_t size) const;

 private:
  struct MemBlock {
    // The memfd used for the shared memory.
    int fd;
    // Defines the range of the allocated memory.
    absl::Span<uint8_t> buffer;
  };
  RdmaMemBlock(const RdmaMemBlock& base, size_t offset, size_t size);

  // Creates the actual file backed shared memory of 'size'.
  static std::shared_ptr<MemBlock> Create(size_t size);

  // Custom deleters to cleanup fd's and shared memory.
  static void MemBlockDeleter(MemBlock* memblock);

  // Offset into the original file backed memory that this buffer is allocated
  // to.
  size_t offset_;

  // Maintain a span to encapsulate the alignment/size of the requested
  // buffer.
  absl::Span<uint8_t> span_;

  // Underlying file backed memory. Defined as a shared ptr to deallocate
  // the memory block when there are no more references.
  std::shared_ptr<MemBlock> memblock_;
};
std::ostream& operator<<(std::ostream& os, const RdmaMemBlock& block);

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_RDMA_MEMBLOCK_H_
