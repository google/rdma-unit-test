#include "internal/handle_garble.h"

namespace rdma_unit_test {

HandleGarble::HandleGarble(uint32_t& handle)
    : original_handle_(handle), handle_ptr_(handle) {
  handle_ptr_ ^= 0xDEADBEEF;
}

HandleGarble::~HandleGarble() { handle_ptr_ = original_handle_; }

}  // namespace rdma_unit_test
