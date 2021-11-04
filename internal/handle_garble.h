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
