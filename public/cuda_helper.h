#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_CUDA_HELPER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_CUDA_HELPER_H_

#include <cstdint>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/gpus/cuda/include/cuda.h"

#include "public/status_matchers.h"

namespace rdma_unit_test {
namespace cuda {

constexpr int kGpuPageSize = 64 * 1024;

struct GpuDeviceInfo {
  const int device_index;
  const std::string device_name;
  const int device_id;
  const int pci_bus_id;
  const bool has_dmabuf;
  CUdevice cu_device;
  CUcontext cu_context;
};

// Helper function and macro to convert a CUresult to an absl::Status.
absl::Status CUresultToStatus(CUresult result, const char* expr);
#define RETURN_IF_CUDA_ERROR(expression) \
  RETURN_IF_ERROR(                       \
      rdma_unit_test::cuda::CUresultToStatus(expression, #expression))
#define ASSERT_OK_CUDA(expression) \
  ASSERT_OK(rdma_unit_test::cuda::CUresultToStatus(expression, #expression))

// Attempts to initialize the CUDA Driver API.
// Returns absl::OkStatus() if libcuda.so is loaded and initialized
// successfully. Returns an error status if loading or initialization fails.
absl::Status InitializeCudaDriver();
absl::Status InitializeGpus();
absl::StatusOr<GpuDeviceInfo> InitializeGpu(int device_index);

// Tear down CUDA context for all devices. The libraries need to not be
// unloaded.
absl::Status TearDownGpus();

// Checks if the CUDA Driver API is available and initialized.
bool IsCudaDriverAvailable();

absl::StatusOr<int> GetGpuDeviceCount();

absl::StatusOr<const GpuDeviceInfo*> GetGpuDeviceInfo(int device_index);

absl::StatusOr<void*> AllocateGpuMemory(int64_t size,
                                        const GpuDeviceInfo& gpu_device_info);

absl::Status FreeGpuMemory(void* ptr, const GpuDeviceInfo& gpu_device_info);

absl::StatusOr<int> GetGpuMemoryDmaBufFd(void* ptr, int64_t size,
                                         const GpuDeviceInfo& gpu_device_info);

absl::Status CopyHostToGpu(CUdeviceptr gpu_ptr, const void* host_ptr,
                           int64_t size, const GpuDeviceInfo& gpu_device_info);

absl::Status CopyGpuToHost(void* host_ptr, CUdeviceptr gpu_ptr, int64_t size,
                           const GpuDeviceInfo& gpu_device_info);

}  // namespace cuda
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_CUDA_HELPER_H_
