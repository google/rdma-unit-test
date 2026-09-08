#include "public/cuda_helper.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "absl/base/const_init.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "third_party/gpus/cuda/include/cuda.h"
#include "util/task/status_macros.h"

namespace rdma_unit_test {
namespace cuda {

enum class CudaDriverState {
  kUninitialized,  // Initial state, InitializeCudaDriverApi() not yet called.
  kInitialized,    // Cuda driver is found, cuInit() is successful, and at least
                   // one device found.
  kFailed  // Cuda driver is not found or cuInit() failed for some other reason.
};

// Helper class to save and restore the CUDA context. The push happens in the
// constructor and the pop in the destructor.
class ScopedCudaContext {
 public:
  // Saves the current CUDA context and pushes the given context if different
  // from the current context.
  static absl::StatusOr<std::unique_ptr<ScopedCudaContext>> Create(
      CUcontext context) {
    if (!IsCudaDriverAvailable()) {
      return absl::FailedPreconditionError("CUDA Driver API not available.");
    }
    if (!context) {
      return absl::InvalidArgumentError("Target context cannot be null.");
    }

    auto scoped_ctx = absl::WrapUnique(new ScopedCudaContext(context));
    RETURN_IF_ERROR(scoped_ctx->Initialize());
    return scoped_ctx;
  }

  ~ScopedCudaContext() {
    if (context_changed_) {
      CUcontext popped_ctx;
      CUresult result = cuCtxPopCurrent(&popped_ctx);
      EXPECT_EQ(result, CUDA_SUCCESS)
          << "cuCtxPopCurrent failed in destructor: " << result;
      EXPECT_EQ(popped_ctx, target_ctx_)
          << "Popped context " << popped_ctx
          << " does not match target context " << target_ctx_;
    }
  }

  ScopedCudaContext(const ScopedCudaContext&) = delete;
  ScopedCudaContext& operator=(const ScopedCudaContext&) = delete;

 private:
  explicit ScopedCudaContext(CUcontext context) : target_ctx_(context) {}

  absl::Status Initialize() {
    CUcontext current_ctx = nullptr;
    RETURN_IF_CUDA_ERROR(cuCtxGetCurrent(&current_ctx));

    if (current_ctx != target_ctx_) {
      RETURN_IF_CUDA_ERROR(cuCtxPushCurrent(target_ctx_));
      context_changed_ = true;
    } else {
      context_changed_ = false;
    }
    return absl::OkStatus();
  }

  CUcontext target_ctx_ = nullptr;
  bool context_changed_ = false;
};

std::vector<GpuDeviceInfo> gpu_device_info;

static absl::Mutex cuda_init_mutex(absl::kConstInit);
static CudaDriverState cuda_driver_state ABSL_GUARDED_BY(cuda_init_mutex) =
    CudaDriverState::kUninitialized;
static int gpu_device_count = -1;

absl::Status CUresultToStatus(CUresult result, const char* expr) {
  if (result == CUDA_SUCCESS) {
    return absl::OkStatus();
  }
  const char* name = nullptr;
  const char* reason = nullptr;
  // We ignore the return CUresult from these calls to avoid complexity.
  cuGetErrorName(result, &name);
  cuGetErrorString(result, &reason);

  return absl::InternalError(absl::StrFormat(
      "%s failed: %s - %s (Code: %d)", expr, name ? name : "UNKNOWN_CUDA_ERROR",
      reason ? reason : "UNKNOWN_REASON", static_cast<int>(result)));
}

// We only call cuInit() once, but we don't record the possible error so it's
// only returned the first time it's called.
absl::Status InitializeCudaDriver() {
  absl::MutexLock lock(cuda_init_mutex);
  if (cuda_driver_state != CudaDriverState::kUninitialized) {
    return cuda_driver_state == CudaDriverState::kInitialized
               ? absl::OkStatus()
               : absl::UnavailableError(
                     "CUDA driver library not found or failed to initialize.");
  }

  CUresult result = cuInit(0);
  cuda_driver_state = result == CUDA_SUCCESS ? CudaDriverState::kInitialized
                                             : CudaDriverState::kFailed;

  if (result == CUDA_SUCCESS) {
    LOG(INFO) << "CUDA Driver API initialized successfully.";
    RETURN_IF_CUDA_ERROR(cuDeviceGetCount(&gpu_device_count));
    if (gpu_device_count == 0) {
      return absl::NotFoundError("No CUDA-capable device found.");
    }
    gpu_device_info.reserve(gpu_device_count);
    return absl::OkStatus();
  } else if (result == CUDA_ERROR_NO_DEVICE) {
    LOG(WARNING) << "CUDA driver loaded, but no NVIDIA GPU detected.";
    gpu_device_count = 0;
    return absl::NotFoundError("No CUDA-capable device found.");
  } else if (result == CUDA_ERROR_SHARED_OBJECT_INIT_FAILED) {
    LOG(WARNING) << "Failed to load libcuda.so. CUDA Driver API not available.";
    return absl::NotFoundError("Failed to load libcuda.so.");
  } else {
    const char* error_string = nullptr;
    cuGetErrorString(result, &error_string);
    LOG(WARNING) << "Failed to initialize CUDA Driver API: " << error_string
                 << " (Code: " << result << ")";
    return absl::InternalError(absl::StrCat("cuInit failed: ", error_string));
  }
}

// Probe GPU device properties such as PCI
// bus address, dma buf capabilities, etc.
absl::StatusOr<GpuDeviceInfo> InitializeGpu(int device_index) {
  CUdevice cu_device;
  RETURN_IF_CUDA_ERROR(cuDeviceGet(&cu_device, device_index));

  char device_name[128] = {};
  RETURN_IF_CUDA_ERROR(
      cuDeviceGetName(device_name, sizeof(device_name), cu_device));

  int device_id;
  RETURN_IF_CUDA_ERROR(cuDeviceGetAttribute(
      &device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_device));

  int pci_bus_id;
  RETURN_IF_CUDA_ERROR(cuDeviceGetAttribute(
      &pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, cu_device));

  CUcontext cu_context;
  RETURN_IF_CUDA_ERROR(cuDevicePrimaryCtxRetain(&cu_context, cu_device));

  int has_dmabuf;
  RETURN_IF_CUDA_ERROR(cuDeviceGetAttribute(
      &has_dmabuf, CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED, cu_device));

  LOG(INFO) << "Initialized GPU: " << device_index
            << " device name: " << device_name << " device id: " << device_id
            << " PCI bus id: " << pci_bus_id
            << " DMA buf supported: " << has_dmabuf;

  return GpuDeviceInfo{
      .device_index = device_index,
      .device_name = device_name,
      .device_id = device_id,
      .pci_bus_id = pci_bus_id,
      .has_dmabuf = static_cast<bool>(has_dmabuf),
      .cu_device = cu_device,
      .cu_context = cu_context,
  };
}

absl::Status InitializeGpus() {
  for (int i = 0; i < gpu_device_count; ++i) {
    ASSIGN_OR_RETURN(GpuDeviceInfo device_info, InitializeGpu(i));
    gpu_device_info.push_back(std::move(device_info));
  }
  return absl::OkStatus();
}

absl::Status TearDownGpus() {
  absl::MutexLock lock(cuda_init_mutex);
  if (cuda_driver_state != CudaDriverState::kInitialized) {
    return absl::OkStatus();
  }
  for (const auto& device_info : gpu_device_info) {
    RETURN_IF_CUDA_ERROR(cuDevicePrimaryCtxRelease(device_info.cu_device));
  }
  gpu_device_info.clear();
  return absl::OkStatus();
}

bool IsCudaDriverAvailable() {
  absl::MutexLock lock(cuda_init_mutex);
  return cuda_driver_state == CudaDriverState::kInitialized;
}

absl::StatusOr<int> GetGpuDeviceCount() {
  absl::MutexLock lock(cuda_init_mutex);
  if (gpu_device_count == -1) {
    return absl::InternalError("CUDA driver not initialized");
  }
  return gpu_device_count;
}

absl::StatusOr<const GpuDeviceInfo*> GetGpuDeviceInfo(int device_index) {
  if (!IsCudaDriverAvailable()) {
    return absl::UnavailableError(
        "CUDA driver not initialized or no CUDA-capable device found.");
  }
  if (device_index < 0 || device_index >= gpu_device_info.size()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Device index out of bounds: ", device_index));
  }
  return &gpu_device_info[device_index];
}

absl::StatusOr<void*> AllocateGpuMemory(int64_t size,
                                        const GpuDeviceInfo& gpu_device_info) {
  if (!IsCudaDriverAvailable()) {
    return absl::UnavailableError(
        "CUDA driver not initialized or no CUDA-capable device found.");
  }

  if (size % kGpuPageSize != 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Size must be a multiple of GPU page size: ", kGpuPageSize));
  }

  ASSIGN_OR_RETURN(std::unique_ptr<ScopedCudaContext> cuda_context,
                   ScopedCudaContext::Create(gpu_device_info.cu_context));

  CUdeviceptr d_ptr;
  RETURN_IF_CUDA_ERROR(cuMemAlloc(&d_ptr, size));
  return reinterpret_cast<void*>(d_ptr);
}

absl::Status FreeGpuMemory(void* ptr, const GpuDeviceInfo& gpu_device_info) {
  if (!IsCudaDriverAvailable()) {
    return absl::UnavailableError(
        "CUDA driver not initialized or no CUDA-capable device found.");
  }

  ASSIGN_OR_RETURN(std::unique_ptr<ScopedCudaContext> cuda_context,
                   ScopedCudaContext::Create(gpu_device_info.cu_context));

  RETURN_IF_CUDA_ERROR(cuMemFree(reinterpret_cast<CUdeviceptr>(ptr)));
  return absl::OkStatus();
}

absl::StatusOr<int> GetGpuMemoryDmaBufFd(void* ptr, int64_t size,
                                         const GpuDeviceInfo& gpu_device_info) {
  if (!IsCudaDriverAvailable()) {
    return absl::UnavailableError(
        "CUDA driver not initialized or no CUDA-capable device found.");
  }

  ASSIGN_OR_RETURN(std::unique_ptr<ScopedCudaContext> cuda_context,
                   ScopedCudaContext::Create(gpu_device_info.cu_context));

  CUdeviceptr cu_ptr = reinterpret_cast<CUdeviceptr>(ptr);
  int fd;
  RETURN_IF_CUDA_ERROR(cuMemGetHandleForAddressRange(
      &fd, cu_ptr, size, CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0));
  return fd;
}

// Helper to copy from host to GPU
absl::Status CopyHostToGpu(CUdeviceptr gpu_ptr, const void* host_ptr,
                           int64_t size, const GpuDeviceInfo& gpu_device_info) {
  ASSIGN_OR_RETURN(std::unique_ptr<ScopedCudaContext> cuda_context,
                   ScopedCudaContext::Create(gpu_device_info.cu_context));

  RETURN_IF_CUDA_ERROR(cuMemcpyHtoD(gpu_ptr, host_ptr, size));
  return absl::OkStatus();
}

// Helper to copy from GPU to host
absl::Status CopyGpuToHost(void* host_ptr, CUdeviceptr gpu_ptr, int64_t size,
                           const GpuDeviceInfo& gpu_device_info) {
  ASSIGN_OR_RETURN(std::unique_ptr<ScopedCudaContext> cuda_context,
                   ScopedCudaContext::Create(gpu_device_info.cu_context));

  RETURN_IF_CUDA_ERROR(cuMemcpyDtoH(host_ptr, gpu_ptr, size));
  return absl::OkStatus();
}

}  // namespace cuda
}  // namespace rdma_unit_test
