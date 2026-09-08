#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/gloop/util/status/status_macros.h"
#include "infiniband/verbs.h"
#include "public/cuda_helper.h"
#include "public/flags.h"
#include "public/gpu_rdma_memblock.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {
namespace cuda {

using ::testing::IsNull;
using ::testing::NotNull;

class GpuMrTest : public RdmaVerbsFixture {
 protected:
  struct IbvResources {
    ibv_context* context;
    ibv_pd* pd;
  };

  // These are allowed access flags for DMA buf memory regions according to:
  // https://man7.org/linux/man-pages/man3/ibv_reg_mr.3.html
  static constexpr int kAllowedDmaBufAccessFlags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
      IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC |
      IBV_ACCESS_RELAXED_ORDERING;

  int64_t buffer_size_ = cuda::kGpuPageSize;
  int gpu_device_count_ = 0;

  static void SetUpTestSuite() {
    RdmaVerbsFixture::SetUpTestSuite();
    CHECK_EQ(absl::GetFlag(FLAGS_enable_cuda), true)
        << "CUDA tests not enabled.";
    CHECK_OK(cuda::InitializeCudaDriver())
        << "Failed to initialize CUDA driver.";
  }

  static void TearDownTestSuite() { RdmaVerbsFixture::TearDownTestSuite(); }

  void SetUp() override {
    RdmaVerbsFixture::SetUp();
    ASSERT_OK(cuda::InitializeGpus()) << "Failed to initialize all GPUs.";
    ASSERT_OK_AND_ASSIGN(gpu_device_count_, cuda::GetGpuDeviceCount());
    ASSERT_GT(gpu_device_count_, 0) << "No CUDA-capable device found.";
  }

  void TearDown() override {
    ASSERT_OK(cuda::TearDownGpus()) << "Failed to tear down all GPUs.";
    RdmaVerbsFixture::TearDown();
  }

  absl::StatusOr<IbvResources> CreateBasicSetup() {
    IbvResources setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    return setup;
  }
};

TEST_F(GpuMrTest, AllocateGpuMemory) {
  for (int gpu_index = 0; gpu_index < gpu_device_count_; ++gpu_index) {
    ASSERT_OK_AND_ASSIGN(auto gpu_device_info,
                         cuda::GetGpuDeviceInfo(gpu_index));
    GpuRdmaMemBlock gpu_memblock(buffer_size_, gpu_device_info);
    // Check that the memory allocation is not null and dma-buf is valid.
    ASSERT_NE(gpu_memblock.data(), nullptr);
    ASSERT_GT(gpu_memblock.GetFd(), 0);

    // The GPU memory is automatically freed when the GpuRdmaMemBlock goes out
    // of scope.
  }
}

TEST_F(GpuMrTest, RegisterGpuMemory) {
  ASSERT_OK_AND_ASSIGN(GpuMrTest::IbvResources setup, CreateBasicSetup());

  for (int gpu_index = 0; gpu_index < gpu_device_count_; ++gpu_index) {
    ASSERT_OK_AND_ASSIGN(auto gpu_device_info,
                         cuda::GetGpuDeviceInfo(gpu_index));
    GpuRdmaMemBlock gpu_memblock(buffer_size_, gpu_device_info);
    ASSERT_NE(gpu_memblock.data(), nullptr);
    ASSERT_GT(gpu_memblock.GetFd(), 0);

    ibv_mr* mr = ibv_.RegMr(setup.pd, gpu_memblock, kAllowedDmaBufAccessFlags);
    ASSERT_THAT(mr, NotNull());
    EXPECT_EQ(mr->pd, setup.pd);
    // GPU memory backed mr has nullptr as addr. We can't verify if the mr is
    // mapped to the GPU memory easily without traffic.
    // EXPECT_EQ(mr->addr, gpu_memblock.data());
    EXPECT_EQ(mr->length, gpu_memblock.size());
    EXPECT_NE(mr->lkey, 0);
    EXPECT_NE(mr->rkey, 0);

    EXPECT_EQ(ibv_.DeregMr(mr), 0);

    // The GPU memory is automatically freed when the GpuRdmaMemBlock goes out
    // of scope.
  }
}

TEST_F(GpuMrTest, ReregisterGpuMemory) {
  ASSERT_OK_AND_ASSIGN(GpuMrTest::IbvResources setup, CreateBasicSetup());

  for (int gpu_index = 0; gpu_index < gpu_device_count_; ++gpu_index) {
    ASSERT_OK_AND_ASSIGN(auto gpu_device_info,
                         cuda::GetGpuDeviceInfo(gpu_index));
    GpuRdmaMemBlock gpu_memblock(buffer_size_, gpu_device_info);
    ASSERT_NE(gpu_memblock.data(), nullptr);
    ASSERT_GT(gpu_memblock.GetFd(), 0);

    // Initial Registration
    int current_access = IBV_ACCESS_LOCAL_WRITE;
    ibv_mr* mr = ibv_.RegMr(setup.pd, gpu_memblock, current_access);
    ASSERT_THAT(mr, NotNull());
    EXPECT_EQ(mr->pd, setup.pd);
    EXPECT_EQ(mr->length, gpu_memblock.size());
    uint32_t current_lkey = mr->lkey;
    EXPECT_NE(current_lkey, 0);

    // Reregister to change access flags -- This should fail
    int new_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    int ret = ibv_.ReregMr(mr, IBV_REREG_MR_CHANGE_ACCESS, setup.pd,
                           &gpu_memblock, new_access);

    EXPECT_NE(ret, 0)
        << "Should not be able to change access flags for DMA_BUF MR";
    if (ret == 0) {
      current_access = new_access;
    }

    // Reregister to change PD - This should fail
    ibv_pd* other_pd = ibv_.AllocPd(setup.context);
    ASSERT_THAT(other_pd, NotNull()) << "Failed to allocate other pd.";
    ret = ibv_.ReregMr(mr, IBV_REREG_MR_CHANGE_PD, other_pd, &gpu_memblock,
                       kAllowedDmaBufAccessFlags);
    EXPECT_NE(ret, 0) << "Should not be able to change PD for DMA_BUF MR";
    if (ret != 0) {
      ret = ibv_.DeallocPd(other_pd);
      ASSERT_EQ(ret, 0) << "Failed to deallocate other pd.";
    }
    EXPECT_EQ(ibv_.DeregMr(mr), 0);
  }
}

TEST_F(GpuMrTest, RegisterGpuMemoryWithUnsupportedAccessFlags) {
  ASSERT_OK_AND_ASSIGN(GpuMrTest::IbvResources setup, CreateBasicSetup());

  for (int gpu_index = 0; gpu_index < gpu_device_count_; ++gpu_index) {
    ASSERT_OK_AND_ASSIGN(auto gpu_device_info,
                         cuda::GetGpuDeviceInfo(gpu_index));
    GpuRdmaMemBlock gpu_memblock(buffer_size_, gpu_device_info);
    ASSERT_NE(gpu_memblock.data(), nullptr);
    ASSERT_GT(gpu_memblock.GetFd(), 0);

    // All access flags other than kAllowedDmaBufAccessFlags are not supported
    // for DMA buf memory regions. By adding IBV_ACCESS_MW_BIND, the
    // registration should fail and return nullptr.
    ibv_mr* mr = ibv_.RegMr(setup.pd, gpu_memblock,
                            kAllowedDmaBufAccessFlags | IBV_ACCESS_MW_BIND);
    ASSERT_THAT(mr, IsNull());
  }
}

}  // namespace cuda
}  // namespace rdma_unit_test
