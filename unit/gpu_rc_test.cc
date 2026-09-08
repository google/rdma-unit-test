#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/gloop/util/status/ret_check.h"
#include "third_party/gpus/cuda/include/cuda.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/cuda_helper.h"
#include "public/flags.h"
#include "public/gpu_rdma_memblock.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/loopback_fixture.h"

namespace rdma_unit_test {
namespace cuda {

using ::testing::TestParamInfo;
using ::testing::ValuesIn;
using ::testing::WithParamInterface;

// Helper to fill host memory with a pattern
void FillHostBuffer(void* buffer, size_t size, uint8_t value) {
  memset(buffer, value, size);
}

void InitMem(void* host_ptr, CUdeviceptr gpu_ptr, bool is_gpu, size_t size,
             uint8_t value, const GpuDeviceInfo& gpu_device_info) {
  std::vector<uint8_t> temp(size, value);
  if (is_gpu) {
    ASSERT_OK(CopyHostToGpu(gpu_ptr, temp.data(), size, gpu_device_info));
  } else {
    memcpy(host_ptr, temp.data(), size);
  }
}

// Data verification is done at the host memory in the host code. When
// destination is GPU memory, do a host transfer from GPU to host. This
// eliminates the need of CUDA kernel code (.cu).
void VerifyMem(void* host_ptr, CUdeviceptr gpu_ptr, bool is_gpu, size_t size,
               const std::vector<uint8_t>& expected,
               const GpuDeviceInfo& gpu_device_info) {
  std::vector<uint8_t> actual(size);
  if (is_gpu) {
    ASSERT_OK(CopyGpuToHost(actual.data(), gpu_ptr, size, gpu_device_info));
  } else {
    memcpy(actual.data(), host_ptr, size);
  }
  EXPECT_EQ(actual, expected);
}

// Enum to define memory transfer direction.
// It always tells the data flow direction. For RDMA Read, the first part means
// the remote memory type, and the second part is the local memory type.
enum class MemoryTransferType {
  GPU_TO_GPU,  // Src and dst GPU are different for multi-GPU systems.
  GPU_TO_HOST,
  HOST_TO_GPU
};

std::string MemoryTransferTypeToString(
    MemoryTransferType memory_transfer_type) {
  switch (memory_transfer_type) {
    case MemoryTransferType::GPU_TO_GPU:
      return "GpuToGpu";
    case MemoryTransferType::GPU_TO_HOST:
      return "GpuToHost";
    case MemoryTransferType::HOST_TO_GPU:
      return "HostToGpu";
  }
}

class GpuLoopbackRcQpTest : public LoopbackFixture,
                            public WithParamInterface<MemoryTransferType> {
 protected:
  struct GpuClient {
    Client base;
    int gpu_index;
    const cuda::GpuDeviceInfo* gpu_device_info;
    std::unique_ptr<GpuRdmaMemBlock> gpu_memblock;
    ibv_mr* gpu_mr = nullptr;
    std::unique_ptr<RdmaMemBlock> host_memblock;
    ibv_mr* host_mr = nullptr;

    explicit GpuClient(Client base) : base(std::move(base)) {};
    GpuClient(GpuClient&& other) = default;
    GpuClient& operator=(GpuClient&& other) = default;
    // Let VerbsHelperSuite handle the deregistration. Don't double free mr.
    ~GpuClient() = default;
  };

  // Helper struct to unify handling host and GPU buffers.
  struct TestBufferInfo {
    RdmaMemBlock* memblock;
    ibv_mr* mr;
    bool is_gpu;
    void* host_ptr;
    CUdeviceptr gpu_ptr;
    const GpuDeviceInfo* gpu_device_info;
  };

  TestBufferInfo GetTestBufferInfo(const GpuClient& client, bool use_gpu) {
    if (use_gpu) {
      return {client.gpu_memblock.get(), client.gpu_mr,         true, nullptr,
              GpuCuPtr(client),          client.gpu_device_info};
    } else {
      return {
          client.host_memblock.get(), client.host_mr, false, HostPtr(client), 0,
          client.gpu_device_info};
    }
  }

  // Initial content of the destination buffer before RDMA.
  static constexpr uint8_t kInitialContent = 0xEE;
  // Content of the source buffer. Used to verify RDMA operations on the
  // destination buffer.
  static constexpr uint8_t kTestSourceContent = 0xAA;

  static constexpr int kGpuMemoryAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
  static constexpr int kHostMemoryAccess =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

  // The buffer is large enough for the alignment requirement for host memory
  // (4KB). Currently it's fixed to 64KB.
  int64_t buffer_size_ = cuda::kGpuPageSize;
  int gpu_device_count_ = 0;

  static void SetUpTestSuite() {
    LoopbackFixture::SetUpTestSuite();
    CHECK_EQ(absl::GetFlag(FLAGS_enable_cuda), true)
        << "CUDA tests not enabled.";
    CHECK_OK(cuda::InitializeCudaDriver())
        << "CUDA or GPU failed to initialize.";
  }

  static void TearDownTestSuite() { LoopbackFixture::TearDownTestSuite(); }

  void SetUp() override {
    LoopbackFixture::SetUp();
    if (!Introspection().SupportsRcQp()) {
      GTEST_SKIP() << "Nic does not support RC QP";
    }

    ASSERT_OK(cuda::InitializeGpus()) << "Failed to initialize all GPUs.";

    ASSERT_OK_AND_ASSIGN(gpu_device_count_, cuda::GetGpuDeviceCount());
    ASSERT_GT(gpu_device_count_, 0) << "No CUDA-capable device found.";
    if (gpu_device_count_ < 2) {
      LOG(INFO) << "Only " << gpu_device_count_
                << " GPU available. Will use the same GPU 0 for both clients.";
    }
  }

  void TearDown() override {
    ASSERT_OK(cuda::TearDownGpus()) << "Failed to tear down all GPUs.";
    LoopbackFixture::TearDown();
  }

  absl::StatusOr<GpuClient> CreateGpuClient(int gpu_index = 0) {
    ASSIGN_OR_RETURN(Client base,
                     CreateClient(IBV_QPT_RC, 1, QpInitAttribute()));

    GpuClient client(std::move(base));
    client.gpu_index = gpu_index;
    ASSIGN_OR_RETURN(client.gpu_device_info, cuda::GetGpuDeviceInfo(gpu_index));

    client.gpu_memblock =
        std::make_unique<GpuRdmaMemBlock>(buffer_size_, client.gpu_device_info);
    RET_CHECK_NE(client.gpu_memblock->data(), nullptr)
        << "Failed to allocate GPU memory.";
    client.gpu_mr =
        ibv_.RegMr(client.base.pd, *client.gpu_memblock, kGpuMemoryAccess);
    RET_CHECK_NE(client.gpu_mr, nullptr) << "Failed to register GPU memory.";

    client.host_memblock = std::make_unique<RdmaMemBlock>(buffer_size_);
    RET_CHECK_NE(client.host_memblock->data(), nullptr)
        << "Failed to allocate host memory.";
    client.host_mr =
        ibv_.RegMr(client.base.pd, *client.host_memblock, kHostMemoryAccess);
    RET_CHECK_NE(client.host_mr, nullptr) << "Failed to register host memory.";
    return client;
  }

  absl::Status ConnectGpuClients(GpuClient& local, GpuClient& remote) {
    // One caveat whether the GID of the iRDMA device is using IPv4 or IPv6,
    // should match the usage in the test.
    RETURN_IF_ERROR(ibv_.ModifyRcQpResetToRts(
        local.base.qp, local.base.port_attr, remote.base.port_attr.gid,
        remote.base.qp->qp_num));
    RETURN_IF_ERROR(ibv_.ModifyRcQpResetToRts(
        remote.base.qp, remote.base.port_attr, local.base.port_attr.gid,
        local.base.qp->qp_num));
    return absl::OkStatus();
  }

  CUdeviceptr GpuCuPtr(const GpuClient& client) {
    return reinterpret_cast<CUdeviceptr>(client.gpu_memblock->data());
  }
  void* HostPtr(const GpuClient& client) {
    return client.host_memblock->data();
  }
};

TEST_P(GpuLoopbackRcQpTest, SendRecv) {
  MemoryTransferType memory_transfer_type = GetParam();
  ASSERT_OK_AND_ASSIGN(GpuClient local, CreateGpuClient(0));
  ASSERT_OK_AND_ASSIGN(GpuClient remote,
                       CreateGpuClient(gpu_device_count_ > 1 ? 1 : 0));
  ASSERT_OK(ConnectGpuClients(local, remote));

  bool local_is_gpu = (memory_transfer_type != MemoryTransferType::HOST_TO_GPU);
  bool remote_is_gpu =
      (memory_transfer_type != MemoryTransferType::GPU_TO_HOST);

  TestBufferInfo local_src = GetTestBufferInfo(local, local_is_gpu);
  TestBufferInfo remote_dest = GetTestBufferInfo(remote, remote_is_gpu);

  // Fill source and destination buffers with specific patterns for later
  // verification after RDMA.
  InitMem(local_src.host_ptr, local_src.gpu_ptr, local_src.is_gpu, buffer_size_,
          kTestSourceContent, *local_src.gpu_device_info);
  InitMem(remote_dest.host_ptr, remote_dest.gpu_ptr, remote_dest.is_gpu,
          buffer_size_, kInitialContent, *remote_dest.gpu_device_info);

  ibv_sge recv_sge =
      verbs_util::CreateSge(remote_dest.memblock->span(), remote_dest.mr);
  ibv_recv_wr recv_wr = verbs_util::CreateRecvWr(/*wr_id=*/0, &recv_sge, 1);
  verbs_util::PostRecv(remote.base.qp, recv_wr);

  ibv_sge send_sge =
      verbs_util::CreateSge(local_src.memblock->span(), local_src.mr);
  ibv_send_wr send_wr = verbs_util::CreateSendWr(/*wr_id=*/1, &send_sge, 1);
  verbs_util::PostSend(local.base.qp, send_wr);

  // Always poll both completions
  absl::StatusOr<ibv_wc> local_completion =
      verbs_util::WaitForCompletion(local.base.cq);
  absl::StatusOr<ibv_wc> remote_completion =
      verbs_util::WaitForCompletion(remote.base.cq);

  ASSERT_OK(local_completion) << "Local WaitForCompletion failed.";
  ASSERT_OK(remote_completion) << "Remote WaitForCompletion failed.";
  ASSERT_EQ(local_completion->status, IBV_WC_SUCCESS)
      << "Local completion status not IBV_WC_SUCCESS, status: "
      << local_completion->status;
  ASSERT_EQ(remote_completion->status, IBV_WC_SUCCESS)
      << "Remote completion status not IBV_WC_SUCCESS, status: "
      << remote_completion->status;

  std::vector<uint8_t> source_pattern(buffer_size_, kTestSourceContent);
  VerifyMem(remote_dest.host_ptr, remote_dest.gpu_ptr, remote_dest.is_gpu,
            buffer_size_, source_pattern, *remote_dest.gpu_device_info);
}

TEST_P(GpuLoopbackRcQpTest, RdmaWrite) {
  MemoryTransferType memory_transfer_type = GetParam();
  ASSERT_OK_AND_ASSIGN(GpuClient local, CreateGpuClient(0));
  ASSERT_OK_AND_ASSIGN(GpuClient remote,
                       CreateGpuClient(gpu_device_count_ > 1 ? 1 : 0));
  ASSERT_OK(ConnectGpuClients(local, remote));

  bool local_is_gpu = (memory_transfer_type != MemoryTransferType::HOST_TO_GPU);
  bool remote_is_gpu =
      (memory_transfer_type != MemoryTransferType::GPU_TO_HOST);

  TestBufferInfo local_src = GetTestBufferInfo(local, local_is_gpu);
  TestBufferInfo remote_dest = GetTestBufferInfo(remote, remote_is_gpu);

  InitMem(local_src.host_ptr, local_src.gpu_ptr, local_src.is_gpu, buffer_size_,
          kTestSourceContent, *local_src.gpu_device_info);
  InitMem(remote_dest.host_ptr, remote_dest.gpu_ptr, remote_dest.is_gpu,
          buffer_size_, kInitialContent, *remote_dest.gpu_device_info);

  ibv_sge send_sge =
      verbs_util::CreateSge(local_src.memblock->span(), local_src.mr);
  ibv_send_wr write_wr = verbs_util::CreateWriteWr(
      /*wr_id=*/1, &send_sge, 1, remote_dest.memblock->data(),
      remote_dest.mr->rkey);
  verbs_util::PostSend(local.base.qp, write_wr);

  ASSERT_OK_AND_ASSIGN(ibv_wc local_completion,
                       verbs_util::WaitForCompletion(local.base.cq));
  ASSERT_EQ(local_completion.status, IBV_WC_SUCCESS)
      << "Local completion status not IBV_WC_SUCCESS, status: "
      << local_completion.status;

  std::vector<uint8_t> source_pattern(buffer_size_, kTestSourceContent);
  VerifyMem(remote_dest.host_ptr, remote_dest.gpu_ptr, remote_dest.is_gpu,
            buffer_size_, source_pattern, *remote_dest.gpu_device_info);
}

TEST_P(GpuLoopbackRcQpTest, RdmaRead) {
  MemoryTransferType memory_transfer_type = GetParam();
  ASSERT_OK_AND_ASSIGN(GpuClient local, CreateGpuClient(0));
  ASSERT_OK_AND_ASSIGN(GpuClient remote,
                       CreateGpuClient(gpu_device_count_ > 1 ? 1 : 0));
  ASSERT_OK(ConnectGpuClients(local, remote));

  bool remote_src_is_gpu =
      (memory_transfer_type != MemoryTransferType::GPU_TO_HOST);
  bool local_dest_is_gpu =
      (memory_transfer_type == MemoryTransferType::GPU_TO_GPU ||
       memory_transfer_type == MemoryTransferType::HOST_TO_GPU);
  TestBufferInfo remote_src = GetTestBufferInfo(remote, remote_src_is_gpu);
  TestBufferInfo local_dest = GetTestBufferInfo(local, local_dest_is_gpu);

  InitMem(remote_src.host_ptr, remote_src.gpu_ptr, remote_src.is_gpu,
          buffer_size_, kTestSourceContent, *remote_src.gpu_device_info);
  InitMem(local_dest.host_ptr, local_dest.gpu_ptr, local_dest.is_gpu,
          buffer_size_, kInitialContent, *local_dest.gpu_device_info);

  ibv_sge send_sge =
      verbs_util::CreateSge(local_dest.memblock->span(), local_dest.mr);
  ibv_send_wr read_wr = verbs_util::CreateReadWr(
      /*wr_id=*/1, &send_sge, 1, remote_src.memblock->data(),
      remote_src.mr->rkey);
  verbs_util::PostSend(local.base.qp, read_wr);

  ASSERT_OK_AND_ASSIGN(ibv_wc local_completion,
                       verbs_util::WaitForCompletion(local.base.cq));
  ASSERT_EQ(local_completion.status, IBV_WC_SUCCESS)
      << "Local completion status not IBV_WC_SUCCESS, status: "
      << local_completion.status;

  std::vector<uint8_t> source_pattern(buffer_size_, kTestSourceContent);
  VerifyMem(local_dest.host_ptr, local_dest.gpu_ptr, local_dest.is_gpu,
            buffer_size_, source_pattern, *local_dest.gpu_device_info);
}

INSTANTIATE_TEST_SUITE_P(
    GpuRdmaTests, GpuLoopbackRcQpTest,
    ValuesIn({MemoryTransferType::GPU_TO_GPU, MemoryTransferType::GPU_TO_HOST,
              MemoryTransferType::HOST_TO_GPU}),
    [](const TestParamInfo<GpuLoopbackRcQpTest::ParamType>& info) {
      return MemoryTransferTypeToString(info.param);
    });

}  // namespace cuda
}  // namespace rdma_unit_test
