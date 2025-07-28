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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/barrier.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"

#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

using ::testing::NotNull;

// This is a test fixture for simple multithreaded test, such as
// creating/destroying MR/MW/PD/CQ/etc in multiple threads. For more complex
// multithreaded test, we still put them in the test files corresponding to each
// individual resource being test (e.g. mw_test.cc).
class ThreadedTest : public RdmaVerbsFixture {
 protected:
  static constexpr int kThreadCount = 5;
  static constexpr int kResourcePerThread = 100;

  // A simple thread pool. Supports Add and JoinAll.
  class ThreadPool {
   public:
    // Adds a thread to the pool. The thread will run the function f.
    template <typename Func>
    void Add(Func f) {
      pool.push_back(std::thread(f));
    }

    // Joins all the threads in the pool.
    void JoinAll() {
      for (auto& thread : pool) {
        thread.join();
      }
    }

   private:
    std::vector<std::thread> pool;
  };

  struct BasicSetup {
    ibv_context* context;
    PortAttribute port_attr;
    RdmaMemBlock buffer;
    ibv_pd* pd;
    ibv_cq* cq;
    ibv_mr* mr;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.cq = ibv_.CreateCq(setup.context);
    if (!setup.cq) {
      return absl::InternalError("Failed to create cq.");
    }
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    setup.buffer = ibv_.AllocBuffer(/*pages=*/1);
    return setup;
  }
};

TEST_F(ThreadedTest, CreateDestroyAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kAhPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_ah / kThreadCount);
  LOG(INFO) << "Creating " << kAhPerThread << " AHs per thread on "
            << kThreadCount << "threads.";
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kAhPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &results, kAhPerThread]() {
      for (int i = 0; i < kAhPerThread; ++i) {
        ibv_ah_attr ah_attr = AhAttribute().GetAttribute(
            setup.port_attr.port, setup.port_attr.gid_index,
            setup.port_attr.gid);
        ibv_ah* ah = ibv_.extension().CreateAh(setup.pd, ah_attr);
        ASSERT_THAT(ah, NotNull());
        results[thread_id][i] = ibv_destroy_ah(ah);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, CreateDestroyCq) {
  const size_t kCqPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_cq / kThreadCount);
  ThreadPool pool;
  LOG(INFO) << "Creating " << kCqPerThread << " Cqs per thread on "
            << kThreadCount << "threads.";
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kCqPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results, kCqPerThread]() {
      for (int i = 0; i < kCqPerThread; ++i) {
        ibv_cq* cq = ibv_create_cq(setup.context, 10, nullptr, nullptr, 0);
        ASSERT_THAT(cq, NotNull());
        results[thread_id][i] = ibv_destroy_cq(cq);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, CreateDestroyCqEx) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kCqPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_cq / kThreadCount);
  LOG(INFO) << "Creating " << kCqPerThread << " Extended Cqs per thread on "
            << kThreadCount << "threads.";
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kCqPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results, kCqPerThread]() {
      for (int i = 0; i < kCqPerThread; ++i) {
        ibv_cq_init_attr_ex cq_attr{.cqe = 10};
        ibv_cq_ex* cq = ibv_create_cq_ex(setup.context, &cq_attr);
        ASSERT_THAT(cq, NotNull());
        results[thread_id][i] = ibv_destroy_cq(ibv_cq_ex_to_cq(cq));
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, AllocDeallocPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kPdPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_pd / kThreadCount);
  LOG(INFO) << "Allocating " << kPdPerThread << " PDs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kPdPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results, kPdPerThread]() {
      for (int i = 0; i < kPdPerThread; ++i) {
        ibv_pd* pd = ibv_alloc_pd(setup.context);
        ASSERT_THAT(pd, NotNull());
        results[thread_id][i] = ibv_dealloc_pd(pd);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, RegDeregMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kMrPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_mr / kThreadCount);
  LOG(INFO) << "Registering " << kMrPerThread << " Mrs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kMrPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &results, kMrPerThread]() {
      for (int i = 0; i < kMrPerThread; ++i) {
        ibv_mr* mr = ibv_.extension().RegMr(
            setup.pd, setup.buffer,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC |
                IBV_ACCESS_MW_BIND);
        ASSERT_THAT(mr, NotNull());
        results[thread_id][i] = ibv_dereg_mr(mr);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, AllocDeallocMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kMwPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_mw / kThreadCount);
  LOG(INFO) << "Allocating " << kMwPerThread << " MWs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kMwPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results, kMwPerThread]() {
      for (int i = 0; i < kMwPerThread; ++i) {
        ibv_mw_type type = IBV_MW_TYPE_1;
        if (Introspection().SupportsType2() && (i % 2 == 0)) {
          type = IBV_MW_TYPE_2;
        }
        ibv_mw* mw = ibv_alloc_mw(setup.pd, type);
        ASSERT_THAT(mw, NotNull());
        results[thread_id][i] = ibv_dealloc_mw(mw);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, CreateDestroyQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kQpPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_qp / kThreadCount);
  LOG(INFO) << "Creating " << kQpPerThread << " Qps per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kQpPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &results, kQpPerThread]() {
      for (int i = 0; i < kQpPerThread; ++i) {
        ibv_qp_init_attr attr =
            QpInitAttribute().GetAttribute(setup.cq, setup.cq, IBV_QPT_RC);
        ibv_qp* qp = ibv_.extension().CreateQp(setup.pd, attr);
        ASSERT_THAT(qp, NotNull());
        results[thread_id][i] = ibv_destroy_qp(qp);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, CreateDestroySrq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kSrqPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_srq / kThreadCount);
  std::vector<std::vector<int>> results(kThreadCount,
                                        std::vector<int>(kSrqPerThread, 1));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results, kSrqPerThread]() {
      for (int i = 0; i < kSrqPerThread; ++i) {
        ibv_srq_init_attr init_attr{.attr = verbs_util::DefaultSrqAttr()};
        ibv_srq* srq = ibv_create_srq(setup.pd, &init_attr);
        ASSERT_THAT(srq, NotNull());
        results[thread_id][i] = ibv_destroy_srq(srq);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_results : results) {
    for (const auto& result : thread_results) {
      EXPECT_EQ(result, 0);
    }
  }
}

TEST_F(ThreadedTest, CreateAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kAhPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_ah / kThreadCount);
  LOG(INFO) << "Creating " << kAhPerThread << " AHs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_ah*>> ahs(
      kThreadCount, std::vector<ibv_ah*>(kAhPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &ahs, kAhPerThread]() {
      for (int i = 0; i < kAhPerThread; ++i) {
        ibv_ah_attr ah_attr = AhAttribute().GetAttribute(
            setup.port_attr.port, setup.port_attr.gid_index,
            setup.port_attr.gid);
        ahs[thread_id][i] = ibv_.extension().CreateAh(setup.pd, ah_attr);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_ahs : ahs) {
    for (const auto& ah : thread_ahs) {
      ASSERT_THAT(ah, NotNull());
      ASSERT_EQ(ibv_destroy_ah(ah), 0);
    }
  }
}

TEST_F(ThreadedTest, CreateCq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kCqPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_cq / kThreadCount);
  LOG(INFO) << "Creating " << kCqPerThread << " Cqs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_cq*>> cqs(
      kThreadCount, std::vector<ibv_cq*>(kCqPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &cqs, kCqPerThread]() {
      for (int i = 0; i < kCqPerThread; ++i) {
        cqs[thread_id][i] =
            ibv_create_cq(setup.context, 10, nullptr, nullptr, 0);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_cqs : cqs) {
    for (const auto& cq : thread_cqs) {
      ASSERT_THAT(cq, NotNull());
      ASSERT_EQ(ibv_destroy_cq(cq), 0);
    }
  }
}

TEST_F(ThreadedTest, CreateCqEx) {
  if (!Introspection().SupportsExtendedCqs()) {
    GTEST_SKIP();
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kCqPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_cq / kThreadCount);
  LOG(INFO) << "Creating " << kCqPerThread << " Extended Cqs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_cq_ex*>> cqs_ex(
      kThreadCount, std::vector<ibv_cq_ex*>(kCqPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &cqs_ex, kCqPerThread]() {
      for (int i = 0; i < kCqPerThread; ++i) {
        ibv_cq_init_attr_ex cq_attr{.cqe = 10};
        cqs_ex[thread_id][i] = ibv_create_cq_ex(setup.context, &cq_attr);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_cqs : cqs_ex) {
    for (const auto& cq : thread_cqs) {
      ASSERT_THAT(cq, NotNull());
      ASSERT_EQ(ibv_destroy_cq(ibv_cq_ex_to_cq(cq)), 0);
    }
  }
}

TEST_F(ThreadedTest, AllocPd) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kPdPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_pd / kThreadCount);
  LOG(INFO) << "Allocating " << kPdPerThread << " PDs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_pd*>> pds(
      kThreadCount, std::vector<ibv_pd*>(kPdPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &pds, kPdPerThread]() {
      for (int i = 0; i < kPdPerThread; ++i) {
        pds[thread_id][i] = ibv_alloc_pd(setup.context);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_pds : pds) {
    for (const auto& pd : thread_pds) {
      ASSERT_THAT(pd, NotNull());
      ASSERT_EQ(ibv_dealloc_pd(pd), 0);
    }
  }
}

TEST_F(ThreadedTest, RegMr) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kMrPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_mr / kThreadCount);
  LOG(INFO) << "Creating " << kMrPerThread << " MRs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_mr*>> mrs(
      kThreadCount, std::vector<ibv_mr*>(kMrPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &mrs]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
        mrs[thread_id][i] = ibv_.extension().RegMr(
            setup.pd, setup.buffer,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC |
                IBV_ACCESS_MW_BIND);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_mrs : mrs) {
    for (const auto& mr : thread_mrs) {
      ASSERT_THAT(mr, NotNull());
      ASSERT_EQ(ibv_dereg_mr(mr), 0);
    }
  }
}

TEST_F(ThreadedTest, AllocMw) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kMwPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_mw / kThreadCount);
  LOG(INFO) << "Allocating " << kMwPerThread << " MWs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_mw*>> mws(
      kThreadCount, std::vector<ibv_mw*>(kMwPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &mws, kMwPerThread]() {
      for (int i = 0; i < kMwPerThread; ++i) {
        ibv_mw_type type = IBV_MW_TYPE_1;
        if (Introspection().SupportsType2() && (i % 2 == 0)) {
          type = IBV_MW_TYPE_2;
        }
        mws[thread_id][i] = ibv_alloc_mw(setup.pd, type);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_mws : mws) {
    for (const auto& mw : thread_mws) {
      ASSERT_THAT(mw, NotNull());
      ASSERT_EQ(ibv_dealloc_mw(mw), 0);
    }
  }
}

TEST_F(ThreadedTest, CreateQp) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kQpPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_qp / kThreadCount);
  LOG(INFO) << "Creating " << kQpPerThread << " Qps per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_qp*>> qps(
      kThreadCount, std::vector<ibv_qp*>(kQpPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &qps, kQpPerThread]() {
      for (int i = 0; i < kQpPerThread; ++i) {
        ibv_qp_init_attr attr =
            QpInitAttribute().GetAttribute(setup.cq, setup.cq, IBV_QPT_RC);
        qps[thread_id][i] = ibv_.extension().CreateQp(setup.pd, attr);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_qps : qps) {
    for (const auto& qp : thread_qps) {
      ASSERT_THAT(qp, NotNull());
      ASSERT_EQ(ibv_destroy_qp(qp), 0);
    }
  }
}

TEST_F(ThreadedTest, CreateSrq) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  const size_t kSrqPerThread = std::min(
      kResourcePerThread, Introspection().device_attr().max_srq / kThreadCount);
  LOG(INFO) << "Creating " << kSrqPerThread << " Srqs per thread on "
            << kThreadCount << " threads.";
  std::vector<std::vector<ibv_srq*>> srqs(
      kThreadCount, std::vector<ibv_srq*>(kSrqPerThread, nullptr));

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &srqs, kSrqPerThread]() {
      for (int i = 0; i < kSrqPerThread; ++i) {
        ibv_srq_init_attr init_attr{.attr = verbs_util::DefaultSrqAttr()};
        srqs[thread_id][i] = ibv_create_srq(setup.pd, &init_attr);
      }
    });
  }
  pool.JoinAll();

  for (const auto& thread_srqs : srqs) {
    for (const auto& srq : thread_srqs) {
      ASSERT_THAT(srq, NotNull());
      ASSERT_THAT(ibv_destroy_srq(srq), 0);
    }
  }
}

class ThreadedWorkloadTest : public ThreadedTest {
 protected:
  struct WorkloadGeometry {
    int num_threads;
    int num_iters;
    int num_reads;
  };
  static constexpr WorkloadGeometry kFastNicGeometry = {
      .num_threads = 256, .num_iters = 64, .num_reads = 2048};
  static constexpr WorkloadGeometry kSlowNicGeometry = {
      .num_threads = 8, .num_iters = 8, .num_reads = 8};

  WorkloadGeometry GetWorkloadGeometry() {
    if (Introspection().IsSlowNic()) {
      return kSlowNicGeometry;
    }
    return kFastNicGeometry;
  }

  absl::Status IssueRead(BasicSetup& setup, ibv_qp* qp, ibv_cq* cq) {
    ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
    ibv_send_wr wr = verbs_util::CreateRdmaWr(
        IBV_WR_RDMA_READ, 0xDEADBEEF, &sge, /*num_sge=*/1, setup.buffer.data(),
        setup.mr->rkey);
    verbs_util::PostSend(qp, wr);

    ASSIGN_OR_RETURN(
        ibv_wc completion,
        verbs_util::WaitForCompletion(cq, verbs_util::kDefaultCompletionTimeout,
                                      absl::ZeroDuration()));
    if (completion.status != IBV_WC_SUCCESS) {
      return absl::InternalError(
          absl::StrCat("Unexpected completion status: ", completion.status));
    }
    return absl::OkStatus();
  }

  absl::Status MaintainReadBacklog(BasicSetup& setup, ibv_qp* qp, ibv_cq* cq,
                                   uint64_t depth,
                                   std::atomic<uint64_t>& curr_backlog) {
    ibv_sge sge = verbs_util::CreateSge(setup.buffer.span(), setup.mr);
    ibv_send_wr wr = verbs_util::CreateRdmaWr(
        IBV_WR_RDMA_READ, 0xDEADBEEF, &sge, /*num_sge=*/1, setup.buffer.data(),
        setup.mr->rkey);

    while (true) {
      while (curr_backlog < depth) {
        ibv_send_wr* bad_wr = nullptr;
        if (ibv_post_send(qp, &wr, &bad_wr)) {
          return absl::InternalError(
              absl::StrCat("Unexpected error posting send"));
        }
        curr_backlog++;
      }
      ASSIGN_OR_RETURN(ibv_wc completion,
                       verbs_util::WaitForCompletion(cq, absl::Seconds(5),
                                                     absl::ZeroDuration()));
      if (completion.status != IBV_WC_SUCCESS) {
        return absl::InternalError(
            absl::StrCat("Unexpected completion status: ", completion.status));
      }
      curr_backlog--;
    }
  }
};

TEST_F(ThreadedWorkloadTest, QpLifecycle) {
  const WorkloadGeometry geometry = GetWorkloadGeometry();

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(setup.mr, NotNull());

  ThreadPool pool;
  absl::Barrier thundering_herd(geometry.num_threads);

  for (int thread_id = 0; thread_id < geometry.num_threads; ++thread_id) {
    pool.Add([&]() {
      thundering_herd.Block();
      for (int i = 0; i < geometry.num_iters; ++i) {
        ibv_cq* cq = ibv_.CreateCq(setup.context);
        ASSERT_THAT(cq, NotNull());
        ibv_qp* local = ibv_.CreateQp(setup.pd, cq);
        ASSERT_THAT(local, NotNull());
        ibv_qp* remote = ibv_.CreateQp(setup.pd, cq);
        ASSERT_THAT(remote, NotNull());
        ASSERT_OK(ibv_.SetUpLoopbackRcQps(remote, local, setup.port_attr));
        for (int j = 0; j < geometry.num_reads; ++j) {
          ASSERT_OK(IssueRead(setup, local, cq));
        }
        ASSERT_EQ(ibv_.DestroyQp(local), 0);
        ASSERT_EQ(ibv_.DestroyQp(remote), 0);
        ASSERT_EQ(ibv_.DestroyCq(cq), 0);
      }
    });
  }

  pool.JoinAll();
}

// Create a group of threads that continuously perform RDMA READs on a loopback
// QP pair. Then, while this is running, delete the target QP.
// Additionally, run a group of canary threads to ensure that unrelated
// QPs continue to function as expected.
TEST_F(ThreadedWorkloadTest, QpDestroyTargetWhileActive) {
  const int kNumCanaryThreads = 1;
  const int kNumDeleteThreads = Introspection().IsSlowNic() ? 1 : 8;
  const int kBacklogDepth = 100;
  const absl::Duration kDeleteCycleDuration = absl::Seconds(30);

  LOG(INFO) << "Running QpDestroyTargetWhileActive with " << kNumCanaryThreads
            << " canary threads and " << kNumDeleteThreads
            << " delete threads.";

  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  setup.mr = ibv_.RegMr(setup.pd, setup.buffer);
  ASSERT_THAT(setup.mr, NotNull());

  std::atomic<bool> terminate = false;

  ThreadPool canary_pool;
  absl::Barrier canary_barrier(kNumCanaryThreads + 1);
  for (int thread_id = 0; thread_id < kNumCanaryThreads; ++thread_id) {
    canary_pool.Add([&]() {
      ibv_cq* cq = ibv_.CreateCq(setup.context);
      ASSERT_THAT(cq, NotNull());
      ibv_qp* local = ibv_.CreateQp(setup.pd, cq);
      ASSERT_THAT(local, NotNull());
      ibv_qp* remote = ibv_.CreateQp(setup.pd, cq);
      ASSERT_THAT(remote, NotNull());
      ASSERT_OK(ibv_.SetUpLoopbackRcQps(remote, local, setup.port_attr));
      canary_barrier.Block();
      while (!terminate) {
        ASSERT_OK(IssueRead(setup, local, cq));
        // This is not a performance test...
        absl::SleepFor(absl::Milliseconds(1));
      }
      ASSERT_EQ(ibv_.DestroyQp(local), 0);
      ASSERT_EQ(ibv_.DestroyQp(remote), 0);
      ASSERT_EQ(ibv_.DestroyCq(cq), 0);
    });
  }

  // Make an attempt to ensure that the canary threads are running before
  // starting the delete threads. Best effort; not critical.
  canary_barrier.Block();

  ThreadPool delete_pool;
  std::atomic<uint64_t> target_deletions = 0;
  for (int thread_id = 0; thread_id < kNumDeleteThreads; ++thread_id) {
    canary_pool.Add([&]() {
      while (!terminate) {
        ibv_cq* cq = ibv_.CreateCq(setup.context);
        ASSERT_THAT(cq, NotNull());
        ibv_qp* local = ibv_.CreateQp(setup.pd, cq);
        ASSERT_THAT(local, NotNull());
        ibv_qp* remote = ibv_.CreateQp(setup.pd, cq);
        ASSERT_THAT(remote, NotNull());
        ASSERT_OK(ibv_.SetUpLoopbackRcQps(remote, local, setup.port_attr));

        std::atomic<uint64_t> curr_backlog = 0;
        std::thread thread([&]() {
          // Do a single read to ensure the QP is ready to go.
          ASSERT_OK(IssueRead(setup, local, cq));
          // Read until failure.
          MaintainReadBacklog(setup, local, cq, kBacklogDepth, curr_backlog)
              .IgnoreError();
        });

        // Wait for transfers to be running before deleting the QP (best effort)
        // If MaintainReadBacklog() fails before the first send, then this
        // will spin until the test times out.
        do {
          absl::SleepFor(absl::Microseconds(10));
        } while (!curr_backlog);

        // Delete target QP.
        ASSERT_EQ(ibv_.DestroyQp(remote), 0);

        // Wait for sending thread to exit as a result of QP deletion.
        thread.join();

        ASSERT_EQ(ibv_.DestroyQp(local), 0);
        ASSERT_EQ(ibv_.DestroyCq(cq), 0);
        target_deletions++;
      }
    });
  }

  absl::SleepFor(kDeleteCycleDuration);

  ASSERT_GT(target_deletions, 0);

  terminate = true;
  delete_pool.JoinAll();
  canary_pool.JoinAll();
}

}  // namespace rdma_unit_test
