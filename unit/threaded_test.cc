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
#include <array>
#include <cstddef>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "internal/verbs_extension.h"
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ThreadPool pool;
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<int, kResourcePerThread>, kThreadCount> results;
  std::fill(results.front().begin(), results.back().end(), 1);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &results]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_ah*, kResourcePerThread>, kThreadCount> ahs;
  std::fill(ahs.front().begin(), ahs.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &ahs]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_cq*, kResourcePerThread>, kThreadCount> cqs;
  std::fill(cqs.front().begin(), cqs.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &cqs]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_cq_ex*, kResourcePerThread>, kThreadCount> cqs_ex;
  std::fill(cqs_ex.front().begin(), cqs_ex.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &cqs_ex]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_pd*, kResourcePerThread>, kThreadCount> pds;
  std::fill(pds.front().begin(), pds.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &pds]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_mr*, kResourcePerThread>, kThreadCount> mrs;
  std::fill(mrs.front().begin(), mrs.back().end(), nullptr);

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
  std::array<std::array<ibv_mw*, kResourcePerThread>, kThreadCount> mws;
  std::fill(mws.front().begin(), mws.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &mws]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_qp*, kResourcePerThread>, kThreadCount> qps;
  std::fill(qps.front().begin(), qps.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([this, setup, thread_id, &qps]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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
  std::array<std::array<ibv_srq*, kResourcePerThread>, kThreadCount> srqs;
  std::fill(srqs.front().begin(), srqs.back().end(), nullptr);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    pool.Add([setup, thread_id, &srqs]() {
      for (int i = 0; i < kResourcePerThread; ++i) {
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

}  // namespace rdma_unit_test
