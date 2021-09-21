#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_CLEANUP_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_CLEANUP_H_

#include <vector>

#include "absl/container/flat_hash_set.h"
#include "infiniband/verbs.h"
#include "public/rdma_memblock.h"

namespace rdma_unit_test {

// This class supports tracking libibverbs allocated objects, such as ibv_qp,
// ibv_mr, etc in order to clean them up as their scope expired.
class VerbsCleanup {
 public:
  VerbsCleanup() = default;
  VerbsCleanup(VerbsCleanup&& cleanup) = default;
  VerbsCleanup& operator=(VerbsCleanup&& cleanup) = default;
  VerbsCleanup(const VerbsCleanup& cleanup) = delete;
  VerbsCleanup& operator=(const VerbsCleanup& cleanup) = delete;
  ~VerbsCleanup() = default;

  // Set of helpers for automatically cleaning up objects when the tracker is
  // torn down.
  static void ContextDeleter(ibv_context* context);
  static void ChannelDeleter(ibv_comp_channel* channel);
  static void CqDeleter(ibv_cq* cq);
  static void PdDeleter(ibv_pd* pd);
  static void AhDeleter(ibv_ah* ah);
  static void SrqDeleter(ibv_srq* srq);
  static void QpDeleter(ibv_qp* qp);
  static void MrDeleter(ibv_mr* mr);
  static void MwDeleter(ibv_mw* mw);

  // Methods to add ibverbs allocated objects to the tracking pool in order for
  // auto deletion.
  void AddCleanup(ibv_context* context);
  void AddCleanup(ibv_comp_channel* channel);
  void AddCleanup(ibv_cq* cq);
  void AddCleanup(ibv_pd* pd);
  void AddCleanup(ibv_ah* ah);
  void AddCleanup(ibv_srq* srq);
  void AddCleanup(ibv_qp* qp);
  void AddCleanup(ibv_mr* mr);
  void AddCleanup(ibv_mw* mw);

  // Methods to release ibverbs allocated objects from the tracking pool when
  // they are already manually cleanup-ed.
  // Note: These methods will assumes the corresponding objects are already
  // added to the tracking pool by AddCleanup. Calling ReleaseCleanup otherwise
  // will result in CHECK failures.
  void ReleaseCleanup(ibv_context* context);
  void ReleaseCleanup(ibv_comp_channel* channel);
  void ReleaseCleanup(ibv_cq* cq);
  void ReleaseCleanup(ibv_pd* pd);
  void ReleaseCleanup(ibv_ah* ah);
  void ReleaseCleanup(ibv_srq* srq);
  void ReleaseCleanup(ibv_qp* qp);
  void ReleaseCleanup(ibv_mr* mr);
  void ReleaseCleanup(ibv_mw* mw);

 private:
  absl::flat_hash_set<std::unique_ptr<ibv_context, decltype(&ContextDeleter)>>
      contexts_ ABSL_GUARDED_BY(mtx_contexts_);
  absl::flat_hash_set<
      std::unique_ptr<ibv_comp_channel, decltype(&ChannelDeleter)>>
      channels_ ABSL_GUARDED_BY(mtx_channels_);
  absl::flat_hash_set<std::unique_ptr<ibv_cq, decltype(&CqDeleter)>> cqs_
      ABSL_GUARDED_BY(mtx_cqs_);
  absl::flat_hash_set<std::unique_ptr<ibv_pd, decltype(&PdDeleter)>> pds_
      ABSL_GUARDED_BY(mtx_pds_);
  absl::flat_hash_set<std::unique_ptr<ibv_ah, decltype(&AhDeleter)>> ahs_
      ABSL_GUARDED_BY(mtx_ahs_);
  absl::flat_hash_set<std::unique_ptr<ibv_srq, decltype(&SrqDeleter)>> srqs_
      ABSL_GUARDED_BY(mtx_srqs_);
  absl::flat_hash_set<std::unique_ptr<ibv_qp, decltype(&QpDeleter)>> qps_
      ABSL_GUARDED_BY(mtx_qps_);
  absl::flat_hash_set<std::unique_ptr<ibv_mr, decltype(&MrDeleter)>> mrs_
      ABSL_GUARDED_BY(mtx_mrs_);
  absl::flat_hash_set<std::unique_ptr<ibv_mw, decltype(&MwDeleter)>> mws_
      ABSL_GUARDED_BY(mtx_mws_);

  // Locks for containers above.
  absl::Mutex mtx_contexts_;
  absl::Mutex mtx_pds_;
  absl::Mutex mtx_ahs_;
  absl::Mutex mtx_channels_;
  absl::Mutex mtx_cqs_;
  absl::Mutex mtx_srqs_;
  absl::Mutex mtx_qps_;
  absl::Mutex mtx_mrs_;
  absl::Mutex mtx_mws_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_CLEANUP_H_
