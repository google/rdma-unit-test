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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_LOGGING_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_LOGGING_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "public/rdma_memblock.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

// An abstract class that represents an entry in the log.
class LogEntry {
 public:
  explicit LogEntry(uint64_t entry_id);
  virtual ~LogEntry() = default;
  uint64_t entry_id() const;
  absl::Time timestamp() const;
  virtual std::string ToString() const = 0;

 private:
  uint64_t entry_id_;
  absl::Time timestamp_;
};

// The set of concrete classes for different types of logs entries. Each class
// represent either an Action or a processed completion.

class CreateCq : public LogEntry {
 public:
  CreateCq(uint64_t entry_id, ibv_cq* cq);
  std::string ToString() const override;

 private:
  ibv_cq* cq_;
};

class DestroyCq : public LogEntry {
 public:
  DestroyCq(uint64_t entry_id, ibv_cq* cq);
  std::string ToString() const override;

 private:
  ibv_cq* cq_;
};

class AllocPd : public LogEntry {
 public:
  AllocPd(uint64_t entry_id, ibv_pd* pd);
  std::string ToString() const override;

 private:
  ibv_pd* pd_;
};

class DeallocPd : public LogEntry {
 public:
  DeallocPd(uint64_t entry_id, ibv_pd* pd);
  std::string ToString() const override;

 private:
  ibv_pd* pd_;
};

class RegMr : public LogEntry {
 public:
  RegMr(uint64_t entry_id, ibv_pd* pd, const RdmaMemBlock& memblock,
        ibv_mr* mr);
  std::string ToString() const override;

 private:
  ibv_pd* pd_;
  uint64_t addr_;
  uint64_t length_;
  ibv_mr* mr_;
};

class DeregMr : public LogEntry {
 public:
  DeregMr(uint64_t entry_id, ibv_mr* mr);
  std::string ToString() const override;

 private:
  ibv_mr* mr_;
};

class AllocMw : public LogEntry {
 public:
  AllocMw(uint64_t entry_id, ibv_pd* pd, ibv_mw_type mw_type, ibv_mw* mw);
  std::string ToString() const override;

 private:
  ibv_pd* pd_;
  ibv_mw_type mw_type_;
  ibv_mw* mw_;
};

class DeallocMw : public LogEntry {
 public:
  DeallocMw(uint64_t entry_id, ibv_mw* mw);
  std::string ToString() const override;

 private:
  ibv_mw* mw_;
};

class BindMw : public LogEntry {
 public:
  BindMw(uint64_t entry_id, const ibv_mw_bind& bind, ibv_mw* mw);
  BindMw(uint64_t entry_id, const ibv_send_wr& bind);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  uint32_t rkey_;  // For type 2 MW.
  ibv_mw* mw_;
  ibv_mw_bind_info bind_info_;
};

class CreateQp : public LogEntry {
 public:
  CreateQp(uint64_t entry_id, ibv_qp* qp);
  std::string ToString() const override;

 private:
  ibv_qp* qp_;
};

class CreateAh : public LogEntry {
 public:
  CreateAh(uint64_t entry_id, ibv_pd* pd, ClientId client_id, ibv_ah* ah);
  std::string ToString() const override;

 private:
  ibv_pd* pd_;
  ClientId client_id_;
  ibv_ah* ah_;
};

class DestroyAh : public LogEntry {
 public:
  DestroyAh(uint64_t entry_id, ibv_ah* ah);
  std::string ToString() const override;

 private:
  ibv_ah* ah_;
};

class Send : public LogEntry {
 public:
  Send(uint64_t entry_id, const ibv_send_wr& send);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  std::vector<ibv_sge> sges_;
};

class SendInv : public LogEntry {
 public:
  SendInv(uint64_t entry_id, const ibv_send_wr& send, uint32_t rkey);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  std::vector<ibv_sge> sges_;
  uint32_t rkey_;
};

class Recv : public LogEntry {
 public:
  Recv(uint64_t entry_id, const ibv_recv_wr& recv);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  std::vector<ibv_sge> sges_;
};

class Read : public LogEntry {
 public:
  Read(uint64_t entry_id, const ibv_send_wr& read);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  std::vector<ibv_sge> sges_;
  uint64_t remote_addr_;
  uint32_t rkey_;
};

class Write : public LogEntry {
 public:
  Write(uint64_t entry_id, const ibv_send_wr& write);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  std::vector<ibv_sge> sges_;
  uint64_t remote_addr_;
  uint32_t rkey_;
};

class FetchAdd : public LogEntry {
 public:
  FetchAdd(uint64_t entry_id, const ibv_send_wr& fetch_add);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  ibv_sge sge_;
  uint64_t remote_addr_;
  uint32_t rkey_;
  uint64_t compare_add_;
};

class CompSwap : public LogEntry {
 public:
  CompSwap(uint64_t entry_id, const ibv_send_wr& comp_swap);
  std::string ToString() const override;

 private:
  uint64_t wr_id_;
  ibv_sge sge_;
  uint64_t remote_addr_;
  uint32_t rkey_;
  uint64_t compare_add_;
  uint64_t swap_;
};

class Completion : public LogEntry {
 public:
  Completion(uint64_t entry_id, const ibv_wc& wc);
  std::string ToString() const override;

 private:
  ibv_wc completion_;
};

// The class provides logging services for the RandomWalkClients. It provides a
// fixed capacity circular queue to store the last portions of commands and
// completions witnessed by the RandomWalkClient.
class RandomWalkLogger {
 public:
  explicit RandomWalkLogger(size_t log_capacity);
  // Movable but not copyable.
  RandomWalkLogger(RandomWalkLogger&& logger) = default;
  RandomWalkLogger& operator=(RandomWalkLogger&& logger) = default;
  RandomWalkLogger(const RandomWalkLogger& logger) = delete;
  RandomWalkLogger& operator=(const RandomWalkLogger& logger) = delete;
  ~RandomWalkLogger() = default;

  void PushCreateCq(ibv_cq* cq);
  void PushDestroyCq(ibv_cq* cq);
  void PushAllocPd(ibv_pd* pd);
  void PushDeallocPd(ibv_pd* pd);
  void PushRegMr(ibv_pd* pd, const RdmaMemBlock& memblock, ibv_mr* mr);
  void PushDeregMr(ibv_mr* mr);
  void PushAllocMw(ibv_pd* pd, ibv_mw_type mw_type, ibv_mw* mw);
  void PushDeallocMw(ibv_mw* mw);
  void PushBindMw(const ibv_mw_bind& bind, ibv_mw* mw);
  void PushBindMw(const ibv_send_wr& bind);
  void PushCreateQp(ibv_qp* qp);
  void PushCreateAh(ibv_pd* pd, ClientId client_id, ibv_ah* ah);
  void PushDestroyAh(ibv_ah* ah);
  void PushSend(const ibv_send_wr& send);
  void PushRecv(const ibv_recv_wr& recv);
  void PushRead(const ibv_send_wr& read);
  void PushWrite(const ibv_send_wr& write);
  void PushFetchAdd(const ibv_send_wr& fetch_add);
  void PushCompSwap(const ibv_send_wr& comp_swap);
  void PushCompletion(const ibv_wc& cqe);

  void PrintLogs() const;

 private:
  // Call logs_.pop_front() until logs_.size() is not larger than log_capacity_.
  void Flush();

  uint64_t next_entry_id_ = 1;
  // The capacity of the log. The log will only keep the last [log_capcity_]
  // entries.
  const uint32_t log_capacity_;
  // The circular queue storing all log entries.
  std::deque<std::unique_ptr<LogEntry>> logs_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_LOGGING_H_
