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

#include "random_walk/internal/logging.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>

#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "public/rdma_memblock.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

LogEntry::LogEntry(uint64_t entry_id)
    : entry_id_(entry_id), timestamp_(absl::Now()) {}

uint64_t LogEntry::entry_id() const { return entry_id_; }

absl::Time LogEntry::timestamp() const { return timestamp_; }

CreateCq::CreateCq(uint64_t entry_id, ibv_cq* cq)
    : LogEntry(entry_id), cq_(cq) {}

std::string CreateCq::ToString() const {
  return absl::StrCat("AllocPd {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", cq = ", reinterpret_cast<uint64_t>(cq_), "}");
}

DestroyCq::DestroyCq(uint64_t entry_id, ibv_cq* cq)
    : LogEntry(entry_id), cq_(cq) {}

std::string DestroyCq::ToString() const {
  return absl::StrCat("AllocPd {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", cq = ", reinterpret_cast<uint64_t>(cq_), "}");
}

AllocPd::AllocPd(uint64_t entry_id, ibv_pd* pd) : LogEntry(entry_id), pd_(pd) {}

std::string AllocPd::ToString() const {
  return absl::StrCat("AllocPd {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", pd = ", reinterpret_cast<uint64_t>(pd_), "}");
}

DeallocPd::DeallocPd(uint64_t entry_id, ibv_pd* pd)
    : LogEntry(entry_id), pd_(pd) {}

std::string DeallocPd::ToString() const {
  return absl::StrCat("AllocPd {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", pd = ", reinterpret_cast<uint64_t>(pd_), "}");
}

RegMr::RegMr(uint64_t entry_id, ibv_pd* pd, const RdmaMemBlock& memblock,
             ibv_mr* mr)
    : LogEntry(entry_id),
      pd_(pd),
      addr_(reinterpret_cast<uint64_t>(memblock.data())),
      length_(memblock.size()),
      mr_(mr) {}

std::string RegMr::ToString() const {
  return absl::StrCat("RegMr {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", pd = ", reinterpret_cast<uint64_t>(pd_),
                      ", addr = ", addr_, ", length = ", length_, "}.");
}

DeregMr::DeregMr(uint64_t entry_id, ibv_mr* mr) : LogEntry(entry_id), mr_(mr) {}

std::string DeregMr::ToString() const {
  return absl::StrCat("DeregMr {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", mr = ", reinterpret_cast<uint64_t>(mr_), "}.");
}

AllocMw::AllocMw(uint64_t entry_id, ibv_pd* pd, ibv_mw_type mw_type, ibv_mw* mw)
    : LogEntry(entry_id), pd_(pd), mw_type_(mw_type), mw_(mw) {}

std::string AllocMw::ToString() const {
  return absl::StrCat("AllocMw {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", pd = ", reinterpret_cast<uint64_t>(pd_),
                      ", mw_type = ", mw_type_, "}.");
}

DeallocMw::DeallocMw(uint64_t entry_id, ibv_mw* mw)
    : LogEntry(entry_id), mw_(mw) {}

std::string DeallocMw::ToString() const {
  return absl::StrCat("DeallocMw {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", mw = ", reinterpret_cast<uint64_t>(mw_), "}.");
}

BindMw::BindMw(uint64_t entry_id, const ibv_mw_bind& bind, ibv_mw* mw)
    : LogEntry(entry_id),
      wr_id_(bind.wr_id),
      rkey_(0),
      mw_(mw),
      bind_info_(bind.bind_info) {}

BindMw::BindMw(uint64_t entry_id, const ibv_send_wr& bind)
    : LogEntry(entry_id),
      wr_id_(bind.wr_id),
      rkey_(bind.bind_mw.rkey),
      mw_(bind.bind_mw.mw),
      bind_info_(bind.bind_mw.bind_info) {}

std::string BindMw::ToString() const {
  return absl::StrCat(
      "BindMw {entry_id = ", entry_id(),
      ", timestamp = ", absl::FormatTime(timestamp()), ", wr_id = ", wr_id_,
      ", mw = ", reinterpret_cast<uint64_t>(mw_), ", rkey = ", rkey_,
      ", addr = ", bind_info_.addr, ", length = ", bind_info_.length,
      ", mr = ", reinterpret_cast<uint64_t>(bind_info_.mr), "}.");
}

CreateQp::CreateQp(uint64_t entry_id, ibv_qp* qp)
    : LogEntry(entry_id), qp_(qp) {}

std::string CreateQp::ToString() const {
  return absl::StrCat("CreateQp {entry_id = ", entry_id(),
                      ", qp = ", reinterpret_cast<uint64_t>(qp_), "}.");
}

CreateAh::CreateAh(uint64_t entry_id, ibv_pd* pd, ClientId client_id,
                   ibv_ah* ah)
    : LogEntry(entry_id), pd_(pd), client_id_(client_id), ah_(ah) {}

std::string CreateAh::ToString() const {
  return absl::StrCat("CreateAh {entry_id = ", entry_id(),
                      ", pd = ", reinterpret_cast<uint64_t>(pd_),
                      ", client id = ", client_id_,
                      ", ah = ", reinterpret_cast<uint64_t>(ah_), "}.");
}

DestroyAh::DestroyAh(uint64_t entry_id, ibv_ah* ah)
    : LogEntry(entry_id), ah_(ah) {}

std::string DestroyAh::ToString() const {
  return absl::StrCat("DestroyAh {entry_id = ", entry_id(),
                      ", ah = ", reinterpret_cast<uint64_t>(ah_), "}.");
}

Send::Send(uint64_t entry_id, const ibv_send_wr& send)
    : LogEntry(entry_id),
      wr_id_(send.wr_id),
      sges_(send.sg_list, send.sg_list + send.num_sge) {}

std::string Send::ToString() const {
  std::string ret =
      absl::StrCat("Send {entry_id = ", entry_id(),
                   ", timestamp = ", absl::FormatTime(timestamp()),
                   ", wr_id = ", wr_id_, ", sges = ");
  for (const ibv_sge& sge : sges_) {
    absl::StrAppend(&ret, "{", sge.addr, ", ", sge.length, ",", sge.lkey,
                    "}, ");
  }
  absl::StrAppend(&ret, "}");
  return ret;
}

SendInv::SendInv(uint64_t entry_id, const ibv_send_wr& send, uint32_t rkey)
    : LogEntry(entry_id),
      wr_id_(send.wr_id),
      sges_(send.sg_list, send.sg_list + send.num_sge),
      rkey_(rkey) {}

std::string SendInv::ToString() const {
  std::string ret =
      absl::StrCat("SendWithInvalidate {entry_id = ", entry_id(),
                   ", timestamp = ", absl::FormatTime(timestamp()),
                   ", wr_id = ", wr_id_, ", sges = ");
  for (const ibv_sge& sge : sges_) {
    absl::StrAppend(&ret, "{", sge.addr, ", ", sge.length, ",", sge.lkey,
                    "}, ");
  }
  absl::StrAppend(&ret, "rkey = ", rkey_, "}");
  return ret;
}

Recv::Recv(uint64_t entry_id, const ibv_recv_wr& recv)
    : LogEntry(entry_id),
      wr_id_(recv.wr_id),
      sges_(recv.sg_list, recv.sg_list + recv.num_sge) {}

std::string Recv::ToString() const {
  std::string ret =
      absl::StrCat("Recv {entry_id = ", entry_id(),
                   ", timestamp = ", absl::FormatTime(timestamp()),
                   ", wr_id = ", wr_id_, ", sges = ");
  for (const ibv_sge& sge : sges_) {
    absl::StrAppend(&ret, "{", sge.addr, ", ", sge.length, ", ", sge.lkey,
                    "}, ");
  }
  absl::StrAppend(&ret, "}");
  return ret;
}

Read::Read(uint64_t entry_id, const ibv_send_wr& read)
    : LogEntry(entry_id),
      wr_id_(read.wr_id),
      sges_(read.sg_list, read.sg_list + read.num_sge),
      remote_addr_(read.wr.rdma.remote_addr),
      rkey_(read.wr.rdma.rkey) {}

std::string Read::ToString() const {
  std::string ret =
      absl::StrCat("Recv {entry_id = ", entry_id(),
                   ", timestamp = ", absl::FormatTime(timestamp()),
                   ", wr_id = ", wr_id_, ", sges = ");
  for (const ibv_sge& sge : sges_) {
    absl::StrAppend(&ret, "{", sge.addr, ", ", sge.length, ", ", sge.lkey,
                    "}, ");
  }
  absl::StrAppend(&ret, "remote addr = ", remote_addr_, ", rkey = ", rkey_,
                  "}");
  return ret;
}

Write::Write(uint64_t entry_id, const ibv_send_wr& write)
    : LogEntry(entry_id),
      wr_id_(write.wr_id),
      sges_(write.sg_list, write.sg_list + write.num_sge),
      remote_addr_(write.wr.rdma.remote_addr),
      rkey_(write.wr.rdma.rkey) {}

std::string Write::ToString() const {
  std::string ret =
      absl::StrCat("Write {entry_id = ", entry_id(),
                   ", timestamp = ", absl::FormatTime(timestamp()),
                   ", wr_id = ", wr_id_, ", sges = ");
  for (const ibv_sge& sge : sges_) {
    absl::StrAppend(&ret, "{", sge.addr, ", ", sge.length, ", ", sge.lkey,
                    "}, ");
  }
  absl::StrAppend(&ret, "remote addr = ", remote_addr_, ", rkey = ", rkey_,
                  "}");
  return ret;
}

FetchAdd::FetchAdd(uint64_t entry_id, const ibv_send_wr& fetch_add)
    : LogEntry(entry_id),
      wr_id_(fetch_add.wr_id),
      sge_(*fetch_add.sg_list),
      remote_addr_(fetch_add.wr.atomic.remote_addr),
      rkey_(fetch_add.wr.atomic.rkey),
      compare_add_(fetch_add.wr.atomic.compare_add) {}

std::string FetchAdd::ToString() const {
  return absl::StrCat("Recv {entry_id = ", entry_id(),
                      ", timestamp = ", absl::FormatTime(timestamp()),
                      ", wr_id = ", wr_id_, ", sge = [", sge_.addr, ", ",
                      sge_.length, ", ", sge_.lkey,
                      "], compare add = ", compare_add_, "}");
}

CompSwap::CompSwap(uint64_t entry_id, const ibv_send_wr& comp_swap)
    : LogEntry(entry_id),
      wr_id_(comp_swap.wr_id),
      sge_(*comp_swap.sg_list),
      remote_addr_(comp_swap.wr.atomic.remote_addr),
      rkey_(comp_swap.wr.atomic.rkey),
      compare_add_(comp_swap.wr.atomic.compare_add),
      swap_(comp_swap.wr.atomic.swap) {}

std::string CompSwap::ToString() const {
  return absl::StrCat(
      "Recv {entry_id = ", entry_id(),
      ", timestamp = ", absl::FormatTime(timestamp()), ", wr_id = ", wr_id_,
      ", sge = [", sge_.addr, ", ", sge_.length, ", ", sge_.lkey,
      "], compare add = ", compare_add_, ", swap = ", swap_, "}");
}

Completion::Completion(uint64_t entry_id, const ibv_wc& wc)
    : LogEntry(entry_id), completion_(wc) {}

std::string Completion::ToString() const {
  return absl::StrCat("Completion {wr_id = ", completion_.wr_id,
                      ", status = ", completion_.status,
                      ", opcode = ", completion_.opcode, "}");
}

RandomWalkLogger::RandomWalkLogger(size_t log_capacity)
    : log_capacity_(log_capacity) {}

void RandomWalkLogger::PushCreateCq(ibv_cq* cq) {
  logs_.emplace_back(std::make_unique<CreateCq>(next_entry_id_++, cq));
  Flush();
}

void RandomWalkLogger::PushDestroyCq(ibv_cq* cq) {
  logs_.emplace_back(std::make_unique<DestroyCq>(next_entry_id_++, cq));
  Flush();
}

void RandomWalkLogger::PushAllocPd(ibv_pd* pd) {
  logs_.emplace_back(std::make_unique<AllocPd>(next_entry_id_++, pd));
  Flush();
}

void RandomWalkLogger::PushDeallocPd(ibv_pd* pd) {
  logs_.emplace_back(std::make_unique<DeallocPd>(next_entry_id_++, pd));
  Flush();
}

void RandomWalkLogger::PushRegMr(ibv_pd* pd, const RdmaMemBlock& memblock,
                                 ibv_mr* mr) {
  logs_.emplace_back(
      std::make_unique<RegMr>(next_entry_id_++, pd, memblock, mr));
  Flush();
}

void RandomWalkLogger::PushDeregMr(ibv_mr* mr) {
  logs_.emplace_back(std::make_unique<DeregMr>(next_entry_id_++, mr));
  Flush();
}

void RandomWalkLogger::PushAllocMw(ibv_pd* pd, ibv_mw_type mw_type,
                                   ibv_mw* mw) {
  logs_.emplace_back(
      std::make_unique<AllocMw>(next_entry_id_++, pd, mw_type, mw));
  Flush();
}

void RandomWalkLogger::PushDeallocMw(ibv_mw* mw) {
  logs_.emplace_back(std::make_unique<DeallocMw>(next_entry_id_++, mw));
  Flush();
}

void RandomWalkLogger::PushBindMw(const ibv_mw_bind& bind, ibv_mw* mw) {
  logs_.emplace_back(std::make_unique<BindMw>(next_entry_id_++, bind, mw));
  Flush();
}

void RandomWalkLogger::PushBindMw(const ibv_send_wr& bind) {
  logs_.emplace_back(std::make_unique<BindMw>(next_entry_id_++, bind));
  Flush();
}

void RandomWalkLogger::PushCreateQp(ibv_qp* qp) {
  logs_.emplace_back(std::make_unique<CreateQp>(next_entry_id_++, qp));
  Flush();
}

void RandomWalkLogger::PushCreateAh(ibv_pd* pd, ClientId client_id,
                                    ibv_ah* ah) {
  logs_.emplace_back(
      std::make_unique<CreateAh>(next_entry_id_++, pd, client_id, ah));
  Flush();
}
void RandomWalkLogger::PushDestroyAh(ibv_ah* ah) {
  logs_.emplace_back(std::make_unique<DestroyAh>(next_entry_id_++, ah));
  Flush();
}

void RandomWalkLogger::PushSend(const ibv_send_wr& send_wr) {
  logs_.emplace_back(std::make_unique<Send>(next_entry_id_++, send_wr));
  Flush();
}

void RandomWalkLogger::PushRecv(const ibv_recv_wr& recv_wr) {
  logs_.emplace_back(std::make_unique<Recv>(next_entry_id_++, recv_wr));
  Flush();
}

void RandomWalkLogger::PushRead(const ibv_send_wr& read_wr) {
  logs_.emplace_back(std::make_unique<Read>(next_entry_id_++, read_wr));
  Flush();
}

void RandomWalkLogger::PushWrite(const ibv_send_wr& write_wr) {
  logs_.emplace_back(std::make_unique<Write>(next_entry_id_++, write_wr));
  Flush();
}

void RandomWalkLogger::PushFetchAdd(const ibv_send_wr& fetch_add_wr) {
  logs_.emplace_back(
      std::make_unique<FetchAdd>(next_entry_id_++, fetch_add_wr));
  Flush();
}

void RandomWalkLogger::PushCompSwap(const ibv_send_wr& comp_swap_wr) {
  logs_.emplace_back(
      std::make_unique<CompSwap>(next_entry_id_++, comp_swap_wr));
  Flush();
}

void RandomWalkLogger::PushCompletion(const ibv_wc& cqe) {
  logs_.emplace_back(std::make_unique<Completion>(next_entry_id_++, cqe));
  Flush();
}

void RandomWalkLogger::PrintLogs() const {
  for (const auto& entry : logs_) {
    LOG(INFO) << entry->ToString();
  }
}

void RandomWalkLogger::Flush() {
  while (logs_.size() > log_capacity_) {
    logs_.pop_front();
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
