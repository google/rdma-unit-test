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
#include <memory>
#include <string>
#include <vector>

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

CreateCq::CreateCq(uint64_t entry_id) : LogEntry(entry_id) {}

void CreateCq::FillInCq(ibv_cq* cq) { cq_ = cq; }

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

AllocPd::AllocPd(uint64_t entry_id) : LogEntry(entry_id) {}

void AllocPd::FillInPd(ibv_pd* pd) { pd_ = pd; }

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

RegMr::RegMr(uint64_t entry_id, ibv_pd* pd, const RdmaMemBlock& memblock)
    : LogEntry(entry_id),
      pd_(pd),
      addr_(reinterpret_cast<uint64_t>(memblock.data())),
      length_(memblock.size()) {}

void RegMr::FillInMr(ibv_mr* mr) { mr_ = mr; }

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

AllocMw::AllocMw(uint64_t entry_id, ibv_pd* pd, ibv_mw_type mw_type)
    : LogEntry(entry_id), pd_(pd), mw_type_(mw_type) {}

void AllocMw::FillInMw(ibv_mw* mw) { mw_ = mw; }

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

std::shared_ptr<CreateCq> RandomWalkLogger::PushCreateCqInput() {
  std::shared_ptr<CreateCq> create_cq =
      std::make_shared<CreateCq>(next_entry_id_++);
  PushToLog(create_cq);
  return create_cq;
}

void RandomWalkLogger::PushDestroyCq(ibv_cq* cq) {
  std::shared_ptr<DestroyCq> destroy_cq =
      std::make_shared<DestroyCq>(next_entry_id_++, cq);
  PushToLog(destroy_cq);
}

std::shared_ptr<AllocPd> RandomWalkLogger::PushAllocPdInput() {
  std::shared_ptr<AllocPd> alloc_pd =
      std::make_shared<AllocPd>(next_entry_id_++);
  PushToLog(alloc_pd);
  return alloc_pd;
}

void RandomWalkLogger::PushDeallocPd(ibv_pd* pd) {
  std::shared_ptr<DeallocPd> dealloc_pd =
      std::make_shared<DeallocPd>(next_entry_id_++, pd);
  PushToLog(dealloc_pd);
}

std::shared_ptr<RegMr> RandomWalkLogger::PushRegMrInput(
    ibv_pd* pd, const RdmaMemBlock& memblock) {
  std::shared_ptr<RegMr> reg_mr =
      std::make_shared<RegMr>(next_entry_id_++, pd, memblock);
  PushToLog(reg_mr);
  return reg_mr;
}

void RandomWalkLogger::PushDeregMr(ibv_mr* mr) {
  std::shared_ptr<DeregMr> dereg_mr =
      std::make_shared<DeregMr>(next_entry_id_++, mr);
  PushToLog(dereg_mr);
}

std::shared_ptr<AllocMw> RandomWalkLogger::PushAllocMwInput(
    ibv_pd* pd, ibv_mw_type mw_type) {
  std::shared_ptr<AllocMw> alloc_mw =
      std::make_shared<AllocMw>(next_entry_id_++, pd, mw_type);
  PushToLog(alloc_mw);
  return alloc_mw;
}

void RandomWalkLogger::PushDeallocMw(ibv_mw* mw) {
  std::shared_ptr<DeallocMw> dealloc_mw =
      std::make_shared<DeallocMw>(next_entry_id_++, mw);
  PushToLog(dealloc_mw);
}

void RandomWalkLogger::PushBindMw(const ibv_mw_bind& bind, ibv_mw* mw) {
  std::shared_ptr<BindMw> bind_mw =
      std::make_shared<BindMw>(next_entry_id_++, bind, mw);
  PushToLog(bind_mw);
}

void RandomWalkLogger::PushBindMw(const ibv_send_wr& bind) {
  std::shared_ptr<BindMw> bind_mw =
      std::make_shared<BindMw>(next_entry_id_++, bind);
  PushToLog(bind_mw);
}

void RandomWalkLogger::PushCreateQp(ibv_qp* qp) {
  std::shared_ptr<CreateQp> create_qp =
      std::make_shared<CreateQp>(next_entry_id_++, qp);
  PushToLog(create_qp);
}

void RandomWalkLogger::PushCreateAh(ibv_pd* pd, ClientId client_id,
                                    ibv_ah* ah) {
  std::shared_ptr<CreateAh> create_ah =
      std::make_shared<CreateAh>(next_entry_id_++, pd, client_id, ah);
  PushToLog(create_ah);
}
void RandomWalkLogger::PushDestroyAh(ibv_ah* ah) {
  std::shared_ptr<DestroyAh> destroy_ah =
      std::make_shared<DestroyAh>(next_entry_id_++, ah);
  PushToLog(destroy_ah);
}

void RandomWalkLogger::PushSend(const ibv_send_wr& send_wr) {
  std::shared_ptr<Send> send =
      std::make_shared<Send>(next_entry_id_++, send_wr);
  PushToLog(send);
}

void RandomWalkLogger::PushRecv(const ibv_recv_wr& recv_wr) {
  std::shared_ptr<Recv> recv =
      std::make_shared<Recv>(next_entry_id_++, recv_wr);
  PushToLog(recv);
}

void RandomWalkLogger::PushRead(const ibv_send_wr& read_wr) {
  std::shared_ptr<Read> read =
      std::make_shared<Read>(next_entry_id_++, read_wr);
  PushToLog(read);
}

void RandomWalkLogger::PushWrite(const ibv_send_wr& write_wr) {
  std::shared_ptr<Write> write =
      std::make_shared<Write>(next_entry_id_++, write_wr);
  PushToLog(write);
}

void RandomWalkLogger::PushFetchAdd(const ibv_send_wr& fetch_add_wr) {
  std::shared_ptr<FetchAdd> fetch_add =
      std::make_shared<FetchAdd>(next_entry_id_++, fetch_add_wr);
  PushToLog(fetch_add);
}

void RandomWalkLogger::PushCompSwap(const ibv_send_wr& comp_swap_wr) {
  std::shared_ptr<CompSwap> comp_swap =
      std::make_shared<CompSwap>(next_entry_id_++, comp_swap_wr);
  PushToLog(comp_swap);
}

void RandomWalkLogger::PushCompletion(const ibv_wc& cqe) {
  std::shared_ptr<Completion> completion =
      std::make_shared<Completion>(next_entry_id_++, cqe);
  PushToLog(completion);
}

void RandomWalkLogger::PrintLogs() const {
  for (const auto& entry : logs_) {
    LOG(INFO) << entry->ToString();
  }
}

void RandomWalkLogger::PushToLog(std::shared_ptr<LogEntry> log_entry) {
  logs_.push_back(log_entry);
  while (logs_.size() > log_capacity_) {
    logs_.pop_front();
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
