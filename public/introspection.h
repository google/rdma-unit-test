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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_INTROSPECTION_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_INTROSPECTION_H_

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// A class to consolidate NIC introspection logic. It is a trivially
// destructable object which is created from an opened context.
class NicIntrospection {
 public:
  // Abstract hardware counter which might be supported by some providers.
  enum HardwareCounter {
    // Basic RDMA counters.
    kRdmaRxRead,      // RDMA read ops received.
    kRdmaRxWrite,     // RDMA write ops received.
    kRdmaRxSend,      // RDMA send ops received.
    kRdmaRxAtomic,    // RDMA atomic ops received.
    kRdmaTxRead,      // RDMA read ops sent.
    kRdmaTxWrite,     // RDMA write ops sent.
    kRdmaTxSend,      // RDMA send ops sent.
    kRdmaTxAtomic,    // RDMA atomics ops sent.
    kRdmaBind,        // RDMA bind ops carried out.
    kRdmaInvalidate,  // RDMA invalidate ops carried out.
    // Counters for verbs errors.
    kBadRespErr,       // bad response errors.
    kLocLenErr,        // local length errors.
    kLocProtErr,       // local protection errors.
    kLocQpOpErr,       // local QP operation errors.
    kMwBindErr,        // local MW bind errors.
    kOutOfSeqNaks,     // out of sequence NAKs received.
    kRemAbrtErr,       // remote abort error.
    kRemAccessErr,     // remote access error.
    kRemInvReqErr,     // remote invalid request error.
    kRnrNak,           // RNR NAKs received.
    kRemOpErr,         // remote operation error.
    kRnrRetryExcErr,   // RNR retry counter exceeded error.
    kRemSync,          // TODO(author2): No information.
    kTrptRetryExcErr,  // transport retry counter exceeded error.
    kWrCmpltErr,       // all CQE with an error.
    // Ipv6 counters:
    kIpv6Discards,            // IPv6 packets received and discarded.
    kIpv6RxBytes,             // IPv6 bytes received.
    kIpv6RxPackets,           // IPv6 packets received.
    kIpv6RxTruncatedPackets,  // IPv6 packets received and truncated due to
                              // insufficient buffering space in UDA RQ.
    kIpv6TxNoRoutes,  // IPv6 datagrams discarded due to routing problem.
    kIpv6TxBytes,     // IPv6 bytes sent.
    kIpv6TxPackets,   // IPv6 packets sent.
    // Ipv6 counters:
    kIpv4Discards,            // IPv4 packets received and discarded.
    kIpv4RxBytes,             // IPv4 bytes received.
    kIpv4RxPackets,           // IPv4 packets received.
    kIpv4RxTruncatedPackets,  // IPv4 packets received and truncated due to
                              // insufficient buffering space in UDA RQ.
    kIpv4TxNoRoutes,  // IPv4 datagrams discarded due to routing problem.
    kIpv4TxBytes,     // IPv4 bytes sent.
    kIpv4TxPackets,   // IPv4 packets sent.
  };

  // Snapshot of all hardware counters. Represented by a map to counter type
  // to its value.
  using CounterSnapshot = absl::flat_hash_map<HardwareCounter, uint64_t>;

  NicIntrospection() = delete;
  // Must use this constructor in order to use any of the other methods.
  NicIntrospection(const std::string& name, const ibv_device_attr& attr)
      : name_(name), attr_(attr) {}
  virtual ~NicIntrospection() = default;

  // Returns if the device supports target.
  virtual bool CheckCapability(ibv_device_cap_flags target) const {
    return (attr_.device_cap_flags & target) > 0;
  }

  // Returns true if the device allows empty SGLs in the WR.
  virtual bool AllowsEmptySgl() const { return true; }

  // Returns true if the device supports Ipv6.
  virtual bool SupportsIpV6() const { return true; }

  // Returns true if the device supports rereg_mr operations.
  virtual bool SupportsReRegMr() const { return false; }

  // Returns true if a full Completion Queue stops processing QP work.
  // The alternative is to overwrite completions and continue operation.
  virtual bool FullCqIdlesQp() const { return true; }

  // Returns true if the NIC supports Type 1 Memory Windows.
  virtual bool SupportsType1() const {
    return CheckCapability(IBV_DEVICE_MEM_WINDOW);
  }

  // Returns true if the NIC supports Type 2B Memory Windows.
  virtual bool SupportsType2() const {
    return CheckCapability(IBV_DEVICE_MEM_WINDOW_TYPE_2B);
  }

  // Returns true if the NIC supports zero length MR.
  virtual bool SupportsZeroLengthMr() const { return true; }

  // Returns true if the NIC supports UD Queue Pairs.
  virtual bool SupportsUdQp() const { return true; }

  // Returns true if the NIC supports RC Queue Pairs.
  virtual bool SupportsRcQp() const { return true; }

  // Returns a optional string indicating whether the NIC has known issue
  // (and should be skipped). The string contains details about the known issue.
  // If the NIC does not have known issue (and the test should proceed
  // normally), returns nullopt.
  std::optional<std::string> KnownIssue() const;

  // Returns true if the NIC supports RC Remote Memory Window Atomic.
  virtual bool SupportsRcRemoteMwAtomic() const { return true; }

  // Returns a boolean indicating if the NIC supports extended CQs.
  virtual bool SupportsExtendedCqs() const { return true; }

  // Returns a boolean indicating if the NIC supports the following:
  // 1. Generate a RnR NAK response
  // 2. Respects the RnR timeout value and retry thresholds set by RDMA.
  virtual bool SupportsRnrRetries() const { return true; }

  // Returns true if the provider requires the use of file backed shared
  // memory.
  virtual bool RequiresSharedMemory() const { return false; }

  // IBTA spec specified when a send WR posted to a SQ, the QP should return
  // immediate error. However, to avoid fastpath examination, many provider
  // will still allow the post to succeed. This boolean indicates whether
  // the provider will silently drop the WR or keep the WR in the SQ to be
  // processsed when the QP is in RTS.
  virtual bool SilentlyDropSendWrWhenResetInitRtr() const { return true; }

  // When a queue pair receive an incoming message but is not in RTR or RTS
  // state, some providers (such as Mellanox) will buffer the message
  // to be processed after the QP is in RTS. IB spec indicates in this case
  // the message should be dropped.
  virtual bool BuffersMessagesWhenNotReadyToReceive() const { return false; }

  // Returns if the providers provides a hardware counter (in sysfs).
  bool HasCounter(HardwareCounter counter) const;

  // Tries to extract a hardware counter value. Returns the value if succeeded.
  // Otherwise, returns a status indicating the reason of failure.
  absl::StatusOr<uint64_t> GetCounterValue(HardwareCounter counter) const;

  // Returns a snapshot to all provided hardware counters.
  CounterSnapshot GetCounterSnapshot() const;

  // Dumps all hardware counter values into a string.
  std::string DumpHardwareCounters() const;

  // Returns the name of the ibverbs device.
  const std::string device_name() const { return name_; }

  // Returns the device attributes.
  const ibv_device_attr& device_attr() const { return attr_; }

 protected:
  // <TestSuite, TestCase>.
  typedef std::tuple<std::string, std::string> TestcaseKey;

  // Returns a set of <TestSuite,Name> that we should skip because of known
  // issues. Maps values are error message.
  virtual const absl::flat_hash_map<TestcaseKey, std::string>& GetDeviations()
      const {
    static const absl::flat_hash_map<TestcaseKey, std::string> deviations;
    return deviations;
  }

  // Returns a set of <HardwareCounter, std::string> which indicates the
  // set of hardware counters the provider supports and their corresponding
  // names.
  virtual const absl::flat_hash_map<HardwareCounter, std::string>&
  GetHardwareCounters() const {
    static const absl::flat_hash_map<HardwareCounter, std::string> counters;
    return counters;
  }

  const std::string name_;
  ibv_device_attr attr_;
};

// Returns an introspection object which can be queried for device capabilities.
const NicIntrospection& Introspection();

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_INTROSPECTION_H_
