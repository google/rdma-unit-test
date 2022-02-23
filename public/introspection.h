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

#include <optional>
#include <string>
#include <tuple>

#include "absl/container/flat_hash_map.h"
#include "infiniband/verbs.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

// A class to consolidate NIC introspection logic. It is a trivially
// destructable object which is created from an opened context.
class NicIntrospection {
 public:
  NicIntrospection() = delete;
  // Must use this constructor in order to use any of the other methods.
  explicit NicIntrospection(const ibv_device_attr& attr) : attr_(attr) {}
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

  // Returns true if the NIC supports RC SendWithInvalidate.
  virtual bool SupportsRcSendWithInvalidate() const { return true; }

  // Returns true if the NIC supports RC Remote Memory Window Atomic.
  virtual bool SupportsRcRemoteMwAtomic() const { return true; }

  // Returns a boolean indicating if the NIC supports extended CQs.
  virtual bool SupportsExtendedCqs() const { return true; }

  // Returns true if the provider requires the use of file backed shared
  // memory.
  virtual bool RequiresSharedMemory() const { return false; }

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

  ibv_device_attr attr_;
};

// Returns an introspection object which can be queried for device capabilities.
const NicIntrospection& Introspection();

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_INTROSPECTION_H_
