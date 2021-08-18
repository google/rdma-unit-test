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

#include "absl/container/flat_hash_set.h"
#include "infiniband/verbs.h"
#include "public/util.h"

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

  // Returns true if the NIC supports UD Queue Pairs.
  virtual bool SupportsUdQp() const { return true; }

  // Returns true if the NIC supports RC Queue Pairs.
  virtual bool SupportsRcQp() const { return true; }

  // Returns true if the NIC requires a deviation for the current test.
  // Typically this is due to the test exploring (per the spec) undefined
  // behaviors. It is up to each test how they handle deviations, but most skip
  // execution.
  // |identifier| is used to disambiguate among multiple deviations in a single
  // test.
  bool ShouldDeviateForCurrentTest(const std::string& identifier = "") const;

  // Returns true if the NIC supports RC SendWithInvalidate.
  virtual bool SupportsRcSendWithInvalidate() const { return true; }

  // Returns true if the NIC supports RC Remote Memory Window Atomic.
  virtual bool SupportsRcRemoteMwAtomic() const { return true; }

  // Returns true if the provider requires the use of file backed shared
  // memory.
  virtual bool RequiresSharedMemory() const { return false; }

  // Returns the device attributes.
  const ibv_device_attr& device_attr() const { return attr_; }

 protected:
  // <TestSuite, TestCase, DeviationIdentifier>
  // See ShouldDeviateForCurrentTest for meaning of DeviationIdentifier.
  typedef std::tuple<std::string, std::string, std::string> DeviationEntry;

  // Returns a set of <TestSuite,Name,Identifier> which should deviate.
  virtual const absl::flat_hash_set<DeviationEntry>& GetDeviations() const {
    static const absl::flat_hash_set<DeviationEntry> deviations;
    return deviations;
  }

  ibv_device_attr attr_;
};

// Returns an introspection object which can be queried for device capabilities.
const NicIntrospection& Introspection();

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_INTROSPECTION_H_
