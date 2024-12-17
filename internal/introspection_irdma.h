#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_IRDMA_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_IRDMA_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "infiniband/verbs.h"
#include "internal/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

// Enable irdma according to
// https://github.com/linux-rdma/rdma-core/blob/master/kernel-boot/rdma-persistent-naming.rules
const absl::string_view kNetworkInterfaceName = "roce[8086:145c]";

// Concrete class to override specific behaviour for irdma NIC.
class IntrospectionIrdma : public NicIntrospection {
 public:
  // Register Irdma with the Introspection Registrar.
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "irdma", [](const std::string& name, const ibv_device_attr& attr) {
          return new IntrospectionIrdma(name, attr);
        });
    IntrospectionRegistrar::GetInstance().Register(
        kNetworkInterfaceName,
        [](const std::string& name, const ibv_device_attr& attr) {
          return new IntrospectionIrdma(name, attr);
        });
  }

  bool SupportsZeroLengthMr() const override { return false; }

  bool SupportsReRegMr() const override { return true; }

  bool GeneratesRetryExcOnConnTimeout() const { return true; }

  bool FullCqIdlesQp() const override { return false; }

  bool SilentlyDropSendWrWhenResetInitRtr() const override { return false; }

  bool NoNakOnSendInvalidateErrors() const override { return false; }

 protected:
  const absl::flat_hash_map<TestcaseKey, std::string>& GetDeviations()
      const override {
    static const absl::flat_hash_map<TestcaseKey, std::string> deviations{
        {{"LoopbackRcQpTest", "CompareSwapInvalidSize"}, ""},
        {{"QpStateTest", "ReuseQp"}, ""},
        {{"QpStateTest", "PostSendInit"}, ""},
        {{"QpStateTest", "PostSendRtr"}, ""},
        {{"QpStateTest", "PostSendReset"}, ""},
        {{"QpStateTest", "PostSendErrConcurrent"}, ""},
        {{"LoopbackUdQpTest", "SendTrafficClass"}, ""},
        {{"AdvancedLoopbackTest", "RcSendToUd"}, ""},
        {{"AdvancedLoopbackTest", "UdSendToRc"}, ""},
        {{"LoopbackRcQpTest", "FetchAddSplitSgl"}, "b/197723945#comment5"},
        {{"CompChannelTest", "RequestNotificationInvalidCq"}, ""},
        {{"CompChannelTest", "RequestNotificationOnCqWithoutCompChannel"},
         ""},
        {{"DeviceLimitTest", "MaxAh"}, ""},
        {{"MwType2Test", "CrossQpInvalidate"}, "b/235330853#comment21"},
        {{"QpPostTest", "OverflowSendWr"}, ""},
        {{"SrqTest", "ExceedDeviceMaxWr"}, ""},
        {{"LoopbackUdQpTest", "SendWithTooSmallRecv"}, ""},
        {{"PdSrqTest", "QpSrqPdMismatchWriteWithImm"}, ""},
        {{"RdmaAccessTest", "ZeroBasedAccess"}, ""},
        {{"MwGeneralTest", "ReadZeroBased"}, ""},
    };
    return deviations;
  }

  const absl::flat_hash_map<HardwareCounter, std::string>& GetHardwareCounters()
      const override {
    static const absl::flat_hash_map<HardwareCounter, std::string> counters{
        {HardwareCounter::kRdmaRxRead, "InRdmaReads"},
        {HardwareCounter::kRdmaRxWrite, "InRdmaWrites"},
        {HardwareCounter::kRdmaRxSend, "InRdmaSends"},
        {HardwareCounter::kRdmaRxAtomic, "Tx atomics"},
        {HardwareCounter::kRdmaTxRead, "OutRdmaReads"},
        {HardwareCounter::kRdmaTxWrite, "OutRdmaWrites"},
        {HardwareCounter::kRdmaTxSend, "OutRdmaSends"},
        {HardwareCounter::kRdmaTxAtomic, "Rx atomics"},
        {HardwareCounter::kRdmaBind, "RdmaBnd"},
        {HardwareCounter::kRdmaInvalidate, "RdmaInv"},
        {HardwareCounter::kIpv6Discards, "ip6InDiscards"},
        {HardwareCounter::kIpv6RxBytes, "ip6InOctets"},
        {HardwareCounter::kIpv6RxPackets, "ip6InPkts"},
        {HardwareCounter::kIpv6RxTruncatedPackets, "ip6InTruncatedPkts"},
        {HardwareCounter::kIpv6TxNoRoutes, "ip6OutNoRoutes"},
        {HardwareCounter::kIpv6TxBytes, "ip6OutOctets"},
        {HardwareCounter::kIpv6TxPackets, "ip6OutPkts"},
        {HardwareCounter::kIpv4Discards, "ip4InDiscards"},
        {HardwareCounter::kIpv4RxBytes, "ip4InOctets"},
        {HardwareCounter::kIpv4RxPackets, "ip4InPkts"},
        {HardwareCounter::kIpv4RxTruncatedPackets, "ip4InTruncatedPkts"},
        {HardwareCounter::kIpv4TxNoRoutes, "ip4OutNoRoutes"},
        {HardwareCounter::kIpv4TxBytes, "ip4OutOctets"},
        {HardwareCounter::kIpv4TxPackets, "ip4OutPkts"},
    };
    return counters;
  }

 private:
  IntrospectionIrdma() = delete;
  ~IntrospectionIrdma() = default;
  IntrospectionIrdma(const std::string& name, const ibv_device_attr& attr)
      : NicIntrospection(name, attr) {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_IRDMA_H_
