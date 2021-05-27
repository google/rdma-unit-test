# rdma-unit-test

The repository contains the **rdma-unit-test** framework. **rdma-unit-test** is an
open-source collection of unit tests to extensively test an
[ibverbs](https://github.com/linux-rdma/rdma-core) library/driver.

## Setup

These are single-machine single-nic tests which focus on stressing the
ibverbs interface. The NIC must be able to issue loopback operations to
itself (via internal loopback, bouncing off a switch, or any other setup
which reflects the packets). The tests assume global routing. See flags
for ways to control IP version and MTU.

Explicitly out of scope is stressing the fabric and transport.

## Compliance

These are practical tests designed to understand the behavior of a stack. In
many places they check constraints beyond what is stated in the Infiniband
specification. Do not assume test failures indicate bad hardware, it could be
that your device diverged in an undefined area of the specification. Patches
to fix overly-specified tests are encouraged.

## Installation

**rdma-unit-test** has been built/tested on **CentOS 8.3.2011** using a Mellonox
ConnectX-4 Dual port 25Gbe adapter.  Installation requirements may vary
depending upon the distro used. CentOS installation uses the standard Server configuration with the following
options;

* Infiniband support
* Development Tools

Additional requirements;

* ivberbs development libraries; **sudo yum install libibverbs-devel**
* bazel build tool; instructions [here](https://docs.bazel.build/versions/master/install-redhat.html)

The user space libraries are supported and packaged [here](https://github.com/linux-rdma/rdma-core)

## Introspection

Introspection is used to selectively enable tests depending on hardware
capabilities. device\_attr is incomplete and miss-reported by some NICs. In
many cases introspection dictates expected behavior in undefined areas of the
specification. Adding introspection support for a new NIC requires 2 changes:

1.  Extend NicIntrospection (ex. introspection\_rxe.h)
2.  Update gunit\_main.cc to register the new introspection.

## Building/Testing
First clone **rdma-unit-test** into a workspace directory.

    git clone https://github.com/google/rdma-unit-test.git

Change to the project directory and run the tests. The following will
recursively build and execute all the test targets.

    cd rdma-unit-test
    bazel test ... [--test_output=all] [--test_arg=<flag>]

An example using SoftRoce;

    cd rdma-unit-test
    bazel test ... --test_output=all --test_arg=--verbs_mtu=1024 --test_arg=--no_ipv6_for_gid

It has been observed that some ibverb libraries require root priviledges to
create verb objects. If this is the case you can either run bazel under root
or execute each individual test.

bazel's caches the workspace so if you run bazel as root it will rebuild the
targets as root.  In the project directory;

    cd rdma-unit-test
    sudo bazel test ... [--test_output=all] [--test_arg=<flag>]

The tests are divided into categories in the 'cases' directory. To run a
specific category of tests run bazel test in the 'cases' directory;

    cd rdma-unit-test/cases
    bazel test device_test [--test_output=all] [--test_arg=<flag>]

You can also provide provide a filter to only run specific test within a
category. In the cases directory;

    cd rdma-unit-test/cases
    bazel test device_test [--test_output=all] [--test_arg=<flag>]

You can also run the test executables directly. This method does not support
bazel test flag options; e.g. --test_output, --test_arg

    cd rdma-unit-test/bazel-bin/cases
    ./device_test [--verbs_mtu=1024] [--no_ipv6_for_gid]

### Test Arguments
Several command line flags select specific behaviour to accomodate adapter
limitations or behaviour modification;

flag | default | description
-----|---------|------------
verbs_mtu | 4096 | Changes the mtu size for qp configuration. Note that some adapters may have limited support for 4096 so this flag must be set to be within device contraints
no_ipv6_for_mtu | false | If set, will only enumerate ports with ipv4 addresses.
device_name | none | If set, will attempt to open the named device for all tests. If the device is not found **rdma_unit_test** will list the available devices.


## Device Support
**rdma-unit-test** uses ibv_get_device_list to find all available devices. By
default **rdma-unit-test** will use the first device found with an active port.
To select a difference device name or port use the **--device_name** flag.

This package has been tested on the following adapters;

* Mellonox ConnectX-3
* Mellonox ConnectX-4              (may require **--verbs_mtu=1024**)
* SoftRoce (*limited support*, requires **--no_ipvp6_for_gid --verbs_mtu=1024**)
