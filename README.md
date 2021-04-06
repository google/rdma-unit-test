# rdma-unit-test

The repository contains the rdma-unit-test framework. rdma-unit-test is an
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
to fix overly-specificified tests are encouraged.

## Installation

Prior to building the ibverb user space libraries must be installed.

    sudo apt install libibverbs libibverbs-dev

The user space libraries are supported and packaged [here](https://github.com/linux-rdma/rdma-core)

rdma-unit-test also requires [nl3](https://www.infradead.org/~tgr/libnl/).

## Introspection

Introspection is used to selectively enable tests depending on hardware
capabilities. device\_attr is incomplete and miss-reported by some NICs. In
many cases introspection dictates expected behavior in undefined areas of the
specification. Adding introspection support for a new NIC requires 2 changes:

1.  Extend NicIntrospection (ex. introspection\_rxe.h)
2.  Update gunit\_main.cc to register the new introspection.
