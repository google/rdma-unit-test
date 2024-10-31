# rdma-unit-test

The repository contains the **rdma-unit-test** framework. **rdma-unit-test** is an
open-source collection of unit tests to extensively test an
[ibverbs](https://github.com/linux-rdma/rdma-core) library/driver.

## Instructions

For instructions on how to setup the entire environment, see the Installation
Steps section in go/sync-rdma-unit-test-github. Once setup is complete, you can
clone the repo from https://github.com/google/rdma-unit-test and run your tests
with the help of the flags listed below.

For instructions on how to validate all changes before syncing to Github, please
read the full doc here go/sync-rdma-unit-test-github.

### Test Arguments

Several command line flags select specific behaviour to accommodate adapter
limitations or behaviour modification;

| flag        | default | description                                          |
| ----------- | ------- | ---------------------------------------------------- |
| verbs_mtu   | 4096    | Changes the mtu size for qp configuration. Note that |
:             :         : some adapters may have limited support for 4096 so   :
:             :         : this flag must be set to be within device            :
:             :         : constraints                                          :
| ipv4_only   | false   | If set, will only enumerate ports with ipv4          |
:             :         : addresses.                                           :
| device_name | none    | If set, will attempt to open the named device for    |
:             :         : all tests. If the device is not found                :
:             :         : **rdma_unit_test** will list the available devices.  :
