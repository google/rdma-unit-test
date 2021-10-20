# RDMA Random Walker

go/rdma-random-walker

<!--*
# Document freshness: For more information, see go/fresh-source.
freshness: { owner: 'daweihuang' reviewed: '2021-10-15' }
*-->

[TOC]

## Overview

The RDMA random walker is a random walk tester for stressing any RDMA providers/
NICs and explore state spaces of the HW/SW systems that are hard to reach using
regular unit test. It does so by repeatedly issuing random but mostly valid RDMA
verbs and observing any failures or crashes.

### How to run the test.

The random walker test target can be run using the following command:

./run_random_walk --clients=5 --duration=20 --multinode=false

Test flags:

*   `clients` The number of random walker clients that issues random verbs
    commands. The default number is 2.
*   `duration` The number of seconds that each client will try to run. The
    default value is 20.
*   `multinode` A boolean flag that indicate whether the random walker will be
    using gRPC to synchronize out-of-band metadata across different clients.
    Disabled by default.

## Architecture

The random walker mainly consists of multiple *random walk clients*, each runs
on its own thread and with a single opened device context (`ibv_context`). The
client randomly issues valid Verbs commands to the NIC. The random walk clients
exchanges out-of-band metadata with the *OOB backend* that either uses shared
thread-safe queue or gRPC. Both random walk clients and OOB backend is set up
and managed by the *test orchestrator*.

## Random Walk Specification

The random walker runs in steps. Each step it take an *action* drawn randomly
from its action space according to some customizable weighted distribution.
Paramters for each action are also randomly generated. Metadata for actions that
have implication on other clients (such as registration of a memory region or
binding of a memory windows) will get propagated to other clients using in OOB
backend.

### Action Space

The following table summarize the list of actions.

| Action                 | Verbs API        | Note                      |
| ---------------------- | ---------------- | ------------------------- |
| Create an `ibv_cq`     | `ibv_create_cq`  |                           |
| Destroy an `ibv_cq`    | `ibv_destroy_cq` |                           |
| Allocate an `ibv_pd`   | `ibv_alloc_pd`   |                           |
| Deallocate an `ibv_pd` | `ibv_dealloc_pd` |                           |
| Register an `ibv_mr`   | `ibv_reg_mr`     |                           |
| Deregister an `ibv_mr` | `ibv_dereg_mr`   |                           |
| Allocate an `ibv_mw`   | `ibv_alloc_mw`   |                           |
| Deallocate an `ibv_mw` | `ibv_dealloc_mw` |                           |
| Bind an `ibv_mw`       | `ibv_bind_mw` ,  |                           |
:                        : `ibv_post_send`  :                           :
| Create a pair of       | `ibv_create_qp`, |                           |
: interconnected RC      : `ibv_modify_qp`  :                           :
: `ibv_qp`               :                  :                           :
| Create a UD `ibv_qp`   | `ibv_create_qp`, |                           |
:                        : `ibv_modify_qp`  :                           :
| Modify an `ibv_qp` to  | `ibv_modify_qp`  |                           |
: ERROR state            :                  :                           :
| Destroy an `ibv_qp`    |                  | QP must be in ERROR state |
| Create an `ibv_ah`     | `ibv_create_ah`  |                           |
| Destroy an `ibv_ah`    | `ibv_destroy_ah` |                           |
| Post a `SEND` WR       | `ibv_post_send`  |                           |
| Post a `SEND_WITH_INV` | `ibv_post_send`  |                           |
: WR                     :                  :                           :
| Post a `RECV` WR       | `ibv_post_recv`  |                           |
| Post a `READ` WR       | `ibv_post_send`  |                           |
| Post a `WRITE` WR      | `ibv_post_send`  |                           |
| Post a `FETCH_ADD` WR  | `ibv_post_send`  |                           |
| Post a `COMP_SWAP` WR  | `ibv_post_send`  |                           |

For parameters and how it is sampled, please refer to the
[random_walk_client.cc](https://github.com/google/rdma-unit-test/blob/master/random_walk/internal/random_walk_client.cc)
for details.

### Result Validation

Validation of action results on random walk testers is a notoriously difficult
problem. Ideally one would like to assert the actual result of each verb, such
as return codes, completion statuses and buffer contents. However, this is
difficult or even infeasible because data races (as applied to buffer content)
and inability to access provider’s internal state (e.g. whether a ibv_post_send
should succeed depends on the number of outstanding work requests on the work
queue).

Therefore, we make no or little assertion on the results of each action.
Specifically, we make no assertion on both completion status and buffer content.
Assertions are only made if the random walk client cannot proceed without it
(e.g. failure to open a device for a client). Some additional assertions can be
made by the rdma_unit_test framework, e.g. asserting posting to send/recv queue
will always be successful, which is not fully justified and can lead to (rare)
false positives.

To assist triaging, all return status and return error codes that are anything
but success will be recorded and logged. Moreover, the random walker will also
keep track of a number (default 200) of most recent actions with their
parameters and results to assist triaging of any failures.
