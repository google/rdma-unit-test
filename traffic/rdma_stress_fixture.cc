// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "traffic/rdma_stress_fixture.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/status_matchers.h"
#include "traffic/client.h"
#include "traffic/latency_measurement.h"
#include "traffic/op_types.h"
#include "traffic/qp_state.h"
#include "traffic/transport_validation.h"

namespace rdma_unit_test {

RdmaStressFixture::RdmaStressFixture() {
  validation_ = std::make_unique<TransportValidation>();
  latency_measure_ = std::make_unique<LatencyMeasurement>();
  // Open the verbs device available.
  absl::Status status = ibv_.OpenAllDevices(contexts_);
  CHECK_OK(status);  // Crash OK
  for (auto& context : contexts_) {
    port_attrs_.push_back(ibv_.GetPortAttribute(context));
    // Change the blocking mode of the async event queue.
    LOG(INFO) << "Allow getting asynchronous events in nonblocking mode.";
    int flags = TEMP_FAILURE_RETRY(fcntl(context->async_fd, F_GETFL));
    if (flags < 0) {
      LOG(ERROR) << "Failed reading async_fd file status flags on device "
                 << context->device->name
                 << ". Calls to PollAndAckAsyncEvents will remain blocking.";
      return;
    }
    int ret = TEMP_FAILURE_RETRY(
        fcntl(context->async_fd, F_SETFL, flags | O_NONBLOCK));
    LOG_IF(ERROR, ret < 0)
        << "Failed setting async events queue to nonblocking"
        << " mode on device " << context->device->name
        << ". Calls to PollAndAckAsyncEvents will remain blocking.";
  }
}

absl::Status RdmaStressFixture::SetUpRcClientsQPs(Client* local,
                                                  uint32_t local_qp_id,
                                                  Client* remote,
                                                  uint32_t remote_qp_id,
                                                  QpAttribute qp_attr) {
  if (local_qp_id >= local->num_qps() || remote_qp_id >= remote->num_qps()) {
    return absl::InvalidArgumentError(
        "Please create qps before setting up the connection!");
  }
  QpState* local_qp = local->qp_state(local_qp_id);
  QpState* remote_qp = remote->qp_state(remote_qp_id);
  local_qp->set_remote_qp_state(remote_qp);
  remote_qp->set_remote_qp_state(local_qp);
  auto status = ibv_.ModifyLoopbackRcQpResetToRts(
      local->qp_state(local_qp_id)->qp(), remote->qp_state(remote_qp_id)->qp(),
      port_attr(), qp_attr);
  if (status.ok()) {
    LOG(INFO) << absl::StrCat("Connect local Client", local->client_id(),
                              ", QP (id): ", local_qp_id, ", to remote Client",
                              remote->client_id(), " QP (id): ", remote_qp_id);
  }
  return status;
}

void RdmaStressFixture::CreateSetUpRcQps(Client& initiator, Client& target,
                                         uint16_t qps_per_client,
                                         QpAttribute qp_attr) {
  for (int i = 0; i < qps_per_client; ++i) {
    absl::StatusOr<uint32_t> initiator_qp_id =
        initiator.CreateQp(/*is_rc=*/true);
    absl::StatusOr<uint32_t> target_qp_id = target.CreateQp(/*is_rc=*/true);
    CHECK_OK(initiator_qp_id);  // Crash OK.
    CHECK_OK(target_qp_id);     // Crash OK.

    // Set up Qpairs.
    EXPECT_OK(SetUpRcClientsQPs(&initiator, initiator_qp_id.value(), &target,
                                target_qp_id.value(), qp_attr));
    EXPECT_OK(SetUpRcClientsQPs(&target, target_qp_id.value(), &initiator,
                                initiator_qp_id.value(), qp_attr));
  }
  LOG(INFO) << "Successfully created " << qps_per_client
            << " new qps per client. Total qps: "
            << initiator.num_qps() + target.num_qps();
}

void RdmaStressFixture::CreateSetUpOneToOneUdQps(Client& initiator,
                                                 Client& target,
                                                 uint16_t qps_per_client) {
  EXPECT_OK(validation_->TransportSnapshot());
  for (uint32_t i = 0; i < qps_per_client; ++i) {
    absl::StatusOr<uint32_t> initiator_qp_id =
        initiator.CreateQp(/*is_rc=*/false);
    absl::StatusOr<uint32_t> target_qp_id = target.CreateQp(/*is_rc=*/false);

    CHECK_OK(initiator_qp_id);  // Crash OK
    CHECK_OK(target_qp_id);     // Crash OK
    QpState* initiator_qp = initiator.qp_state(initiator_qp_id.value());
    QpState* target_qp = target.qp_state(target_qp_id.value());

    ibv_ah* ah = initiator.CreateAh(port_attr());
    initiator_qp->add_ud_destination(target_qp, ah);
  }
}

void RdmaStressFixture::CreateSetUpMultiplexedUdQps(
    Client& initiator, Client& target, uint16_t initiator_qps,
    uint16_t target_qps, AddressHandleMapping ah_mapping) {
  std::vector<uint32_t> initiator_qp_ids;
  std::vector<uint32_t> target_qp_ids;

  // Create initiator and target QPs.
  for (uint32_t i = 0; i < initiator_qps; ++i) {
    absl::StatusOr<uint32_t> initiator_qp_id =
        initiator.CreateQp(/*is_rc=*/false);
    CHECK_OK(initiator_qp_id);  // Crash OK
    initiator_qp_ids.push_back(initiator_qp_id.value());
  }
  for (uint32_t i = 0; i < target_qps; ++i) {
    absl::StatusOr<uint32_t> target_qp_id = target.CreateQp(/*is_rc=*/false);
    CHECK_OK(target_qp_id);  // Crash OK
    target_qp_ids.push_back(target_qp_id.value());
  }

  switch (ah_mapping) {
    case AddressHandleMapping::kIndependent:
      // For each unique initiator-target pairing, a separate independent
      // AddressHandle is created.
      for (const auto initiator_qp_id : initiator_qp_ids) {
        for (const auto target_qp_id : target_qp_ids) {
          ibv_ah* ah = initiator.CreateAh(port_attr());
          QpState* initiator_qp = initiator.qp_state(initiator_qp_id);
          QpState* target_qp = target.qp_state(target_qp_id);
          CHECK(initiator_qp);  // Crash OK
          initiator_qp->add_ud_destination(target_qp, ah);
        }
      }
      break;
    case AddressHandleMapping::kShared:
      // All initiator-target pairings use the same shared AddressHandle.
      ibv_ah* ah = initiator.CreateAh(port_attr());
      for (const auto initiator_qp_id : initiator_qp_ids) {
        for (const auto target_qp_id : target_qp_ids) {
          QpState* initiator_qp = initiator.qp_state(initiator_qp_id);
          CHECK(initiator_qp);  // Crash OK
          QpState* target_qp = target.qp_state(target_qp_id);
          initiator_qp->add_ud_destination(target_qp, ah);
        }
      }
      break;
  }
}

void RdmaStressFixture::HaltExecution(Client& client) {
  // Log the operations in flight, for debugging purposes.
  client.DumpPendingOps();
  client.CheckAllDataLanded();

  // Log a summary of all qps.
  for (uint32_t qp_id = 0; qp_id < client.num_qps(); ++qp_id) {
    LOG(INFO) << client.qp_state(qp_id)->ToString();
  }

  // Keep polling async events for possible errors until no more events exist.
  while (true) {
    auto async_event_status = PollAndAckAsyncEvents(client.GetContext());
    if (async_event_status.ok() || absl::IsUnavailable(async_event_status))
      break;
    LOG(ERROR) << async_event_status.message();
  }
}

absl::Status RdmaStressFixture::PollAndAckAsyncEvents() {
  // Poll and Ack AE for all contexts in the fixture.
  for (auto context : contexts_) {
    auto status = PollAndAckAsyncEvents(context);
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

absl::Status RdmaStressFixture::PollAndAckAsyncEvents(ibv_context* context) {
  // Poll on the async fd of the RDMA context, check if an event is available.
  pollfd poll_fd{};
  poll_fd.fd = context->async_fd;
  poll_fd.events = POLLIN;
  int millisec_timeout = 0;
  int ret = TEMP_FAILURE_RETRY(poll(&poll_fd, 1, millisec_timeout));
  if (0 == ret) {
    return absl::OkStatus();
  }

  if (ret < 0) {
    return absl::InternalError(
        absl::StrCat("poll failed with errno ", errno, " on async event fd."));
  }

  // Read the ready event.
  ibv_async_event event{};
  ret = ibv_get_async_event(context, &event);
  if (ret) {
    return absl::UnavailableError("Async event doesn't exist.");
  }

  auto status = absl::InternalError(absl::StrCat(
      "Verbs async event received event type: ", event.event_type));
  // Acknowledge the event, or else we can't destroy the verbs resources
  // involving the received async event.
  ibv_ack_async_event(&event);
  return status;
}

int RdmaStressFixture::LimitNumOps(int op_size, int num_ops) {
  // When testing large op_size (16k), limit the total number of ops no greater
  // than 100k.
  if (op_size >= 16384) {
    return std::min(num_ops, 100000);
  }
  return num_ops;
}

bool RdmaStressFixture::AllowRetxCheck(int num_qp) {
  return true;
}

void RdmaStressFixture::ConfigureLatencyMeasurements(OpTypes op_type) {
  latency_measure_->ConfigureLatencyMeasurements(op_type);
}

void RdmaStressFixture::CollectClientLatencyStats(const Client& client) {
  latency_measure_->CollectClientLatencyStats(client);
}

void RdmaStressFixture::CheckLatencies() { latency_measure_->CheckLatencies(); }

}  // namespace rdma_unit_test
