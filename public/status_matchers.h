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

#ifndef RDMA_UNIT_TEST_CASES_STATUS_MATCHERS_H_
#define RDMA_UNIT_TEST_CASES_STATUS_MATCHERS_H_

#include "gmock/gmock.h"

// A set of helpers to test the result of absl::Status
#ifndef TESTING_BASE_PUBLIC_GMOCK_UTILS_STATUS_MATCHERS_H_
#define CHECK_OK(expr) CHECK_EQ((expr), absl::OkStatus())  // Crash ok
#define DCHECK_OK(expr) DCHECK_EQ((expr), absl::OkStatus())
#define EXPECT_OK(expr) EXPECT_EQ((expr), absl::OkStatus())
#define ASSERT_OK(expr) ASSERT_EQ((expr), absl::OkStatus())

#define CONCAT_IMPL(x, y) x##y
#define CONCAT_MACRO(x, y) CONCAT_IMPL(x, y)

#define ASSERT_OK_AND_ASSIGN(lhs, rexpr) \
  ASSERT_OK_AND_ASSIGN_IMPL(CONCAT_MACRO(_status_or, __COUNTER__), lhs, rexpr)

#define ASSERT_OK_AND_ASSIGN_IMPL(statusor, lhs, rexpr)     \
  auto statusor = (rexpr);                                  \
  ASSERT_TRUE(statusor.status().ok()) << statusor.status(); \
  lhs = std::move(statusor.value())

#define ASSIGN_OR_RETURN(lhs, rexpr) \
  ASSIGN_OR_RETURN_IMPL(CONCAT_MACRO(_status_or, __COUNTER__), lhs, rexpr)

#define ASSIGN_OR_RETURN_IMPL(statusor, lhs, rexpr) \
  auto statusor = (rexpr);                          \
  if (!statusor.ok()) {                             \
    return statusor.status();                       \
  }                                                 \
  lhs = std::move(statusor.value())

#define RETURN_IF_ERROR(expr)                                                \
  do {                                                                       \
    const ::absl::Status _status = (expr);                                   \
    if (!_status.ok()) {                                                     \
      LOG(ERROR) << "Return Error: " << #expr << " failed with " << _status; \
      return _status;                                                        \
    }                                                                        \
  } while (0)

#endif

#endif  // RDMA_UNIT_TEST_CASES_STATUS_MATCHERS_H_