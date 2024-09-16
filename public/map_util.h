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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_MAP_UTIL_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_MAP_UTIL_H_

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"

namespace rdma_unit_test {
namespace map_util {

template <typename K, typename V>
V* FindOrNull(absl::flat_hash_map<K, V>& map, const K& key) {
  auto iter = map.find(key);
  if (iter == map.end()) {
    return nullptr;
  }
  return &iter->second;
}

template <typename K, typename V>
V FindOrDie(const absl::flat_hash_map<K, V>& map, const K& key) {
  auto iter = map.find(key);
  CHECK(iter != map.end());  // Crash ok
  return iter->second;
}

template <typename K, typename V>
void CheckPresentAndErase(absl::flat_hash_map<K, V>& map, const K& key) {
  CHECK_EQ(map.erase(key), 1ul);  // Crash ok
}

template <typename T>
void CheckPresentAndErase(absl::flat_hash_set<T>& set, const T& elem) {
  CHECK_EQ(set.erase(elem), 1ul);  // Crash ok
}

template <typename E, typename K>
void CheckPresentAndErase(absl::flat_hash_set<E>& set, const K& key) {
  auto iter = set.find(key);
  CHECK(iter != set.end());  // Crash ok
  set.erase(iter);
}

template <typename K, typename V>
void InsertOrDie(absl::flat_hash_map<K, V>& map, const K& k, const V& v) {
  auto result = map.emplace(k, v);
  CHECK(result.second);  // Crash ok
}

template <typename T>
void InsertOrDie(absl::flat_hash_set<T>& set, const T& t) {
  auto result = set.emplace(t);
  CHECK(result.second);  // Crash ok
}

template <typename K, typename V>
V ExtractOrDie(absl::flat_hash_map<K, V>& map, const K& key) {
  auto iter = map.find(key);
  CHECK(iter != map.end());  // Crash ok
  V ret = iter->second;
  map.erase(iter);
  return ret;
}

}  // namespace map_util
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_MAP_UTIL_H_
