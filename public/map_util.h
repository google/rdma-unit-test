#ifndef THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_MAP_UTIL_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_PUBLIC_MAP_UTIL_H_

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

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
  CHECK_EQ(1, map.erase(key));  // Crash ok
}

template <typename T>
void CheckPresentAndErase(absl::flat_hash_set<T>& set, const T& elem) {
  CHECK_EQ(1, set.erase(elem));  // Crash ok
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
