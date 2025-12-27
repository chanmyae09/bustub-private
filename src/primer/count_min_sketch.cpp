//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <atomic>
#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width <= 0 || depth <= 0) {
    throw std::invalid_argument("Width and depth must be greater than 0");
  }
  counters_ = std::vector<std::atomic<uint32_t>>(width * depth);

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_),
      depth_(other.depth_),
      hash_functions_(std::move(other.hash_functions_)),
      counters_(std::move(other.counters_)) {
  /** @TODO(student) Implement this function! */
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  width_ = other.width_;
  depth_ = other.depth_;
  counters_ = std::move(other.counters_);
  hash_functions_ = std::move(other.hash_functions_);
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (uint32_t i = 0; i < depth_; ++i) {
    size_t column = hash_functions_[i](item);
    counters_[width_ * i + column].fetch_add(1);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (uint32_t i = 0; i < depth_; ++i) {
    for (uint32_t j = 0; j < width_; ++j) {
      counters_[width_ * i + j].fetch_add(other.counters_[width_ * i + j].load());
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min = counters_[hash_functions_[0](item)].load();
  for (uint32_t i = 1; i < depth_; ++i) {
    if (counters_[width_ * i + hash_functions_[i](item)].load() < min) {
      min = counters_[width_ * i + hash_functions_[i](item)].load();
    }
  }
  return min;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (uint32_t i = 0; i < depth_; ++i) {
    for (uint32_t j = 0; j < width_; ++j) {
      counters_[width_ * i + j].store(0);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  std::vector<std::pair<KeyType, uint32_t>> vec;
  for (size_t i = 0; i < candidates.size(); ++i) {
    vec.push_back(std::make_pair(candidates[i], Count(candidates[i])));
  }
  std::sort(vec.begin(), vec.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
  size_t result_size = std::min(static_cast<size_t>(k), vec.size());
  return std::vector<std::pair<KeyType, uint32_t>>(vec.begin(), vec.begin() + result_size);

  return {};
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
