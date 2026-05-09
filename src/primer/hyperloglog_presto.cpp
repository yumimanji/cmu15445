//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  if (n_leading_bits < 0) {
    return;
  }
  bucket_nums_ = n_leading_bits;
  dense_bucket_.resize(1 << n_leading_bits, 0);
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  if (bucket_nums_ < 0) {
    return;
  }
  auto bits = ComputeBinary(CalculateHash(val));

  uint16_t idx = 0;
  if (bucket_nums_ > 0) {
    idx = static_cast<uint16_t>((bits >> (64 - bucket_nums_)).to_ullong());
  }

  // 后导0的最大数量就是64 - bucket_nums;
  uint8_t tail_zero = std::min(ComputeBackZero(bits), static_cast<uint8_t>(64U - bucket_nums_));

  // 计算当前的值
  auto val_dense_bucket = static_cast<uint8_t>(dense_bucket_[idx].to_ullong());
  if (overflow_bucket_.count(idx) > 0) {
    val_dense_bucket += static_cast<uint8_t>(overflow_bucket_[idx].to_ullong());
  }
  if (tail_zero > val_dense_bucket) {
    {
      std::lock_guard<std::mutex> lock(mtx_);
      // 保留低4位
      dense_bucket_[idx] = std::bitset<DENSE_BUCKET_SIZE>(tail_zero & 0x0F);
      // 保留高3位
      overflow_bucket_[idx] = std::bitset<OVERFLOW_BUCKET_SIZE>(tail_zero >> 4);
    }
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (bucket_nums_ < 0 || dense_bucket_.empty()) {
    return;
  }
  // Use `double` here to match Presto's behavior and ensure deterministic
  // rounding in the provided test cases.
  double ans = 0.0;
  for (std::size_t i = 0; i < dense_bucket_.size(); ++i) {
    // 要把空桶的贡献也算上, 还有dense_bucket_和overflow_bucket_的下标的含义是不同的
    uint64_t ct = dense_bucket_[i].to_ullong();
    if (overflow_bucket_.count(static_cast<uint16_t>(i)) > 0) {
      ct += (overflow_bucket_[static_cast<uint16_t>(i)].to_ullong() << 4);
    }
    ans += std::exp2(-static_cast<double>(ct));
  }
  if (ans == 0) {
    return;
  }
  {
    std::unique_lock<std::mutex> lock(mtx_);
    cardinality_ = static_cast<uint64_t>(CONSTANT * dense_bucket_.size() * dense_bucket_.size() / ans);
  }
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
