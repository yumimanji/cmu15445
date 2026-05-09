//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  if (n_bits < 0) {
    return;
  }
  bucket_nums_ = n_bits;
  bucket_.resize(1 << n_bits, 0);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return std::bitset<BITSET_CAPACITY>{hash};
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  // 这里的实现考虑了不同位数的情况, 不考虑直接返回std::countl_zero(val)即可
  // uint64_t val = bset.to_ullong();
  // if (val == 0)
  // {
  //   return BITSET_CAPACITY;
  // }
  // constexpr int bits = std::numeric_limits<uint64_t>::digits;
  // return std::countl_zero(val) - (bits - BITSET_CAPACITY);
  return std::__countl_zero(bset.to_ullong());
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  // 从左到右开始二进制的高b位,作为当前存储元素在bucket_的下标.
  // 从b+1开始找第一个非0的位置idx, 使用idx-b作为存储元素的值, 存在多个时取最大值
  // 这里锁要包含整个计算过程还是只在更新值的时候加锁呢
  // 首先需要对键进行哈希
  hash_t h = CalculateHash(val);
  // 得到哈希值的二进制形式
  auto tmp_bitset = ComputeBinary(h);
  // 计算这个二进制串前bucket_nums代表的十进制数, 就是bucket_中的下标
  uint8_t idx = (tmp_bitset >> (BITSET_CAPACITY - bucket_nums_)).to_ulong();
  //从b+1开始找第一个非0的位置idx, 使用idx-b作为存储元素的值, 存在多个时取最大值
  // 计算前导0的数量, 计算剩余位置全是0也要遵循这个前导0数量加1的规则
  uint8_t leading_zero_count = PositionOfLeftmostOne(std::move(tmp_bitset << bucket_nums_)) + 1;
  {
    std::unique_lock<std::mutex> lock(mtx_);
    bucket_[idx] = std::max(leading_zero_count, bucket_[idx]);
  }
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  // 1. constant = CONSTANT * accumlate(bucket_), 这是错的, 应该是官网写错了, 没有这一步
  // 2. 遍历bucket_, 计算pow(2, (-bucket_[i]))并累加至count
  // 3. 基数 = constant * m * (m/count)

  // long double constant = CONSTANT * std::accumulate(bucket_.begin(), bucket_.end(), 0.0);
  // 测试程序里有传参为负数的情况
  if (bucket_nums_ == -1) {
    return;
  }
  long double count = std::accumulate(bucket_.begin(), bucket_.end(), 0.0,
                                      [](long double a, long double b) { return a + std::exp2(-b); });
  {
    std::unique_lock<std::mutex> lock(mtx_);
    cardinality_ = static_cast<std::size_t>(CONSTANT * bucket_.size() * bucket_.size() / count);
  }
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
