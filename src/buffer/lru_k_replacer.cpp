//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <climits>
#include <utility>
#include "common/exception.h"
namespace bustub {

LRUKNode::LRUKNode(const frame_id_t frame, std::size_t k) : k_(k), fid_(frame) {}

auto LRUKNode::GetKDistance() -> std::size_t {
  if (history_.size() < k_) {
    return ULONG_MAX;
  }
  auto iter = history_.rbegin();
  for (std::size_t i = 0; i < k_ - 1; ++i) {
    ++iter;
  }
  return *iter;
}
/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
// 按照规则移除不应该留在缓存里的帧
// 从当前时间戳的块向过去数K个帧, 如果存在, 就移除
// 如果不存在第K个帧, 就标记为inf + 对应值
// 存在多个时就移除时间戳最早的那个
// 淘汰当前记录的每个帧里的所有可淘汰的节点
struct Com {
  auto operator()(std::tuple<std::size_t, std::size_t, frame_id_t> &a,
                  std::tuple<std::size_t, std::size_t, frame_id_t> &b) -> bool {
    if (std::get<0>(a) != std::get<0>(b)) {
      // 当k-dis相同时, 比较第k+1个元素, 如果不存在记为inf, 存在则记录这个值, 当k-dis相同的时候, 比较这个值,
      // 大的优先删除
      return std::get<0>(a) > std::get<0>(b);
    }
    return std::get<1>(a) < std::get<1>(b);
  }
};

auto LRUKNode::GetKDistance(std::size_t k) -> std::size_t {
  if (history_.size() < k) {
    return ULONG_MAX;
  }
  auto iter = history_.rbegin();
  for (std::size_t i = 1; i < k; ++i) {
    ++iter;
  }
  return *iter;
}
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> lock(latch_);
  std::vector<std::tuple<std::size_t, std::size_t, frame_id_t>> v;
  for (auto &[frame, node] : node_store_) {
    if (node.is_evictable_) {
      std::size_t last_k_vis_timestamp = node.GetKDistance();
      std::size_t backward_k_dis =
          (last_k_vis_timestamp == ULONG_MAX) ? ULONG_MAX : current_timestamp_.load() - last_k_vis_timestamp;

      std::size_t earliest = node.history_.front();
      v.emplace_back(backward_k_dis, earliest, frame);
    }
  }
  if (v.empty()) {
    return std::nullopt;
  }

  std::sort(v.begin(), v.end(), Com());

  frame_id_t frame_to_evict = std::get<2>(v.front());
  node_store_.erase(frame_to_evict);
  curr_size_.fetch_sub(1);
  return frame_to_evict;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  current_timestamp_++;
  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    node_store_.emplace(frame_id, LRUKNode{frame_id, k_.load()});
  }
  node_store_[frame_id].history_.push_back(current_timestamp_.load());
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    return;
  }

  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    node_store_[frame_id].is_evictable_ = false;
    curr_size_.fetch_sub(1);
  } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    node_store_[frame_id].is_evictable_ = true;
    curr_size_.fetch_add(1);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    return;
  }

  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (node_store_[frame_id].is_evictable_) {
    curr_size_.fetch_sub(1);
    node_store_.erase(frame_id);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_.load(); }

}  // namespace bustub
