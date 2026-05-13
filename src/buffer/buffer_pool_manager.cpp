//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }
FrameHeader::FrameHeader(frame_id_t frame_id, page_id_t page_id) : frame_id_(frame_id), page_id_(page_id) { Reset(); }

// FrameHeader::FrameHeader(frame_id_t frame_id, bool is_dirty)
//   : frame_id_(frame_id), is_dirty_(is_dirty), data_(BUSTUB_PAGE_SIZE, 0)
//   { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : BufferPoolManager(num_frames, disk_manager, LRUK_REPLACER_K, log_manager) {}

BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 需要从磁盘管理器那里获取新的磁盘页, 然后获取一个新的page
  // 是会存在竞态条件的, 需要加锁
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  next_page_id_.fetch_add(1);
  if (!free_frames_.empty()) {
    // 有的话直接取, 添加到页表里
    frame_id_t fram_id = free_frames_.front();
    free_frames_.pop_front();
    page_table_.emplace(next_page_id_.load(), fram_id);
    frames_[fram_id]->Reset();
    return next_page_id_.load();
  }
  // 为空, 需要淘汰帧
  // 淘汰失败了
  auto frame = replacer_->Evict();
  if (frame != std::nullopt) {
    frame_id_t evict_fid = frame.value();
    auto evict_frame = frames_[evict_fid];

    // 找到被驱逐帧对应的旧 page_id
    page_id_t old_page_id = INVALID_PAGE_ID;
    for (auto &[pid, fid] : page_table_) {
      if (fid == evict_fid) {
        old_page_id = pid;
        break;
      }
    }
    // 脏页要写回磁盘
    // 官方测试应该是不需要判断脏页, 都要写回 evict_frame->is_dirty_
    if (old_page_id != INVALID_PAGE_ID) {
      std::promise<bool> flush_promise;
      auto flush_future = flush_promise.get_future();
      DiskRequest dr{true, evict_frame->GetDataMut(), old_page_id, std::move(flush_promise)};
      std::vector<DiskRequest> v_dr;
      v_dr.push_back(std::move(dr));
      disk_scheduler_->Schedule(v_dr);
      flush_future.wait();
    }
    // 删除旧映射
    if (old_page_id != INVALID_PAGE_ID) {
      page_table_.erase(old_page_id);
    }
    // 复用已有 frame
    evict_frame->Reset();
    page_table_.emplace(next_page_id_.load(), evict_fid);
    return next_page_id_.load();
  }
  // 没有有效的帧可以被分配
  return INVALID_PAGE_ID;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
// 如果页还存在于Bufferpool, 从数据库, 内存和磁盘删除一个页, 如果页的状态是pinned, 什么都不做且返回false,
// 空间不足就调用DeallocatePage给新的pages分配空间
// 需要依赖的成员变量
// page_table_ 是否还被管理
// 当前页是否被多个线程同时访问
// frames_ 是否这个存在被管理的frameHeader, 依赖frame_id
// free_frames 被删除后, 页对应的frame就会变成空闲状态
// 可能会使用到disk_scheduler_
// 还需要判断是否是脏页
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 一样存在并发
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  // 先判断是否存在这个页
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 页不在buffer pool中(已被驱逐或从未加载到内存), 直接从磁盘释放并返回true
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }
  // 存在判断这个页是否正在被多个线程访问, 是就拒绝删除
  if (frames_[it->second]->pin_count_.load() == 0) {
    // 判断是否脏页
    if (frames_[it->second]->is_dirty_) {
      // 做一个请求
      std::promise<bool> flush_promise;
      std::future<bool> flush_future = flush_promise.get_future();
      DiskRequest dr{true, frames_[it->second]->GetDataMut(), page_id, std::move(flush_promise)};
      std::vector<DiskRequest> v_dr;
      v_dr.push_back(std::move(dr));
      // 发起请求
      disk_scheduler_->Schedule(v_dr);
      flush_future.wait();
      frames_[it->second]->is_dirty_ = false;
    }
    // 页面写回了磁盘, 同时还是可以移除的, 进行内部管理操作
    frame_id_t id = frames_[it->second]->frame_id_;
    frames_[id]->is_dirty_ = false;
    page_table_.erase(page_id);
    replacer_->Remove(id);
    frames_[id]->Reset();
    free_frames_.push_back(id);
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }
  return false;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
// 确保一个页面可以被安全的写入
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 后面在处理AccessType的类型
  // 要写的页在缓存里
  // 主线程阻塞在了这里, 等待获取锁, 持有bpm_latch_, 等待rwlatch_构造
  // 另一个线程阻塞在WritePageGuard的构造函数里了, 它持有rwlatch_, 调用SetEvictable需要等待bpm_latch_,
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 当前页面已经被一个线程持有
    // frames_持有的是帧id
    // 在缓存里就通过缓存更改状态
    auto fram_ptr = frames_[page_table_[page_id]];
    fram_ptr->pin_count_.fetch_add(1);
    // 直接返回, 并设置状态为不可淘汰
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    lock.unlock();
    return WritePageGuard{page_id, fram_ptr, replacer_, bpm_latch_, disk_scheduler_};
  }
  // 缓存里没有, 从free_frames要
  if (!free_frames_.empty()) {
    frame_id_t free_frame_id = free_frames_.front();
    free_frames_.pop_front();
    frames_[free_frame_id]->Reset();
    frames_[free_frame_id]->pin_count_.fetch_add(1);
    page_table_.emplace(page_id, free_frame_id);
    // 从磁盘里得到这个页, 把数据写到帧里去
    std::promise<bool> promise;
    auto future = promise.get_future();
    DiskRequest dr(false, frames_[free_frame_id]->GetDataMut(), page_id, std::move(promise));
    std::vector<DiskRequest> v_dr;
    v_dr.push_back(std::move(dr));
    disk_scheduler_->Schedule(v_dr);
    future.wait();
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    lock.unlock();
    return WritePageGuard{page_id, frames_[free_frame_id], replacer_, bpm_latch_, disk_scheduler_};
  }
  if (free_frames_.empty()) {
    auto evict_result = replacer_->Evict();
    if (!evict_result.has_value()) {
      return std::nullopt;
    }
    frame_id_t evict_frame_id = evict_result.value();
    // 找到被驱逐帧对应的旧 page_id
    page_id_t old_page_id = INVALID_PAGE_ID;
    for (auto &[pid, fid] : page_table_) {
      if (fid == evict_frame_id) {
        old_page_id = pid;
        break;
      }
    }

    if (old_page_id == INVALID_PAGE_ID) {
      return std::nullopt;
    }
    if (frames_[evict_frame_id]->is_dirty_) {
      // 脏页直接写回磁盘（不能调 FlushPage，会死锁）
      std::promise<bool> flush_promise;
      auto flush_future = flush_promise.get_future();
      DiskRequest dr{true, frames_[evict_frame_id]->GetDataMut(), old_page_id, std::move(flush_promise)};
      std::vector<DiskRequest> v_dr;
      v_dr.push_back(std::move(dr));
      disk_scheduler_->Schedule(v_dr);
      flush_future.wait();
    }
    page_table_.erase(old_page_id);
    frames_[evict_frame_id]->Reset();
    std::promise<bool> promise2;
    auto future2 = promise2.get_future();
    DiskRequest dr2(false, frames_[evict_frame_id]->GetDataMut(), page_id, std::move(promise2));
    std::vector<DiskRequest> v_dr2;
    v_dr2.push_back(std::move(dr2));
    disk_scheduler_->Schedule(v_dr2);
    future2.wait();
    frames_[evict_frame_id]->pin_count_.fetch_add(1);
    page_table_.emplace(page_id, evict_frame_id);
    replacer_->SetEvictable(evict_frame_id, false);
    replacer_->RecordAccess(evict_frame_id);
    lock.unlock();
    return WritePageGuard{page_id, frames_[evict_frame_id], replacer_, bpm_latch_, disk_scheduler_};
  }
  // 返回错误
  return std::nullopt;
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 要读的页在缓存里
  // 测试通过后, 对重复的逻辑进行提取
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 当前页面已经被一个线程持有
    // frames_持有的是帧id
    // 在缓存里就通过缓存更改状态
    auto fram_ptr = frames_[page_table_[page_id]];
    fram_ptr->pin_count_.fetch_add(1);
    // 直接返回, 并设置状态为不可淘汰
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    lock.unlock();
    return ReadPageGuard{page_id, fram_ptr, replacer_, bpm_latch_, disk_scheduler_};
  }
  // 缓存里没有, 从free_frames要
  // 那么这个frame是被没在frames_里的
  // 更改状态通过free_frames
  if (!free_frames_.empty()) {
    frame_id_t free_frame_id = free_frames_.front();
    free_frames_.pop_front();
    frames_[free_frame_id]->Reset();
    frames_[free_frame_id]->pin_count_.fetch_add(1);
    page_table_.emplace(page_id, free_frame_id);
    // 从磁盘里得到这个页, 把数据读到帧里去
    std::promise<bool> promise;
    auto future = promise.get_future();
    DiskRequest dr(false, frames_[free_frame_id]->GetDataMut(), page_id, std::move(promise));
    std::vector<DiskRequest> v_dr;
    v_dr.push_back(std::move(dr));
    disk_scheduler_->Schedule(v_dr);
    future.wait();
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    lock.unlock();
    return ReadPageGuard{page_id, frames_[free_frame_id], replacer_, bpm_latch_, disk_scheduler_};
  }
  if (free_frames_.empty()) {
    auto evict_result = replacer_->Evict();
    if (!evict_result.has_value()) {
      return std::nullopt;
    }
    frame_id_t evict_frame_id = evict_result.value();
    // 找到被驱逐帧对应的旧 page_id
    page_id_t old_page_id = INVALID_PAGE_ID;
    for (auto &[pid, fid] : page_table_) {
      if (fid == evict_frame_id) {
        old_page_id = pid;
        break;
      }
    }

    if (old_page_id == INVALID_PAGE_ID) {
      return std::nullopt;
    }
    if (frames_[evict_frame_id]->is_dirty_) {
      // 脏页直接写回磁盘
      std::promise<bool> flush_promise;
      auto flush_future = flush_promise.get_future();
      DiskRequest dr{true, frames_[evict_frame_id]->GetDataMut(), old_page_id, std::move(flush_promise)};
      std::vector<DiskRequest> v_dr;
      v_dr.push_back(std::move(dr));
      disk_scheduler_->Schedule(v_dr);
      flush_future.wait();
    }
    page_table_.erase(old_page_id);
    frames_[evict_frame_id]->Reset();
    std::promise<bool> promise2;
    auto future2 = promise2.get_future();
    DiskRequest dr2(false, frames_[evict_frame_id]->GetDataMut(), page_id, std::move(promise2));
    std::vector<DiskRequest> v_dr2;
    v_dr2.push_back(std::move(dr2));
    disk_scheduler_->Schedule(v_dr2);
    future2.wait();
    frames_[evict_frame_id]->pin_count_.fetch_add(1);
    page_table_.emplace(page_id, evict_frame_id);
    replacer_->SetEvictable(evict_frame_id, false);
    replacer_->RecordAccess(evict_frame_id);
    lock.unlock();
    return ReadPageGuard{page_id, frames_[evict_frame_id], replacer_, bpm_latch_, disk_scheduler_};
  }
  // 返回错误
  return std::nullopt;
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
// 刷新页数据到磁盘, 非线程安全的,
// 页面数据修改了就写回磁盘, 也就是判断脏位
// 不在内存就返回false
// 不使用锁, 需要考虑如何触发is_dirty_
//
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 不可以使用当前提供的成员变量锁
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 判断脏位
    if (frames_[page_table_[page_id]]->is_dirty_) {
      // 刷新回磁盘
      frames_[page_table_[page_id]]->pin_count_.fetch_add(1);
      std::promise<bool> pr;
      std::future<bool> fu = pr.get_future();
      DiskRequest dr(true, frames_[page_table_[page_id]]->GetDataMut(), page_id, std::move(pr));
      std::vector<DiskRequest> v_dr;
      v_dr.push_back(std::move(dr));
      disk_scheduler_->Schedule(v_dr);
      fu.wait();
      frames_[page_table_[page_id]]->is_dirty_ = false;
      frames_[page_table_[page_id]]->pin_count_.fetch_sub(1);
      // return true;
    }
    return true;  // 页面存在但不是脏页
  }
  return false;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 判断脏位
    if (frames_[page_table_[page_id]]->is_dirty_) {
      // 刷新回磁盘
      // 得先进行pin_count的增加, 保证帧不会被淘汰
      frames_[page_table_[page_id]]->pin_count_.fetch_add(1);
      {
        std::promise<bool> pr;
        std::future<bool> fu = pr.get_future();
        // std::scoped_lock<std::shared_mutex> sh_lock(frames_[page_table_[page_id]]->rwlatch_);
        DiskRequest dr(true, frames_[page_table_[page_id]]->GetDataMut(), page_id, std::move(pr));
        std::vector<DiskRequest> v_dr;
        v_dr.push_back(std::move(dr));
        disk_scheduler_->Schedule(v_dr);
        fu.wait();
        frames_[page_table_[page_id]]->is_dirty_ = false;
      }
      // 刷新完成, 减少持有
      frames_[page_table_[page_id]]->pin_count_.fetch_sub(1);
    }
    return true;  // 页面存在
  }
  return false;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 这里不加锁了, 因为涉及到调用另一个加锁的函数, 防止死锁
  for (auto &[page_id, _] : page_table_) {
    FlushPageUnsafe(page_id);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 同上
  for (auto &[page_id, _] : page_table_) {
    FlushPage(page_id);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::scoped_lock<std::mutex> sh_lock(*bpm_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    return frames_[page_table_[page_id]]->pin_count_.load();
  }
  return std::nullopt;
}

}  // namespace bustub
