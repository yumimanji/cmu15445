//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT
#include <vector>

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;
  DiskRequest() = default;
  DiskRequest(bool is_write, char *dt, page_id_t page_id, std::promise<bool> pr) noexcept;

  // 不知道为啥这两个函数加了就会报错
  // DiskRequest(DiskRequest& dr) noexcept {};
  // auto operator=(DiskRequest& dr) -> DiskRequest& {return *this;};

  DiskRequest(DiskRequest &&other) noexcept;
  auto operator=(DiskRequest &&other) noexcept -> DiskRequest &;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
// 后台线程在DiskScheduler构造时进行创建, 析构时join
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);

  // DiskScheduler(DiskScheduler &ds) {};
  // auto operator=(DiskScheduler &ds) ->DiskScheduler& { return *this;};
  // DiskScheduler(DiskScheduler &&ds) noexcept {}
  // auto operator=(DiskScheduler &&ds) noexcept ->DiskScheduler&{return *this;}

  ~DiskScheduler();

  void Schedule(DiskRequest request);
  void Schedule(std::vector<DiskRequest> &requests);

  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  // 应该是触发某种条件后调用DiskRequest内部的回调函数
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** Pointer to the disk manager. */
  // 所有的操作通过它完成
  // 支持读写页的数据, 删除页, 写日志等操作
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  //  析构函数时放入一个std::nullopt, 然后通知后台线程停止执行
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  // 给磁盘管理器发出预定请求的后台线程
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
