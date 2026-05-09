//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskRequest::DiskRequest(bool is_write, char *dt, page_id_t page_id, std::promise<bool> pr) noexcept
  : is_write_(is_write), data_(dt), page_id_(page_id), callback_(std::move(pr))
{}

DiskRequest::DiskRequest(DiskRequest&& other) noexcept 
  : is_write_(other.is_write_), data_(other.data_), page_id_(other.page_id_), callback_(std::move(other.callback_))
{
  other.is_write_ = false;
  other.data_ = nullptr;
  other.page_id_ = INVALID_PAGE_ID;
}
auto DiskRequest::operator=(DiskRequest&& other) noexcept -> DiskRequest& 
{
  if (&other != this)
  {
    is_write_ = other.is_write_;
    data_ = other.data_;
    page_id_ = other.page_id_;
    callback_ = std::move(other.callback_);
    other.is_write_ = false;
    other.data_ = nullptr;
    other.page_id_ = INVALID_PAGE_ID;
  }
  return *this;
}

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) 
{
  for (auto &request : requests)
  {
    if (request.page_id_ != INVALID_PAGE_ID)
    {      
      request_queue_.Put(std::move(request));
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() 
{
  while (true)
  {
    auto req = request_queue_.Get();
    if (req == std::nullopt)
    {
      // continue;
      return;
    }
    if (!req->is_write_)
    {
      disk_manager_->ReadPage(req->page_id_, req->data_);
    }
    else if (req->is_write_)
    {
      disk_manager_->WritePage(req->page_id_, req->data_);
    }
    req->callback_.set_value(true);
  }
}

}  // namespace bustub
