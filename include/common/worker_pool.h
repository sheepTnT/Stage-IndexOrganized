//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_pool.h
//
// Identification: src/include/threadpool/worker_pool.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "lock_free_queue.h"
#include "logger.h"

namespace mvstore {

using TaskQueue = LockFreeQueue<std::function<void()>>;

/**
 * @brief A worker pool that maintains a group of worker threads. This pool is
 * restartable, meaning it can be started again after it has been shutdown.
 * Calls to Startup() and Shutdown() are thread-safe and idempotent.
 */
class WorkerPool {
 public:
  WorkerPool(const std::string &pool_name, uint32_t num_workers,
             TaskQueue &task_queue);

  /**
   * @brief Start this worker pool. Thread-safe and idempotent.
   */
  void Startup();

  /**
   * @brief Shutdown this worker pool. Thread-safe and idempotent.
   */
  void Shutdown();

  /**
   * @brief Access the number of worker threads in this pool
   *
   * @return The number of worker threads assigned to this pool
   */
  uint32_t NumWorkers() const { return num_workers_; }

 private:
  // The name of this pool
  std::string pool_name_;
  // The worker threads
  std::vector<std::thread> workers_;
  // The number of worker threads
  uint32_t num_workers_;
  // Flag indicating whether the pool is running
  std::atomic_bool is_running_;
  // The queue where workers pick up tasks
  TaskQueue &task_queue_;
};

namespace {

    void WorkerFunc(std::string thread_name, std::atomic_bool *is_running,
                    TaskQueue *task_queue) {
        constexpr auto kMinPauseTime = std::chrono::microseconds(1);
        constexpr auto kMaxPauseTime = std::chrono::microseconds(1000);

        LOG_INFO("Thread %s starting ...", thread_name.c_str());

        auto pause_time = kMinPauseTime;
        while (is_running->load() || !task_queue->IsEmpty()) {
            std::function<void()> task;
            if (!task_queue->Dequeue(task)) {
                // Polling with exponential back-off
                std::this_thread::sleep_for(pause_time);
                pause_time = std::min(pause_time * 2, kMaxPauseTime);
            } else {
                task();
                pause_time = kMinPauseTime;
            }
        }

        LOG_INFO("Thread %s exiting ...", thread_name.c_str());
    }

}  // namespace

inline WorkerPool::WorkerPool(const std::string &pool_name, uint32_t num_workers,
                       TaskQueue &task_queue)
        : pool_name_(pool_name),
          num_workers_(num_workers),
          is_running_(false),
          task_queue_(task_queue) {}

inline void WorkerPool::Startup() {
    bool running = false;
    if (is_running_.compare_exchange_strong(running, true)) {
        for (size_t i = 0; i < num_workers_; i++) {
            std::string name = pool_name_ + "-worker-" + std::to_string(i);
            workers_.emplace_back(WorkerFunc, name, &is_running_, &task_queue_);
        }
    }
}

inline void WorkerPool::Shutdown() {
    bool running = true;
    if (is_running_.compare_exchange_strong(running, false)) {
        is_running_ = false;
        for (auto &worker : workers_) {
            worker.join();
        }
        workers_.clear();
    }
}

}
