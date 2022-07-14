//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dedicated_thread_registry.h
//
// Identification: src/include/common/dedicated_thread_registry.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <memory>
#include <unordered_map>
#include <vector>
#include <thread>
#include "../common/macros.h"
#include "../common/dedicated_thread_task.h"
#include "../common/dedicated_thread_owner.h"
#include "../common/spin_latch.h"
#include <unordered_set>

namespace mvstore {

/**
 * Singleton class responsible for maintaining and dispensing long running
 * (dedicated) threads to other system components. The class also serves
 * as a control panel for the brain component to be able to collect information
 * on threads in the system and modify how threads are allocated.
 */
class DedicatedThreadRegistry {
 public:
  DedicatedThreadRegistry() = default;

  ~DedicatedThreadRegistry() {
    // Note that if registry is shutting down, it doesn't matter whether
    // owners are notified as this class should have the same life cycle
    // as the entire peloton process.

    for (auto &entry : thread_owners_table_) {
      for (auto &task : entry.second) {
        task->Terminate();
        threads_table_[task].join();
        delete task;
      }
    }
    thread_owners_table_.clear();
    threads_table_.clear();
  }

  // TODO(tianyu): Remove when we remove singletons
  static DedicatedThreadRegistry &GetInstance()  {
    static DedicatedThreadRegistry registry;
    return registry;
  }

  /**
   *
   * Register a thread under requester to run the given task
   *
   * @param requester The owner to assign the new thread to
   * @param args the arguments to pass to constructor of task
   * @return the DedicatedThreadTask running on new thread
   */
  template <typename Task>
  void RegisterDedicatedThread(DedicatedThreadOwner *requester,
                               std::shared_ptr<Task> task) {
    thread_owners_table_[requester].insert(task.get());
    requester->NotifyNewThread();
    threads_table_.emplace(task.get(), std::thread([=] { task->RunTask(); }));
  }

/**
* Stop a registered task. This is a blocking call, and will only return once the task is terminated and the thread
* returns. This function will free the task if it is stopped, thus, it is up to the requester (as well as the task's
* Terminate method) to do any necessary cleanup to the task before calling StopTask. StopTask will call the owners
* OnThreadRemoval method in order to allow the owner to clean up the task.
* @param requester the owner who registered the task
* @param task the task that was registered
* @warning StopTask should not be called multiple times with the same task.
* @return true if task was stopped, false otherwise
*/
bool StopTask(DedicatedThreadOwner *requester,std::shared_ptr<DedicatedThreadTask> task) {
    DedicatedThreadTask *task_ptr;
    std::thread *task_thread;
    {
        SpinLatch::ScopedSpinLatch guard(&table_latch_);
        // Exposing the raw pointer like this is okay because we own the underlying raw pointer
        auto search = threads_table_.find(task.operator->());
        PELOTON_ASSERT(search != threads_table_.end(), "Stop Task fail.");
        task_ptr = search->first;
        task_thread = &search->second;
    }

    // Notify requester of removal

    requester->NotifyThreadRemoved(task) ;

    // Terminate task, unlock during termination of thread since we aren't touching the metadata tables
    task->Terminate();
    task_thread->join();

    // Clear Metadata
    {
        SpinLatch::ScopedSpinLatch guard(&table_latch_);
        requester->NotifyThreadRemoved(task);
        threads_table_.erase(task_ptr);
        thread_owners_table_[requester].erase(task_ptr);
    }
    delete task_ptr;
    return true;
}

  // TODO(tianyu): Add code for thread removal

 private:
    // Latch to protect internal tables
    SpinLatch table_latch_;
  // Using raw pointer is okay since we never dereference said pointer,
  // but only use it as a lookup key
  std::unordered_map<DedicatedThreadTask *, std::thread> threads_table_;
  // Using raw pointer here is also fine since the owner's life cycle is
  // not controlled by the registry
//  std::unordered_map<DedicatedThreadOwner *,
//      std::vector<std::shared_ptr<DedicatedThreadTask>>>
//      thread_owners_table_;
   std::unordered_map<DedicatedThreadOwner *, std::unordered_set<DedicatedThreadTask *>> thread_owners_table_;
};

}
