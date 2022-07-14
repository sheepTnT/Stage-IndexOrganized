//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// harness.h
//
// Identification: test/include/common/harness.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <iostream>
#include <atomic>
#include "../include/common/macros.h"
#include "../include/common/object_pool.h"
#include "../include/common/constants.h"
#include "../include/common/logger.h"
//#include "../third_party/gmock/gmock/gmock.h"
#include "../include/common/ephemeral_pool.h"
#include "../include/vstore/version_store.h"
#include "../include/execute/txn.h"
#include "gtest/gtest.h"
//#include <google/protobuf/stubs/common.h>
//#include <gflags/gflags.h>

namespace mvstore {

namespace test {

//===--------------------------------------------------------------------===//
// Test Harness (common routines)
//===--------------------------------------------------------------------===//

#define MAX_THREADS 1024

/**
 * Testing Harness
 */
class TestingHarness {
 public:
  TestingHarness(const TestingHarness &) = delete;
  TestingHarness &operator=(const TestingHarness &) = delete;
  TestingHarness(TestingHarness &&) = delete;
  TestingHarness &operator=(TestingHarness &&) = delete;

  // global singleton
  static TestingHarness &GetInstance(void);

  uint64_t GetThreadId();

  cid_t GetNextTransactionId();

  EphemeralPool *GetTestingPool();

  oid_t GetNextVersionBlockId();

  VersionStore *GetVersionStore();

 private:
  TestingHarness();

  // Txn id counter
  std::atomic<txn_id_t> txn_id_counter;

  // Commit id counter
  std::atomic<cid_t> cid_counter;

  // version block id counter
  std::atomic<oid_t> version_block_id_counter;

  // Testing pool
  std::unique_ptr<EphemeralPool> pool_;

  UndoBuffer *undo_buffer_;

};

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

//===--------------------------------------------------------------------===//
// version store Test
//===--------------------------------------------------------------------===//

// All tests inherit from this class
class VStoreTest : public ::testing::Test {
 protected:

  virtual void SetUp() {

    // turn off gc under test mode
//    gc::GCManagerFactory::GetInstance().StopGC();
//    gc::GCManagerFactory::Configure(0);

  }

  virtual void TearDown() {

    // shutdown protocol buf library
//    google::protobuf::ShutdownProtobufLibrary();

    // Shut down GFLAGS.
//    ::google::ShutDownCommandLineFlags();
  }


};

}  // namespace test
}  // namespace mvstore
