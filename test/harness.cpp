//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// harness.cpp
//
// Identification: test/common/harness.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
//#include "../include/vstore/valen_buffer.h"

namespace mvstore {
namespace test {

/**
 * @brief Return the singleton testing harness instance
 */
TestingHarness& TestingHarness::GetInstance() {
  static TestingHarness testing_harness;
  return testing_harness;
}

TestingHarness::TestingHarness()
    : txn_id_counter(INVALID_TXN_ID),
      cid_counter(INVALID_CID),
      version_block_id_counter(START_OID) {
    RecordBufferPool *record_buffer_pool = new RecordBufferPool(5000,5000);
    undo_buffer_ = new UndoBuffer(record_buffer_pool);
    pool_.reset(new EphemeralPool(undo_buffer_));


}

uint64_t TestingHarness::GetThreadId() {
  std::hash<std::thread::id> hash_fn;

  uint64_t id = hash_fn(std::this_thread::get_id());
  id = id % MAX_THREADS;

  return id;
}

cid_t TestingHarness::GetNextTransactionId() {
  auto txn_manager = SnapshotTransactionManager::GetInstance(nullptr,pool_.get());
  auto txn = txn_manager->BeginTransaction(0,IsolationLevelType::SNAPSHOT);
  txn_id_t txn_id = txn->GetReadId();
  txn_manager->CommitTransaction(txn);

  return txn_id;
}

EphemeralPool* TestingHarness::GetTestingPool() {
  // return pool
  return pool_.get();
}
//VersionStore *TestingHarness::GetVersionStore(){
//    return version_store;
//}
oid_t TestingHarness::GetNextVersionBlockId() { return ++version_block_id_counter; }

}  // namespace test
}  // namespace mvstore
