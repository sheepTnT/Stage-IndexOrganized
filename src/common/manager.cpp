//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.cpp
//
// Identification: src/catalog/manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../include/common/manager.h"


namespace mvstore {

std::shared_ptr<IndirectionArray> Manager::empty_indirection_array_;

Manager &Manager::GetInstance() {
  static Manager manager;
  return manager;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void Manager::AddIndirectionArray(
    const oid_t oid, std::shared_ptr<IndirectionArray> location) {
  // add/update the catalog reference to the indirection array
  auto ret = indirection_array_locator_[oid] = location;
}

void Manager::AddRecordIndirectionArray(
        const oid_t oid, std::shared_ptr<RecordIndirectionArray> location) {
    // add/update the catalog reference to the indirection array
    auto ret = record_indirection_array_locator_[oid] = location;
}

void Manager::DropIndirectionArray(const oid_t oid) {
  // drop the catalog reference to the tile group
  indirection_array_locator_[oid] = empty_indirection_array_;
}

// used for logging test
void Manager::ClearIndirectionArray() { indirection_array_locator_.clear(); }

}
