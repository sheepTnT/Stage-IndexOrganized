//
// Created by zhangqian on 2022-03-1.
//

#ifndef MVSTORE_TPCC_WORKLOAD_H
#define MVSTORE_TPCC_WORKLOAD_H

#include "tpcc_configuration.h"
#include "tpcc_loader.h"

namespace mvstore {
namespace benchmark {
namespace tpcc {

extern configuration state;

void RunWarmupWorkload(VersionStore *version_store, EphemeralPool *conflict_buffer);

void RunWorkload(VersionStore *version_store, EphemeralPool *conflict_buffer);

bool RunNewOrder(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer);

bool RunPayment(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer);

bool RunDelivery(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer);

bool RunOrderStatus(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer);

bool RunStockLevel(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer);

bool RunQuery2(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer,
               std::vector<std::vector<std::pair<int32_t, int32_t>>> &supp_stock_map);

size_t GenerateWarehouseId(const size_t &thread_id);


void PinToCore(size_t core);


}
}
}

#endif