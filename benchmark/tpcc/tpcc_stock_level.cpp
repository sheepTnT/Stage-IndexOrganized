//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_stock_level.cpp
//
// Identification: src/main/tpcc/tpcc_stock_level.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "tpcc_record.h"
#include "tpcc_configuration.h"
#include "tpcc_loader.h"
#include "tpcc_workload.h"


namespace mvstore {
namespace benchmark {
namespace tpcc {

bool RunStockLevel(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer) {
    /*
       "STOCK_LEVEL": {
       "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
       "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
       WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND
       S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
       }
     */
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    // Prepare random data
    int w_id = GenerateWarehouseId(thread_id);
    int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
    int threshold = GetRandomInteger(stock_min_threshold, stock_max_threshold);

    int32_t o_id;
    LOG_TRACE( "getOId: SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?, w_id=%d,d_id=%d",
               w_id,d_id);
    {
        District::DistrictKey key{w_id, d_id};
        bool point_lookup = true;
        bool is_for_update = false;
        uint32_t scan_sz = 0;
        uint32_t key_size = sizeof(uint64_t) + sizeof(uint64_t);
        auto predicate = nullptr;
        const char *k_district = reinterpret_cast<const char *>(&key);
        IndexScanExecutor<const char *, District> executor(district_table,
                                                                    key_size,
                                                                    k_district,
                                                                    scan_sz,
                                                                    point_lookup,
                                                                    predicate,
                                                                    is_for_update,
                                                                    txn_ctx,
                                                                    version_store);
        auto res = executor.Execute();
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_DEBUG("abort transaction when select district table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
        auto districts = executor.GetResults();
        if(districts.size() != 1){
            LOG_DEBUG("abort transaction when select district table");
            return false;
        }
        o_id = districts[0]->D_NEXT_O_ID;
    }



    LOG_TRACE("getStockCount: SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  "
            "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND "
            "S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?");

    int max_o_id = o_id;
    int min_o_id = max_o_id - 20;

    std::unordered_set<int> distinct_items;
    for (int curr_o_id = min_o_id; curr_o_id < max_o_id; ++curr_o_id) {
        std::vector<int32_t> ol_i_ids;
        OrderLine::OrderLineKey start_key{w_id, d_id, curr_o_id, 5};
        bool point_lookup = false;
        bool is_for_update = false;
//            auto predicate = nullptr;
        auto predicate = [&](const OrderLine *ol, bool &should_end_scan) -> bool {
            if (ol->OL_O_ID == curr_o_id && ol->OL_W_ID == w_id && ol->OL_D_ID == d_id){
                ol_i_ids.emplace_back(ol->OL_I_ID);
                return true;
            }
            should_end_scan = true;
            return true;
        };
        uint32_t k_sz = sizeof(uint64_t) + sizeof(uint64_t) +
                        sizeof(uint64_t) + sizeof(uint64_t);
        uint32_t scan_sz =  10;
        const char *k_orderline = reinterpret_cast<const char *>(&start_key);
        IndexScanExecutor<const char *, OrderLine> executor(order_line_table,
                                                            k_sz,
                                                            k_orderline,
                                                            scan_sz,
                                                            point_lookup,
                                                            predicate,
                                                            is_for_update,
                                                            txn_ctx,
                                                            version_store);
        auto res = executor.Execute();
        assert(res);
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when select order line table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        if (ol_i_ids.size() == 0) {
            LOG_TRACE("order line return size incorrect : %lu", order_lines.size());
            continue;
        }

//        LOG_INFO("order line return size   : %lu", ol_i_ids.size());

        {
            int32_t item_id = ol_i_ids[0];
            Stock::StockKey key{w_id, item_id};
            bool point_lookup = true;
            bool is_for_update = false;
            auto predicate = nullptr;

            uint32_t scan_sz = 0;
            uint32_t key_size = sizeof(uint64_t) + sizeof(uint64_t);
            const char *k_stock = reinterpret_cast<const char *>(&key);
            IndexScanExecutor<const char *, Stock> stock_scan_executor(stock_table,
                                                                       key_size,
                                                                       k_stock,
                                                                       scan_sz,
                                                                       point_lookup,
                                                                       predicate,
                                                                       is_for_update,
                                                                       txn_ctx,
                                                                       version_store);
            auto res = stock_scan_executor.Execute();
            //assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_DEBUG("abort transaction when select stock table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }

            auto stock_ret = stock_scan_executor.GetResults();
            if (stock_ret.size() == 0) {
                LOG_DEBUG("stock_ret size <1 ");
                continue;
            }
            auto stock = stock_ret[0];
            if (stock->S_QUANTITY < threshold) {
                distinct_items.insert(stock->S_I_ID);
            }
        }
    }

    LOG_TRACE("number of distinct items=%lu", distinct_items.size());

//    assert(txn_ctx->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn_ctx);

    if (result == ResultType::SUCCESS) {
//        delete txn_ctx;
//        txn_ctx = nullptr;

        return true;
    } else {
//        delete txn_ctx;
//        txn_ctx = nullptr;
        return false;
    }

}
}
}
}
