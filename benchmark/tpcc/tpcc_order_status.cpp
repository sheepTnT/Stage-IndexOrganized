//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_order_status.cpp
//
// Identification: src/main/tpcc/tpcc_order_status.cpp
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
#include <vector>
#include "tpcc_record.h"
#include "tpcc_configuration.h"
#include "tpcc_loader.h"
#include "tpcc_workload.h"

namespace mvstore {
namespace benchmark {
namespace tpcc {

bool RunOrderStatus(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer) {
    /*
      "ORDER_STATUS": {
      "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
      C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", #
      w_id, d_id, c_id
      "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
      FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY
      C_FIRST", # w_id, d_id, c_last
      "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE
      O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", #
      w_id, d_id, c_id
      "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT,
      OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID
      = ?", # w_id, d_id, o_id
      }
     */

    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    // Generate w_id, d_id, c_id, c_last
    // int w_id = GetRandomInteger(0, state.warehouse_count - 1);
    int w_id = GenerateWarehouseId(thread_id);
    int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);

    int c_id = -1;
    std::string c_last;

    if (GetRandomInteger(1, 100) <= 60) {
        c_last = GetRandomLastName(state.customers_per_district);
    } else {
        c_id = GetNURand(1023, 0, state.customers_per_district - 1);
    }

    // Run queries
    if (c_id != -1) {
        LOG_DEBUG("getCustomerByCustomerId: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, "
                "C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?  "
                "# w_id, d_id, c_id");
        {
            auto key = Customer::CustomerKey{w_id, d_id, c_id};
            auto point_lookup = true;
            auto predicate = [&](const Customer *c, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return c->C_W_ID == w_id && c->C_D_ID == d_id && c->C_ID == c_id;
            };
            bool is_for_update = false;
            uint32_t scan_sz = 0;
            uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t);
            const char *k_customer = reinterpret_cast<const char *>(&key);
            IndexScanExecutor<const char *, Customer> executor(customer_table,
                                                                        key_size,
                                                                        k_customer,
                                                                        scan_sz,
                                                                        point_lookup,
                                                                        predicate,
                                                                        is_for_update,
                                                                        txn_ctx,
                                                                        version_store);
            auto res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_INFO("abort transaction when select customer table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }

            auto customers = executor.GetResults();
            if (customers.size() == 0) {
                LOG_ERROR("wrong result size : %lu", customers.size());
                assert(false);
            }
        }
    } else {
        LOG_DEBUG("getCustomersByLastName: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, "
                "C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = "
                "? ORDER BY C_FIRST, # w_id, d_id, c_last");
        {
            // Finc c_id using secondary index
            auto key = CustomerIndexedColumns();
            key.C_W_ID = w_id;
            key.C_D_ID = d_id;
            key.C_ID = -1;
            strcpy(key.C_LAST, c_last.c_str());
            auto point_lookup = false;
            auto predicate = [&](const CustomerIndexedColumns *c, bool &should_end_scan) -> bool {
                if (c->C_W_ID == w_id && c->C_D_ID == d_id && c->C_LAST == c_last) {
                    return true;
                }
                should_end_scan = true;
                return false;
            };
            bool is_for_update = false;
            uint32_t scan_sz = state.warehouse_count * state.districts_per_warehouse *
                               state.customers_per_district;
            uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t)+32;
            const char *k_customer_index = reinterpret_cast<const char *>(&key);
            IndexScanExecutor<const char *, CustomerIndexedColumns> executor(customer_skey_table,
                                                                                       key_size,
                                                                                       k_customer_index,
                                                                                       scan_sz,
                                                                                       point_lookup,
                                                                                       predicate,
                                                                                       is_for_update,
                                                                                       txn_ctx,
                                                                                       version_store);

            auto res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_INFO("abort transaction when select customer skey table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
            auto customers = executor.GetResults();
//            sort(customers.begin(), customers.end(),
//                 [](const CustomerIndexedColumns &lhs, const CustomerIndexedColumns &rhs) {
//                     return std::string(lhs.C_LAST) < std::string(rhs.C_LAST);
//                 });
            assert(customers.size() > 0);
            // Get the middle one
            size_t name_count = customers.size();
            auto &customer = customers[name_count / 2];
            c_id = customer->C_ID;
        }
    }

    if (c_id < 0) {
        LOG_ERROR("wrong c_id");
        assert(false);
    }

    LOG_DEBUG("getLastOrder: SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE "
            "O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1, # "
            "w_id, d_id, c_id");

    int32_t last_o_id = -1;
    {
        // Find o_id using secondary index
        auto oic = OrderIndexedColumns();
        oic.O_W_ID = w_id;
        oic.O_D_ID = d_id;
        oic.O_C_ID = c_id;
        oic.O_ID = -1;
        auto point_lookup = false;
        bool is_for_update = false;
        uint32_t scan_sz = state.warehouse_count * state.districts_per_warehouse *
                           state.customers_per_district;
        uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t)+ sizeof(uint32_t);
        // Hack: we get the last order id in the predicate.
        auto predicate = [&](const OrderIndexedColumns *oic, bool &should_end_scan) -> bool {
            if (oic->O_W_ID == w_id && oic->O_D_ID == d_id && oic->O_C_ID == c_id) {
                return true;
            }
            should_end_scan = true;
            return false;
        };
        const char *k_order_index = reinterpret_cast<const char *>(&oic);
        IndexScanExecutor<const char *, OrderIndexedColumns> executor(orders_skey_table,
                                                                             key_size,
                                                                             k_order_index,
                                                                             scan_sz,
                                                                             point_lookup,
                                                                             predicate,
                                                                             is_for_update,
                                                                             txn_ctx,
                                                                             version_store);

        auto res = executor.Execute();
        assert(res);
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when select order skey table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
        auto oics = executor.GetResults();
        for (int i = 0; i < oics.size(); ++i) {
            if (oics[i]->O_ID > last_o_id) {
                last_o_id = oics[i]->O_ID;
            }
        }
    }
    if (last_o_id != -1) {
        {
            Order::OrderKey key{w_id, d_id, last_o_id};
            auto point_lookup = true;
            auto predicate = [&](const Order *o, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return o->O_W_ID == w_id && o->O_D_ID == d_id && o->O_C_ID == c_id;
            };
            bool is_for_update = false;
            uint32_t scan_sz = 0;
            uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t);
            const char *k_order = reinterpret_cast<const char *>(&key);
            IndexScanExecutor<const char *, Order> executor(orders_table,
                                                               key_size,
                                                               k_order,
                                                               scan_sz,
                                                               point_lookup,
                                                               predicate,
                                                               is_for_update,
                                                               txn_ctx,
                                                               version_store);
            auto res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_INFO("abort transaction when select order table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
            auto &orders = executor.GetResults();
            assert(orders.size() == 1);
        }


        LOG_DEBUG("getOrderLines: SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, "
                "OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND "
                "OL_D_ID = ? AND OL_O_ID = ?, # w_id, d_id, o_id");
        {
            OrderLine::OrderLineKey ol_key{w_id, d_id, last_o_id, -1};
            bool point_lookup = false;
            auto predicate = [&](const OrderLine *ol, bool &should_end_scan) -> bool {
                if (ol->OL_W_ID == w_id && ol->OL_D_ID == d_id && ol->OL_O_ID == last_o_id)
                    return true;
                should_end_scan = true;
                return false;
            };
            bool is_for_update = false;
            uint32_t scan_sz = state.warehouse_count * state.districts_per_warehouse *
                               state.customers_per_district * 15;
            uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t)+ sizeof(uint32_t);
            const char *k_orderline = reinterpret_cast<const char *>(&ol_key);
            IndexScanExecutor<const char*, OrderLine> executor(order_line_table,
                                                                           key_size,
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
                LOG_INFO("abort transaction when select order line table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
            auto &order_lines = executor.GetResults();
        }
    }

    assert(txn_ctx->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn_ctx);

    if (result == ResultType::SUCCESS) {
        return true;
    } else {
        return false;
    }
}
}
}
}
