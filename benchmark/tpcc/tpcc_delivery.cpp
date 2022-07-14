//
// Created by zhangqian on 2022-03-1.
//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_payment.cpp
//
// Identification: src/main/tpcc/tpcc_delivery.cpp
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

bool RunDelivery(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer) {
    /*
     "DELIVERY": {
     "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID =
     ? AND NO_O_ID > -1 LIMIT 1", #
     "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ?
     AND NO_O_ID = ?", # d_id, w_id, no_o_id
     "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID
     = ?", # no_o_id, d_id, w_id
     "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID
     = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
     "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ?
     AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
     "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND
     OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
     "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID =
     ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
     }
     */

    LOG_TRACE("-------------------------------------");

    /////////////////////////////////////////////////////////
    // PREPARE ARGUMENTS
    /////////////////////////////////////////////////////////
    int warehouse_id = GenerateWarehouseId(thread_id);
    int o_carrier_id =
            GetRandomInteger(orders_min_carrier_id, orders_max_carrier_id);

//    std::vector<expression::AbstractExpression *> runtime_keys;

    /////////////////////////////////////////////////////////
    // BEGIN TRANSACTION
    /////////////////////////////////////////////////////////

    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);


    for (int d_id = 0; d_id < state.districts_per_warehouse; ++d_id) {
        LOG_DEBUG("getNewOrder: SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND "
                "NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1");

        std::vector<int32_t> new_order_ids;
        {
            bool point_lookup = false;
            bool is_for_update = false;
            auto start_key = NewOrder::NewOrderKey{d_id, warehouse_id, 0};
            auto predicate = [&](const NewOrder *no, bool &should_end_scan) -> bool {
                if (no->NO_W_ID == warehouse_id && no->NO_D_ID == d_id) {
                    if (no->NO_O_ID > -1) {
                        should_end_scan = true; // Got one, quit scannnig
                        return true;
                    } else {
                        return false; // Keep scanning
                    }
                }
                // Quit scanning
                should_end_scan = true;
                return false;
            };
            uint32_t k_n_sz = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
            uint32_t scan_sz =  state.warehouse_count * state.districts_per_warehouse *
                                state.customers_per_district;
            const char *k_new_order = reinterpret_cast<const char *>(&start_key);
            IndexScanExecutor<const char *, NewOrder> executor(new_order_table,
                                                                       k_n_sz,
                                                                        k_new_order,
                                                                       scan_sz,
                                                                       point_lookup,
                                                                       predicate,
                                                                       is_for_update,
                                                                       txn_ctx,
                                                                       version_store);

            auto res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_INFO("abort transaction select new order table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
            auto new_orders = executor.GetResults();
            for (int i = 0; i < new_orders.size(); ++i) {
                new_order_ids.push_back(new_orders[i]->NO_O_ID);
            }
        }

        if (new_order_ids.size() == 0) {
            // TODO:  No orders for this district: skip it. Note: This must be
            // reported if > 1%
            continue;
        }

        assert(new_order_ids.size() == 1);

        // result: NO_O_ID
        auto no_o_id = new_order_ids[0];

        LOG_DEBUG("no_o_id = %d", (int)(no_o_id));



        LOG_DEBUG("getCId: SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND " "O_W_ID = ?");
        std::vector<int32_t> order_c_ids;
        {
            bool point_lookup = true;
            bool is_for_update = false;
            auto key = Order::OrderKey{warehouse_id, d_id, no_o_id};
            auto predicate = [&](const Order *o, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return o->O_W_ID == warehouse_id && o->O_D_ID == d_id && o->O_ID == no_o_id;
            };
            uint32_t k_sz = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
            uint32_t scan_sz = 0;
            const char *k_order = reinterpret_cast<const char *>(&key);
            IndexScanExecutor<const char *, Order> executor(orders_table,
                                                               k_sz,
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
            auto orders = executor.GetResults();
            for (int i = 0; i < orders.size(); ++i) {
                order_c_ids.push_back(orders[i]->O_C_ID);
            }
        }

        assert(order_c_ids.size() == 1);

        // Result: O_C_ID
        auto c_id = order_c_ids[0];



        LOG_DEBUG("sumOLAmount: SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? "
                  "AND OL_D_ID = ? AND OL_W_ID = ?");
        double ol_total = 0;
        {
            bool point_lookup = false;
            bool is_for_update = false;
            auto start_key = OrderLine::OrderLineKey{warehouse_id, d_id,  no_o_id, -1};
            auto predicate = [&](const OrderLine *ol, bool &should_end_scan) -> bool {
                if (ol->OL_W_ID == warehouse_id && ol->OL_D_ID == d_id && ol->OL_O_ID == no_o_id) {
                    return true;
                }
                should_end_scan = true;
                return false;
            };
            uint32_t k_sz = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
            uint32_t scan_sz =  state.warehouse_count * state.districts_per_warehouse *
                                state.customers_per_district * 15;
            const char *k_order_line = reinterpret_cast<const char *>(&start_key);
            IndexScanExecutor<const char *, OrderLine> executor(order_line_table,
                                                                           k_sz,
                                                                           k_order_line,
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
            auto order_lines = executor.GetResults();
            for (int i = 0; i < order_lines.size(); ++i) {
                ol_total += std::atof(order_lines[i]->OL_AMOUNT);
            }
        }


        LOG_DEBUG("deleteNewOrder: DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = "
                "? AND NO_O_ID = ?");
        {
            auto key = NewOrder::NewOrderKey{d_id, warehouse_id, no_o_id};
//            auto predicate = [&](const NewOrder &no) -> bool {
//                return no.NO_O_ID == no_o_id && no.NO_D_ID == d_id && no.NO_W_ID == warehouse_id;
//            };
            bool is_for_update = true;
            uint32_t k_sz = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
            const char *k_new_order = reinterpret_cast<const char *>(&key);
            PointDeleteExecutor<const char *, NewOrder> executor(new_order_table,
                                                                          k_new_order,
                                                                          k_sz,
                                                                          is_for_update,
                                                                          txn_ctx,
                                                                          version_store);
            auto res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction when delete new order table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
        }


        LOG_DEBUG("updateOrders: UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND "
                "O_D_ID = ? AND O_W_ID = ?");
        {
            auto key = Order::OrderKey{warehouse_id, d_id,  no_o_id};
//            auto predicate = [&](const Order &o) -> bool {
//                return o.O_W_ID == warehouse_id && o.O_D_ID == d_id && o.O_ID == no_o_id;
//            };
//            auto updater = [&](Order &o) {
//                o.O_CARRIER_ID = o_carrier_id;
//            };
            auto col_num = Order::GetColumnNum();
            std::vector<oid_t> up_col;
            for (int p = 0; p < col_num; ++p){
                up_col.push_back(0);
            }
            up_col[5]=1;
            uint32_t delta_len = sizeof(uint32_t);
            uint32_t delta = o_carrier_id;
            bool is_for_update = true;
            uint32_t key_size = sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t);
            const char *k_order = reinterpret_cast<const char *>(&key);
            PointUpdateExecutor<const char *, Order> executor(orders_table,
                                                                 k_order,
                                                                 key_size,
                                                                 reinterpret_cast<const char *>(&delta),
                                                                 up_col,
                                                                 delta_len,
                                                                 is_for_update,
                                                                 txn_ctx,
                                                                 version_store);
            auto res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction when update order table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
        }




//        LOG_DEBUG("updateOrderLine: UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE "
//                "OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?");
//        {
//            auto start_key = OrderLine::OrderLineKey{warehouse_id, d_id, no_o_id, -1};
//            auto predicate = [&](const OrderLine &ol) -> bool {
//                return ol.OL_W_ID == warehouse_id && ol.OL_D_ID == d_id && ol.OL_O_ID == no_o_id;
//            };
//
//            auto updater = [&](OrderLine &ol) {
//                ol.OL_DELIVERY_D = 0;
//            };
//            ScanUpdateExecutor<OrderLine::OrderLineKey, OrderLine> executor(*order_line_table, start_key, predicate,
//                                                                            updater, txn, buf_mgr);
//            auto res = executor.Execute();
//            if (txn->GetResult() != ResultType::SUCCESS) {
//                LOG_TRACE("abort transaction");
//                txn_manager->AbortTransaction(txn);
//                return false;
//            }
//        }



        LOG_DEBUG("updateCustomer: UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE "
                "C_ID = ? AND C_D_ID = ? AND C_W_ID = ?");
        {
            Customer::CustomerKey customer_key{warehouse_id, d_id, c_id};
//            auto predicate = [&](const Customer &c) -> bool {
//                return c.C_ID == c_id && c.C_D_ID == d_id && c.C_W_ID == warehouse_id;
//            };
            auto predicate_c = [&](const Customer *c, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return c->C_W_ID == warehouse_id && c->C_D_ID == d_id && c->C_ID == c_id;
            };
            bool is_for_update = true;
            uint16_t key_size = sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t);

            //first: get the balance of the customer
            LOG_DEBUG("getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE "
                      "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", warehouse_id, d_id, c_id);
            double c_balance = 0.0;
            bool point_lookup = true;
            uint32_t scan_sz = 0;
            const char *k_customer = reinterpret_cast<const char *>(&customer_key);
            IndexScanExecutor<const char *, Customer> executor_c(customer_table,
                                                                          key_size,
                                                                          k_customer,
                                                                          scan_sz,
                                                                          point_lookup,
                                                                          predicate_c,
                                                                          is_for_update,
                                                                          txn_ctx,
                                                                          version_store);
            auto res = executor_c.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_INFO("abort transaction when select customer table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
            auto customers = executor_c.GetResults();
            assert(customers.size() == 1);
            c_balance = std::atof(customers[0]->C_BALANCE);

            //second: update balance of the customer
//            auto updater = [&](Customer &c) {
//                c.C_BALANCE += ol_total;
//            };
            auto col_num = Customer::GetColumnNum();
            std::vector<oid_t> up_col;
            for (int p = 0; p < col_num; ++p){
                up_col.push_back(0);
            }
            up_col[16]=1;
            uint32_t delta_len = sizeof(int64_t) ;
            int64_t delta = c_balance + ol_total;
            const char *k_customer_ = reinterpret_cast<const char *>(&customer_key);
            PointUpdateExecutor<const char *, Customer> executor(customer_table,
                                                                          k_customer_,
                                                                          key_size,
                                                       reinterpret_cast<const char *>(delta),
                                                                          up_col,
                                                                          delta_len,
                                                                          is_for_update,
                                                                          txn_ctx,
                                                                          version_store);
            res = executor.Execute();
            assert(res);
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction when update customer table balance");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
        }

    }

    assert(txn_ctx->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn_ctx);

    if (result == ResultType::SUCCESS) {
        LOG_DEBUG("commit successfully");
        return true;
    } else {
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);
        return false;
    }
}
}
}
}
