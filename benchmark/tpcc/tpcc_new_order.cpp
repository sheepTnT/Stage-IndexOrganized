//
// Created by zhangqian on 2022-03-1.
//
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_new_order.cpp
//
// Identification: src/main/tpcc/tpcc_new_order.cpp
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
#include "tpcc_configuration.h"
#include "tpcc_loader.h"
#include "tpcc_workload.h"
#include "tpcc_record.h"


namespace mvstore {
namespace benchmark {
namespace tpcc {

bool RunNewOrder(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer) {
    /*
       "NEW_ORDER": {
       "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
       "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND
       D_W_ID = ?", # d_id, w_id
       "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE
       C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
       "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ?
       AND D_W_ID = ?", # d_next_o_id, d_id, w_id
       "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID,
       O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?,
       ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt,
       o_all_local
       "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES
       (?, ?, ?)", # o_id, d_id, w_id
       "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", #
       ol_i_id
       "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT,
       S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", #
       d_id, ol_i_id, ol_supply_w_id
       "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT =
       ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity,
       s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
       "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID,
       OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT,
       OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id,
       ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
       }
     */

    LOG_TRACE("-------------------------------------");

    /////////////////////////////////////////////////////////
    // PREPARE ARGUMENTS
    /////////////////////////////////////////////////////////
    int32_t warehouse_id = GenerateWarehouseId(thread_id);
    int32_t district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
    int32_t customer_id = GetRandomInteger(0, state.customers_per_district - 1);
    int o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

    std::vector<int> i_ids, ol_w_ids, ol_qtys;
    bool o_all_local = true;

    for (auto ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
        // in the original TPC-C benchmark, it is possible to read an item that does
        // not exist.
        // for simplicity, we ignore this case.
        // this essentially makes the processing of NewOrder transaction more
        // time-consuming.
        i_ids.push_back(GetRandomInteger(0, state.item_count - 1));
        bool remote = GetRandomBoolean(new_order_remote_txns);
        ol_w_ids.push_back(warehouse_id);

        if (remote == true) {
            ol_w_ids[ol_itr] =
                    GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
            o_all_local = false;
        }

        ol_qtys.push_back(GetRandomInteger(0, order_line_max_ol_quantity));
    }

    /////////////////////////////////////////////////////////
    // BEGIN TRANSACTION
    /////////////////////////////////////////////////////////
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    for (auto item_id : i_ids) {
        //LOG_DEBUG("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %d", item_id);
        uint64_t key = item_id;
        uint32_t scan_sz = 0;
        bool point_lookup = true;
        bool is_for_update = false;
        auto predicate = nullptr;

        const char *k_item = reinterpret_cast<const char *>(&key);
        IndexScanExecutor<const char * , Item> executor(item_table,
                                                  sizeof(uint64_t),
                                                  k_item,
                                                  scan_sz,
                                                  point_lookup,
                                                  predicate,
                                                  is_for_update,
                                                   txn_ctx,
                                                  version_store);

        auto res = executor.Execute();
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when select item table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto &items = executor.GetResults();

        if (items.size() != 1) {
            LOG_ERROR("getItemInfo return size incorrect : %lu", items.size());
            assert(false);
            return false;
        }
    }

    LOG_TRACE("getWarehouseTaxRate: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %d", warehouse_id);

    double w_tax = 0.0;
    double w_y=0.0;
    {
        int vv = 0;
        uint64_t warehouse_key = warehouse_id;
        uint32_t scan_sz = 0;
        bool point_lookup = true;
        bool is_for_update = false;
        auto predicate = nullptr;

        const char *k_warehouse = reinterpret_cast<const char *>(&warehouse_key);
        IndexScanExecutor<const char *, Warehouse> executor(warehouse_table,
                                                       sizeof(uint64_t),
                                                       k_warehouse,
                                                       scan_sz,
                                                       point_lookup,
                                                       predicate,
                                                       is_for_update,
                                                       txn_ctx,
                                                       version_store);
        auto res = executor.Execute();
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when select warehouse table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto warehouses = executor.GetResults();
        if (executor.GetResults().size() != 1) {
            LOG_TRACE("abort transaction when select warehouse table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
        assert(executor.GetResults().size() == 1);

        assert(warehouses.size() == 1);
        w_tax = std::atof(warehouses[0]->W_TAX);
        w_y = std::atof(warehouses[0]->W_YTD);
    }

    LOG_TRACE("w_tax: %s, w_y: %s.", std::to_string(w_tax).c_str(), std::to_string(w_y).c_str());


    LOG_TRACE( "getDistrict: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %d "
                "AND D_W_ID = %d", district_id, warehouse_id);

    double d_tax = 0.0;
    int32_t d_next_o_id = 0;
    {
        auto key = District::DistrictKey{
                warehouse_id,
                district_id
        };
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
            LOG_TRACE("abort transaction when select district table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto districts = executor.GetResults();
        if(districts.size() != 1){
            LOG_TRACE("abort transaction when select district table");
            return false;
        }
        d_tax = std::atof(districts[0]->D_TAX);
        d_next_o_id = districts[0]->D_NEXT_O_ID;
    }

    LOG_TRACE("d_tax: %s, d_next_o_id: %s", std::to_string(d_tax).c_str(),
              std::to_string(d_next_o_id).c_str());


    LOG_TRACE("getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE "
              "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d",
               warehouse_id, district_id, customer_id);

    char c_last[32];
    char c_credit[2];
    double c_discount = 0.0;

    {
        auto key = Customer::CustomerKey{
                warehouse_id,
                district_id,
                customer_id
        };
        bool point_lookup = true;
        bool is_for_update = false;
        uint32_t scan_sz = 0;
        uint32_t key_size = sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
        auto predicate = nullptr;

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
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when select customer table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto customers = executor.GetResults();
        if (customers.size() != 1){
            LOG_TRACE("abort transaction when select customer table");
            return false;
        }
        c_discount = std::atof(customers[0]->C_DISCOUNT);
        memcpy(c_credit, customers[0]->C_CREDIT, sizeof(c_credit));
        memcpy(c_last, customers[0]->C_LAST, sizeof(c_last));
    }

    std::string c_last_str = c_last;
    std::string c_credit_str = c_credit;
    LOG_TRACE("c_last: %s, c_credit: %s, c_discount: %s",
              c_last_str.c_str(), c_credit_str.c_str(), std::to_string(c_discount).c_str());

    int district_update_value = d_next_o_id + 1;
    LOG_TRACE("district update value = %d", district_update_value);



    LOG_TRACE("incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = %d WHERE D_ID = "
             "%d AND D_W_ID = %d", district_update_value, district_id, warehouse_id);
    {
        auto key = District::DistrictKey{warehouse_id, district_id};
        uint32_t key_size = sizeof(uint64_t) + sizeof(uint64_t);

        auto updater = [&](District &d) {
            d.D_NEXT_O_ID = district_update_value;
        };
        auto col_num = District::GetColumnNum();
        std::vector<oid_t> up_col;
        up_col.emplace_back(2);
        uint32_t delta = district_update_value;
        uint32_t delta_len = sizeof(uint32_t);
        bool is_for_update = false;

        const char *k_district = reinterpret_cast<const char *>(&key);
        PointUpdateExecutor<const char *, District> executor(district_table,
                                                                  k_district,
                                                                  key_size,
                                             reinterpret_cast<const char *>(&delta),
                                                                  up_col,
                                                                  delta_len,
                                                                  is_for_update,
                                                                  txn_ctx,
                                                                  version_store);
        auto res = executor.Execute();
        if (executor.GetResultCode().IsNotNeeded()){
            return true;
        }
        if (!res || txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when update district table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
    }



    LOG_TRACE("createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, "
              "O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL),key=%d,%d,%d.",
              warehouse_id,district_id,d_next_o_id);
    {
        Order o;
        o.O_ID = d_next_o_id;
        o.O_C_ID = customer_id;
        o.O_D_ID = district_id;
        o.O_W_ID = warehouse_id;
        o.O_ENTRY_D = 1;
        o.O_CARRIER_ID = 0;
        o.O_OL_CNT = o_ol_cnt;
        o.O_ALL_LOCAL = o_all_local;
        Order::OrderKey k_o_{warehouse_id,district_id,d_next_o_id};
        uint32_t k_o_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
        const char *k_order = reinterpret_cast<const char *>(&k_o_);
        InsertExecutor<const char *, Order> executor(orders_table,
                                                          k_order,
                                                          k_o_sz,
                                                          o,
                                                          txn_ctx,
                                                          version_store);

        auto res = executor.Execute();
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when inserting order table, thread_id = %d, d_id = "
                        "%d, next_o_id = %d",
                        (int) thread_id, (int) district_id, (int) d_next_o_id);
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

    }



    LOG_TRACE("createNewOrder: INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) "
              "VALUES (?, ?, ?),%d,%d,%d.",warehouse_id,district_id,d_next_o_id);
    {
        NewOrder no;
        no.NO_W_ID = warehouse_id;
        no.NO_D_ID = district_id;
        no.NO_O_ID = d_next_o_id;
        NewOrder::NewOrderKey k_n_{district_id,warehouse_id,d_next_o_id};
        uint32_t k_n_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
        uint32_t retry2_count =0;
        const char *k_new_order = reinterpret_cast<const char *>(&k_n_);
        retry2:
        InsertExecutor<const char *, NewOrder> executor(new_order_table,
                                                                   k_new_order,
                                                                   k_n_sz,
                                                                 no,
                                                                   txn_ctx,
                                                                   version_store);


        auto res = executor.Execute();
        if (executor.GetResultCode().IsCASFailure() || executor.GetResultCode().IsRetryFailure()){
            if (retry2_count > 5){
                LOG_TRACE("abort transaction when inserting new order table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }else{
                retry2_count ++;
                goto retry2;
            }
        }
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when inserting new order table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
    }



    for (size_t i = 0; i < i_ids.size(); ++i) {
        int item_id = i_ids.at(i);
        int ol_w_id = ol_w_ids.at(i);
        int ol_qty = ol_qtys.at(i);

        LOG_TRACE("getStockInfo: SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, "
                "S_REMOTE_CNT, S_DIST_? FROM STOCK WHERE S_I_ID = %d AND S_W_ID = %d",
                item_id, ol_w_id);

        auto key = Stock::StockKey{
                ol_w_id,
                item_id,
        };
        bool point_lookup = true;
        bool is_for_update = false;
        uint32_t scan_sz = 0;
        uint32_t key_size = sizeof(uint64_t) + sizeof(uint64_t) ;
        auto predicate = nullptr;

        const char *k_stock = reinterpret_cast<const char *>(&key);
        IndexScanExecutor<const char *, Stock> executor(stock_table,
                                                           key_size,
                                                           k_stock,
                                                           scan_sz,
                                                           point_lookup,
                                                           predicate,
                                                           is_for_update,
                                                           txn_ctx,
                                                           version_store);
        auto res = executor.Execute();

        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when select stock table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto stocks = executor.GetResults();
        if (stocks.size() != 1){
            LOG_TRACE("abort transaction when select stock table");
            continue;
        }

        int s_quantity = stocks[0]->S_QUANTITY;

        if (s_quantity >= ol_qty + 10) {
            s_quantity = s_quantity - ol_qty;
        } else {
            s_quantity = s_quantity + 91 - ol_qty;
        }

        char s_data[64];
        memcpy(s_data, stocks[0]->S_DATA, sizeof(s_data));

        int s_ytd = stocks[0]->S_YTD;

        int s_order_cnt = stocks[0]->S_ORDER_CNT;

        int s_remote_cnt = stocks[0]->S_REMOTE_CNT;

        if (ol_w_id != warehouse_id) {
            s_remote_cnt += 1;
        }

        LOG_TRACE( "updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT "
                "= ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");

        auto stock_key = Stock::StockKey{ol_w_id, item_id};
        auto col_num = Stock::GetColumnNum();
        std::vector<oid_t> up_col;
        up_col.push_back(2);
        up_col.push_back(3);
        up_col.push_back(4);
        up_col.push_back(5);
        uint32_t delta_len = sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t);
        char *delta = new char[delta_len];
        memcpy(delta, reinterpret_cast<const char *>(&s_quantity), sizeof(uint32_t));
        memcpy(delta+sizeof(uint32_t), reinterpret_cast<const char *>(&s_ytd), sizeof(uint32_t));
        memcpy(delta+sizeof(uint32_t)*2, reinterpret_cast<const char *>(&s_order_cnt), sizeof(uint32_t));
        memcpy(delta+sizeof(uint32_t)*3, reinterpret_cast<const char *>(&s_remote_cnt), sizeof(uint32_t));
        is_for_update = false;
        key_size = sizeof(uint64_t)+sizeof(uint64_t);
        const char *k_stock_ = reinterpret_cast<const char *>(&stock_key);
        PointUpdateExecutor<const char *, Stock> stock_update_executor(stock_table,
                                                                          k_stock_,
                                                                          key_size,
                                           reinterpret_cast<const char *>(delta),
                                                                          up_col,
                                                                          delta_len,
                                                                          is_for_update,
                                                                          txn_ctx,
                                                                          version_store);
        res = stock_update_executor.Execute();
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when update stock table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }



        LOG_TRACE("createOrderLine: INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, "
                "OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, "
                "OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"
                "key=%d,%d,%d,%d",warehouse_id,district_id,d_next_o_id,(int)i);
        {
            OrderLine ol;
            ol.OL_O_ID = d_next_o_id;
            ol.OL_D_ID = district_id;
            ol.OL_W_ID = warehouse_id;
            ol.OL_NUMBER = i;
            ol.OL_I_ID = item_id;
            ol.OL_SUPPLY_W_ID = ol_w_id;
            ol.OL_DELIVERY_D = 1;
            ol.OL_QUANTITY = ol_qty;
//            ol.OL_AMOUNT = 0;
            strncpy(ol.OL_AMOUNT, std::to_string(0).c_str(), sizeof(ol.OL_AMOUNT));
            memcpy(ol.OL_DIST_INFO, s_data, sizeof(ol.OL_DIST_INFO));
            OrderLine::OrderLineKey k_ol_{warehouse_id,district_id,d_next_o_id,(int)i};
            uint32_t k_ol_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
            uint32_t retry3_count =0;
            const char *k_orderline = reinterpret_cast<const char *>(&k_ol_);
            retry3:
            InsertExecutor<const char *, OrderLine> order_line_insert_executor(order_line_table,
                                                                           k_orderline,
                                                                           k_ol_sz,
                                                                           ol,
                                                                           txn_ctx,
                                                                           version_store);

            res = order_line_insert_executor.Execute();
            if (order_line_insert_executor.GetResultCode().IsCASFailure() ||
                            order_line_insert_executor.GetResultCode().IsRetryFailure()){
                if (retry3_count > 5){
                    LOG_TRACE("abort transaction when inserting order line table");
                    txn_manager->AbortTransaction(txn_ctx);
                    return false;
                }else{
                    retry3_count ++;
                    goto retry3;
                }
            }
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction when inserting order line table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }

        }
    }

    // transaction passed execution.
//    assert(txn_ctx->GetResult() == ResultType::SUCCESS);

    auto ret = txn_manager->CommitTransaction(txn_ctx);

    LogManager *log_manager = LogManager::GetInstance();
    bool status_ret = true;

    if (ret == ResultType::SUCCESS) {
        // transaction passed commitment.
        LOG_TRACE("commit txn, thread_id = %d, d_id = %d, next_o_id = %d",
                  (int) thread_id, (int) district_id, (int) d_next_o_id);
        if (log_manager->IsLogStart()){
            auto rw_set = txn_ctx->GetReadWriteSet();
            auto end_commit_id = txn_ctx->GetCommitId();
            if (!txn_ctx->IsReadOnly() && !rw_set.empty()){
                std::vector<LSN_T> result;
                auto on_complete = [&status_ret, &result](bool status,
                                                          TransactionContext *txn_,
                                                          std::vector<LSN_T> &&values) {
                    result = values;
                    status_ret = status;
//                txn_->ClearLogRecords();
                };
                LogManager::LogWrite(txn_ctx, end_commit_id, on_complete);
            }

        }

//        delete txn_ctx;
//        txn_ctx = nullptr;

        return status_ret;
    } else {
        // transaction failed commitment.
        assert(ret == ResultType::ABORTED || ret == ResultType::FAILURE);
        LOG_TRACE("abort txn, thread_id = %d, d_id = %d, next_o_id = %d",
                  (int) thread_id, (int) district_id, (int) d_next_o_id);
//        delete txn_ctx;
//        txn_ctx = nullptr;

        return false;
    }
}


bool RunQuery2(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer,
               std::vector<std::vector<std::pair<int32_t, int32_t>>> &supp_stock_map) {
    /*
       "query_2": {
                   "SELECT su_suppkey, "
                    + "su_name, "
                    + "n_name, "
                    + "i_id, "
                    + "i_name, "
                    + "su_address, "
                    + "su_phone, "
                    + "su_comment "
                    + "FROM item, supplier, stock, nation, region, "
                    + "(SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity "
                    + "FROM stock, "
                    + "supplier, "
                    + "nation, "
                    + "region "
                    + "WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey "
                    + "AND su_nationkey=n_nationkey "
                    + "AND n_regionkey=r_regionkey "
                    + "AND r_name LIKE 'Europ%' "
                    + "GROUP BY s_i_id) m "
                    + "WHERE i_id = s_i_id "
                    + "AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                    + "AND su_nationkey = n_nationkey "
                    + "AND n_regionkey = r_regionkey "
                    + "AND i_data LIKE '%b' "
                    + "AND r_name LIKE 'Europ%' "
                    + "AND i_id=m_i_id "
                    + "AND s_quantity = m_s_quantity "
                    + "ORDER BY n_name, "
                    + "su_name, "
                    + "i_id"
       }
     */
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    std::vector<Region> regions_vec;
    std::vector<Nation> nations_vec;
    // Prepare random data
    //ScanRegion
    {
//        for (uint reg_id = 0; reg_id < 5; reg_id++) {
//            //LOG_TRACE("getRegionInfo: SELECT R_REGIONKEY, R_NAME, R_COMMENT FROM REGION WHERE R_REGIONKEY = %d", reg_id);
//            uint64_t key = reg_id;
//            uint32_t scan_sz = 0;
//            bool point_lookup = true;
//            //need not transaction check
//            bool is_for_update = false;
//            auto predicate = nullptr;
//
//            const char *k_reg = reinterpret_cast<const char *>(&key);
//            IndexScanExecutor<const char * , Region> executor(region_table,
//                                                              sizeof(uint64_t),
//                                                              k_reg,
//                                                              scan_sz,
//                                                              point_lookup,
//                                                              predicate,
//                                                              is_for_update,
//                                                              txn_ctx,
//                                                              version_store);
//
//            auto res = executor.Execute();
//            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
//                LOG_TRACE("abort transaction when select region table");
//                txn_manager->AbortTransaction(txn_ctx);
//                return false;
//            }
//
//            auto regions = executor.GetResults();
//            if (regions.size() != 1) {
//                LOG_ERROR("getRegionInfo return size incorrect : %lu", regions.size());
//                assert(false);
//            }
//            regions_vec.emplace_back(*regions[0]);
//        }
        uint64_t st_key = 0;
        const char *k_ = reinterpret_cast<const char *>(&st_key);
        uint32_t k_sz = sizeof(uint64_t);
        uint32_t scan_sz = 6;
        TableScanExecutor<const char *, Region> scan_executor(region_table,
                                                                k_,
                                                                k_sz,
                                                              scan_sz,
                                                              txn_ctx,
                                                                version_store);

        auto res = scan_executor.Execute();
        if (!res) {
            assert(txn_ctx->GetResult() != ResultType::SUCCESS);
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
        regions_vec = scan_executor.GetResults();
    }

    //ScanNation
    {
//        for (uint nat_id = 0; nat_id < 62; nat_id++) {
//            //LOG_TRACE("getNationInfo: SELECT N_NATIONKEY, N_REGIONKEY, N_NAME, N_COMMENT
//            //          FROM NATION WHERE N_NATIONKEY = %d", nat_id);
//            uint64_t key = nat_id;
//            uint32_t scan_sz = 0;
//            bool point_lookup = true;
//            //need not transaction check
//            bool is_for_update = false;
//            auto predicate = nullptr;
//
//            const char *k_nat = reinterpret_cast<const char *>(&key);
//            IndexScanExecutor<const char * , Nation> executor(nation_table,
//                                                              sizeof(uint64_t),
//                                                              k_nat,
//                                                              scan_sz,
//                                                              point_lookup,
//                                                              predicate,
//                                                              is_for_update,
//                                                              txn_ctx,
//                                                              version_store);
//
//            auto res = executor.Execute();
//            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
//                LOG_TRACE("abort transaction when select nation table");
//                txn_manager->AbortTransaction(txn_ctx);
//                return false;
//            }
//            auto nations = executor.GetResults();
//            if (nations.size() != 1) {
//                LOG_ERROR("getNationInfo return size incorrect : %lu", nations.size());
//                assert(false);
//            }
//            nations_vec.emplace_back(*nations[0]);
//        }

        uint64_t st_key = 0;
        const char *k_ = reinterpret_cast<const char *>(&st_key);
        uint32_t k_sz = sizeof(uint64_t);
        uint32_t scan_sz = 65;
        TableScanExecutor<const char *, Nation> scan_executor(nation_table,
                                                              k_,
                                                              k_sz,
                                                              scan_sz,
                                                              txn_ctx,
                                                              version_store);

        auto res = scan_executor.Execute();
        if (!res) {
            assert(txn_ctx->GetResult() != ResultType::SUCCESS);
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
        nations_vec = scan_executor.GetResults();
    }

    {
        // Pick a target region
        auto target_region = 3;
        assert(0 <= target_region and target_region <= 4);

        //Scan region
        for(auto reg_itr=0; reg_itr<regions_vec.size(); ++reg_itr){
            auto reg = regions_vec[reg_itr];
            // filtering region
            if (reg.R_NAME != std::string(regions[target_region])) continue;

            // Scan nation
            for (auto nat_itr=0; nat_itr<nations_vec.size(); ++nat_itr) {
                auto nat = nations_vec[nat_itr];
                // filtering nation
                if (nat.N_REGIONKEY != reg.R_REGIONKEY) continue;

                uint64_t supp_key = 0;
                const char *k_supp = reinterpret_cast<const char *>(&supp_key);
                uint32_t scan_size = -1;
                uint32_t k_sz = sizeof(uint64_t);
                TableScanExecutor<const char *, Supplier> scan_executor(supplier_table,
                                                                        k_supp,
                                                                        k_sz,
                                                                        scan_size,
                                                                        txn_ctx,
                                                                        version_store);

                auto res = scan_executor.Execute();
                if (!res) {
                    assert(txn_ctx->GetResult() != ResultType::SUCCESS);
                    txn_manager->AbortTransaction(txn_ctx);
                    return false;
                }
                auto supps = scan_executor.GetResults();
                assert(supps.size() == 10000);

                // Scan suppliers
                for (auto supp_id = 0; supp_id< 10000; supp_id++) {
                    auto supplier_ = supps[supp_id];
                    auto supp_key = supplier_.SU_SUPPKEY;

                    // Filtering suppliers
                    if (nat.N_NATIONKEY != supplier_.SU_NATIONKEY) continue;

                    // aggregate - finding a stock tuple having min. stock level
                    //s_w_id/s_i_id
                    std::vector<uint64_t> stock_0={0,0};
                    //s_quantity/s_ytd/s_order_cnt/s_remote_cnt
                    std::vector<uint32_t> stock_1={0,0,0,0};

                    int16_t min_qty = std::numeric_limits<int16_t>::max();
                    for (auto &it : supp_stock_map[supp_key])  // already know
                        // "mod((s_w_id*s_i_id),10000)=su_suppkey"
                        // items
                    {
                        int s_w_id = it.first;
                        int s_i_id = it.second;
                        auto stock_key =  Stock::StockKey{s_w_id, s_i_id};
                        bool point_lookup = true;
                        bool is_for_update = false;
                        uint32_t scan_sz = 0;
                        uint32_t key_size = sizeof(uint64_t) + sizeof(uint64_t) ;
                        auto predicate = nullptr;

                        const char *k_stock = reinterpret_cast<const char *>(&stock_key);
                        IndexScanExecutor<const char *, Stock> executor(stock_table,
                                                                        key_size,
                                                                        k_stock,
                                                                        scan_sz,
                                                                        point_lookup,
                                                                        predicate,
                                                                        is_for_update,
                                                                        txn_ctx,
                                                                        version_store);
                        auto res = executor.Execute();

                        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                            LOG_TRACE("abort transaction when select stock table");
                            txn_manager->AbortTransaction(txn_ctx);
                            return false;
                        }
                        auto stocks = executor.GetResults();

                        assert(stocks.size() == 1);
                        if (stocks.size() != 1){
                            LOG_TRACE("abort transaction when select stock table");
                            txn_manager->AbortTransaction(txn_ctx);
                            return false;
                        }

                        auto stock_ = stocks[0];

                        assert((stock_key.S_W_ID+1) * (stock_key.S_I_ID+1) % 10000 == supp_key);

                        if (min_qty > stock_->S_QUANTITY) {
                            stock_0[0] = stock_->S_W_ID;
                            stock_0[1] = stock_->S_I_ID;
                            stock_1[0] = stock_->S_QUANTITY;
                            stock_1[1] = stock_->S_YTD;
                            stock_1[2] = stock_->S_ORDER_CNT;
                            stock_1[3] = stock_->S_REMOTE_CNT;
                        }
                    }

                    // fetch the (lowest stock level) item info
                    uint64_t item_id = stock_0[1];
                    uint32_t scan_sz_item = 0;
                    bool point_lookup_item = true;
                    //need transaction check
                    bool is_for_update_item = false;
                    auto predicate_item = nullptr;

                    const char *k_item = reinterpret_cast<const char *>(&item_id);
                    IndexScanExecutor<const char * , Item> executor_item(item_table,
                                                                         sizeof(uint64_t),
                                                                         k_item,
                                                                         scan_sz_item,
                                                                         point_lookup_item,
                                                                         predicate_item,
                                                                         is_for_update_item,
                                                                         txn_ctx,
                                                                         version_store);

                    auto res_item = executor_item.Execute();
                    if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                        LOG_TRACE("abort transaction when select item table");
                        txn_manager->AbortTransaction(txn_ctx);
                        return false;
                    }
                    auto items = executor_item.GetResults();

                    assert(items.size() == 1);

                    auto item_ = items[0];

                    //  filtering item (i_data like '%b')
                    std::string itm_idata = std::string(item_->I_DATA);
                    auto found = itm_idata.find('b');
                    if (found != std::string::npos) continue;

                    // XXX. read-mostly txn: update stock or item here (stock_1[0] < 15)
                    //if (stock_1[0] < 15) {
                    if (stock_1[0] < 10) {
                        int s_quantity = stock_1[0] + 50;
                        int s_ytd = stock_1[1] ;
                        int s_order_cnt = stock_1[2] ;
                        int s_remote_cnt = stock_1[3] ;

                        int ol_w_id_u = stock_0[0];
                        int item_id_u = stock_0[1];

                        LOG_TRACE( "updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT "
                                   "= ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?; %d,%d,%d,%d,%d,%d;",
                                   s_quantity,s_ytd,s_order_cnt,s_remote_cnt,ol_w_id_u,item_id_u);
                        auto stock_key = Stock::StockKey{ol_w_id_u, item_id_u};
                        auto col_num = Stock::GetColumnNum();
                        std::vector<oid_t> up_col;
                        up_col.push_back(2);
                        up_col.push_back(3);
                        up_col.push_back(4);
                        up_col.push_back(5);
                        uint32_t delta_len = sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t);
                        char *delta = new char[delta_len];
                        memcpy(delta, reinterpret_cast<const char*>(&s_quantity), sizeof(uint32_t));
                        memcpy(delta+sizeof(uint32_t), reinterpret_cast<const char*>(&s_ytd), sizeof(uint32_t));
                        memcpy(delta+sizeof(uint32_t)*2, reinterpret_cast<const char*>(&s_order_cnt), sizeof(uint32_t));
                        memcpy(delta+sizeof(uint32_t)*3, reinterpret_cast<const char*>(&s_remote_cnt), sizeof(uint32_t));
                        bool is_for_update = false;
                        uint32_t key_size = sizeof(uint64_t)+sizeof(uint64_t);
                        const char *k_stock_ = reinterpret_cast<const char *>(&stock_key);
                        PointUpdateExecutor<const char *, Stock> stock_update_executor(stock_table,
                                                                                       k_stock_,
                                                                                       key_size,
                                                                                       reinterpret_cast<const char *>(delta),
                                                                                       up_col,
                                                                                       delta_len,
                                                                                       is_for_update,
                                                                                       txn_ctx,
                                                                                       version_store);
                        res = stock_update_executor.Execute();
                        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                            LOG_TRACE("abort transaction when update stock table");
                            txn_manager->AbortTransaction(txn_ctx);
                            return false;
                        }
                    }


//                    std::cout <<  "supplier: " <<    supp_key   << ","
//                              << "supplier_name: " <<  supplier_.SU_NAME               << ","
//                              << "nation_name:" << nat.N_NAME                  << ","
//                              << "item_id:" << item_.I_ID                    << ","
//                              << "item_name:" << item_.I_NAME                << std::endl;

                }
            }
        }
    }




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
