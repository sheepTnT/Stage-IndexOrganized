//
// Created by zhangqian on 2022-03-1.
//
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_payment.cpp
//
// Identification: src/main/tpcc/tpcc_payment.cpp
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

bool RunPayment(const size_t &thread_id, VersionStore *version_store, EphemeralPool *conflict_buffer) {
    /*
       "PAYMENT": {
       "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE,
       W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
       "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE
       W_ID = ?", # h_amount, w_id
       "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE,
       D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
       "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE
       D_W_ID = ? AND D_ID = ?", # h_amount, d_w_id, d_id
       "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
       C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,
       C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
       FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id,
       c_id
       "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
       C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,
       C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
       FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY
       C_FIRST", # w_id, d_id, c_last
       "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?,
       C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID =
       ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
       "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?,
       C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", #
       c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
       "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
       }
     */

    LOG_TRACE("-------------------------------------");

    /////////////////////////////////////////////////////////
    // PREPARE ARGUMENTS
    /////////////////////////////////////////////////////////
    int warehouse_id = GenerateWarehouseId(thread_id);
    int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
    int customer_warehouse_id;
    int customer_district_id;
    int customer_id = -1;
    std::string customer_lastname;
    double h_amount =
            GetRandomFixedPoint(2, payment_min_amount, payment_max_amount);
    // WARN: Hard code the date as 0. may cause problem
    int h_date = 0;

    int x = GetRandomInteger(1, 100);
    // currently we only retrieve data by id.
    int y = 100;  // GetRandomInteger(1, 100);

    // 85%: paying through own warehouse ( or there is only 1 warehouse)
    if (state.warehouse_count == 1 || x <= 85) {
        customer_warehouse_id = warehouse_id;
        customer_district_id = district_id;
    }
        // 15%: paying through another warehouse
    else {
        customer_warehouse_id =
                GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
        assert(customer_warehouse_id != warehouse_id);
        customer_district_id =
                GetRandomInteger(0, state.districts_per_warehouse - 1);
    }

    // 60%: payment by last name
    if (y <= 60) {
        LOG_TRACE("By last name");
        customer_lastname = GetRandomLastName(state.customers_per_district);
    }
        // 40%: payment by id
    else {
        LOG_TRACE("By id");
        customer_id = GetRandomInteger(0, state.customers_per_district - 1);
    }

    /////////////////////////////////////////////////////////
    // BEGIN TRANSACTION
    /////////////////////////////////////////////////////////

    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    Customer customer;
    if (customer_id >= 0) {
        LOG_DEBUG("getCustomerByCustomerId:  WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = "
                "? , # w_id = %d, d_id = %d, c_id = %d",
                warehouse_id, district_id, customer_id);
        Customer::CustomerKey customer_key{warehouse_id, district_id, customer_id};
        bool point_lookup = true;
        bool is_for_update = false;
        uint32_t scan_sz = 0;
        uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t);
        auto predicate = [&](const Customer *c, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return c->C_W_ID == warehouse_id && c->C_D_ID == district_id && c->C_ID == customer_id;
        };
        const char *k_customer = reinterpret_cast<const char *>(&customer_key);
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
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when select customer");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto &customers = executor.GetResults();
        if (customers.size() != 1) {
            assert(false);
        }
        customer = *customers[0];
    } else {
        CustomerIndexedColumns cic;
        cic.C_W_ID = warehouse_id;
        cic.C_D_ID = district_id;
        cic.C_ID = -1;
        strcpy(cic.C_LAST, customer_lastname.c_str());
        bool point_lookup = false;
        bool is_for_update = false;
        uint32_t scan_sz = state.warehouse_count * state.districts_per_warehouse * state.customers_per_district;
        uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t)+32;
        auto predicate = [&](const CustomerIndexedColumns *cic, bool &should_end_scan) -> bool {
            if (cic->C_W_ID == warehouse_id && cic->C_D_ID == district_id &&
                std::string(cic->C_LAST) == customer_lastname) {
                return true;
            }
            should_end_scan = true;
            return false;
        };
        const char *k_customer_index = reinterpret_cast<const char *>(&cic);
        IndexScanExecutor<const char *, CustomerIndexedColumns> cic_scan_executor(customer_skey_table,
                                                                    key_size,
                                                                    k_customer_index,
                                                                    scan_sz,
                                                                    point_lookup,
                                                                    predicate,
                                                                    is_for_update,
                                                                    txn_ctx,
                                                                    version_store);

        auto res = cic_scan_executor.Execute();
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when select customer skey table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        auto cics = cic_scan_executor.GetResults();
        assert(cics.size() > 0);
        std::vector<Customer> customers;
        for (int i = 0; i < cics.size(); ++i) {
            int32_t c_id = cics[i]->C_ID;
            auto customer_key = Customer::CustomerKey{warehouse_id, district_id, c_id};
            bool point_lookup = true;
            bool is_for_update = false;
            uint32_t scan_sz = 0;
            uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t)+ sizeof(uint32_t);
            auto predicate = [&](const Customer *cic, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return cic->C_W_ID == warehouse_id && cic->C_D_ID == district_id && cic->C_ID == c_id;
            };
            const char *k_customer = reinterpret_cast<const char *>(&customer_key);
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
            // Check if aborted
            if (txn_ctx->GetResult() != ResultType::SUCCESS) {
                LOG_INFO("abort transaction when select customer table");
                txn_manager->AbortTransaction(txn_ctx);
                return false;
            }
            assert(executor.GetResults().size() == 1);
            customers.push_back(*executor.GetResults()[0]);
        }

        // Hack: sort customers by C_FIRST in-memory
//        std::sort(customers.begin(), customers.end(), [&](const Customer &lhs, const Customer &rhs) {
//            return std::string(lhs.C_FIRST) < std::string(rhs.C_FIRST);
//        });
        // Get the midpoint customer
        customer = customers[(customers.size() - 1) / 2];
    }


    LOG_DEBUG("getWarehouse:WHERE W_ID = ? # w_id = %d", warehouse_id);
    Warehouse warehouse;
    {
        int vv = 0;
        int32_t warehouse_key = warehouse_id;
        bool point_lookup = true;
        uint32_t scan_sz = 0;
        bool is_for_update = false;
        auto predicate = [&](const Warehouse *w, bool &should_end_scan) -> bool {
            should_end_scan = true;
            auto v = w->W_ID == warehouse_id;
            assert(v);
//            if (!v)
//                ++vv;
            return v;
        };
        const char *k_warehouse = reinterpret_cast<const char *>(&warehouse_key);
        IndexScanExecutor<const char *, Warehouse> executor(warehouse_table,
                                                       sizeof(uint32_t),
                                                       k_warehouse,
                                                       scan_sz,
                                                       point_lookup,
                                                       predicate,
                                                       is_for_update,
                                                       txn_ctx,
                                                       version_store);
        auto res = executor.Execute();
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when select warehouse table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        assert(executor.GetResults().size() == 1);
        warehouse = *executor.GetResults()[0];
    }



    LOG_DEBUG("getDistrict: WHERE D_W_ID = ? AND D_ID = ?, # w_id = %d, d_id = %d",
            warehouse_id, district_id);
    // We also retrieve the original D_YTD from this query,
    // which is not the standard TPCC approach
    const District *district;
    {
        District::DistrictKey district_key{warehouse_id, district_id};

        bool point_lookup = true;
        bool is_for_update = false;
        uint32_t scan_sz = 0;
        uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t);
        auto predicate = [&](const District *d, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return d->D_W_ID == warehouse_id && d->D_ID == district_id;
        };
        const char *k_district = reinterpret_cast<const char *>(&district_key);
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
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when select district table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
        assert(executor.GetResults().size() == 1);
        district = executor.GetResults()[0];
    }


    double warehouse_new_balance = std::atof(warehouse.W_YTD) + h_amount;


    LOG_DEBUG("updateWarehouseBalance: UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE "
            "W_ID = ?,# h_amount = %f, w_id = %d",
            h_amount, warehouse_id);
    {
        uint32_t warehouse_key = warehouse_id;
//        auto predicate = [&](const Warehouse &w) -> bool {
//            return w.W_ID == warehouse_id;
//        };
//        auto updater = [&](Warehouse &w) {
//            w.W_YTD = warehouse_new_balance;
//        };
        auto col_num = Warehouse::GetColumnNum();
        std::vector<oid_t> up_col;
        for (int p = 0; p < col_num; ++p){
            up_col.push_back(0);
        }
        up_col[8]=1;
        int64_t delta = district_initial_ytd;
        uint32_t delta_len = sizeof(int64_t);
        bool is_for_update = true;
        const char *k_warehouse = reinterpret_cast<const char *>(&warehouse_key);
        PointUpdateExecutor<const char *, Warehouse> executor(warehouse_table,
                                                          k_warehouse,
                                                         sizeof(uint32_t),
                                           reinterpret_cast<const char *>(&delta),
                                                         up_col,
                                                         delta_len,
                                                         is_for_update,
                                                         txn_ctx,
                                                         version_store);


        auto res = executor.Execute();
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when update warehouse table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
    }

    double district_new_balance = std::atoll(district->D_YTD )+ h_amount;


    LOG_DEBUG("updateDistrictBalance: UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE "
            "D_W_ID = ? AND D_ID = ?,# h_amount = %f, d_w_id = %d, d_id = %d",
            h_amount, district_id, warehouse_id);
    {
        auto district_key = District::DistrictKey{warehouse_id, district_id};
        uint32_t key_size = sizeof(uint32_t) + sizeof(uint32_t);
//        auto predicate = [&](const District &d) -> bool {
//            return d.D_W_ID == warehouse_id && d.D_ID == district_id;
//        };
//        auto updater = [&](District &d) {
//            d.D_YTD = district_new_balance;
//        };
        auto col_num = District::GetColumnNum();
        std::vector<oid_t> up_col;
        for (int p = 0; p < col_num; ++p){
            up_col.push_back(0);
        }
        up_col[9]=1;
        int64_t delta = district_new_balance;
        uint32_t delta_len = sizeof(int64_t);
        bool is_for_update = false;
        const char *k_district = reinterpret_cast<const char *>(&district_key);
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
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when update district table");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
    }


    std::string customer_credit(customer.C_CREDIT, sizeof(customer.C_CREDIT));

    double customer_balance = std::atof(customer.C_BALANCE) - h_amount;

    double customer_ytd_payment = std::atof(customer.C_YTD_PAYMENT) + h_amount;

    int customer_payment_cnt = customer.C_PAYMENT_CNT + 1;

    customer_id = customer.C_ID;



    // Check the credit record of the user
    if (customer_credit == customers_bad_credit) {
        LOG_DEBUG("updateBCCustomer:# c_balance = %f, c_ytd_payment = %f, c_payment_cnt "
                "= %d, c_data = %s, c_w_id = %d, c_d_id = %d, c_id = %d",
                customer_balance, customer_ytd_payment, customer_payment_cnt,
                data_constant.c_str(), customer_warehouse_id, customer_district_id,
                customer_id);
//        auto predicate = [&](const Customer &c) -> bool {
//            return c.C_W_ID == customer_warehouse_id && c.C_D_ID == customer_district_id && c.C_ID == customer_id;
//        };
//        auto updater = [&](Customer &c) {
//            c.C_BALANCE = customer_balance;
//            c.C_YTD_PAYMENT = customer_ytd_payment;
//            c.C_PAYMENT_CNT = customer_payment_cnt;
//            memcpy(c.C_DATA, data_constant.c_str(), sizeof(c.C_DATA));
//        };

        Customer::CustomerKey customer_key{customer_warehouse_id, customer_district_id, customer_id};
        auto col_num = Customer::GetColumnNum();
        std::vector<oid_t> up_col;
        for (int p = 0; p < col_num; ++p){
            up_col.push_back(0);
        }
        up_col[16]=1;
        up_col[17]=1;
        up_col[18]=1;
        up_col[20]=1;
        uint32_t delta_len = sizeof(int64_t)+sizeof(int64_t)+sizeof(uint32_t)+500;
        char *delta = new char[delta_len];
        memcpy(delta, &customer_balance, sizeof(int64_t));
        memcpy(delta+sizeof(int64_t), &customer_ytd_payment, sizeof(int64_t));
        memcpy(delta+sizeof(int64_t)*2, &customer_payment_cnt, sizeof(int32_t));
        memcpy(delta+sizeof(int64_t)*2+sizeof(int32_t), data_constant.c_str(), 500);
        bool is_for_update = true;
        uint32_t key_size = sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t);
        const char *k_customer = reinterpret_cast<const char *>(&customer_key);
        PointUpdateExecutor<const char *, Customer> executor(customer_table,
                                                                      k_customer,
                                                                      key_size,
                                                      reinterpret_cast<const char *>(delta),
                                                                      up_col,
                                                                      delta_len,
                                                                      is_for_update,
                                                                      txn_ctx,
                                                                      version_store);
        auto res = executor.Execute();
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when update customer table updateBCCustomer");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

    } else {
        LOG_DEBUG("updateGCCustomer: # c_balance = %f, c_ytd_payment = %f, c_payment_cnt "
                "= %d, c_w_id = %d, c_d_id = %d, c_id = %d",
                customer_balance, customer_ytd_payment, customer_payment_cnt,
                customer_warehouse_id, customer_district_id, customer_id);
//        auto predicate = [&](const Customer &c) -> bool {
//            return c.C_W_ID == customer_warehouse_id && c.C_D_ID == customer_district_id && c.C_ID == customer_id;
//        };
//        auto updater = [&](Customer &c) {
//            c.C_BALANCE = customer_balance;
//            c.C_YTD_PAYMENT = customer_ytd_payment;
//            c.C_PAYMENT_CNT = customer_payment_cnt;
//        };
        Customer::CustomerKey customer_key{customer_warehouse_id, customer_district_id, customer_id};
        auto col_num = Customer::GetColumnNum();
        std::vector<oid_t> up_col;
        for (int p = 0; p < col_num; ++p){
            up_col.push_back(0);
        }
        up_col[16]=1;
        up_col[17]=1;
        up_col[18]=1;
        uint32_t delta_len = sizeof(int64_t)+sizeof(int64_t)+sizeof(uint32_t);
        char *delta = new char[delta_len];
        memcpy(delta, &customer_balance, sizeof(int64_t));
        memcpy(delta+sizeof(int64_t), &customer_ytd_payment, sizeof(int64_t));
        memcpy(delta+sizeof(int64_t)*2, &customer_payment_cnt, sizeof(int32_t));
        bool is_for_update = true;
        uint32_t key_size = sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint32_t);
        const char *k_customer = reinterpret_cast<const char *>(&customer_key);
        PointUpdateExecutor<const char *, Customer> executor(customer_table,
                                                                      k_customer,
                                                                      key_size,
                                                     reinterpret_cast<const char *>(delta),
                                                                      up_col,
                                                                      delta_len,
                                                                      is_for_update,
                                                                      txn_ctx,
                                                                      version_store);
        auto res = executor.Execute();
        assert(res);
        // Check if aborted
        if (txn_ctx->GetResult() != ResultType::SUCCESS) {
            LOG_INFO("abort transaction when update customer table updateGCCustomer");
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }
    }



    LOG_DEBUG("insertHistory: INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

    History history_tuple = BuildHistoryTuple(customer_id, customer_district_id, customer_warehouse_id,
                                              district_id, warehouse_id);
    history_tuple.H_DATE = h_date;
//    history_tuple.H_AMOUNT = h_amount;
    strncpy(history_tuple.H_AMOUNT, std::to_string(h_amount).c_str(), sizeof(history_tuple.H_AMOUNT));
    memcpy(history_tuple.H_DATA, data_constant.data(), std::min(sizeof(history_tuple.H_DATA), data_constant.size()));
    uint32_t k_h_ = history_tuple.H_ID;
    const char *k_history = reinterpret_cast<const char *>(&k_h_);
    InsertExecutor<const char *, History> history_insert_executor(history_table,
                                                             k_history,
                                                            sizeof(uint32_t),
                                                            history_tuple,
                                                            txn_ctx,
                                                            version_store);

    auto res = history_insert_executor.Execute();
    assert(res);
//   Check result
    if (txn_ctx->GetResult() != ResultType::SUCCESS) {
        LOG_INFO("abort transaction when insert history");
        txn_manager->AbortTransaction(txn_ctx);
        return false;
    }

    assert(txn_ctx->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn_ctx);

    if (result == ResultType::SUCCESS) {
        return true;
    } else {
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);
        return false;
    }
}
}
}
}
