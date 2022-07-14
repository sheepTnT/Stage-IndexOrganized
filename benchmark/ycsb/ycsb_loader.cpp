//
// Created by zhangqian on 2022/2/10.
//
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <random>

#include "ycsb_loader.h"
#include "ycsb_configuration.h"
#include "../benchmark_common.h"

namespace mvstore {
//table id, BTree(main)
std::unordered_map<oid_t, BTree *> database_tables;
std::vector<Catalog *> ycsb_catalogs={};
namespace benchmark {
namespace ycsb {

YCSBTable *user_table = nullptr;
std::vector<std::string> eml_keys = {};

void CreateYCSBDatabase(ParameterSet param, VersionStore *buf_mgr ,
                        DramBlockPool *leaf_node_pool ,
                        InnerNodeBuffer *inner_node_pool ,
                        EphemeralPool *conflict_buffer ) {
    std::vector<Catalog *> schemas;

    Catalog *table_catalog = new Catalog();
    uint8_t *key_bitmaps = new uint8_t[11];
    key_bitmaps[0]=1;
    for(int i=0; i<10; ++i){
        key_bitmaps[i]=0;
    }

    if (state.string_mode == true) {
        table_catalog->init("table0",11,key_bitmaps,16,1,false);
        table_catalog->add_col("Key", 16, "VARCHAR");
    } else {
        table_catalog->init("table0",11,key_bitmaps,8,1,false);
        table_catalog->add_col("Key", 8, "INTEGER");
    }

    for(int i=0; i<10; ++i){
        std::string col_name = "filed";
        col_name.append(std::to_string(i));
        table_catalog->add_col(col_name.c_str(), 100, "VARCHAR");
    }

    auto data_table = new YCSBTable(param, *table_catalog,
                                    leaf_node_pool, inner_node_pool, conflict_buffer);

    //Log manager
    Catalog *log_catalog = new Catalog();
    log_catalog->table_id = 0;
    log_catalog->is_log = true;
    ycsb_catalogs.push_back(table_catalog);
    schemas.push_back(log_catalog);
    schemas.push_back(table_catalog);
    buf_mgr->Init(schemas);

    database_tables.insert(std::make_pair(0, data_table));
    user_table = data_table;

}

void DestroyYCSBDatabase(VersionBlockManager *version_block_mng,
                         VersionStore *version_store,
                         SSNTransactionManager *txn_mng) {

    //free the Emulated NVM and NVM space
    version_block_mng->ClearVersionBlock();
    version_store->CleanVersionStore();
    txn_mng->ClearInstance();

    //free the
    delete user_table;
    user_table = nullptr;
    ycsb_catalogs.clear();
    database_tables.clear();
    eml_keys.clear();
}

void LoadYCSBRows(VersionStore *version_store, const int begin_rowid, const int end_rowid, int thread_id) {
    /////////////////////////////////////////////////////////
    // Load in the data
    /////////////////////////////////////////////////////////

    // Insert kBatchSize Tuples at a time
    constexpr int kBatchSize = 1024;
    //insert failed tuples
    std::list<int> retry_rowids;
    // Insert tuples into the data table.
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    txn_ctx->SetReadOnly();
    FastRandom rnd_var(rand());

    if(state.string_mode){
        for (int rowid = begin_rowid; rowid < end_rowid; ) {
            int next_batch_rowid = std::min(end_rowid, rowid + kBatchSize);
            for (; rowid < next_batch_rowid; rowid++) {
                retry:
                YCSBTupleEml tuple;
                std::string eml_str = rnd_var.next_string(16);
                eml_keys[rowid] = eml_str;
                std::memcpy(tuple.key, eml_str.data(), 16);
                for (int i = 0; i < COLUMN_COUNT; ++i)
                    memset(tuple.cols[i], 'a', sizeof(tuple.cols[i]));

                const char *k_ = eml_str.data();
                uint32_t k_len = 16;

                RecordMeta meta;
                auto txn_id = txn_ctx->GetReadId();
                ReturnCode insert_res = user_table->Insert(k_, k_len,
                                                           tuple.GetData(),
                                                           &meta,
                                                           txn_id);


                if(insert_res.IsKeyExists()){
                    goto retry;
                }
                if (insert_res.IsRetryFailure() || insert_res.IsCASFailure() ){
                    retry_rowids.push_back(rowid);
                }
            }
        }
    }else{
        for (int rowid = begin_rowid; rowid < end_rowid; ) {
            int next_batch_rowid = std::min(end_rowid, rowid + kBatchSize);
            for (; rowid < next_batch_rowid; rowid++) {
                YCSBTupleInt tuple;
                tuple.key = rowid;
                for (int i = 0; i < COLUMN_COUNT; ++i)
                    memset(tuple.cols[i], rowid, sizeof(tuple.cols[i]));

                uint32_t k_ = rowid;
                RecordMeta meta;
                auto txn_id = txn_ctx->GetReadId();
                ReturnCode insert_res = user_table->Insert((reinterpret_cast<const char *>(&k_)),
                                                           sizeof(uint32_t),
                                                           tuple.GetData(),
                                                           &meta,
                                                           txn_id);
                if (insert_res.IsRetryFailure() || insert_res.IsCASFailure()){
                    retry_rowids.push_back(k_);
                }
            }
        }
    }

    auto result = txn_manager->CommitTransaction(txn_ctx);

    //insert retry
    txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    txn_ctx->SetReadOnly();
    if(state.string_mode){
        while (!retry_rowids.empty()) {
            int retry_rowid = retry_rowids.front();
            retry_2:
            YCSBTupleEml tuple;
            std::string eml_str = rnd_var.next_string(16);
            eml_keys[retry_rowid] = eml_str;
            std::memcpy(tuple.key, eml_str.data(), 16);
            for (int i = 0; i < COLUMN_COUNT; ++i)
                memset(tuple.cols[i], 'a', sizeof(tuple.cols[i]));

            const char *k_ = eml_str.data();
            uint32_t k_len = 16;
            RecordMeta meta;
            auto txn_id = txn_ctx->GetReadId();
            ReturnCode insert_res = user_table->Insert(k_, k_len,
                                                       tuple.GetData(),
                                                       &meta,
                                                       txn_id);
            if(insert_res.IsKeyExists()){
                goto retry_2;
            }
            if (!insert_res.IsCASFailure() && !insert_res.IsRetryFailure()){
                retry_rowids.pop_front();
            }
        }
    }else{
        while (!retry_rowids.empty()) {
            int retry_rowid = retry_rowids.front();
            uint32_t k_ = retry_rowid;
            YCSBTupleInt tuple;
            tuple.key = k_;
            for (int i = 0; i < COLUMN_COUNT; ++i)
                memset(tuple.cols[i], retry_rowid, sizeof(tuple.cols[i]));

            RecordMeta meta;
            auto txn_id = txn_ctx->GetReadId();
            ReturnCode insert_res = user_table->Insert((reinterpret_cast<const char *>(&k_)),
                                                       sizeof(uint32_t),
                                                       tuple.GetData(),
                                                       &meta,
                                                       txn_id);
            if (!insert_res.IsCASFailure() && !insert_res.IsRetryFailure()){
                retry_rowids.pop_front();
            }

        }
    }

    result = txn_manager->CommitTransaction(txn_ctx);

    delete txn_ctx;
    txn_ctx = nullptr;
}

void LoadYCSBDatabase(VersionStore *version_store) {

    std::chrono::steady_clock::time_point start_time;
    start_time = std::chrono::steady_clock::now();
    int load_thread_num = state.loader_count;
    sync::ThreadPool  the_tp(load_thread_num);

    const int tuple_count = state.scale_factor;
    int row_per_thread = tuple_count / state.loader_count;

    eml_keys.clear();
    if(state.string_mode){
        eml_keys.resize(tuple_count);
    }


    std::vector<std::unique_ptr<std::thread>> load_threads(state.loader_count);

    for (int thread_id = 0; thread_id < state.loader_count - 1; ++thread_id) {
        int begin_rowid = row_per_thread * thread_id;
        int end_rowid = row_per_thread * (thread_id + 1);
        load_threads[thread_id].reset(new std::thread(LoadYCSBRows, version_store, begin_rowid, end_rowid,thread_id));
    }

    int thread_id = state.loader_count - 1;
    int begin_rowid = row_per_thread * thread_id;
    int end_rowid = tuple_count;
    load_threads[thread_id].reset(new std::thread(LoadYCSBRows, version_store, begin_rowid, end_rowid,thread_id));

    for (int thread_id = 0; thread_id < state.loader_count; ++thread_id) {
        load_threads[thread_id]->join();
    }

    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG_INFO("database table loading time = %lf ms", diff);

    //LOG_INFO("%sTABLE SIZES%s", peloton::GETINFO_HALF_THICK_LINE.c_str(), peloton::GETINFO_HALF_THICK_LINE.c_str());
    LOG_INFO("user count = %d", tuple_count);

}

}
}
}
