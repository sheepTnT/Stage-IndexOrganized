//
// Created by zhangqian on 2022-03-1.
//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc.cpp
//
// Identification: src/main/tpcc/tpcc.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <iomanip>
#include "../include/common/mono_queue_pool.h"
#include "../include/common/logger.h"
#include "tpcc_configuration.h"
#include "tpcc_loader.h"
#include "tpcc_workload.h"


namespace mvstore {
namespace benchmark {
namespace tpcc {

configuration state;

// Main Entry Point
void RunBenchmark() {
    LOG_INFO("%s : %d", "Run exponential backoff", state.exp_backoff);
//    LOG_INFO("%s : %f", "scale_factor", state.scale_factor);
    LOG_INFO("%s : %lf", "profile_duration", state.profile_duration);
    LOG_INFO("%s : %lf", "duration", state.duration);
    LOG_INFO("%s : %lf", "warmup duration", state.warmup_duration);
    LOG_INFO("%s : %d", "warehouse_count", state.warehouse_count);
    LOG_INFO("%s : %d", "lodaer_count", state.loader_count);
    LOG_INFO("%s : %d", "backend_count", state.backend_count);
    LOG_INFO("%s : %lf", "hybrid_rate", state.scan_rate);
    LOG_INFO("%s : %lf", "new_order_rate", state.new_order_rate);
    LOG_INFO("%s : %lf", "stock_level_rate", state.stock_level_rate);
//    LOG_INFO("%s : %s", "wal_path", state.wal_path.c_str());

//    if (!state.wal_path.empty()) {
//        std::string wal_file_path = state.wal_path + "/wal";
//        state.wal_path = wal_file_path;
//    }

    VersionBlockManager *version_block_mng;
    VersionStore *version_store;
    DramBlockPool *leaf_node_pool;
    InnerNodeBuffer *inner_node_pool;
    EphemeralPool *conflict_buffer;

    //Version Block Manager
    version_block_mng = VersionBlockManager::GetInstance();
    Status v_st = version_block_mng->Init();
    PELOTON_ASSERT(v_st.ok(),"VersionBlockManager initialization fail.");
    //Version store
    version_store = VersionStore::GetInstance();
    //for leaf node
    leaf_node_pool = new DramBlockPool(default_blocks, default_blocks);
    //for inner node
    RecordBufferPool *_pool = new RecordBufferPool(500000000,500000000 );
    inner_node_pool = new InnerNodeBuffer(_pool);
    //for undo buffer
    RecordBufferPool *buffer_pool = new RecordBufferPool(500000000,500000000 );
    UndoBuffer *undo_buffer_pool = new UndoBuffer(buffer_pool);
    conflict_buffer = new EphemeralPool(undo_buffer_pool);
    //initialize the transaction's undo buffer
    SSNTransactionManager *transaction_manager = SSNTransactionManager::GetInstance();
    transaction_manager->Init(conflict_buffer);

    ParameterSet param(64*1024, 32*1024,64*1024, 0);

    PELOTON_ASSERT(leaf_node_pool, "leaf_node_pool new fail.");
    PELOTON_ASSERT(inner_node_pool, "inner_node_pool new fail.");
    PELOTON_ASSERT(undo_buffer_pool, "undo_buffer_pool new fail.");
    PELOTON_ASSERT(conflict_buffer, "conflict_buffer new fail.");
    PELOTON_ASSERT(version_store,"version_store get instance fail.");


    LogManager *log_mng = LogManager::GetInstance();
    log_mng->Init();

    LOG_INFO("Loading database from scratch");
    //Create the database, initialize the version store
    CreateTPCCDatabase(param, version_store, leaf_node_pool, inner_node_pool, conflict_buffer);

    //Load the databases
    LoadTPCCDatabase(version_store);

    //Start logging
    //log_mng->LogStart();
    if(log_mng->IsLogStart()){
        if(nvm_emulate){
            LOG_INFO("DRAM emulate log file.");
        }else{
            LOG_INFO("NVM(PMM) log file.");
        }
    }else{
        LOG_INFO("no logging.");
    }

    if (state.warmup_duration != 0) {
        RunWarmupWorkload(version_store, conflict_buffer);
    }

    // Run the workload
    RunWorkload(version_store, conflict_buffer);

    // Emit throughput
    WriteOutput();

    DestroyTPCCDatabase(version_block_mng,version_store,transaction_manager);

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace mvstore

int main(int argc, char **argv) {
    mvstore::benchmark::tpcc::ParseArguments(argc, argv, mvstore::benchmark::tpcc::state);

    mvstore::benchmark::tpcc::RunBenchmark();

    return 0;
}
