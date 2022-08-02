//
// Created by zhangqian on 2022/2/10.
//
#include <string>
#include "../include/common/mono_queue_pool.h"
#include "ycsb_configuration.h"
#include "ycsb_loader.h"
#include "ycsb_workload.h"

namespace mvstore {
namespace benchmark {
namespace ycsb {

configuration state;

// Main Entry Point
void RunBenchmark() {
    LOG_INFO("%s : %d", "Run exponential backoff", state.exp_backoff);
//    if (!state.wal_path.empty()) {
//        std::string wal_file_path = state.wal_path + "/wal";
//        state.wal_path = wal_file_path;
//    }
    LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
    LOG_INFO("%s : %lf", "profile_duration", state.profile_duration);
    LOG_INFO("%s : %lf", "duration", state.duration);
//    LOG_INFO("%s : %lf", "warmup duration", state.warmup_duration);
    LOG_INFO("%s : %d", "lodaer_count", state.loader_count);
    LOG_INFO("%s : %d", "backend_count", state.backend_count);
    LOG_INFO("%s : %d", "operation_count", state.operation_count);
    LOG_INFO("%s : %lf", "update_ratio", state.update_ratio);
    LOG_INFO("%s : %lf", "key zipf_theta", state.zipf_theta);
    LOG_INFO("%s : %d", "key string mode", state.string_mode);
    LOG_INFO("%s : %d", "key random mode", state.random_mode);
    LOG_INFO("%s : %d", "run scan mode", state.scan_mode);
//    LOG_INFO("%s : %s", "wal_path", state.wal_path.c_str());


    VersionBlockManager *version_block_mng;
    VersionStore *version_store;
    DramBlockPool *leaf_node_pool;
    InnerNodeBuffer *inner_node_pool;
    EphemeralPool *overwritten_buffer;

    //Version Block Manager
    version_block_mng = VersionBlockManager::GetInstance();
    Status v_st = version_block_mng->Init();
    PELOTON_ASSERT(v_st.ok(),"VersionBlockManager initialization fail.");
    //Version store
    version_store = VersionStore::GetInstance();
    //for leaf node
    leaf_node_pool = new DramBlockPool(default_blocks, default_blocks);
    //for inner node
    RecordBufferPool *_pool = new RecordBufferPool(50000000000 ,50000000000);
    inner_node_pool = new InnerNodeBuffer(_pool);
    //for undo buffer
    RecordBufferPool *buffer_pool = new RecordBufferPool(50000000000,50000000000);
    UndoBuffer *undo_buffer_pool = new UndoBuffer(buffer_pool);
    overwritten_buffer = new EphemeralPool(undo_buffer_pool);
    //initialize the transaction's undo buffer
    SSNTransactionManager *transaction_manager = SSNTransactionManager::GetInstance();
    transaction_manager->Init(overwritten_buffer);

    PELOTON_ASSERT(leaf_node_pool, "leaf_node_pool new fail.");
    PELOTON_ASSERT(inner_node_pool, "inner_node_pool new fail.");
    PELOTON_ASSERT(undo_buffer_pool, "undo_buffer_pool new fail.");
    PELOTON_ASSERT(overwritten_buffer, "conflict_buffer new fail.");
    PELOTON_ASSERT(version_store,"version_store get instance fail.");

    ParameterSet param(64*1024, 32*1024,64*1024,100*10);

    LogManager *log_mng = LogManager::GetInstance();
    log_mng->Init();

    LOG_INFO("Loading database from scratch");

    //Create the database, initialize the version store
    CreateYCSBDatabase(param, version_store, leaf_node_pool, inner_node_pool, overwritten_buffer);

    //Load the database
    LoadYCSBDatabase(version_store);

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

    size_t num_keys = (state.scale_factor);
    std::vector<uint32_t> keys;
    for(int i = 0; i < num_keys; ++i)
        keys.push_back(i + 1);

//    if (state.warmup_duration != 0) {
//        RunWarmupWorkload(version_store, keys);
//    }
    // Run the workload
    RunWorkload(version_store, keys);

    // Emit throughput
    WriteOutput();

    DestroyYCSBDatabase(version_block_mng,version_store,transaction_manager);

}

}
}
}

int main(int argc, char **argv) {
    mvstore::benchmark::ycsb::ParseArguments(argc, argv,
                                             mvstore::benchmark::ycsb::state);

    mvstore::benchmark::ycsb::RunBenchmark();

    return 0;
}
