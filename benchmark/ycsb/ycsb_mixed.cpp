//
// Created by zhangqian on 2022/2/10.
//
#include <cassert>
#include <chrono>
#include <cstddef>
#include <random>
#include <vector>
#include "ycsb_configuration.h"
#include "ycsb_loader.h"
#include "ycsb_workload.h"


namespace mvstore {
namespace benchmark {
namespace ycsb {

bool RunMixed(VersionStore *version_store, const size_t thread_id, ZipfDistribution &zipf,
              FastRandom &rng, std::vector<uint32_t>& keys) {
    auto txn_manager = SSNTransactionManager::GetInstance();
    TransactionContext *txn = txn_manager->BeginTransaction(thread_id,
                                                            IsolationLevelType::SERIALIZABLE);
    if (state.update_ratio == 0){txn->SetReadOnly();}

    for (int i = 0; i < state.operation_count; i++) {
        auto rng_val = rng.NextUniform();
        //random a key
        auto lookup_key_idx = 0;
        if (state.random_mode){
			
            lookup_key_idx = (rand() % ((keys.size())-0+1))+0;
        }else{
            //Zipf
            lookup_key_idx = zipf.GetNextNumber();
        }
        uint32_t lookup_key = keys[lookup_key_idx - 1];
        //update ratio[0,100]
        if (rng_val < state.update_ratio) {
            /////////////////////////////////////////////////////////
            // PERFORM UPDATE
            /////////////////////////////////////////////////////////
            //perform delta update
            char *delta_upt = new char[100];
            char chr=rng.next_char();
            memset(delta_upt, chr, 100);

            uint32_t upt_len = 100 * 1;

            std::vector<oid_t> up_col;
            up_col.push_back(1);

            bool is_for_update = true;
            PointUpdateExecutor<const char *, YCSBTupleInt> update_executor(user_table,
                                                                        reinterpret_cast<const char *>(&lookup_key),
                                                                        sizeof(uint32_t),
                                                                        delta_upt,
                                                                        up_col,
                                                                        upt_len,
                                                                        is_for_update,
                                                                        txn,
                                                                        version_store);

            auto res = update_executor.Execute();
            if (!res) {
                assert(txn->GetResult() != ResultType::SUCCESS);
                txn_manager->AbortTransaction(txn);
                return false;
            }
        } else {
            /////////////////////////////////////////////////////////
            // PERFORM READ
            /////////////////////////////////////////////////////////
            //read ratio[0,100]
            bool point_lookup = true;
            //if key lookup,this does not matter
            uint32_t scan_sz = 0;
            //whether can read the uncommited updates
            bool is_for_update = false;
            auto predicate = nullptr;

            IndexScanExecutor<const char *, YCSBTupleInt> lookup_executor(user_table,
                                                                          sizeof(uint32_t),
                                                                          reinterpret_cast<const char *>(&lookup_key),
                                                                          scan_sz,
                                                                          point_lookup,
                                                                          predicate,
                                                                          is_for_update,
                                                                          txn,
                                                                          version_store);

            auto res = lookup_executor.Execute();
            if (!res) {
                assert(txn->GetResult() != ResultType::SUCCESS);
                txn_manager->AbortTransaction(txn);
                return false;
            }
//                    auto ret_size = lookup_executor.GetResults().size();
//                    LOG_DEBUG("read result size : %zu", ret_size);

        }

        ++num_rw_ops;
    }

    // transaction passed execution.
    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    LogManager *log_manager = LogManager::GetInstance();
    bool status_ret = true;

    if (result == ResultType::SUCCESS) {
        // transaction passed commitment.
        LOG_TRACE("commit txn, thread_id = %d, d_id = %d, next_o_id = %d",
                  (int) thread_id, (int) district_id, (int) d_next_o_id);
        if (log_manager->IsLogStart()){
            auto rw_set = txn->GetReadWriteSet();
            auto end_commit_id = txn->GetCommitId();
            if (!txn->IsReadOnly() && !rw_set.empty()){
                std::vector<LSN_T> result;
                auto on_complete = [&status_ret, &result](bool status,
                                                          TransactionContext *txn_,
                                                          std::vector<LSN_T> &&values) {
                    result = values;
                    status_ret = status;
//                txn_->ClearLogRecords();
                };
                LogManager::LogWrite(txn, end_commit_id, on_complete);
            }

        }

        return status_ret;

    } else {
        // transaction failed commitment.
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);

        return false;
    }
}
bool RunInsert(VersionStore *version_store, const size_t thread_id){
    auto txn_manager = SSNTransactionManager::GetInstance();
    TransactionContext *txn_ctx = txn_manager->BeginTransaction(thread_id,
                                                            IsolationLevelType::SERIALIZABLE);

    for (int i = 0; i < state.operation_count; i++) {
       // int re2 = 0;
        uint32_t rowid = min_rowid.fetch_add(1);
//        LOG_DEBUG("rowid: %u",rowid);
        YCSBTupleInt tuple;
        tuple.key = rowid;
        for (int i = 0; i < COLUMN_COUNT; ++i)
            memset(tuple.cols[i], rowid, sizeof(tuple.cols[i]));

        uint32_t k_ = rowid;
        InsertExecutor<const char *, YCSBTupleInt> executor(user_table,
                                                            (reinterpret_cast<const char *>(&k_)),
                                                            sizeof(uint32_t),
                                                            tuple,
                                                            txn_ctx,
                                                            version_store);
        auto res = executor.Execute();

        if (res) {
            auto ret_code = executor.GetResultCode();

        }else{
            assert(txn_ctx->GetResult() != ResultType::SUCCESS);
            txn_manager->AbortTransaction(txn_ctx);
            return false;
        }

        ++num_rw_ops;
    }

    assert(txn_ctx->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn_ctx);

    LogManager *log_manager = LogManager::GetInstance();
    bool status_ret = true;

    if (result == ResultType::SUCCESS) {
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

        return status_ret;

    } else {
        // transaction failed commitment.
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);

        return false;
    }
}

bool RunMixedString(VersionStore *version_store, const size_t thread_id, ZipfDistribution &zipf,
              FastRandom &rng, std::vector<uint32_t>& keys) {
    auto txn_manager = SSNTransactionManager::GetInstance();
    TransactionContext *txn = txn_manager->BeginTransaction(thread_id,
                                                            IsolationLevelType::SERIALIZABLE);
    if (state.update_ratio == 0){txn->SetReadOnly();}

    for (int i = 0; i < state.operation_count; i++) {
        auto rng_val = rng.NextUniform();
        //random a key
        auto lookup_key_idx = 0;
        if (state.random_mode){
		 
            lookup_key_idx = (rand() % ((keys.size())-0+1))+0;
        }else{
            //Zipf
            lookup_key_idx = zipf.GetNextNumber();
        }
        uint64_t lookup_key = keys[lookup_key_idx -1];
        //update ratio[0,100]
        if (rng_val < state.update_ratio) {
            /////////////////////////////////////////////////////////
            // PERFORM UPDATE
            /////////////////////////////////////////////////////////
            //perform delta update
            char *delta_upt = new char[100];
            char chr=rng.next_char();
            memset(delta_upt, chr, 100);

            uint32_t upt_len = 100 * 1;

            std::vector<oid_t> up_col;
            up_col.push_back(1);

            bool is_for_update = true;

            const char *k_ = eml_keys[lookup_key-1].data();
            uint32_t k_sz = 16;
            PointUpdateExecutor<const char *, YCSBTupleEml> update_executor(user_table, k_,
                                                                            k_sz,
                                                                            delta_upt,
                                                                            up_col,
                                                                            upt_len,
                                                                            is_for_update,
                                                                            txn,
                                                                            version_store);

            auto res = update_executor.Execute();
            if (!res) {
                assert(txn->GetResult() != ResultType::SUCCESS);
                txn_manager->AbortTransaction(txn);
                return false;
            }
        }else {
            /////////////////////////////////////////////////////////
            // PERFORM READ
            /////////////////////////////////////////////////////////
            //read ratio[0,100]
            bool point_lookup = true;
            auto predicate = nullptr;
            //if key lookup,this does not matter
            uint32_t scan_sz = 0;
            //whether can read the uncommited updates
            bool is_for_update = false;
            const char *k_ =  eml_keys[lookup_key-1].data();
            uint32_t k_sz = 16;

            IndexScanExecutor<const char *, YCSBTupleEml> lookup_executor(user_table,
                                                                          k_sz,
                                                                          k_,
                                                                          scan_sz,
                                                                          point_lookup,
                                                                          predicate,
                                                                          is_for_update,
                                                                          txn,
                                                                          version_store);

            auto res = lookup_executor.Execute();
            if (!res) {
                assert(txn->GetResult() != ResultType::SUCCESS);
                txn_manager->AbortTransaction(txn);
                return false;
            }
//                auto ret_size = lookup_executor.GetResults().size();
//                    LOG_DEBUG("read result size : %zu", ret_size);
        }

        ++num_rw_ops;
    }


    // transaction passed execution.
    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
//        delete txn;
//        txn = nullptr;

        return true;
    } else {
        // transaction failed commitment.
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);
//        delete txn;
//        txn = nullptr;

        return false;
    }
}

bool RunScan(VersionStore *version_store, const size_t thread_id, ZipfDistribution &zipf,
              FastRandom &rng, std::vector<uint32_t>& keys) {
    auto txn_manager = SSNTransactionManager::GetInstance();
    TransactionContext *txn = txn_manager->BeginTransaction(thread_id,
                                                            IsolationLevelType::SERIALIZABLE);
    txn->SetReadOnly();

    //random a key
    auto lookup_key_idx = 0;
    if (state.random_mode){
        lookup_key_idx = (rand() % ((keys.size())-0+1))+0;
    }else{
        //Zipf
        lookup_key_idx = zipf.GetNextNumber();
    }

    uint32_t start_key = keys[lookup_key_idx - 1];
    uint32_t scan_size = 1000;

    if(state.string_mode){
        const char *k_ = eml_keys[start_key].data();
        uint32_t k_sz = 16;

        TableScanExecutor<const char *, YCSBTupleEml> scan_executor(user_table,
                                                                    k_,
                                                                    k_sz,
                                                                    scan_size,
                                                                    txn,
                                                                    version_store);
        auto res = scan_executor.Execute();
        if (!res) {
            assert(txn->GetResult() != ResultType::SUCCESS);
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto rts = scan_executor.GetResults();
        auto sz_rt = rts.size();
        char *data = new char[100];
        for (int j = 0; j < sz_rt; ++j) {
            auto ycsb_tpl = rts[j];
            auto cols = ycsb_tpl.cols;
            auto ke = ycsb_tpl.key;
            char *ky = new char[16];
            memcpy(ky, reinterpret_cast<char *>(ke), 16);
            char *cols_loc = reinterpret_cast<char *>(&cols);
            for (int k = 0; k < 10; ++k) {
                memcpy(data, (cols_loc+k*100), 100);
            }
        }
//           assert(scan_executor.GetResults().size() == 100);
           LOG_DEBUG("scan executor size: %zu", scan_executor.GetResults().size());
    }else{
        TableScanExecutor<const char *, YCSBTupleInt> scan_executor(user_table,
                                                                    reinterpret_cast<const char *>(&start_key),
                                                                    sizeof(uint32_t),
                                                                    scan_size,
                                                                    txn,
                                                                    version_store);


        auto res = scan_executor.Execute();
        if (!res) {
            assert(txn->GetResult() != ResultType::SUCCESS);
            txn_manager->AbortTransaction(txn);
            return false;
        }
        auto rts = scan_executor.GetResults();
        auto sz_rt = rts.size();
        char *data = new char[100];
        for (int j = 0; j < sz_rt; ++j) {
            auto ycsb_tpl = rts[j];
            auto cols = ycsb_tpl.cols;
            auto ke = ycsb_tpl.key;
            char *cols_loc = reinterpret_cast<char *>(&cols);
            for (int k = 0; k < 10; ++k) {
                memcpy(data, (cols_loc+k*100), 100);
            }
        }
      LOG_DEBUG("scan executor size: %zu", scan_executor.GetResults().size());
    }


    auto result = txn_manager->CommitTransaction(txn);

    // transaction passed execution.
    assert(txn->GetResult() == ResultType::SUCCESS);

    if (result == ResultType::SUCCESS) {
//        delete txn;
//        txn = nullptr;

        return true;
    } else {
        // transaction failed commitment.
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);
//        delete txn;
//        txn = nullptr;

        return false;
    }
}

}
}
}
