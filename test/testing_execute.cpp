//
// Created by zhangqian on 2022-1-7.
//

#include <iostream>
#include <vector>
#include <fstream>
#include <limits>
#include <gtest/gtest.h>
#include <random>
#include "../include/common/tools.h"
#include "testing_transaction_util.h"

namespace mvstore {
namespace test {

class ExecuteTest : public ::testing::Test{
    void SetUp() override{
        //Version Block Manager
        version_block_mng = VersionBlockManager::GetInstance();
        Status v_st = version_block_mng->Init();
        PELOTON_ASSERT(v_st.ok(),"VersionBlockManager initialization fail.");
        //Version store
        buf_mgr = VersionStore::GetInstance();


        //for leaf node
        leaf_node_pool = new DramBlockPool(default_blocks, default_blocks);
        //for inner node
        RecordBufferPool *_pool = new RecordBufferPool(500000,500000);
        inner_node_pool = new InnerNodeBuffer(_pool);
        //for undo buffer
        RecordBufferPool *buffer_pool = new RecordBufferPool(500000,500000);
        UndoBuffer *undo_buffer_pool = new UndoBuffer(buffer_pool);
        conflict_buffer = new EphemeralPool(undo_buffer_pool);

        SSNTransactionManager *transaction_manager = SSNTransactionManager::GetInstance();
        transaction_manager->Init(conflict_buffer);
    }
    void TearDown() override{
        version_block_mng->ClearVersionBlock();
//        buf_mgr->CleanVersionStore();
//        delete leaf_node_pool;
//        delete inner_node_pool;
//        delete conflict_buffer;
    }


protected:
    VersionBlockManager *version_block_mng;
    VersionStore *buf_mgr;
    DramBlockPool *leaf_node_pool;
    InnerNodeBuffer *inner_node_pool;
    EphemeralPool *conflict_buffer;

    size_t n_threads = 2;
    static sync::ThreadPool tp;

};

static int IntRand() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, std::numeric_limits<int>::max());
    return distribution(generator);
}

std::vector<uint64_t> GetWorkload(int n_ops, int n_kvs, const std::string &workload_filepath) {
    std::vector<uint64_t> workload;
    workload.reserve(n_ops);
    if (workload_filepath.empty()) {
        for (int i = 0; i < n_ops; ++i) {
            workload.push_back(IntRand() % n_kvs);
        }
    } else {
        std::ifstream infile(workload_filepath);
        while (!infile.eof()) {
            uint64_t idx;
            infile >> idx;
            workload.push_back(idx);
        }
    }
    return workload;
}
struct Tuple  {
    uint64_t key;
    uint64_t values[10];

    uint64_t Key() const { return key; }
    char *GetData() const{
        char *data = new char[sizeof(uint64_t)*10];
//        memset(data,key,sizeof(uint64_t));
        uint32_t sz = 0;
        for(int i=0; i<10; ++i){
            memcpy(data + sz, &(values[i]),sizeof(uint64_t));
            sz = sz + sizeof(uint64_t);
        }
        return data;
    }

};

TEST_F(ExecuteTest, BasicTransactionTest) {
    std::vector<Catalog *> schemas;

    ParameterSet param(64*1024, 32*1024,64*1024, 8*10);

    std::unique_ptr<Catalog> table_catalog(new Catalog());
    uint8_t *key_bitmaps = new uint8_t[11];
    key_bitmaps[0]=1;
    for(int i=0; i<10; ++i){
        key_bitmaps[1]=0;
    }
    table_catalog->init("table0",11,key_bitmaps,8,1,false);
    table_catalog->add_col("Key", 8, "INTEGER");
    for(int i=0; i<10; ++i){
        std::string col_name = "filed";
        col_name.append(std::to_string(i));
        table_catalog->add_col(col_name.c_str(), 8, "INTEGER");
    }

    auto data_table = new TestDataTable(param, *table_catalog.get(),
                                        leaf_node_pool, inner_node_pool, conflict_buffer);

    //Log manager
    Catalog *log_catalog = new Catalog();
    log_catalog->table_id = 0;
    log_catalog->is_log = true;
    schemas.push_back(log_catalog);
    schemas.push_back(table_catalog.get());
    buf_mgr->Init(schemas);

    const int n_txns = 10;

    assert(data_table != nullptr);

    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            const char *id_ = reinterpret_cast<const char *>(&(i));
            InsertExecutor<const char *, Tuple> executor(data_table,
                                                          id_,
                                                       sizeof(uint64_t),
                                                         tuple,
                                                       txn_ctx,
                                                       buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            }
        }
        std::cout << "[Insert-Abort Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;

    }

    auto lookup_empty_check = [&]() {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            uint64_t key = i;
            bool point_lookup = true;
            bool acquire_owner = false;
            const char *id_ = reinterpret_cast<const char *>(&(key));
            IndexScanExecutor<const char *,  Tuple> executor(data_table,
                                                        sizeof(uint64_t),
                                                             id_,
                                                        10,
                                                        point_lookup,
                                                             nullptr,
                                                         false,
                                                        txn_ctx,
                                                        buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                auto res = executor.GetResults();
                assert(res.size() == 0);
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Empty-Lookup Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;
    };


    auto scan_check = [&]() {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            auto scan_processor = [](const Tuple & t, bool & should_end) -> void {
                std::cout << t.key << std::endl;
            };
            uint64_t key = 0;
            const char *id_ = reinterpret_cast<const char *>(&(key));
            TableScanExecutor<const char *, Tuple> executor(data_table,
//                                                        scan_processor,
                                                            id_,
                                                        sizeof(uint64_t),
                                                        10,
                                                        txn_ctx,
                                                        buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Scan-Table Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;
    };

    lookup_empty_check();

    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            const char *id_ = reinterpret_cast<const char *>(&(i));
            InsertExecutor<const char *, Tuple> executor(data_table,
                                                         id_,
                                                     sizeof(uint64_t),
                                                         tuple,
                                                     txn_ctx,
                                                     buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Insert Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;
    }

    scan_check();

    auto lookup_check = [&](int inc) {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            uint64_t key = i;
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i + inc;

            bool point_lookup = true;
            bool acquire_owner = false;
            const char *id_ = reinterpret_cast<const char *>(&(key));
            IndexScanExecutor<const char * , Tuple> executor(data_table,
                                                        sizeof(uint64_t),
                                                         id_,
                                                        10,
                                                        point_lookup,
                                                             nullptr,
                                                        false,
                                                        txn_ctx,
                                                        buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                auto res = executor.GetResults();
                assert(res.size() == 1);
                assert(std::memcmp(res[0]->values, tuple.values, sizeof(tuple.values)) == 0);
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Lookup Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;
    };

    lookup_check(0);
    cid_t snapshot_tid = SSNTransactionManager::GetInstance()->GetCurrentTidCounter() - 1;
    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            uint64_t key = i;
//            auto predicate = [key](const Tuple &t) -> bool {
//                return t.key == key;
//            };
            auto updater = [](Tuple &t) {
                for (int j = 0; j < 10; ++j)
                    t.values[j]++;
            };

            std::vector<oid_t> up_col;
            up_col.reserve(10);
            for (int p = 1; p < 11; ++p){
                up_col.push_back(p);
            }

            //update by key
            //update payload
            updater(tuple);
//            char *delta = reinterpret_cast<char *>(tuple.GetData());
            const char *id_ = reinterpret_cast<const char *>(&(key));
            PointUpdateExecutor<const char * , Tuple> executor(data_table,
                                                               id_,
                                                          sizeof(uint64_t),
                                                          reinterpret_cast<const char *>(tuple.values),
                                                          up_col,
                                                          sizeof(uint64_t)*10,
                                                          false,
                                                          txn_ctx,
                                                          buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Update Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;
    }

    lookup_check(1);

    //read old txn updates
    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            // read the versions before the updates
            txn_ctx->SetCommitId(snapshot_tid);
            txn_ctx->SetTxnId(snapshot_tid);
            txn_ctx->SetReadId(snapshot_tid);
            uint64_t key = i;
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            bool point_lookup = true;
            bool acquire_owner = false;
            const char *id_ = reinterpret_cast<const char *>(&(key));
            IndexScanExecutor<const char * , Tuple> executor(data_table,
                                                        sizeof(uint64_t),
                                                             id_,
                                                        10,
                                                        point_lookup,
                                                             nullptr,
                                                        false,
                                                        txn_ctx,
                                                        buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                auto res = executor.GetResults();
                assert(res.size() == 1);
                assert(std::memcmp(res[0]->values, tuple.values, sizeof(tuple.values)) == 0);
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Lookup-Old Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns
                  << std::endl;
    }

    //duplicate update
    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            uint64_t key = i;
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

//            uint64_t key = i;
//            auto predicate = [key](const Tuple &t) -> bool {
//                return t.key == key;
//            };
            auto updater = [](Tuple &t) {
                for (int j = 0; j < 10; ++j)
                    t.values[j]++;
            };
            std::vector<oid_t> up_col;
            for (int p = 0; p < 10; ++p)
                up_col.push_back(1);

            updater(tuple);
//            char *delta = reinterpret_cast<char *>(tuple.GetData());
            const char *id_ = reinterpret_cast<const char *>(&(key));
            PointUpdateExecutor<const char * , Tuple> executor(data_table,
                                                           id_,
                                                          sizeof(uint64_t),
                                                          reinterpret_cast<const char *>(tuple.values),
                                                          up_col,
                                                          sizeof(uint64_t)*10,
                                                          false,
                                                          txn_ctx,
                                                          buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            }
        }
        std::cout << "[Update Transactions Abort] Committed " << committed_txns
                  << ", Aborted " << aborted_txns
                  << std::endl;
    }

    lookup_check(1);

    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_ctx = transaction_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
            uint64_t key = i;
//            auto predicate = [key](const Tuple &t) -> bool {
//                return t.key == key;
//            };
            const char *id_ = reinterpret_cast<const char *>(&(key));
            PointDeleteExecutor<const char * , Tuple> executor(data_table,
                                                           id_,
                                                           sizeof(uint64_t),
                                                          false,
                                                          txn_ctx,
                                                          buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Delete Transactions] Committed " << committed_txns
                  << ", Aborted " << aborted_txns << std::endl;
    }

    lookup_empty_check();


    delete data_table;
    delete[] key_bitmaps;
    auto t_c = table_catalog.release();
    delete t_c;

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, AbortVersionChainTest) {

        SSNTransactionManager::ClearInstance();

    TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                               inner_node_pool, conflict_buffer);
    auto txn_manager = SSNTransactionManager::GetInstance();
    txn_manager->Init(conflict_buffer);

    {
        TransactionScheduler scheduler(2, table, txn_manager, buf_mgr);
        scheduler.Txn(0).Update(1, 100,false);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(1,false);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(scheduler.schedules[1].results[0] == 0);

    }

    {
        TransactionScheduler scheduler(2, table, txn_manager, buf_mgr);
        scheduler.Txn(0).Insert(100, 0);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(100,false);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(scheduler.schedules[1].results[0] == -1);
    }

    delete table;

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, SingleTransactionTest) {

        SSNTransactionManager::ClearInstance();


    TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                               inner_node_pool, conflict_buffer);
    auto txn_manager = SSNTransactionManager::GetInstance();
    txn_manager->Init(conflict_buffer);
//    auto txn_conxt = txn_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
    // update, update, update, update, read
    {
        TransactionScheduler scheduler(1, table, txn_manager, buf_mgr);
        scheduler.Txn(0).Update(0, 1, false);
        scheduler.Txn(0).Update(0, 2, true);
        scheduler.Txn(0).Update(0, 3, true);
        scheduler.Txn(0).Update(0, 4, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(4 == scheduler.schedules[0].results[0]);
    }

    // delete not exist, delete exist, read deleted, update deleted,
    // read deleted, insert back, update inserted, read newly updated,
    // delete inserted, read deleted
    {
        TransactionScheduler scheduler(1, table, txn_manager, buf_mgr);
        scheduler.Txn(0).Delete(100, false);
        scheduler.Txn(0).Delete(0, false);
        scheduler.Txn(0).Read(0, true);

        scheduler.Txn(0).Update(0, 1, false);
        scheduler.Txn(0).Read(0, true);

        scheduler.Txn(0).Insert(0, 2);
        scheduler.Txn(0).Update(0, 3, true);
        scheduler.Txn(0).Read(0, true);

        scheduler.Txn(0).Delete(0, true);//update before this delete
        scheduler.Txn(0).Read(0, true);

        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(-1 == scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[0].results[1]);
        assert(3 == scheduler.schedules[0].results[2]);
        assert(-1 == scheduler.schedules[0].results[3]);
        LOG_INFO("FINISH THIS");
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read
    // updated
    {
        TransactionScheduler scheduler(1, table, txn_manager, buf_mgr);

        scheduler.Txn(0).Insert(1000, 0);
        scheduler.Txn(0).Delete(1000, true);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Insert(1000, 1);
        scheduler.Txn(0).Delete(1000, true);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Insert(1000, 2);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Update(1000, 3, true);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(-1 == scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[0].results[1]);
        assert(2 == scheduler.schedules[0].results[2]);
        assert(3 == scheduler.schedules[0].results[3]);
    }

    delete table;

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, SingleTransactionTest2) {

    // Just scan the table
    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance( );
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Scan(-1);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(10 == scheduler.schedules[0].results.size());

        delete table;
    }

    // read, read, read, read, update, read, read not exist
    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Read(100,false);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS ==scheduler.schedules[0].txn_result);
        assert(0 == scheduler.schedules[0].results[0]);
        assert(0 == scheduler.schedules[0].results[1]);
        assert(0 == scheduler.schedules[0].results[2]);
        assert(0 == scheduler.schedules[0].results[3]);
        assert(1 == scheduler.schedules[0].results[4]);
        assert(-1 ==scheduler.schedules[0].results[5]);


        delete table;
    }

    // update, update, update, update, read
    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Update(0, 2,true);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Update(0, 3,true);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Update(0, 4,true);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS ==scheduler.schedules[0].txn_result);
        assert(1  == scheduler.schedules[0].results[0]);
        assert(2  == scheduler.schedules[0].results[1]);
        assert(3  == scheduler.schedules[0].results[2]);
        assert(4  == scheduler.schedules[0].results[3]);

        delete table;
    }

    // delete not exist, delete exist, read deleted, update deleted,
    // read deleted, insert back, update inserted, read newly updated,
    // delete inserted, read deleted
    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Delete(100);
        scheduler.Txn(0).Delete(0,false);
        scheduler.Txn(0).Read(0,true);

        scheduler.Txn(0).Update(0, 1,true);
        scheduler.Txn(0).Read(0,true);

        scheduler.Txn(0).Insert(0, 2);
        scheduler.Txn(0).Update(0, 3,true);
        scheduler.Txn(0).Read(0,true);

        scheduler.Txn(0).Delete(0,true);
        scheduler.Txn(0).Read(0,true);

        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(-1 == scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[0].results[1]);
        assert(3 == scheduler.schedules[0].results[2]);
        assert(-1 == scheduler.schedules[0].results[3]);

        delete table;
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read updated
    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();

        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);
        txn_manager->Init(conflict_buffer);

        scheduler.Txn(0).Insert(1000, 0);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Delete(1000,true);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Insert(1000, 1);
        scheduler.Txn(0).Delete(1000,true);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Insert(1000, 2);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Update(1000, 3,true);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(0 == scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[0].results[1]);
        assert(-1 == scheduler.schedules[0].results[2]);
        assert(2 ==scheduler.schedules[0].results[3]);
        assert(3 ==scheduler.schedules[0].results[4]);


        delete table;
    }

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, ConcurrentTransactionTest) {

    {

        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(2, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Insert(100, 1);
        scheduler.Txn(1).Read(100,false);
        scheduler.Txn(0).Read(100, true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(100,false);
        scheduler.Txn(1).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS ==scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS ==scheduler.schedules[1].txn_result);

        assert(1 ==scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[1].results[0]);
        // TODO: phantom problem.
        // In fact, txn 1 should not see the inserted tuple.
//        assert(1 == scheduler.schedules[1].results[1]);
        assert(-1 == scheduler.schedules[1].results[1]);

        delete table;
    }

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(2, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Read(0,false);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(0,false);
        scheduler.Txn(1).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
//        assert(ResultType::ABORTED ==scheduler.schedules[1].txn_result);
        assert(ResultType::SUCCESS ==scheduler.schedules[1].txn_result);

        assert(1 == scheduler.schedules[0].results[0]);
        //txn1 [0] can not read
        //txn1 [1] read the old version 0, because it can not see the new version
        assert(0 == scheduler.schedules[1].results[0]);
        assert(0 == scheduler.schedules[1].results[1]);

        delete table;
    }
    LOG_INFO("Done");
}

TEST_F(ExecuteTest, MultiTransactionTest) {

        SSNTransactionManager::ClearInstance();


    TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                               inner_node_pool, conflict_buffer);
    auto txn_manager = SSNTransactionManager::GetInstance();
    txn_manager->Init(conflict_buffer);
    // Txn 0: scan + select for update
    // Txn 1: scan
    // Txn 1: commit (should failed for timestamp ordering cc)
    // Txn 2: Scan + select for update
    // Txn 2: commit (should failed)
    // Txn 0: commit (success)
    {
        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Scan(-1, true);
        scheduler.Txn(1).Scan(-1);
        scheduler.Txn(1).Commit();
        scheduler.Txn(2).Scan(-1, true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(2).Commit();

        scheduler.Run();
        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        //TODO: can not scan for update
//        assert(ResultType::ABORTED == scheduler.schedules[1].txn_result);
//        assert(ResultType::ABORTED == scheduler.schedules[2].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[2].txn_result);

        assert(10 == scheduler.schedules[0].results.size());
    }

    // Txn 0: scan + select for update
    // Txn 0: abort
    // Txn 1: Scan + select for update
    // Txn 1: commit (should success)
    {
        TransactionScheduler scheduler(2, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Scan(-1, true);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Scan(-1, true);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        //TODO: can not scan for update
//        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);

        assert(10 == scheduler.schedules[1].results.size());
    }

    // Txn 0: read + select for update
    // Txn 0: abort
    // Txn 1: read + select for update
    // Txn 1: commit (should success)
    {
        TransactionScheduler scheduler(2, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Read(0, false);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(0, false);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);

        assert(1 == scheduler.schedules[1].results.size());
    }

    // read, read, read, read, update, read, read not exist
    // another txn read
    {
        TransactionScheduler scheduler(2, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0, false);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(0).Read(0,true);
        scheduler.Txn(0).Read(100, true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(0, false);
        scheduler.Txn(1).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
        assert(0 == scheduler.schedules[0].results[0]);
        assert(0 == scheduler.schedules[0].results[1]);
        assert(0 == scheduler.schedules[0].results[2]);
        assert(0 == scheduler.schedules[0].results[3]);
        assert(1 == scheduler.schedules[0].results[4]);
        assert(-1 == scheduler.schedules[0].results[5]);
        assert(1 == scheduler.schedules[1].results[0]);
    }


    {
        // Test commit/abort protocol when part of the read-own tuples get updated.
        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Read(3, false);
        scheduler.Txn(0).Read(4, false);
        scheduler.Txn(0).Update(3, 1,false);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(3, false);
        scheduler.Txn(1).Read(4, false);
        scheduler.Txn(1).Update(3, 2,false);
        scheduler.Txn(1).Commit();
        scheduler.Txn(2).Read(3,false);
        scheduler.Txn(2).Read(4,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[2].txn_result);

        assert(0 == scheduler.schedules[1].results[0]);
        assert(0 == scheduler.schedules[1].results[1]);
        assert(2 == scheduler.schedules[2].results[0]);
        assert(0 == scheduler.schedules[2].results[1]);
    }


    delete table;
    LOG_INFO("Done");
}


TEST_F(ExecuteTest, DirtyWriteTest) {

    {
        SSNTransactionManager::ClearInstance();

        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);

        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager, buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T0 commits
        // T1 commits
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Update(0, 2,false);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        assert(schedules[0].txn_result == ResultType::SUCCESS);
        assert(schedules[1].txn_result == ResultType::ABORTED);
        assert(1 == scheduler.schedules[2].results[0]);

        schedules.clear();


        delete table;
    }

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T1 commits
        // T0 commits
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Update(0, 2,false);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        assert(schedules[0].txn_result == ResultType::SUCCESS);
        assert(schedules[1].txn_result == ResultType::ABORTED);
        assert(1 == scheduler.schedules[2].results[0]);

        schedules.clear();

        delete table;
    }

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T0 aborts
        // T1 commits
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Update(0, 2,false);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        //if (conflict == ConflictAvoidanceType::ABORT) {
        assert(schedules[0].txn_result == ResultType::ABORTED);
        assert(schedules[1].txn_result == ResultType::ABORTED);

        assert(0 == scheduler.schedules[2].results[0]);
        //}

        schedules.clear();
//        delete &table->GetPrimaryIndex();
//        delete &table->GetVersionTable();
        delete table;
    }

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T1 commits
        // T0 aborts
        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Update(0, 2,false);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Abort();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        assert(schedules[0].txn_result == ResultType::ABORTED);
        assert(schedules[1].txn_result == ResultType::ABORTED);

        assert(0 == scheduler.schedules[2].results[0]);

        schedules.clear();

        delete table;
    }


    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T0 aborts
        // T1 aborts
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Update(0, 2,false);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Abort();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        assert(schedules[0].txn_result == ResultType::ABORTED);
        assert(schedules[1].txn_result == ResultType::ABORTED);

        assert(0 == scheduler.schedules[2].results[0]);

        schedules.clear();
//        delete &table->GetPrimaryIndex();
//        delete &table->GetVersionTable();
        delete table;
    }


    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T1 aborts
        // T0 aborts
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Update(0, 2,false);
        scheduler.Txn(1).Abort();
        scheduler.Txn(0).Abort();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        assert(schedules[0].txn_result == ResultType::ABORTED);
        assert(schedules[1].txn_result == ResultType::ABORTED);

        assert(0 == scheduler.schedules[2].results[0]);


        schedules.clear();

        delete table;
    }


    LOG_INFO("Done");
}

TEST_F(ExecuteTest, DirtyReadTest) {

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 reads (0, ?)
        // T0 commits
        // T1 commits
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Read(0,false);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        assert(schedules[0].txn_result == ResultType::SUCCESS);

//        assert(schedules[1].txn_result == ResultType::ABORTED);
        assert(schedules[1].txn_result == ResultType::SUCCESS);
        //SERIALIZABLE, txn[1] read the old value
        assert(0 ==scheduler.schedules[1].results[0]);
        assert(1 ==scheduler.schedules[2].results[0]);



        delete table;
    }

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 reads (0, ?)
        // T0 aborts
        // T1 commits
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(1).Read(0,false);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        assert(schedules[0].txn_result == ResultType::ABORTED);
//        assert(schedules[1].txn_result == ResultType::ABORTED);
        //snapshot, so txn[1] can read the old value
        assert(schedules[1].txn_result == ResultType::SUCCESS);
        assert(0 ==scheduler.schedules[1].results[0]);
        assert(0 ==scheduler.schedules[2].results[0]);

        schedules.clear();

        delete table;
    }

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, FuzzyReadTest) {

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T0 obtains a smaller timestamp.
        // T0 reads (0, ?)
        // T1 updates (0, ?) to (0, 1)
        // T1 commits
        // T0 reads (0, ?)
        // T0 commits
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(1).Update(0, 1,false);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Read(0,false); // Should read old version
        scheduler.Txn(0).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        assert(schedules[0].txn_result == ResultType::SUCCESS);
        assert(schedules[1].txn_result == ResultType::SUCCESS);

        assert(0 == scheduler.schedules[0].results[0]);
        assert(0 == scheduler.schedules[0].results[1]);

        assert(1 == scheduler.schedules[2].results[0]);



        delete table;
    }

    {
        SSNTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                                   inner_node_pool, conflict_buffer);
        auto txn_manager = SSNTransactionManager::GetInstance();
        txn_manager->Init(conflict_buffer);

        TransactionScheduler scheduler(3, table, txn_manager,buf_mgr);
        // T1 obtains a smaller timestamp.
        // T1 reads (0, ?)
        // T0 reads (0, ?)
        // T1 updates (0, ?) to (0, 1)
        // T1 commits
        // T0 reads (0, ?)
        // T0 commits
        scheduler.Txn(1).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(1).Update(0, 1,false);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Commit();

        // observer
        scheduler.Txn(2).Read(0,false);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        assert(schedules[0].txn_result == ResultType::SUCCESS);
//        assert(schedules[1].txn_result == ResultType::ABORTED);
        //snapshot, txn[1] can update
        assert(schedules[1].txn_result == ResultType::SUCCESS);

        assert(0 == scheduler.schedules[0].results[0]);
        assert(0 == scheduler.schedules[0].results[1]);
        assert(0 == scheduler.schedules[1].results[0]);

//        assert(0 == scheduler.schedules[2].results[0]);
        assert(1 == scheduler.schedules[2].results[0]);

        schedules.clear();

        delete table;
    }

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, MVCCTest) {

        SSNTransactionManager::ClearInstance();
    TestDataTable *table = TestingTransactionUtil::CreateTable(10, buf_mgr, leaf_node_pool,
                                                               inner_node_pool, conflict_buffer);
    auto txn_manager = SSNTransactionManager::GetInstance();
    txn_manager->Init(conflict_buffer);

    // read, read, read, read, update, read, read not exist
    // another txn read
    {
        TransactionScheduler scheduler(2, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Read(0,false);
        scheduler.Txn(0).Update(0, 1,false);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Read(100, true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(0,false);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(scheduler.schedules[0].results[0] == 0);
        assert(scheduler.schedules[0].results[1] == 0);
        assert(scheduler.schedules[0].results[2] == 0);
        assert(scheduler.schedules[0].results[3] == 0);
        assert(scheduler.schedules[0].results[4] == 1);
        assert(scheduler.schedules[0].results[5] == -1);
        assert(scheduler.schedules[1].results[0] == 1);
    }

    // update, update, update, update, read
    {
        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);
        scheduler.Txn(0).Update(0, 1, false);
        scheduler.Txn(0).Update(0, 2, true);
        scheduler.Txn(0).Update(0, 3, true);
        scheduler.Txn(0).Update(0, 4, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Commit();
        scheduler.Run();
        assert(scheduler.schedules[0].results[0] == 4);
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read updated
    {
        TransactionScheduler scheduler(1, table, txn_manager,buf_mgr);

        scheduler.Txn(0).Insert(1000, 0);
        scheduler.Txn(0).Delete(1000,true);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Insert(1000, 1);
        scheduler.Txn(0).Delete(1000,true);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Insert(1000, 2);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Update(1000, 3,true);
        scheduler.Txn(0).Read(1000,true);

        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(scheduler.schedules[0].results[0] == -1);
        assert(scheduler.schedules[0].results[1] == -1);
        assert(scheduler.schedules[0].results[2] == 2);
        assert(scheduler.schedules[0].results[3] == 3);
    }


    delete table;

    LOG_INFO("Done");
}

TEST_F(ExecuteTest, StressTest) {

        SSNTransactionManager::ClearInstance();

    const int num_txn = 10;  // 5
    const int scale = 3;    // 20
    const int num_key = 256;  // 256
    srand(15721);

    TestDataTable *table = TestingTransactionUtil::CreateTable(num_key, buf_mgr, leaf_node_pool,
                                                               inner_node_pool, conflict_buffer);
    auto txn_manager = SSNTransactionManager::GetInstance();
    txn_manager->Init(conflict_buffer);

    TransactionScheduler scheduler(num_txn, table, txn_manager,buf_mgr);
    scheduler.SetConcurrent(true);
    for (int i = 0; i < num_txn; i++) {
        for (int j = 0; j < scale; j++) {
            // randomly select two unique keys
            int key1 = rand() % num_key;
            int key2 = rand() % num_key;
            int delta = rand() % 1000;
            // Store subtracted value
            scheduler.Txn(i).ReadStore(key1, -delta );
            scheduler.Txn(i).Update(key1, TXN_STORED_VALUE);
            LOG_INFO("Txn %d deducts %d from %d", i, delta, key1);
            // Store increased value
            scheduler.Txn(i).ReadStore(key2, delta );
            scheduler.Txn(i).Update(key2, TXN_STORED_VALUE);
            LOG_INFO("Txn %d adds %d to %d", i, delta, key2);

        }
        scheduler.Txn(i).Commit();
    }
    scheduler.Run();

    // Read all values
    TransactionScheduler scheduler2(1, table, txn_manager,buf_mgr);
    for (int i = 0; i < num_key; i++) {
        scheduler2.Txn(0).Read(i);
    }
    scheduler2.Txn(0).Commit();
    scheduler2.Run();

    assert(ResultType::SUCCESS ==scheduler2.schedules[0].txn_result);
    // The sum should be zero
    int sum = 0;
    for (auto result : scheduler2.schedules[0].results) {
        LOG_INFO("Table has tuple value: %d", result);
        sum += result;
    }

    assert(0 == sum);

    // stats
    int nabort = 0;
    for (auto &schedule : scheduler.schedules) {
        if (schedule.txn_result == ResultType::ABORTED) nabort += 1;
    }
    LOG_INFO("Abort: %d out of %d", nabort, num_txn);


    delete table;
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();

}


}//test
}//namespace



