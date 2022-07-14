//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.cpp
//
// Identification: test/concurrency/transaction_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "testing_transaction_util.h"


namespace mvstore {
namespace test {

sync::ThreadPool TransactionScheduler::tp(16);

TestDataTable *TestingTransactionUtil::CreateTable(int num_key, VersionStore *buf_mgr,
                                                   DramBlockPool *leaf_node_pool,
                                                   InnerNodeBuffer *inner_node_pool,
                                                   EphemeralPool *conflict_buffer) {
    //buf_mgr, for redo buffer. log record, old versions
    assert(buf_mgr != nullptr);

    std::vector<Catalog *> schemas;

//    ParameterSet param(3072, 1024,4096 , 8);
    ParameterSet param(64*1024, 32*1024,64*1024, 8);

    std::unique_ptr<Catalog> table_catalog(new Catalog());
    uint8_t *key_bitmaps = new uint8_t[2];
    key_bitmaps[0]=1;
    key_bitmaps[1]=0;
//    for(int i=0; i<10; ++i){
//        key_bitmaps[1]=0;
//    }
    table_catalog->init("table0",2,key_bitmaps,8,1,false);
    table_catalog->add_col("Key", 8, "INTEGER");
    table_catalog->add_col("filed0", 8, "INTEGER");
//    for(int i=0; i<10; ++i){
//        std::string col_name = "filed";
//        col_name.append(std::to_string(i));
//        table_catalog->add_col(col_name.c_str(), 8, "INTEGER");
//    }

    auto table = new TestDataTable(param, *table_catalog.get(),
                                   leaf_node_pool, inner_node_pool, conflict_buffer);

    //Log manager
    Catalog *log_catalog = new Catalog();
    log_catalog->table_id = 0;
    log_catalog->is_log = true;
    schemas.push_back(log_catalog);
    schemas.push_back(table_catalog.get());
    buf_mgr->Init(schemas);

    auto txn_manager = SSNTransactionManager::GetInstance();
    assert(txn_manager != nullptr);

    auto txn_ctx = txn_manager->BeginTransaction(IsolationLevelType::SERIALIZABLE);
    for (int i = 0; i < num_key; ++i) {
        ExecuteInsert(txn_ctx, table, i, 0, buf_mgr);
    }
    ResultType res = txn_manager->CommitTransaction(txn_ctx);
    assert(res == ResultType::SUCCESS);
    return table;
}

bool TestingTransactionUtil::ExecuteInsert(
        TransactionContext *transaction, TestDataTable *table,
        int id, int value, VersionStore *buf_mgr) {

    TestTuple t;
    t.key =  id;
    t.value = value;

    uint64_t k = id;
//    BaseTuple b_tuple(&table->schema);
//    b_tuple.SetValue(0, reinterpret_cast<const char *>(t.value));
    const char *id_ = reinterpret_cast<const char *>(&(k));
    InsertExecutor<const char * , TestTuple> executor(table,
                                                      id_,
                                                     sizeof(uint64_t),
                                                     t,
                                                     transaction,
                                                     buf_mgr);

    return executor.Execute();
}


bool TestingTransactionUtil::ExecuteRead(
        TransactionContext *transaction, TestDataTable *table,
        int id, int &result, bool select_for_update, VersionStore *buf_mgr) {

    bool point_lookup = true;
    uint64_t k = id;
    const char *id_ = reinterpret_cast<const char *>(&(k));
    IndexScanExecutor<const char * , TestTuple> read_executor(table,
                                                         sizeof(uint64_t),
                                                          id_,
                                                         10,
                                                         point_lookup,
                                                              nullptr,
                                                         select_for_update,
                                                         transaction,
                                                         buf_mgr);
    if (read_executor.Execute() == false)
        return false;
    auto res = read_executor.GetResults();
    if (res.empty()) {
        result = -1;
    } else {
        assert(res.size() == 1);
        result = res[0]->value;
    }
    return true;
}

bool TestingTransactionUtil::ExecuteDelete(
        TransactionContext *transaction, TestDataTable *table,
        int id, bool select_for_update, VersionStore *buf_mgr) {
    uint64_t k = id;
    const char *id_ = reinterpret_cast<const char *>(&(k));
    PointDeleteExecutor<const char * , TestTuple> delete_executor(table,
                                                              id_,
                                                              sizeof(uint64_t),
                                                                select_for_update,
//                                                             [id](const TestTuple &t) { return t.key == id; },
                                                             transaction,
                                                             buf_mgr);

    return delete_executor.Execute();
}

bool TestingTransactionUtil::ExecuteUpdate(
        TransactionContext *transaction, TestDataTable *table,
        int id, int value, bool select_for_update, VersionStore *buf_mgr) {
    bool point_update = true;
    std::vector<oid_t> up_col;
    up_col.push_back(1);
//    char *delta = reinterpret_cast<char *>(value);
    uint64_t k = id;
    uint64_t delta = value;
    const char *id_ = reinterpret_cast<const char *>(&(k));
    PointUpdateExecutor<const char * , TestTuple> update_executor(table,
                                                              id_,
                                                             sizeof(uint64_t),
                                                              reinterpret_cast<const char *>(&delta),
                                                             up_col,
                                                             sizeof(uint64_t),
                                                              select_for_update,
                                                             transaction,
                                                             buf_mgr);

    return update_executor.Execute();
}

//bool TestingTransactionUtil::ExecuteUpdateByValue(
//        TransactionContext *transaction, TestDataTable *table,
//        int old_value, int new_value, bool select_for_update, VersionStore *buf_mgr) {
//    bool point_update = false;
//    ScanUpdateExecutor<uint64_t, TestTuple> update_executor(*table, (uint64_t) 0,
//                                                            [old_value](const TestTuple &t) {
//                                                                return t.value == old_value;
//                                                            },
//                                                            [new_value](TestTuple &t) { t.value = new_value; },
//                                                            transaction,
//                                                            buf_mgr);
//
//    return update_executor.Execute();
//}

bool TestingTransactionUtil::ExecuteScan(
        TransactionContext *transaction, std::vector<int> &results,
        TestDataTable *table, int id, bool select_for_update, VersionStore *buf_mgr) {
    bool point_lookup = false;
    // where t.id >= id
    const char *id_ = reinterpret_cast<const char *>(&(id));
    TableScanExecutor<const char * , TestTuple> scan_executor(table,
//                                                        scan_processor,
                                                          id_,
                                                 sizeof(uint64_t),
                                                 20,
                                                 transaction,
                                                 buf_mgr);

    if (scan_executor.Execute() == false)
        return false;

    auto res = scan_executor.GetResults();

    for (int i = 0; i < res.size(); ++i) {
        results.push_back(res[i].value);
    }
    return true;
}
}
}
