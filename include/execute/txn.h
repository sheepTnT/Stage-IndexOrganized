//
// Created by zhangqian on 2021/10/29.
//

#ifndef MVSTORE_TXN_H_
#define MVSTORE_TXN_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include "../vstore/version_store.h"
#include "txn_context.h"

namespace mvstore {

/**
* @brief      Class for (Repeatable read + SSN) transaction manager.
*/
class SSNTransactionManager {
public:

    SSNTransactionManager() {}

    /**
     * @brief      Destroys the object.
     */
    ~SSNTransactionManager() {}

    /**
     * @brief Returns the minimum id(min_tid) read_id of active transactions in the system.
     * This id is used in garbage collection of txn info ,retired versions and overwritten versions.
     * All tuples whose end_ts < min_tid are considered garbage and can be safely
     * purged from the system.
     * @return The minimum active transaction id in the system.
     */
    cid_t MinActiveTID();

    TransactionContext *GetTransactionContext(const txn_id_t tid);

    bool EraseTid(const txn_id_t tid);

    cid_t FindMaxPstamp(TransactionContext *const current_txn);

    cid_t FindMinSstamp(TransactionContext *const current_txn);

    /**
     * @brief      Gets the instance.
     * @return     The instance.
     */
    static SSNTransactionManager *GetInstance();

    static void ClearInstance();


    Status Init(EphemeralPool *buffer_pool) {
        if (inited == false) {
            overwritten_buffer_pool = buffer_pool;
        }
        inited = true;
        return Status::OK();
    }

    /**
     * Test whether the current transaction is the owner of this tuple.
     *
     * @param      current_txn        The current transaction
     * @param[in]  accessor           The record meta
     *
     * @return     True if owner, False otherwise.
     */
    virtual bool IsOwner(TransactionContext *const current_txn,
                         RecordMeta &accessor);

    /**
     * @param      current_txn        The current transaction
     * @param[in]  location           The location
     */
    virtual bool PerformInsert(TransactionContext *const current_txn,
                               RecordMeta &location);

    virtual bool PerformRead(TransactionContext *const current_txn,
                             RecordMeta &location,
                             cid_t cstamp);

    virtual bool PerformUpdate(TransactionContext *const current_txn,
                               RecordMeta &old_location,
                               TupleHeader *new_location);

    virtual bool PerformDelete(TransactionContext *const current_txn,
                               RecordMeta &meta_location);

    /**
     * @brief      Sets the transaction result.
     * @param      current_txn  The current transaction
     * @param[in]  result       The result
     */
    void SetTransactionResult(TransactionContext *const current_txn, const ResultType result) {
        current_txn->SetResult(result);
    }

    TransactionContext *BeginTransaction(const IsolationLevelType type) {
        return BeginTransaction(0, type);
    }

    TransactionContext *BeginTransaction(const size_t thread_id = 0,
                                         const IsolationLevelType type = isolation_level_);

    txn_id_t GetCurrentTidCounter();
    txn_id_t GetNextCurrentTidCounter();
    void CleanTxnOverwrittenBuffer(TransactionContext *const current_txn);
    /**
     * @brief      Ends a transaction.
     * @param      current_txn  The current transaction
     */
    void EndTransaction(TransactionContext *current_txn);

    virtual ResultType CommitTransaction(TransactionContext *const current_txn);

    virtual ResultType AbortTransaction(TransactionContext *const current_txn);

    IsolationLevelType GetIsolationLevel() {
        return isolation_level_;
    }

    void SetTaskCallback(void (*task_callback)(void *), void *task_callback_arg) {
        task_callback_ = task_callback;
        task_callback_arg_ = task_callback_arg;
    }

    void (*task_callback_)(void *);
    void *task_callback_arg_;


private:
    static IsolationLevelType isolation_level_;
    bool inited = false;
    //transaction undo buffer pool
    //hold the overeritten record versions
    EphemeralPool *overwritten_buffer_pool;
    SpinLatch latch_;

};

}

#endif