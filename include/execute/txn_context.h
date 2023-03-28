//
// Created by zhangqian on 2021/10/29.
//

#ifndef MVSTORE_TXN_CONTEXT_H_
#define MVSTORE_TXN_CONTEXT_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include "../vstore/record_meta.h"

namespace mvstore {

struct RecordMetaHasher {
    size_t operator()(const RecordMeta &item) const {
        return std::hash<uint64_t>()(item.meta_data.meta);
    }

};
using callback_fn = void (*)(void *);
//record the read write version cstamp
typedef tbb::concurrent_unordered_map<RecordMeta, RWType, RecordMetaHasher> ReadWriteSet;

/**
* @brief      Class for transaction context.
*/
class TransactionContext {
    TransactionContext(TransactionContext const &) = delete;

public:
    TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                       const cid_t &read_id);

    TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                       const cid_t &read_id, const cid_t &commit_id);

    TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                       const cid_t &read_id, const cid_t &commit_id,
                       const size_t read_write_set_size);

    /**
     * @brief      Destroys the object.
     */
    ~TransactionContext();

private:
    void Init(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id) {
        Init(thread_id, isolation, read_id, read_id);
    }

    void Init(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id, const cid_t &commit_id);

public:

    /**
     * @brief      Gets the thread identifier.
     * @return     The thread identifier.
     */
    inline size_t GetThreadId() const { return thread_id_; }

    /**
     * @brief      Gets the transaction identifier.
     * @return     The transaction identifier.
     */
    inline txn_id_t GetTransactionId() const { return txn_id_; }


    inline cid_t GetReadId() const { return read_id_; }

    /**
     * @brief      Gets the commit identifier.
     *
     * @return     The commit identifier.
     */
    inline cid_t GetCommitId() const { return commit_id_; }

    inline void SetCommitId(const cid_t commit_id) { commit_id_ = commit_id; }

    inline void SetSuccessor(const cid_t sstamp) {
        auto sstamp_ = std::min(successor.load(std::memory_order_relaxed), sstamp);
        successor.store(sstamp_,std::memory_order_relaxed);
    }

    inline cid_t GetSuccessor() const {
        return successor.load(std::memory_order_relaxed);
    }

    inline void SetPredecessor(const cid_t pstamp)
    {
        auto pstamp_ = std::max(predecessor.load(std::memory_order_relaxed), pstamp);
        predecessor.store(pstamp_, std::memory_order_relaxed);
    }

    inline cid_t GetPredecessor() const
    {
        return predecessor.load(std::memory_order_relaxed);
    }

    inline void SetTxnId(const txn_id_t txn_id) { txn_id_ = txn_id; }

    inline void SetReadId(const cid_t read_id) { read_id_ = read_id; }

    bool RecordRead(const RecordMeta &);

    bool RecordUpdate(const RecordMeta &);

    void RecordInsert(const RecordMeta &);

    void SetFinish()
    {
        txn_cnt_lock.Lock();
        is_finished_ = true;
        txn_cnt_lock.Unlock();
    }

    bool IsFinished()
    {
        bool ret = false;
        txn_cnt_lock.Lock();
        ret = is_finished_;
        txn_cnt_lock.Unlock();
        return ret;
    }

    void SetAbort(){
        txn_cnt_lock.Lock();
        is_aborted_ = true;
        txn_cnt_lock.Unlock();
    }

    bool IsAborted(){
        bool ret = false;
        txn_cnt_lock.Lock();
        ret = is_aborted_;
        txn_cnt_lock.Unlock();
        return ret;
    }

    void SetCommitting(){
        txn_cnt_lock.Lock();
        is_committing_ = true;
        txn_cnt_lock.Unlock();
    }

    bool IsCommitting(){
        bool ret = false;
        txn_cnt_lock.Lock();
        ret = is_committing_;
        txn_cnt_lock.Unlock();
        return ret;
    }

    inline bool CheckExclusion(){
        auto ps = predecessor.load(std::memory_order_relaxed);
        auto ss = successor.load(std::memory_order_relaxed);
        if(ss <= ps){
            return false;
        }
        return true;
    }

    bool RecordDelete(const RecordMeta &);

    RWType GetRWType(const RecordMeta &location);

    bool IsInRWSet(const RecordMeta &location) {
        return rw_set_.find(location) != rw_set_.end();
    }


    inline const ReadWriteSet &GetReadWriteSet() { return rw_set_; }

    /**
     * @brief      Get a string representation for debugging.
     *
     * @return     The information.
     */
    const std::string GetInfo() const;

    /**
     * Set result and status.
     * @param[in]  result  The result
     */
    inline void SetResult(ResultType result) { result_ = result; }

    /**
     * Get result and status.
     * @return     The result.
     */
    inline ResultType GetResult() const { return result_; }

    inline bool IsReadOnly() const {
        return !is_written_ ;
    }

    inline void SetReadOnly() {
        is_written_ = false;
    }

    inline bool IsLoading() const {
        return is_loading_ ;
    }

    inline void SetIsLoading() {
        is_loading_ = true;
    }

    inline void SetLastLogLsn(oid_t table_id, LSN_T lsn, RWType wr_type) {
//        last_log_rec_lsn = lsn;
        if (last_log_rec_lsn.find(table_id) != last_log_rec_lsn.end()){
            last_log_rec_lsn[table_id] = std::make_pair(wr_type,lsn);
        } else{
            last_log_rec_lsn.insert(std::make_pair(table_id,std::make_pair(wr_type,lsn)));
        }
    }

    inline LSN_T GetLastLogLsn(oid_t table_id){
        LSN_T ret = InvalidLSN;
        auto fnd = last_log_rec_lsn.find(table_id);
        if (fnd != last_log_rec_lsn.end()){
            ret = fnd->second.second;
        }
        return ret;
    }
    /**
     * @brief      Gets the isolation level.
     * @return     The isolation level.
     */
    inline IsolationLevelType GetIsolationLevel() const {
        return isolation_level_;
    }

    inline void LogRecordToBuffer(LogRecord *l_rd)  {
        log_record_buffer.emplace_back(l_rd);
    }
    inline std::vector<LogRecord *> &GetLogRecords(){
        return log_record_buffer;
    }
    inline void ClearLogRecords(){
        log_record_buffer.clear();
    }
    inline void BufferOverwrittenVersion(uint64_t location){
        txn_cnt_lock.Lock();
        overwritten_buffer.emplace_back(location);
        txn_cnt_lock.Unlock();
    }
    inline std::vector<uint64_t> &GetOverwrittenBuffer(){
        return overwritten_buffer;
    }

private:
    /** transaction id */
    txn_id_t txn_id_;

    /** id of thread creating this transaction */
    size_t thread_id_;

    /**
     * read id
     * this id determines which tuple versions the transaction can access.
     */
    cid_t read_id_;

    /**
     * commit id
     * this id determines the id attached to the tuple version written by the transaction.
     * it will be reset when the txn commmit
     */
    cid_t commit_id_;

    /**
     * the max forward edge(high watermark) of the current txn
     */
    std::atomic<cid_t> predecessor;

    /**
     * the min backward edge(low watermark) of the current txn
     */
    std::atomic<cid_t> successor;

    /**
     * state of the txn
     */
    bool is_aborted_ = false;
    bool is_finished_ = false;  // commit finished
    bool is_committing_ = false;

    ReadWriteSet rw_set_;

    /** result of the transaction */
    ResultType result_ = ResultType::SUCCESS;

    bool is_written_;
    size_t insert_count_;
    bool is_loading_ = false;
    SpinLatch txn_cnt_lock;

    IsolationLevelType isolation_level_;
//    LSN_T last_log_rec_lsn;
    //<table_id, <write_type, lsn>>
    //if update many times, just only record the last one
    std::map<oid_t, std::pair<RWType,LSN_T>> last_log_rec_lsn;
    std::atomic<uint16_t> count;
    //for redo
    std::vector<LogRecord *> log_record_buffer;
    //for undo
    std::vector<uint64_t> overwritten_buffer;
};

}

#endif