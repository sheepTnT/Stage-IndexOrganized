//
// Created by zhangqian on 2021/10/29.
//


#include "../vstore/b_tree.h"
#include "txn.h"

namespace mvstore {

class Executor {
public:
    virtual bool Execute() = 0;
};

template<class Key, class T>
class InsertExecutor : public Executor {
public:
    InsertExecutor(BTree *data_table_, const Key &keys_, uint32_t key_size_, T &tuple_,
                   TransactionContext *txn_, VersionStore *buf_mgr_) : data_table(data_table_),
                                                                       keys(keys_), key_size(key_size_),
                                                                       tuple(tuple_), txn_ctx(txn_),
                                                                       buf_mgr(buf_mgr_) {
    }

    virtual bool Execute() override {
        auto transaction_manager = SSNTransactionManager::GetInstance();
        oid_t table_id = data_table->GetTableSchema().table_id;
        auto txn_id = txn_ctx->GetReadId();
        RecordMeta meta;
        const char *payload_location;
        Key k_ = keys;
        ret_code = ReturnCode::Ok();

        ReturnCode insert_res = data_table->Insert(k_, key_size,
                                                   tuple.GetData(), &meta,
                                                   txn_id);
        if(insert_res.IsOk()) {
            // RetOk/RetKeyExists, record rw_set
            transaction_manager->PerformInsert(txn_ctx, meta);
            //Log Record
//            payload_location = reinterpret_cast<const char *>(meta.meta_ptr);
            RecordLocation *meta_location = meta.meta_data.GetLocationPtr();
            uint64_t meta_ptr = meta_location->record_meta_ptr;
            uint64_t node_hd_ptr = meta_location->node_header_ptr;
            void *meta_ptr_ = reinterpret_cast<void *>(meta_ptr);
            void *node_hd_ptr_ = reinterpret_cast<void *>(node_hd_ptr);
            auto node_location = reinterpret_cast<char *>(node_hd_ptr_) - 1;
            payload_location = node_location + (reinterpret_cast<RecordMetadata *>(meta_ptr_))->GetOffset();

            LogManager *log_mng = LogManager::GetInstance();
            if(log_mng->IsLogStart()){
                LogRecord *lg_rcd = log_mng->LogInsert(txn_ctx->GetTransactionId(), payload_location,
                                                       data_table->GetTableSchema().get_tuple_size(),
                                                       &data_table->GetTableSchema());

                txn_ctx->SetLastLogLsn(table_id, reinterpret_cast<uint64_t>(lg_rcd->GetIndirectLSN()),
                                       RWType::INSERT);
                txn_ctx->LogRecordToBuffer(lg_rcd);
            }
        }else if(insert_res.IsKeyExists()){
//            std::string k_str = reinterpret_cast<const char *>(k_);
            std::string table_name = data_table->schema.table_name;
            LOG_DEBUG("btree insert, key has already exists, table name: %s", table_name.c_str());
        }else if(insert_res.IsCASFailure()){
//            LOG_INFO("btree insert, CAS failure , key: %lu",k_);
            ret_code = ReturnCode::CASFailure();
        }else if(insert_res.IsRetryFailure()){
//            LOG_INFO("btree insert, retry failure , key: %lu",k_);
            ret_code = ReturnCode::RetryFailure();
        }else{
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
            return false;
        }

        transaction_manager->SetTransactionResult(txn_ctx, ResultType::SUCCESS);

        //auto dt = tuple.GetData();
        //delete dt;

        return true;
    }

    const ReturnCode GetResultCode(){
        return ret_code;
    }

private:
    BTree *data_table;
    T tuple;
    Key keys;
    uint32_t key_size;
    TransactionContext *txn_ctx;
    VersionStore *buf_mgr;
    ReturnCode ret_code;
};

template<class Key, class T>
class PointDeleteExecutor : public Executor {
public:
    PointDeleteExecutor(BTree *data_table_, const Key &keys_,uint32_t key_size_,
                        bool is_for_update_,
                        TransactionContext *txn_,
                        VersionStore *buf_mgr_) : data_table(data_table_), keys(keys_),
                                                   key_size(key_size_),
                                                   is_for_update(is_for_update_),
                                                  txn_ctx(txn_), buf_mgr(buf_mgr_) {}

    virtual bool Execute() override {
        auto transaction_manager = SSNTransactionManager::GetInstance();
        bool failed = false;
        //delete(logically) from the tree, reset the mvcc info
        RecordMeta meta_del;
        TupleHeader tuple_header;
        int retry_count =0;
        oid_t table_id = data_table->GetTableSchema().table_id;
        auto txn_id = txn_ctx->GetReadId();
        //delete, just copy the record to the buffer pool
        //has no new tuple header
        // when btree delete, will lock the tuple record, is written
        //1. three conditions, it is visible: predicate(key==); is not deleted; commit_id<current_txn
        //2. use the record_meta handle the concurrency control
        //   1). btree delete, add to copy pool
        //   2). perform delete, add to rw_set
        //   3). transaction commit. set commit_commit_id, meta=0, visible=false
        Key k_ = keys;

        retry:
        ReturnCode delete_res = data_table->Delete(k_,
                                                   key_size,
                                                   &meta_del,
                                                   &tuple_header,
                                                    is_for_update,
                                                   txn_id);

        if (delete_res.IsOk()) {
            LogManager *log_mng = LogManager::GetInstance();
            if (is_for_update){
                if(log_mng->IsLogStart()) {
                    auto last_lsn = txn_ctx->GetLastLogLsn(table_id);
                    LogRecord *lg_rcd = buf_mgr->GetLogManager()->LogDelta(txn_ctx->GetReadId(),
                                                                           last_lsn, nullptr);
                    txn_ctx->SetLastLogLsn(table_id,reinterpret_cast<uint64_t>(lg_rcd->GetIndirectLSN()),
                                           RWType::DELETE);
                    txn_ctx->LogRecordToBuffer(lg_rcd);
                }
            }else{
                // not own the tuple record, delete from the index and insert to version store
                //second: record delete rw_set
                transaction_manager->PerformDelete(txn_ctx, meta_del);
                if(log_mng->IsLogStart()){
                    LogRecord *lg_rcd = log_mng->LogDelete(txn_ctx->GetReadId(),
                                                           reinterpret_cast<char *>(tuple_header.GetTupleSlot()),
                                                           &data_table->schema);
                    txn_ctx->SetLastLogLsn(table_id,reinterpret_cast<uint64_t>(lg_rcd->GetIndirectLSN()),
                                           RWType::DELETE);
                    txn_ctx->LogRecordToBuffer(lg_rcd);
                }

            }

            failed = false;
        } else if(delete_res.IsNotFound()){
//            LOG_INFO("btree delete, key has not exists, key: %lu",k_);
        }else{
            if(retry_count > 2){
                failed = true;
            } else{
                retry_count = retry_count +1;
                goto retry;
            }
        }

        if (failed) {
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::ABORTED);
        }else{
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::SUCCESS);
        }
        return !failed;
    }

private:
    BTree *data_table;
    Key keys;
    uint32_t key_size;
    bool is_for_update;
    TransactionContext *txn_ctx;
    VersionStore *buf_mgr;
};

template<class Key, class T>
class PointUpdateExecutor : public Executor {
public:
    PointUpdateExecutor(BTree *data_table_, const Key &keys_, uint16_t key_sz_, const char *delta_tuple_,
                        std::vector<oid_t> &columns, uint16_t delta_len_, bool is_for_update_,
                        TransactionContext *txn, VersionStore *buf_mgr_) :
            data_table(data_table_),
            keys(keys_), key_size(key_sz_),
            delta_tuple(delta_tuple_),update_columns(columns),
            delta_len(delta_len_), is_for_update(is_for_update_),
            txn_ctx(txn), buf_mgr(buf_mgr_) {}

    virtual bool Execute() override {
        auto transaction_manager = SSNTransactionManager::GetInstance();
        bool failed = false;
        const char *tuple_data = delta_tuple;
        bool up_ret = true;
        auto txn_id = txn_ctx->GetReadId();
//        int retry_count = 0;
        oid_t table_id = data_table->GetTableSchema().table_id;

        RecordMeta meta_upt;
        // when btree delete, will lock the tuple record, is written
        //1. three conditions, it is visible: predicate(key==); is not deleted; commit_id<=current_txn
        //2. use the record_meta to handle the concurrency control
        //   1). btree update, update commit_id = transaction commit, set is in-inserting
        //       if commit_id==current, then just update in-place, delta update
        //   2). perform update,  reset new tuple header next and pre info
        //   3). transaction commit. check and reset tuple header txn info and record meta info
        //   4). multiple updates, update record payload many times,
        //                         perform update, reset copy pool pre(new tuple header) many times
        //                                         just check one times
        //                         log delta data many times
        Key k_ = keys;

        ReturnCode update_res = data_table->Update(k_,
                                                   key_size,
                                                   tuple_data,
                                                   update_columns,
                                                   &meta_upt,
                                                   is_for_update,
                                                   txn_id);
        // if update delta == btree payload
        if (update_res.IsNotNeeded()) {
//            LOG_DEBUG("btree update not need to update.");
            ret_code = ReturnCode::NotNeededUpdate();
            return true;
        }

        if (update_res.IsOk()) {
            LogManager *log_mng = LogManager::GetInstance();
            if (is_for_update){
                if(log_mng->IsLogStart()){
                    auto last_lsn = txn_ctx->GetLastLogLsn(table_id);
                    if (last_lsn != InvalidLSN){
                        LogRecord *lg_rcd = buf_mgr->GetLogManager()->LogDelta(txn_ctx->GetReadId(),
                                                                               last_lsn,delta_tuple);
                        txn_ctx->SetLastLogLsn(table_id,reinterpret_cast<uint64_t>(lg_rcd->GetIndirectLSN()),
                                               RWType::UPDATE);
                        txn_ctx->LogRecordToBuffer(lg_rcd);
                    }
                }
            }else{
                // not own the tuple record, delete from the index and insert to version store
                //second: move old tuple to retiring version store
                //get a empty location
                //return <block_id, tuple_header>
                std::pair<oid_t, TupleHeader *> tuple_header = buf_mgr->AcquireVersion(&data_table->schema);
                auto tpl_hd = tuple_header.second;
                if(tpl_hd == nullptr){
                  LOG_DEBUG("update, tuple header is null.");
                  return false;
                }
                tpl_hd->SetTableId(data_table->GetTableSchema().table_id);

                up_ret = transaction_manager->PerformUpdate(txn_ctx, meta_upt, tpl_hd);

                Catalog *catalog = &data_table->schema;
                RawBitmap *bitmap=RawBitmap::Allocate(catalog->field_cnt);
                for (int p = 0; p < update_columns.size(); ++p ) {
                    bitmap->Set(update_columns[p], true);
                }
                if (up_ret) {
                    if(log_mng->IsLogStart()){
                        LogRecord *lg_rcd = log_mng->LogUpdate(txn_ctx->GetReadId(),
                                                                                reinterpret_cast<char *>(tuple_header.second->GetTupleSlot()),
                                                                                reinterpret_cast<char *>(meta_upt.meta_data.GetNextPointer()),
                                                                                delta_tuple, delta_len,
                                                                                reinterpret_cast<uint8_t *>(bitmap),
                                                                                &data_table->schema);
                        txn_ctx->SetLastLogLsn(table_id,reinterpret_cast<uint64_t>(lg_rcd->GetIndirectLSN()),
                                               RWType::UPDATE);
                        txn_ctx->LogRecordToBuffer(lg_rcd);
                    }
                }else{
                    LOG_DEBUG("update, transaction fail.");
                    failed = true;
                }
            }

        }else if(update_res.IsNotFound()){
            // transaction failure, maybe key produce randomly
            std::string table_name = data_table->GetTableSchema().table_name;
            LOG_DEBUG("btree update, key has not exists, table name: %s",table_name.c_str());
        }else{
            LOG_DEBUG("btree update fail, %d.",update_res.rc);
            failed = true;
        }


        if (failed) {
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
        }else{
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::SUCCESS);
        }
        return !failed;
    }

    const ReturnCode GetResultCode(){
        return ret_code;
    }

private:
    BTree  *data_table;
    const char *delta_tuple;
    Key keys;
    uint16_t key_size;
    std::vector<oid_t> &update_columns;
    uint16_t delta_len;
    bool is_for_update;
    TransactionContext *txn_ctx;
    VersionStore *buf_mgr;
    ReturnCode ret_code;
};

template<class Key, class T>
class IndexScanExecutor : public Executor {
public:

    // predicate:
    //  A function returns whether a tuple satisfies the predicate.
    //  If predicate returns true, the tuple is added to the result set.
    //  If end_scan is set after the predicate evaluation, the scan will be terminated.
    // point_lookup:
    //  A boolean indicating whether this is a point lookup.
    //  If so, only one tuple will be read, ignoring whether end_scan is set or not.
    IndexScanExecutor(BTree *data_table, uint16_t key_sz, const Key &start_key, uint32_t scan_sz,
                      bool point_lookup, std::function<bool(const T *, bool &end_scan)> predicate,
                      bool is_for_update_,
                      TransactionContext *txn, VersionStore *buf_mgr) : data_table(data_table),
                                                                        key_size(key_sz),
                                                                        scan_size(scan_sz),
                                                                        start_key(start_key),
                                                                        point_lookup(point_lookup),
                                                                        predicate(predicate),
                                                                        is_for_update(is_for_update_),
                                                                        txn(txn),
                                                                        buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = SSNTransactionManager::GetInstance();
        bool failed = false;
        std::unique_ptr<Record> current_record;
        std::unique_ptr<Iterator> iter;
        Key k_ = start_key;
        auto txn_id = txn->GetReadId();

        if (point_lookup) {
            //just return the location offset, it is not deleted and meets the predicates
            current_record = data_table->Read(k_,
                                              key_size,
                                              txn_id,
                                              is_for_update);
            if (current_record != nullptr) {
                //first, transaction check
                //check transaction visible
                auto curr_rcd_comm_id = current_record->meta.meta_data.GetTxnCommitId();
                auto cstamp = current_record->GetCstamp();
                if (txn_id >= curr_rcd_comm_id) {
                    //Record(data)<key,payload>
                    auto data_ = reinterpret_cast<T *>(current_record->GetData());
                    bool read_ret = true;
                    if (!is_for_update){
                        //read own need not check
                        read_ret = transaction_manager->PerformRead(txn,
                                                                    current_record->meta,
                                                                    cstamp);
                    }
                    if (read_ret) {
                        //<key,payload(Tuple)>
                        results.push_back(data_);
                        current_record.release();
                    }else{
                        failed = true;
                    }
                } else{
                    auto next_ptr_ = current_record->meta.next_tuple_ptr;
                    if (next_ptr_ != 0){
                        //LOG_DEBUG("NextTuplePtr, %zu", next_ptr_);
                        void *next_ = nullptr;
                        next_ = reinterpret_cast<void *>(next_ptr_);
                        TupleHeader *tpl_hd = nullptr;
                        tpl_hd = reinterpret_cast<TupleHeader *>(next_);
                        if (next_ != nullptr || tpl_hd != nullptr) {
                            while (true) {
                                auto begn = tpl_hd->GetBeginId();
                                auto comm = tpl_hd->GetEndCommitId();
                                if (begn == INVALID_CID || comm == INVALID_CID){
                                    failed = true;
                                    break;
                                }
                                if (txn_id >= begn && txn_id <= comm) {
                                    //Record(data)<key,payload>
                                    auto data_ = reinterpret_cast<T *>(tpl_hd->GetTupleSlot());
                                    results.push_back(data_);

                                    current_record.release();
                                    break;
                                }else{
                                    auto tpl_nxt_prt = tpl_hd->GetNextHeaderPointer();
                                    if(tpl_nxt_prt == 0 || tpl_nxt_prt == std::numeric_limits<uint64_t>::min() ||
                                       tpl_nxt_prt == std::numeric_limits<uint64_t>::max()){
                                        break;
                                    }

                                    TupleHeader *next_tuple_header = nullptr;
                                    next_tuple_header = reinterpret_cast<TupleHeader *>(tpl_nxt_prt);
                                    if(next_tuple_header == nullptr){
                                        break;
                                    }
                                    tpl_hd = next_tuple_header;
                                }
                            }
                        }
                    } else{
                        LOG_DEBUG("there is no retiring version, table name: %s", data_table->schema.table_name);
                    }
                }
            } else{
                LOG_DEBUG("there is no active version, table name: %s", data_table->schema.table_name);
            }
        } else {
            iter = data_table->RangeScanBySize(k_, key_size, scan_size);
            bool end_scan = false;
            // we have more than one record
            for (int scanned = 0; (scanned < scan_size); ++scanned) {
                current_record = iter->GetNext();
                if(current_record == nullptr){
                    continue;
                }

                auto cstamp = current_record->GetCstamp();
                //if meets the transaction
                //if not meet the visible, continue get next record
                auto curr_rcd_comm_id = current_record->meta.meta_data.GetTxnCommitId();
                if (txn_id >= curr_rcd_comm_id) {
                    bool read_ret = true;
                    if (!is_for_update){
                        //read own need not check
                        read_ret = transaction_manager->PerformRead(txn, current_record->meta, cstamp);
                    }
                    if (read_ret) {
                        auto data_ = reinterpret_cast<T *>(current_record->GetData());
                        bool eval = true;
                        if (predicate != nullptr){
                            predicate(data_, end_scan);
                        }
                        if (eval){
                            //<key,payload(Tuple)>
                            results.push_back(data_);
                        }
                    }
                }else{
                    //this version has commited but after the current, get next
                    // because we always update in place , so the next must be stable
                    // this usually the current txn read lately, means before it read
                    // there are some writes for this record
                    auto next__ = current_record->meta.next_tuple_ptr;
                    if (next__ != 0){
                        //LOG_DEBUG("NextTuplePtr, %zu", next__);
                        void *next_ = nullptr;
                        next_ = reinterpret_cast<void *>(next__);
                        TupleHeader *tpl_hd = nullptr;
                        tpl_hd = reinterpret_cast<TupleHeader *>(next_);
                        if (next_ != nullptr || tpl_hd != nullptr){
                            while (true) {
                                auto begn = tpl_hd->GetBeginId();
                                auto comm = tpl_hd->GetEndCommitId();
                                if (begn == INVALID_CID || comm == INVALID_CID){
                                    break;
                                }
                                if (txn_id >= begn && txn_id <= comm) {
                                    auto data_ = reinterpret_cast<T *>(tpl_hd->GetTupleSlot());
                                    bool eval = true;
                                    if (predicate != nullptr){
                                        predicate(data_, end_scan);
                                    }

                                    if (eval){
                                        //<key,payload(Tuple)>
                                        results.push_back(data_);
                                    }
                                }else{
                                    auto tpl_nxt_prt = tpl_hd->GetNextHeaderPointer();
                                    if(tpl_nxt_prt == 0 || tpl_nxt_prt == std::numeric_limits<uint64_t>::min() ||
                                       tpl_nxt_prt == std::numeric_limits<uint64_t>::max()){
                                        break;
                                    }
                                    void *next_tpl_hdr = nullptr;
                                    next_tpl_hdr = reinterpret_cast<void *>(tpl_nxt_prt);
                                    TupleHeader *next_tuple_header = nullptr;
                                    next_tuple_header = reinterpret_cast<TupleHeader *>(next_tpl_hdr);
                                    if(next_tpl_hdr == nullptr || next_tuple_header == nullptr){
                                        break;
                                    }
                                    tpl_hd = next_tuple_header;
                                }
                            }
                        }
                    }
                }
            }
        }

        //if not find the record or tuple, the result is null
        //if failure, the txn will be Abort
        if (failed) {
            transaction_manager->SetTransactionResult(txn, ResultType::FAILURE);
        }else{
            transaction_manager->SetTransactionResult(txn, ResultType::SUCCESS);
        }

        return failed == false;
    }

    const std::vector<T *> &GetResults() {
        return results;
    }

private:
    BTree *data_table;
    Key start_key;
    uint16_t key_size;
    uint32_t scan_size;
    bool point_lookup;
    std::function<bool(const T *, bool &should_end_scan)> predicate;
    bool is_for_update;
    TransactionContext *txn;
    VersionStore *buf_mgr;
    std::vector<T *> results;
};

template<class Key, class T>
class TableScanExecutor : public Executor {
public:

    TableScanExecutor(BTree *data_table, const Key &start_key, uint32_t key_size, uint32_t scan_size,
                      TransactionContext *txn, VersionStore *buf_mgr) :
            data_table(data_table), start_key(start_key), key_sz(key_size), scan_sz(scan_size),
            txn(txn), buf_mgr(buf_mgr) {}

    void ScanLeafNode(BaseNode *real_root) {
        auto leaf_node = reinterpret_cast<LeafNode *>(real_root) ;
        auto record_count = leaf_node->GetHeader()->GetStatus().GetRecordCount();
        for (uint32_t i = 0; i < record_count; ++i) {
            RecordMetadata meta = leaf_node->GetMetadata(i);
            char *payload = nullptr;
            char *key = nullptr;
            leaf_node->GetRawRecord(meta, &key, &payload);
            results.push_back(*reinterpret_cast<const T*>(key));
        }
    }
    void ScanInnerNode(BaseNode *real_root) {
        auto inner_node = reinterpret_cast<InternalNode *>(real_root);
        auto sorted_count = inner_node->GetHeader()->sorted_count;
        for (uint32_t i = 0; i < sorted_count; ++i) {
            RecordMetadata meta = inner_node->GetMetadata(i);

            char *ptr = reinterpret_cast<char *>(inner_node) + meta.GetOffset() + meta.GetPaddedKeyLength();
            uint64_t node_addr =  *reinterpret_cast<uint64_t *>(ptr);
            BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
            if (node->IsLeaf()) {
                ScanLeafNode(node);
            } else if (node == nullptr){
                break;
            }else {
                ScanInnerNode(node);
            }
        }
    }

    virtual bool Execute() override {
        if (scan_sz == -1){
            auto real_root = data_table->GetRootNodeSafe();

            if (real_root->IsLeaf()){
                ScanLeafNode(real_root);
            }else{
                ScanInnerNode(real_root);
            }
        }else{
            int scanned = 0;
            Key k_ = start_key;

            auto iter = data_table->RangeScanBySize(k_,
                                                    key_sz, scan_sz);
            for (scanned = 0; (scanned < scan_sz); ++scanned) {

                std::shared_ptr<Record> record = iter->GetNext();
                if (record == nullptr) {
                    fail_num++;
                    continue;
                }

//            char *data_ptr = record->GetData();
//            uint64_t k_ = *reinterpret_cast<const uint64_t *>(data_ptr);
//            LOG_DEBUG("get key : %lu", k_);
              results.push_back(*reinterpret_cast<const T*>(record->GetData()));
            }
        }

        return true;
    }

    std::vector<T> &GetResults() {
        return results;
    }
    const uint32_t GetFailNum(){
        return fail_num;
    }


private:
    BTree  *data_table;
    Key start_key;
    uint32_t key_sz;
    uint32_t scan_sz;
    TransactionContext *txn;
    VersionStore *buf_mgr;
    std::vector<T> results;
    uint32_t fail_num = 0;
};

}

