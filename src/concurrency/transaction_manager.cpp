//
// Created by zhangqian on 2021/11/5.
//

#include <ctime>
#include "../include/common/raw_atomics.h"
#include "../include/execute/txn.h"
#include "../include/common/sync.h"

namespace mvstore {

//TxnMngRunning SSNTransactionManager::txn_mng_running = TxnMngRunning::STARTING;
thread_local cid_t current_txn_id = INVALID_CID;
static std::atomic<cid_t> tid_counter(INVALID_CID);
//thread_local SsnTxnContext *current_ssn_txn_ctx;
std::mutex tnx_manager_lock;
static std::atomic<SSNTransactionManager *>  txn_manager(nullptr);
IsolationLevelType SSNTransactionManager::isolation_level_ =  IsolationLevelType::SERIALIZABLE;

/**
 * traverse the write_sets, find the max pstamp from the following readers
 * we should check the readers after T, because they may has not finished,
 *   if T finished first, they may read error, atomicity failure.
 * find the forwards of T, they must first commit
 * T(i)<-(r:w)-T read/write dependency,
 * @param record_meta, current writer
 * @return
 */
bool SSNTransactionManager::FindMaxPstamp(TransactionContext *const current_txn){
    auto rw_set = current_txn->GetReadWriteSet();
    cid_t t_cstamp = current_txn->GetCommitId();
    bool result_=true;

    //iter the wr_sets
    for(auto &entry: rw_set){
        RecordMetadata meta = (entry.first.meta_data);
        auto meta_location = meta.GetLocationPtr()->record_meta_ptr;
        RecordMetadata *meta_ptr = reinterpret_cast<RecordMetadata *>(meta_location);

        if(entry.second == RWType::UPDATE){
            std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                    overwritten_buffer_pool->GetOversionHeader(meta_ptr->GetNextPointer());
            if(hdr != nullptr){
                int reader_size = hdr->GetReadersNum();
                for(int i = 0; i< reader_size; ++i){
                    auto rdr_id = hdr->GetReaders(i);
                    TransactionContext *txn_conxt;
                    bool shold_continue = true;
                    if(active_tids.Find(rdr_id, txn_conxt)){
                        cid_t r_cstamp = txn_conxt->GetCommitId();
                        if (r_cstamp == t_cstamp){
                            continue;
                        }
                        if (txn_conxt->IsAborted()) {
                            continue;
                        }
//                        bool wait_ = true;
                        while(true){
                            if (txn_mng_running == TxnMngRunning::STOP){
                                result_ = false;
                                break;
//                                return false;
                            }
                            r_cstamp = txn_conxt->GetCommitId();
                            if (r_cstamp > INVALID_CID){
                                break;
                            }
                        }

                        //LOG_INFO("r_cstamp:%u,t_cstamp:%u",r_cstamp,t_cstamp);
                        //reader B has not go to the pre commit processing
                        //writer A should be waiting, until B is committing state
                        //then B has commit id, and then verify
                        if (r_cstamp < t_cstamp){
                            if (txn_conxt->IsFinished()){
                                //we just care those commit before me
                                auto r_pstamp = txn_conxt->GetPredecessor();
                                current_txn->SetPredecessor(r_pstamp);
                                if (!current_txn->CheckExclusion()){
                                    shold_continue = false;
                                    result_=false;
                                }
                            }
                        }

                        if (shold_continue){continue;}
                    }else{
                        LOG_DEBUG("can not find the transaction from the active tids.");
                    }
                }

                cid_t v_pstamp = hdr->GetPstamp();
                current_txn->SetPredecessor(v_pstamp);
                if (!current_txn->CheckExclusion()){
                    result_=false;
                }
            }
        }
    }

    return result_;
}
/**
 * travers the read_sets, find the min sstamp from the writers that overwrite the version which T read,
 * the commit timestamp of the writers must after the T commit(backward edges):
 *      1. writer may be in current overwrite buffer,and has not commit;
 *      2. writer has commited;
 * T(i)<-(w:r)-T read/write dependency, have done in perform read
 * T<-(r:w)-T(j) read anti-dependency
 * @param record_meta, current reader
 * @return
 */
bool SSNTransactionManager::FindMinSstamp(TransactionContext *const current_txn){
    auto rw_set = current_txn->GetReadWriteSet();
    cid_t t_cstamp = current_txn->GetCommitId();
    bool result_=true;

    current_txn->SetSuccessor(t_cstamp);

    for(auto &entry: rw_set){
        RecordMetadata meta = (entry.first.meta_data);
        if(entry.second == RWType::READ){
            auto meta_location = meta.GetLocationPtr()->record_meta_ptr;
            RecordMetadata *meta_ptr = reinterpret_cast<RecordMetadata *>(meta_location);

            auto meta_cstamp = meta.GetTxnCommitId();
            auto curr_cstamp = meta_ptr->GetTxnCommitId();
            //we find it, it has been commited, and overwriteen
            if (curr_cstamp != meta_cstamp && (curr_cstamp < t_cstamp)){
                //curr_cstamp is the writer transaction's cstamp
                if (curr_cstamp != INVALID_CID){
                    TransactionContext *txn_conxt;
                    if(active_tids.Find(curr_cstamp, txn_conxt)) {
                        if (!txn_conxt->IsAborted()){
                            assert(txn_conxt->IsFinished());
                            auto u_sstamp = txn_conxt->GetSuccessor();
                            current_txn->SetSuccessor(u_sstamp);
                            if (!current_txn->CheckExclusion()){
                                result_=false;
                            }
                        }
                    }else{
                        LOG_DEBUG("can not find the transaction from the active tids.");
                    }
                }
            }else {
                //we find it in the buffer,if find, it is in writting
                std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr_curr =
                        overwritten_buffer_pool->GetOversionHeader(meta_ptr->GetNextPointer());
                if (hdr_curr != nullptr){
                    assert(meta_cstamp == hdr_curr->GetRstamp());
                    //when update the version, cstamp is the writer txn
                    cid_t u_txn_id = hdr_curr->GetCstamp();
                    TransactionContext *txn_conxt;
                    bool shold_continue = true;
                    if(active_tids.Find(u_txn_id, txn_conxt)) {
                        cid_t u_cstamp = txn_conxt->GetCommitId();
                        if (u_cstamp == t_cstamp){
                            continue;
                        }
                        if (txn_conxt->IsAborted()) {
                            continue;
                        }
//                        bool wait_ = true;
                        while(true){
                            if ( txn_mng_running == TxnMngRunning::STOP){
                                result_ = false;
                                break;
//                                return false;
                            }
                            u_cstamp = txn_conxt->GetCommitId();
                            if (u_cstamp > INVALID_CID){
                                break;
                            }
                        }

                        //LOG_INFO("u_cstamp:%u,t_cstamp:%u",u_cstamp,t_cstamp);
                        //writer B has not go to the pre commit processing
                        //reader A should be waiting, until B is committing state
                        //then B has commit id, and then verify
                        //writer B processing after reader A
                        //so, reader A should commit first
                        u_cstamp = txn_conxt->GetCommitId();
                        if(u_cstamp < t_cstamp){
//                            bool wait_ = true;
                            while (true){
                                if (txn_mng_running == TxnMngRunning::STOP){
                                    result_ = false;
                                    break;
//                                    return false;
                                }
                                if (txn_conxt->IsFinished() || txn_conxt->IsAborted()){
                                    //go to the pre commit, but before me
                                    break;
                                }
                            }
                            if (txn_conxt->IsFinished()){
                                //if the txn is flighting or precommitt
                                auto u_sstamp = txn_conxt->GetSuccessor();
                                current_txn->SetSuccessor(u_sstamp);
                                if (!current_txn->CheckExclusion()){
                                    shold_continue = false;
                                    result_=false;
                                }
                            }
                        }

                        if (shold_continue){continue;}
                    }else{
                        LOG_DEBUG("can not find the transaction from the active tids.");
                    }

                    // set bool, while a b c d state = success, bool = true
                    // if (bool) {}
                }
            }
        }
    }

    return result_;
}

txn_id_t SSNTransactionManager::GetCurrentTidCounter() {
    return tid_counter.load();
}

txn_id_t SSNTransactionManager::GetNextCurrentTidCounter() {
    return tid_counter.fetch_add(1);
}

/**
 * find the min active transaction begin id
 * @return
 */
cid_t SSNTransactionManager::MinActiveTID() {
    cid_t min_tid = tid_counter.load();
    active_tids.Iterate([&](const txn_id_t tid, TransactionContext *txn_conxt) {
//        min_tid = std::min(min_tid, tid);
        if (txn_conxt->IsAborted()==false && txn_conxt->IsFinished() ==false){
            min_tid = std::min(min_tid, txn_conxt->GetReadId());
        }
    });

    return min_tid;
}
/**
 * get the transaction context by txn id
 * @return transaction context
 */
TransactionContext *SSNTransactionManager::GetTransactionContext(const txn_id_t tid) {
    TransactionContext *txn_conxt;
    if(active_tids.Find(tid, txn_conxt) && !txn_conxt->IsAborted()){
        return txn_conxt;
    }
    return nullptr;
}

bool SSNTransactionManager::EraseTid(const txn_id_t tid) {
    auto rel = active_tids.Erase(tid);
    return rel;
}

void SSNTransactionManager::CleanTxnOverwrittenBuffer(
        TransactionContext *const current_txn){
    auto overwritten_buffer = current_txn->GetOverwrittenBuffer();
    for(int i=0; i<overwritten_buffer.size(); i++){
        auto copy_location = overwritten_buffer[i];
        std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                overwritten_buffer_pool->GetOversionHeader(copy_location);
        if (hdr == nullptr){
            LOG_DEBUG("CleanTxnOverwrittenBuffer, has not found the location.");
        }else{
            assert(hdr->waiting_free == true);
            assert(hdr->Count()==0);
            overwritten_buffer_pool->Free(hdr->GetBufferIndex(), copy_location);
        }
    }
}

TransactionContext *SSNTransactionManager::BeginTransaction(
        const size_t thread_id, const IsolationLevelType type) {
    TransactionContext *txn = nullptr;

    if (type == IsolationLevelType::SERIALIZABLE) {
        cid_t read_id, commit_id;
        commit_id = INVALID_CID;
        read_id = tid_counter.fetch_add(1);
        txn = new TransactionContext(thread_id, type, read_id, commit_id);
//        LOG_DEBUG("begin txn  read id :%u,  commit_id:%u.",read_id, commit_id);
    }else{
        cid_t read_id, commit_id;
        commit_id = INVALID_CID;
        read_id = tid_counter.fetch_add(1);
        txn = new TransactionContext(thread_id, type, read_id, commit_id);
    }

    current_txn_id = txn->GetReadId();
//    current_ssn_txn_ctx = new SsnTxnContext(txn);
    active_tids.Insert(current_txn_id, txn);

    auto log_manager = LogManager::GetInstance();
    if(log_manager->IsLogStart()){
        LogRecord *lg_rcd =  log_manager->LogBeginTxn(txn->GetReadId());;
        txn->LogRecordToBuffer(lg_rcd);
    }


    return txn;
}

void SSNTransactionManager::EndTransaction(TransactionContext *current_txn) {
    // fire all on commit triggers

//    auto tid = current_txn->GetTransactionId();

//    current_txn->SetFinished();
    //delete from the current transaction dynamic variables
//    delete current_txn;
//    current_txn = nullptr;
    //delete from the active id sets
//    active_tids.Erase(tid);

}

SSNTransactionManager *SSNTransactionManager::GetInstance() {
    if (txn_manager.load() == nullptr) {
        std::lock_guard<std::mutex> g(tnx_manager_lock);
        if (txn_manager.load() == nullptr) {
            SSNTransactionManager *new_txn_manager = new SSNTransactionManager;
//            Status s = new_txn_manager->Init();
            assert(new_txn_manager != nullptr);
            txn_manager.store(new_txn_manager);
        }
    }
    return txn_manager;
}

void SSNTransactionManager::ClearInstance() {
    txn_manager = nullptr;
}

bool SSNTransactionManager::IsOwner(TransactionContext *const current_txn,
                                    RecordMeta &accessor) {
    auto tuple_txn_id = accessor.meta_data.GetTxnCommitId();

    return tuple_txn_id == current_txn->GetCommitId();
}

/**
 * For a read, there are three occurs:
 *   1) read a active version, perform read;
 *   2) read a overwritten version, perform read;
 *   3) read a retired version, need not perform read
 * only read will call this function
 * check the read set if they are writing by someone
 * and these txns have finished the perform update/delete
 * @param current_txn
 * @param hard_hdr_ptr
 * @param acquire_ownership
 * @return
 */
bool SSNTransactionManager::PerformRead(TransactionContext *const current_txn,
                                        RecordMeta &record_meta, cid_t cstamp) {
    if (current_txn->IsReadOnly()) return true;
    //record_meta may be latest or the overwriting
    //if read the latest or the overwriting version, will call the current function
    //add it to the read set, verify in the commit
    bool read_rcd = current_txn->RecordRead(record_meta);

    if(read_rcd)
    {
        //=============update forward egdes with T(i)<-w:r-T
        current_txn->SetPredecessor(cstamp);

        //if read the latest, recordmeta 's next is in old version
        //then the copy location may be waiting state, hdr == null
        //if read the copy ,  recordmeta 's next is in buffer pool
        //then the copy location is active state, hdr != null
        auto read_copy_location = record_meta.meta_data.GetNextPointer();
        std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                overwritten_buffer_pool->GetOversionHeader(read_copy_location);
        if(hdr != nullptr)
        {
            //if the copy record is going to commit
            //then current read will be failure
            bool read_ret =
                    overwritten_buffer_pool->IncreaseWRCount(read_copy_location);
            if (!read_ret){return false;}
            //then read the copy record
            SpinLatch &latch_ = hdr->Getlatch();
            latch_.Lock();
            cid_t v_ss = hdr->GetSstamp();
            if (v_ss != MAX_CID){
                //=========update backward edges with T<-r:w-T(j)
                current_txn->SetSuccessor(v_ss);

            }
            latch_.Unlock();
        }

        if(!current_txn->CheckExclusion())
        {
            current_txn->SetAbort();
            return false;
        }

    }

    return true;
}

bool SSNTransactionManager::PerformInsert(TransactionContext *const current_txn,
                                          RecordMeta &meta) {
    assert(current_txn->GetIsolationLevel() ==  IsolationLevelType::SERIALIZABLE);

    // check MVCC info
    // the tuple slot must be in-inserting.
    if(meta.meta_data.TxnContextIsRead() == false){
        return false;
    }

    // Add the new tuple into the insert set
    current_txn->RecordInsert(meta);

    return true;

}

/**
 * @param current_txn
 * @param new_meta_location
 * @param new_tuple_location
 * @return
 */
bool SSNTransactionManager::PerformUpdate(TransactionContext *const current_txn,
                                          RecordMeta &new_meta_location,
                                          TupleHeader *new_tuple_location) {
    auto meta_location = new_meta_location.meta_data.GetLocationPtr();
    RecordMetadata *meta_ptr_ = reinterpret_cast<RecordMetadata *>(meta_location->record_meta_ptr);

    auto is_inserting = meta_ptr_->IsInserting();

    if (!is_inserting){
        LOG_DEBUG("record is not inserting. ");
        return false;
    }

    //meta.txn_id is the v_pstamp, v_cstamp
    cid_t v_cstamp = new_meta_location.meta_data.GetTxnCommitId();
    //get the copy record location
    auto copy_location = meta_ptr_->GetNextPointer();
//    LOG_DEBUG("copy location, %lu", copy_location);
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
            overwritten_buffer_pool->GetOversionHeader(copy_location);
    if (hdr == nullptr){
        LOG_DEBUG("overwritten_buffer_pool is null. ");
        return false;
    }

    assert(hdr != nullptr);
    //set new tuple header's next tuple = old next
//    LOG_DEBUG("GetNextTupleHeader %zu", hdr->GetNext());
    new_tuple_location->SetNextHeaderPointer(hdr->GetNext());

    //set the copy record location'pre ptr = new tuple header
    overwritten_buffer_pool->SetPre(copy_location,
                                    reinterpret_cast<uint64_t>(new_tuple_location));
    COMPILER_MEMORY_FENCE;

    //record update set
    bool update_recrd = current_txn->RecordUpdate(new_meta_location);
    //record the overwritten location
    current_txn->BufferOverwrittenVersion(copy_location);

    auto v_pre_pstamp = hdr->GetPstamp();
    if(update_recrd){
        //because the current txn is writting,
        //so, other reader must read write the copy header,
        //the record meta would not be overwriten
        //===================update forwards edges with T<-w:r-T(i)
        current_txn->SetPredecessor(v_pre_pstamp);

        if(!current_txn->CheckExclusion()){
            LOG_INFO("perform update verify fail, transaction abort.");
            current_txn->SetAbort();
            return false;
        }
    }

    return true;
}

bool SSNTransactionManager::PerformDelete(TransactionContext *const current_txn,
                                          RecordMeta &meta_location_) {
    //delete, just copy the record to the buffer pool
    //has no new tuple header
    auto meta_location = meta_location_.meta_data.GetLocationPtr();
    RecordMetadata *meta_ptr_ = reinterpret_cast<RecordMetadata *>(meta_location->record_meta_ptr);

    cid_t pstamp = meta_ptr_->GetTxnCommitId();
    //get the copy record location
    auto copy_location = meta_ptr_->GetNextPointer();
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
            overwritten_buffer_pool->GetOversionHeader(copy_location);
    assert(hdr != nullptr);

    //record update set
    bool del_recrd = current_txn->RecordDelete(meta_location_);

    if(del_recrd){
        cid_t txn_ps = current_txn->GetPredecessor();
        cid_t txn_ss = current_txn->GetSuccessor();
        //because the current txn is writting,
        //so, other reader must read  the copy header,
        //the record meta would not be overwriten
        current_txn->SetPredecessor(std::max(txn_ps, pstamp));

        if(current_txn->GetSuccessor() <= current_txn->GetPredecessor()){
            return false;
        }
    }

    return true;
}

/**
 * 1. commit, first iterate the read write set,
 *    compute the max, min sstamp, pstamp,
 *    check the sstamp<=pstamp, true return Abort
 * 2. install the write set version info
 *    if failure, return txn failure
 * @param current_txn
 * @return
 */
ResultType SSNTransactionManager::CommitTransaction(
        TransactionContext *const current_txn) {

    //if the txn is read only
    if (current_txn->IsReadOnly()) {
        current_txn->SetFinish();
        current_txn->SetResult(ResultType::SUCCESS);
//        EndTransaction(current_txn);
        return ResultType::SUCCESS;
    }

    auto log_manager = LogManager::GetInstance();
    auto version_block_mng = VersionBlockManager::GetInstance();

    //log_manager.StartLogging();
    cid_t begin_id = current_txn->GetReadId();
    //generate transaction commit id.
    cid_t end_commit_id = tid_counter.fetch_add(1) ;
    current_txn->SetCommitId(end_commit_id);
//    LOG_DEBUG("commit txn current commit :%u.", end_commit_id);

    auto &rw_set = current_txn->GetReadWriteSet();
//    LOG_DEBUG("commit rw set size: %lu", rw_set.size());

    //pre-commit
    //transaction init, pstamp=0, sstamp=Max
    cid_t t_cstamp = end_commit_id;
    cid_t t_pstamp = INVALID_CID;
    cid_t t_sstamp = MAX_CID;
    current_txn->SetCommitting();

    // finalize min backward edges
    bool min_ = FindMinSstamp(current_txn);
    //finalize max forward edges
    bool max_ = FindMaxPstamp(current_txn);
    if (min_ && max_){
        t_sstamp = current_txn->GetSuccessor();
        t_pstamp = current_txn->GetPredecessor();
    } else{
        current_txn->SetAbort();
        return ResultType::FAILURE;
    }


//        if (t_sstamp <= t_pstamp){
//            LOG_INFO("pre commit fail, transaction abort, t_sstamp:%u, t_pstamp: %u.",t_sstamp,t_pstamp);
//            current_txn->SetAbort();
//            return ResultType::FAILURE;
//        }

    //transaction finished after pre commit
    current_txn->SetFinish();

    //log commit
    if(log_manager->IsLogStart()){
        LogRecord *lg_rcd = log_manager->LogCommitTxn(begin_id, end_commit_id);
        current_txn->LogRecordToBuffer(lg_rcd);
    }
    //LOG_INFO("txn commit start log ." );

    // post-commit install everything.
    // 1. install a new version for update operations;
    // 2. install an empty version for delete operations;
    // 3. install a new tuple for insert operations.
    // Iterate through each item pointer in the read write set
    for (const auto &tuple_entry : rw_set) {
        RecordMetadata meta_data = tuple_entry.first.meta_data;
//        auto table_id = tuple_entry.first.table_id;
//        auto block_location = tuple_entry.first.block_ptr;
//        auto meta_ptr = tuple_entry.first.GetMetaPtr();
        auto meta_location = meta_data.GetLocationPtr()->record_meta_ptr;
//        void *meta_ptr_ = reinterpret_cast<void *>(meta_location);
        RecordMetadata *meta_ptr = reinterpret_cast<RecordMetadata *>(meta_location);
        auto total_sz = tuple_entry.first.GetTotalSize();

        if (tuple_entry.second == RWType::UPDATE) {
            //if above check is ok
            uint64_t record_copy_location = meta_ptr->GetNextPointer();
            //concurrency, this may be null,
            //or has no blcok space
            std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                    overwritten_buffer_pool->GetOversionHeaderComm(record_copy_location);
            assert(hdr != nullptr);
            hdr->SetSstamp(t_sstamp);
            hdr->SetWaiting(true);

            //acquire new tuple header will not be null
            auto next_tuple_header = hdr->GetPre();
            TupleHeader *new_tuple_header = nullptr;
            // update the retiring version tuple txn info
            if (next_tuple_header != 0){
                new_tuple_header = reinterpret_cast<TupleHeader *>(next_tuple_header);
                new_tuple_header->SetBeginId(hdr->GetRstamp());
                new_tuple_header->SetCommitId(hdr->GetSstamp());
                auto tuple_slot_ptr = new_tuple_header->GetTupleSlot();
                if(tuple_slot_ptr == 0 || tuple_slot_ptr == std::numeric_limits<uint64_t>::max()){
                    LOG_INFO("Failed to get tuple slot location within current version block.");
                    return ResultType::FAILURE;
                }

                auto tuple_slot_loc = reinterpret_cast<char *>(tuple_slot_ptr);
                auto record_cpy = reinterpret_cast<char *>(record_copy_location);
                auto key_len = hdr->GetKeyLen();
                auto payload_len = hdr->GetPayloadSize();
                VSTORE_MEMCPY(tuple_slot_loc, record_cpy, key_len);
                VSTORE_MEMCPY(tuple_slot_loc + key_len, record_cpy + key_len, payload_len);
            }

            //reset  transaction info
            //update the btree record metadata
            overwritten_buffer_pool->UpdateSs(record_copy_location, t_sstamp);

            RecordMetadata finl_meta = *meta_ptr;
            RecordMetadata new_meta = finl_meta;
            //initialize new version, v.cstamp = v.pstamp = t.cstamp
            new_meta.FinalizeForUpdate(t_cstamp);
            new_meta.SetNextPointer(next_tuple_header);
            bool new_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->meta,
                    &(finl_meta.meta),
                    new_meta.meta);
            COMPILER_MEMORY_FENCE;

            //set next = copy record, then other concurrent txns read copy
            bool new_meta_next = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->next_ptr,
                    &(finl_meta.next_ptr),
                    new_meta.next_ptr);
            COMPILER_MEMORY_FENCE;

            if(!(new_meta_ret && new_meta_next)){
                return ResultType::FAILURE;
            }

            assert(!meta_ptr->IsInserting());

            // gc the old version
            // if the version has no dependency txn, then it will be collected
            version_block_mng->EnterCleaner(end_commit_id,
                                            GCVersionType::COMMIT_UPDATE,
                                            new_tuple_header,
                                            sizeof(TupleHeader) );
        }else if (tuple_entry.second == RWType::INSERT) {
            //second, finalize the Btree insert
            RecordMetadata finl_meta = *meta_ptr;
            RecordMetadata new_meta = finl_meta;

            new_meta.FinalizeForInsert(finl_meta.GetOffset(),finl_meta.GetKeyLength(), t_cstamp);
//            new_meta.SetNextPointer(0);

            bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->meta,
                    &(finl_meta.meta),
                    new_meta.meta);

            if (!record_meta_ret){
                return ResultType::FAILURE;
            }else{
                assert(record_meta_ret);
                assert(!meta_ptr->IsInserting());
            }

        }else if (tuple_entry.second == RWType::DELETE) {
            //if above check is ok
            uint64_t record_copy_location = meta_ptr->GetNextPointer();

            //concurrency, this may be null,
            std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                    overwritten_buffer_pool->GetOversionHeaderComm(record_copy_location);
            if (hdr == nullptr) {
                continue;
            }
            hdr->SetSstamp(t_sstamp);
            hdr->SetWaiting(true);

            auto next_tuple_header = hdr->GetNext();
            TupleHeader *old_tuple_header = nullptr;
            //may be delete after insert, next=0
            if(next_tuple_header != 0){
                old_tuple_header = reinterpret_cast<TupleHeader *>(next_tuple_header) ;
            }

            //update copy record ssuccesor
            overwritten_buffer_pool->UpdateSs(record_copy_location, t_sstamp);

            //reset the btree meta = 0
            //update the btree nodeheader deletesize++
            RecordMetadata finl_meta = *meta_ptr;
            RecordMetadata new_meta = finl_meta;
            new_meta.FinalizeForDelete();
            bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->meta,
                    &(finl_meta.meta),
                    new_meta.meta);
            assert(record_meta_ret);

            auto node_hd = reinterpret_cast<NodeHeader *>(hdr->GetNodeHeader());
            NodeHeader::StatusWord old_status = node_hd->GetStatus();
            auto new_status = old_status;
            auto old_delete_size = old_status.GetDeletedSize();
            new_status.SetDeleteSize(old_delete_size + total_sz);
            bool new_header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &(&node_hd->status)->word,
                    &(old_status.word),
                    new_status.word);
            assert(new_header_ret);

            //gc the old version tuple header
            //gc the copy pool record
            version_block_mng->EnterCleaner( end_commit_id,
                                             GCVersionType::COMMIT_DELETE,
                                             old_tuple_header,
                                             sizeof(TupleHeader) );

        }else if(tuple_entry.second == RWType::READ){
            //now, if the record is also latest, then update the recordmeta commitid;
            //otherwise, someone is writing the record, then the recordmeta 's next
            //must be in copy pool, because the writing dependents current read
            auto meta_hdr = meta_data.GetNextPointer();
            std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                    overwritten_buffer_pool->GetOversionHeaderComm(meta_hdr);
            if(hdr != nullptr){
                //update forwards v.pstamp = max(v.pstamp, t.cstamp)
                auto v_ps_tamp = std::max(hdr->GetPstamp(), t_cstamp);
                overwritten_buffer_pool->UpdatePs(meta_hdr, v_ps_tamp);
                //decrease count, if count <= 0, copy pool
                //will free the location
                overwritten_buffer_pool->DecreaseWRCount(meta_hdr);
            }
        }

    }

//    ResultType result = current_txn->GetResult();
//    log_manager.LogEnd();

    //log write flush buffer
//  LOG_INFO("txn commit, log write, ");

//    bool status_ret = false;
//
//    if (log_manager->IsLogStart()){
//        if (!current_txn->IsReadOnly() && !rw_set.empty()){
////            auto log_ret = log_manager->LogWrite(log_rcds,end_commit_id);
//            std::vector<LSN_T> result;
//            auto on_complete = [&status_ret, &result, this](bool status,
//                                                                         TransactionContext *txn_,
//                                                                         std::vector<LSN_T> &&values) {
//                result = values;
//                status_ret = status;
////                txn_->ClearLogRecords();
////                task_callback_(task_callback_arg_);
//            };
//
//            auto &pool = MonoQueuePool::GetInstance();
//            pool.SubmitTask([current_txn, end_commit_id,  on_complete] {
//                LogManager::LogWrite(current_txn, end_commit_id, on_complete);
//            });
//            //      LOG_INFO("txn commit, log write, log write size: %zu, thread id: %zu",log_ret.size(), current_txn->GetThreadId());
////                PELOTON_ASSERT(!result.empty(),"Txn commit, Log Write result is not OK.");
////            current_txn->ClearLogRecords();
//        }
//        while(true){
//            if(status_ret){
//                break;
//            }
//        }
//    }


    current_txn->SetResult(ResultType::SUCCESS);
    current_txn->SetSuccessor(t_sstamp);
    //todo: the mapping table (active_tids) has the same value txn_ctx,
    // but the keys are different, read_id and cstamp
    active_tids.Insert(end_commit_id, current_txn);


//    EndTransaction(current_txn);

    return ResultType::SUCCESS;
}

/**
 * 1.in processing of the perform insert/select/delete/update,
 *    if the execut return false, abort;
 * 2.in processing of the commit transaction
 *    if the commit return false, abort;
 * @param current_txn
 * @return
 */
ResultType SSNTransactionManager::AbortTransaction(
        TransactionContext *const current_txn) {

    auto &rw_set = current_txn->GetReadWriteSet();
    auto buf_mgr_ = VersionStore::GetInstance();
    auto log_manager = LogManager::GetInstance();
    auto version_block_mng = VersionBlockManager::GetInstance();
    auto end_commit_id = current_txn->GetCommitId();

    current_txn->SetAbort();

    current_txn->SetResult(ResultType::ABORTED);
    active_tids.Insert(end_commit_id, current_txn);

    // Iterate through each item pointer in the read write set
    for (const auto &tuple_entry : rw_set) {
        RecordMetadata meta_data = tuple_entry.first.meta_data;
        auto meta_location = meta_data.GetLocationPtr()->record_meta_ptr;
        RecordMetadata *meta_ptr = reinterpret_cast<RecordMetadata *>(meta_location);
        auto node_hd_location = meta_data.GetLocationPtr()->node_header_ptr;
        NodeHeader *node_hd_ptr =  reinterpret_cast<NodeHeader *>(node_hd_location);
        auto total_sz = tuple_entry.first.GetTotalSize();

        if (tuple_entry.second == RWType::READ_OWN) {

        } else if (tuple_entry.second == RWType::UPDATE) {
            //first, copy back to record from copy pool
            uint64_t record_copy_location_ptr = meta_ptr->GetNextPointer();
            EphemeralPool::OverwriteVersionHeader *hdr =
                    overwritten_buffer_pool->GetOversionHeaderComm(record_copy_location_ptr).get();

            assert(hdr != nullptr);
            auto next_tuple_header = hdr->GetNext();
            TupleHeader *new_tuple_header = nullptr;
            uint64_t old_tuple_header = 0;
            if (next_tuple_header != 0){
                new_tuple_header = reinterpret_cast<TupleHeader *>(next_tuple_header) ;
                old_tuple_header = new_tuple_header->GetNextHeaderPointer();
                // update the retiring version tuple txn info
                new_tuple_header->SetBeginId(INVALID_CID);
                new_tuple_header->SetCommitId(INVALID_CID);
                new_tuple_header->SetNextHeaderPointer((uint64_t)(0));
                new_tuple_header->SetTupleSlot((uint64_t)(0));
            }

            //reset  txn info
            //update the btree record metadata
            overwritten_buffer_pool->UpdateSs(record_copy_location_ptr, MAX_CID);
            overwritten_buffer_pool->SetNext(record_copy_location_ptr, (uint64_t)(0));
            overwritten_buffer_pool->SetPre(record_copy_location_ptr, (uint64_t)(0));
            hdr->SetWaiting(true);

            //reset the update data, pstamp = old record commit id
            char *node_hd_loc = reinterpret_cast<char *>(node_hd_ptr);
            auto node_location = node_hd_loc - 16;
            char *data_source_location = node_location + meta_ptr->GetOffset();
            char *record_copy_location = reinterpret_cast<char *>(record_copy_location_ptr);
            auto payload_sz = hdr->GetPayloadSize();
            auto ken_sz = hdr->GetKeyLen();
            auto padd_ken_sz = total_sz - payload_sz;
            VSTORE_MEMCPY(data_source_location, record_copy_location, ken_sz);
            VSTORE_MEMCPY(data_source_location+padd_ken_sz, record_copy_location + ken_sz, payload_sz);
            COMPILER_MEMORY_FENCE;

            //TODO:for test
//            uint64_t k1= *reinterpret_cast<const uint64_t *>(data_source_location);
//            uint64_t k2= *reinterpret_cast<const uint64_t *>(data_source_location+8);
//            LOG_DEBUG("abot, record: %lu, %lu",k1,k2);

            RecordMetadata finl_meta = *meta_ptr;
            RecordMetadata new_meta = finl_meta;
            new_meta.FinalizeForUpdate();
            bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->meta,
                    &(finl_meta.meta),
                    new_meta.meta);
            COMPILER_MEMORY_FENCE;
            //TODO:for test
//            auto off = meta_ptr->GetOffset();
//            auto comm = meta_ptr->GetTxnCommitId();
//            auto k = meta_ptr->GetKeyLength();
//            auto i_ = meta_ptr->IsInserting();

            new_meta.SetNextPointer(old_tuple_header);
            bool record_meta_next_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->next_ptr,
                    &(finl_meta.next_ptr),
                    new_meta.next_ptr);
            COMPILER_MEMORY_FENCE;

            assert(record_meta_ret && record_meta_next_ret);
            assert(!meta_ptr->IsInserting());

            // gc the new tuple version
            // gc the copy pool record
            version_block_mng->EnterCleaner(end_commit_id,
                                            GCVersionType::COMMIT_UPDATE,
                                            new_tuple_header,
                                            sizeof(TupleHeader));

        } else if (tuple_entry.second == RWType::DELETE) {
            //first, copy back to record from copy pool
            uint64_t record_copy_location = meta_ptr->GetNextPointer();

            EphemeralPool::OverwriteVersionHeader *hdr =
                    overwritten_buffer_pool->GetOversionHeaderComm(record_copy_location).get();
            if (hdr == nullptr){
                continue;
            }
            assert(hdr != nullptr);
            overwritten_buffer_pool->UpdateSs(record_copy_location, MAX_CID);
//            overwritten_buffer_pool->AddWriters(tuple_entry.first.GetMetaPtrVal(),
//                                             meta_data.GetTxnCommitId(), end_commit_id);

            //reset btree record meta, pstamp = old record commit id
            RecordMetadata finl_meta = *meta_ptr;
            RecordMetadata new_meta = finl_meta;
            new_meta.FinalizeForUpdate();
            bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->meta,
                    &(finl_meta.meta),
                    new_meta.meta);
            assert(record_meta_ret);
            assert(!meta_ptr->IsInserting());

        } else if (tuple_entry.second == RWType::INSERT) {
//            auto node_hd_ptr = reinterpret_cast<NodeHeader *>(tuple_entry.first.GetNodeHdPtr());
            //first, finalize the Btree delete
            //reset txn info
            //update the btree record metadata
            RecordMetadata finl_meta = *meta_ptr;
            RecordMetadata new_meta = finl_meta;
            new_meta.FinalizeForDelete();
            bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &meta_ptr->meta,
                    &(finl_meta.meta),
                    new_meta.meta);
            COMPILER_MEMORY_FENCE;

            assert(record_meta_ret);

            NodeHeader::StatusWord old_status = node_hd_ptr->GetStatus();
            auto new_status = old_status;
            new_status.FailForInsert(total_sz);
            bool new_header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                    &(&node_hd_ptr->status)->word,
                    &(old_status.word),
                    new_status.word);
            COMPILER_MEMORY_FENCE;

            assert(new_header_ret);
            assert(!meta_ptr->IsInserting());

            //add the record location to gc
            //no need to gc , does not produce a retiring version
//            version_block_mng->EnterCleaner( end_commit_id,
//                                            GCVersionType::ABORT_INSERT,
//                                            meta_ptr,
//                                            sizeof(RecordMetadata),
//                                             );

        }else if(tuple_entry.second == RWType::READ){
            //now, if the record is also latest, then update the recordmeta commitid;
            //otherwise, someone is writing the record, then the recordmeta 's next
            //must be in copy pool, because the writing dependents current read
            auto meta_next = meta_data.GetNextPointer();
            std::shared_ptr<EphemeralPool::OverwriteVersionHeader> hdr =
                    overwritten_buffer_pool->GetOversionHeaderComm(meta_next);
            if(hdr != nullptr){
                //has read the next or record has been overwriten,
                // update copy record
//                assert(meta_ptr->IsInserting());
                //decrease count, if count <= 0, copy pool
                //will free the location
                overwritten_buffer_pool->DecreaseWRCount(meta_next);
            }
        }
    }

    if (log_manager->IsLogStart()){
        auto begin_tid = current_txn->GetReadId();
        log_manager->LogAbortTxn(begin_tid, end_commit_id);
    }

//
//    if (log_manager->IsLogStart()){
//        bool status_ret = false;

//
//      if (!current_txn->IsReadOnly() && !rw_set.empty()){
////            auto log_ret = log_manager->LogWrite(log_rcds,end_commit_id);
//            std::vector<LSN_T> result;
//            auto on_complete = [&status_ret, &result, this](bool status,
//                                                                         TransactionContext *txn_,
//                                                                         std::vector<LSN_T> &&values) {
//                result = values;
//                status_ret = status;
////                txn_->ClearLogRecords();
////                task_callback_(task_callback_arg_);
//            };
//
//            auto &pool = MonoQueuePool::GetInstance();
//            pool.SubmitTask([current_txn, end_commit_id,  on_complete] {
//                LogManager::LogWrite(current_txn, end_commit_id, on_complete);
//            });
//            //      LOG_INFO("txn abort, log write, log write size: %zu, thread id: %zu",log_ret.size(), current_txn->GetThreadId());
////                PELOTON_ASSERT(!result.empty(),"Txn abort, Log Write result is not OK.");
////            current_txn->ClearLogRecords();
//        }
//        while(true){
//            if(status_ret){
//                break;
//            }
//        }
//    }


    current_txn->SetResult(ResultType::ABORTED);
//    EndTransaction(current_txn);

    return ResultType::ABORTED;
}

}