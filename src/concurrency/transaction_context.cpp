
#include "../include/execute/txn_context.h"
#include <iostream>
#include <iosfwd>

namespace mvstore {
#define INTITIAL_RW_SET_SIZE 64

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id)
        : rw_set_(INTITIAL_RW_SET_SIZE) {
    Init(thread_id, isolation, read_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id)
        : rw_set_(INTITIAL_RW_SET_SIZE) {
    Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id,
                                       const size_t rw_set_size)
        : rw_set_(rw_set_size) {
    Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::~TransactionContext() {
    //delete the rw
    rw_set_.clear();

    //delete the redo log record
    for (auto l_r_:log_record_buffer){
        delete l_r_;
    }
    log_record_buffer.clear();
}

void TransactionContext::Init(const size_t thread_id,
                              const IsolationLevelType isolation,
                              const cid_t &read_id, const cid_t &commit_id) {
    read_id_ = read_id;

    // commit id will be reset at the transaction's commit phase.
    commit_id_ = commit_id;

    txn_id_ = read_id;

    thread_id_ = thread_id;

    isolation_level_ = isolation;

    is_written_ = true;
    is_abort_ = false;
    is_finish_ = false;

    insert_count_ = 0;

    predecessor = INVALID_CID;
    successor = MAX_CID;

//    last_log_rec_lsn = InvalidLSN;
    last_log_rec_lsn.clear();
    rw_set_.clear();

    result_ = ResultType::INVALID;
}

RWType TransactionContext::GetRWType(const RecordMeta &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it == rw_set_.end()) {
        rw_type = RWType::INVALID;
    } else {
        rw_type = it->second;
    }

    return rw_type;
}

bool TransactionContext::RecordRead(const RecordMeta &location) {
    auto it = rw_set_.find(location);

    if (it != rw_set_.end()) {
        auto rw_type = it->second;
        assert(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
        return false;
    } else {
        rw_set_.insert(std::make_pair(location, RWType::READ));
        return true;
    }
}

//void TransactionContext::RecordReadOwn(const btree::RecordMetadata &location, cid_t cstamp) {
//    RWType rw_type;
//    auto it = rw_set_.find(location);
//    if (it != rw_set_.end()) {
//        rw_type = it->second.first;
//        assert(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
//        if (rw_type == RWType::READ) {
//            it->second.first = RWType::READ_OWN;
//        }
//    } else {
//        rw_set_.insert(std::make_pair(location, std::make_pair(RWType::READ_OWN, cstamp)));
//    }
//}

bool TransactionContext::RecordUpdate(const RecordMeta &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it != rw_set_.end()) {
        rw_type = it->second;
        if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
            is_written_ = true;
            it->second = RWType::UPDATE;
            return false;
        }
        if (rw_type == RWType::UPDATE) {
            return false;
        }
        if (rw_type == RWType::INSERT) {
            return false;
        }
        if (rw_type == RWType::DELETE) {
            assert(false);
            return false;
        }
        assert(false);
    } else {
        rw_set_.insert(std::make_pair(location, RWType::UPDATE));
        return true;
    }

}

void TransactionContext::RecordInsert(const RecordMeta &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it != rw_set_.end()) {
        rw_type = it->second;
        auto itr_meta = it->first;
//        char *k_rhs = reinterpret_cast<char *>(itr_meta.GetNodePtr()) +
//                reinterpret_cast<RecordMetadata *>(itr_meta.meta_ptr)->GetOffset();
//        uint64_t key_rhs = *reinterpret_cast<const uint64_t *>(k_rhs);
//
//        char *k_  = reinterpret_cast<char *>(location.GetNodePtr()) +
//                      reinterpret_cast<RecordMetadata *>(location.meta_ptr)->GetOffset();
//        uint64_t key_  = *reinterpret_cast<const uint64_t *>(k_);
//        LOG_DEBUG("txn insert, itr key: %lu, curr key: %lu", key_rhs, key_);
        assert(false);
    } else {
        rw_set_.insert(std::make_pair(location, RWType::INSERT));
        ++insert_count_;
    }

}

bool TransactionContext::RecordDelete(const RecordMeta &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it != rw_set_.end()) {
        rw_type = it->second;
        if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
            it->second = RWType::DELETE;
            // record write
            is_written_ = true;
            return false;
        }
        if (rw_type == RWType::UPDATE) {
            it->second = RWType::DELETE;
            return false;
        }
        if (rw_type == RWType::INSERT) {
            it->second = RWType::INS_DEL;
            --insert_count_;
            return true;
        }
        if (rw_type == RWType::DELETE) {
//            assert(false);
            return false;
        }
        assert(false);
    } else {
        rw_set_.insert(std::make_pair(location, RWType::DELETE));
        return true;
    }

}

const std::string TransactionContext::GetInfo() const {
    return "";
    //std::ostringstream os;

//    os << " Txn :: @" << this << " ID : " << std::setw(4) << txn_id_
//       << " Read ID : " << std::setw(4) << read_id_
//       << " Commit ID : " << std::setw(4) << commit_id_
//       << " Result : " << (int)result_;

    //return os.str();
}
}