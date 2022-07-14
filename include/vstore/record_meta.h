//
// Created by zhangqian on 2021/10/29.
//

#ifndef MVSTORE_RECORD_MANAGER_H
#define MVSTORE_RECORD_MANAGER_H


#include "../common/status.h"
#include "../common/spin_latch.h"
#include "../common/catalog.h"
#include "../common/object_pool.h"
#include "../common/manager.h"
#include "../common/logger.h"
#include "../common/ephemeral_pool.h"
#include "../common/env.h"

namespace mvstore{


enum LogRecordType {
    INVALID =0,
    BEGIN_TXN ,
    COMMIT_TXN,
    ABORT_TXN,
    UPDATE,
    INSERT,
    DELETE,
};
struct RecordMetadata{
    //64 bits
    uint64_t meta;
    //64 bits
    uint64_t next_ptr;

    ~RecordMetadata() {};
//    RecordMetadata (const RecordMetadata& x) : meta(x.meta), next_ptr(x.next_ptr) {};  // copy ctor

    bool IsNull() const {
        return (meta == std::numeric_limits<uint64_t>::max() &&
                next_ptr == std::numeric_limits<uint64_t>::max());
    }

    bool operator==(const RecordMetadata &rhs) const {
        return meta == rhs.meta && next_ptr == rhs.next_ptr;
    }

    bool operator!=(const RecordMetadata &rhs) const {
        return !operator==(rhs);
    }

    RecordMetadata() : meta(0),next_ptr(0) {}
    explicit RecordMetadata(uint64_t meta) : meta(meta), next_ptr(0) {}
    RecordMetadata(uint64_t meta, uint64_t next) : meta(meta), next_ptr(next) {}
    //1.1-TxnMask inserting, 0-TxnMask not inserting
    //2.1-visible, 0-not visible
    //3.key length
    //4.record offset
    //5.TxnContext,
    //  commit id of the txn that creats the record version
    static const uint64_t kControlMask = uint64_t{0x1} << 63;          // Bits 64
    static const uint64_t kVisibleMask = uint64_t{0x1} << 62;          // Bit 63
    static const uint64_t kKeyLengthMask = uint64_t{0x2FFF} << 48;     // Bits 62-49
    static const uint64_t kOffsetMask = uint64_t{0xFFFF} << 32;        // Bits 48-33
    static const uint64_t kTxnContextMask = uint64_t{0xFFFFFFFF};      // Bits 32-1


    inline bool IsVacant() { return meta == 0; }

    inline bool TxnContextIsRead() const {
        bool t_i_r = (meta & kControlMask) > 0;
        return t_i_r;
    }

    inline uint16_t GetKeyLength() const { return (uint16_t) ((meta & kKeyLengthMask) >> 48); }

    // Get the padded key length from accurate key length
    inline uint16_t GetPaddedKeyLength() const{
        auto key_length = GetKeyLength();
        return PadKeyLength(key_length);
//        return key_length;
    }

    static inline constexpr uint16_t  PadKeyLength(uint16_t key_length) {
        return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
    }

    inline uint16_t GetOffset() { return (uint16_t) ((meta & kOffsetMask) >> 32); }

    inline void SetOffset(uint16_t offset) {
        meta = (meta & (~kOffsetMask)) | (uint64_t{offset} << 32);
    }

    inline bool IsVisible() {
        bool i_v = (meta & kVisibleMask) > 0;
        return i_v;
    }

    //when the record is deleted, visible=false
    //1-visible, 0-not visible
    inline void SetVisible(bool visible) {
        if (visible) {
            meta = meta | kVisibleMask;
        } else {
            meta = meta & (~kVisibleMask);
        }
    }

    inline void PrepareForInsert(uint64_t offset, uint64_t key_len, uint32_t comm_id) {
        // visible but inserting
        meta =  (key_len << 48) | (offset << 32) | comm_id;
        meta =  meta | (kControlMask) | (kVisibleMask) ;

        assert(IsInserting());
    }

    inline void PrepareForUpdate() {
        meta =  meta | (kControlMask) | (kVisibleMask) ;

        assert(IsInserting());
    }

    inline void FinalizeForInsert(uint64_t offset, uint64_t key_len, uint64_t commit_id) {
        // Set the visible bit,the actual offset,key length, transaction commit id
        meta =  (key_len << 48) | (uint64_t{1} << 62)| (offset << 32) | commit_id;
        meta = meta & (~kControlMask);

        auto g_k_l = GetKeyLength();

        assert(g_k_l == key_len);
        assert(!IsInserting());
    }

    inline void FinalizeForInsert(uint32_t commit_id) {
        meta = (meta & (~kTxnContextMask)) | commit_id;
        meta =  meta & (~kControlMask);

        assert(!IsInserting());
    }

    inline void FinalizeForUpdate(uint32_t sstamp) {
        meta = (meta & (~kTxnContextMask)) | sstamp;
        meta = meta & (~kControlMask);

        assert(!IsInserting());
    }

    inline void FinalizeForRead(uint32_t pstamp) {
        meta = (meta & (~kTxnContextMask)) | pstamp;
        meta = meta & (~kControlMask);

        assert(!IsInserting());
    }

    inline void FinalizeForDelete() {
        // Set the transaction commit id
//        uint64_t commit_id = txn->GetCommitId();
        meta = 0;
    }

    inline void NotInserting() {
        meta = meta | (uint64_t{1} << 63);
    }

    inline bool IsInserting() {
        // record is not deleted and is writing
        return (IsVisible()) && TxnContextIsRead();
    }

    //create timestamp of the tuple latest version cstamp
    inline cid_t GetTxnCommitId() const {
        return (uint32_t) (meta & kTxnContextMask);
    }

    inline uint64_t GetNextPointer() const {
        return  next_ptr;
    }

    inline void SetNextPointer(uint64_t tuple_header) {
        next_ptr = tuple_header;
    }

};


struct CopyRecordMeta {
    RecordMetadata *meta;
    RecordMetadata *pre;

    CopyRecordMeta(){}

    CopyRecordMeta(RecordMetadata *meta_, RecordMetadata *pre_) {
        meta = meta_;
        pre = pre_;
    }

    inline RecordMetadata *GetPre() { return pre; }

    inline RecordMetadata *GetMeta() { return meta; }
};

struct RecordMeta{
    uint64_t block_ptr;
    oid_t table_id;
    RecordMetadata meta_data;
    uint64_t meta_ptr;
    uint64_t node_header_ptr;
    //key+payload
    uint32_t total_size;
    uint64_t next_tuple_ptr;
    cid_t cstamp;

    RecordMeta():block_ptr(0), table_id(0), meta_ptr(0),
                node_header_ptr(0), total_size(0),
                next_tuple_ptr(0), cstamp(0){}
    RecordMeta(uint64_t block_ptr_, oid_t table_id_, RecordMetadata &meta_data_) :
            block_ptr(block_ptr_), table_id(table_id_), meta_data(meta_data_.meta),
            next_tuple_ptr(0),node_header_ptr(0){
        meta_data.SetNextPointer(meta_data_.next_ptr);
    }
    ~RecordMeta()=default;
    bool operator==(const RecordMeta &rhs) const{
        bool equal = block_ptr == rhs.block_ptr && table_id == rhs.table_id
                     && meta_data.meta == rhs.meta_data.meta;

        return equal;
    }
    bool operator!=(const RecordMeta &rhs) const {
        return !operator==(rhs);
    }
    inline RecordMetadata GetMetaData() {
        return meta_data;
    }
    inline void SetMetaPtr(uint64_t ptr){
        meta_ptr = ptr;
    }
    inline RecordMetadata *GetMetaPtr() const{
        return reinterpret_cast<RecordMetadata *>(meta_ptr);
    }
    inline uint64_t GetMetaPtrVal() const{
        return  meta_ptr;
    }
    inline void SetNodeHdPtr(uint64_t ptr){
        node_header_ptr = ptr;
    }
//    inline NodeHeader *GetNodeHdPtr() const{
//        return reinterpret_cast<NodeHeader *>(node_header_ptr);
//    }
    inline char *GetNodeHdPtr() const{
        return reinterpret_cast<char *>(node_header_ptr);
    }
    inline char *GetNodePtr() const{
        return reinterpret_cast<char *>(block_ptr);
    }
    inline void SetTotalSize(uint32_t sz){
        total_size = sz;
    }
    inline uint32_t GetTotalSize() const{
        return total_size;
    }
    inline void SetCstamp(cid_t comm_id){
        cstamp = comm_id;
    }
    inline cid_t GetCstamp() const{
        return cstamp;
    }
    inline void SetNextTuplePtr(uint64_t ptr){
        next_tuple_ptr = ptr;
    }
    inline uint64_t GetNextTupleHeader() const{
        return next_tuple_ptr;
    }

};

class LogRecord {

public:

//    char *GetData() const{ return data;}

    LogRecordType GetType() const { return type_; }

    LSN_T *GetIndirectLSN() const { return const_cast<LSN_T *>(indirect_lsn_); }

    cid_t GetBeginID() const { return begin_id_; }
    cid_t GetCommitID() const { return end_tid_; }

    inline void SetCommitId(cid_t end_commit_id) {
        //reset the commit id by commit_transaction
        this->end_tid_ = end_commit_id;
    }

    char *GetNextTupleSlot() const { return tuple_slot_; }

    const char *GetDeltaData() const{ return delta_;}

    uint32_t GetLength() const{ return len_;}

    uint8_t *GetBits() const{return bits_;}

    const Catalog *GetTupleCatalog() const{ return catalog_;}

    inline void WriteLogData(char *data_ptr){
        if(type_ == LogRecordType::BEGIN_TXN || type_ ==LogRecordType::COMMIT_TXN ||
           type_ == LogRecordType::ABORT_TXN){
            uint32_t sz = sizeof(uint16_t)+sizeof(uint32_t)*2;
            size_t ed_size = 0;
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->type_)), sizeof(uint16_t)) ;
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->begin_id_)), sizeof(uint32_t)) ;
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->end_tid_)), sizeof(uint32_t)) ;
            PELOTON_ASSERT(ed_size == sz,"Log write data begin/abort/comm fail.");
        } else if(type_ == LogRecordType::UPDATE){
            uint32_t bit_sz =0;
            uint32_t sz = 0;
            if (this->bits_ != nullptr){
//                uint16_t filed_count = catalog_->field_cnt;
//                bit_sz = sizeof(uint8_t)*filed_count;
                bit_sz = sizeof(uint64_t);
                sz = sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint64_t)
                     +sizeof(uint16_t) + len_ + bit_sz +sizeof(uint64_t)
                     +sizeof(uint64_t)+sizeof(uint64_t);
            }else{
                sz = sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)
                     +len_
                     +sizeof(uint64_t)+sizeof(uint64_t);
            }

            size_t ed_size = 0;
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->type_)), sizeof(uint16_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->begin_id_)), sizeof(uint32_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->end_tid_)), sizeof(uint32_t));
            ed_size += WriteEntry(data_ptr+ed_size, this->delta_,  this->len_);
            if(this->bits_ != nullptr) {
                uint64_t tp_slot_ptr = reinterpret_cast<uint64_t>(this->tuple_slot_);
                ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(tp_slot_ptr)), sizeof(uint64_t));
                ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->len_)), sizeof(uint16_t));
//                ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(this->bits_), bit_sz);
                uint64_t bits_ptr = reinterpret_cast<uint64_t>(this->bits_);
                ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(bits_ptr)), bit_sz);
                uint64_t tp_catlog_ptr = reinterpret_cast<uint64_t>(this->catalog_);
                ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(tp_catlog_ptr)), sizeof(uint64_t));
            }

            //indirect_lsn
            uint64_t pre_lsn_ptr =  this->pre_lsn_;
            uint64_t lsn_ptr = reinterpret_cast<uint64_t>(this->indirect_lsn_);
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(pre_lsn_ptr)), sizeof(uint64_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(lsn_ptr)), sizeof(uint64_t));
            PELOTON_ASSERT(ed_size == sz,"Log write data update fail.");

            //update the install tuple version data
//            for (uint16_t i=0; i< filed_count; ++i){
//                auto j = bits_[i];
//                if (j==1){
//                    auto off = this->catalog_->get_field_index(i);
//                    auto sz = this->catalog_->get_field_size(i);
//                    PELOTON_MEMCPY(this->tuple_slot_+off, this->payload_location_, sz);
//                }
//            }
            if (!nvm_emulate){
                NVMUtilities::persist(data_ptr, sz);
                LOG_DEBUG("nvm log psersist.");
            }

        } else if(type_ == LogRecordType::INSERT){
            uint32_t sz = sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)
                          +sizeof(uint16_t)+ this->len_+ sizeof(uint64_t)
                          +sizeof(uint64_t);
            size_t ed_size = 0;
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->type_)), sizeof(uint16_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->begin_id_)), sizeof(uint32_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->end_tid_)), sizeof(uint32_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->len_)), sizeof(uint16_t));
            ed_size += WriteEntry(data_ptr+ed_size, this->delta_, this->len_);
            uint64_t tp_catlog_ptr = reinterpret_cast<uint64_t>(this->catalog_);
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(tp_catlog_ptr)), sizeof(uint64_t));
            //indirect_lsn
            uint64_t lsn_ptr = reinterpret_cast<uint64_t>(this->indirect_lsn_);
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(lsn_ptr)), sizeof(uint64_t));
            PELOTON_ASSERT(ed_size == sz,"Log write data insert fail.");
            if (!nvm_emulate){
                NVMUtilities::persist(data_ptr, sz);
                LOG_DEBUG("nvm log psersist.");
            }

        } else if(type_ == LogRecordType::DELETE){
            uint32_t sz = sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)
                          +sizeof(uint64_t) +sizeof(uint64_t)
                          +sizeof(uint64_t);
            size_t ed_size = 0;
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->type_)), sizeof(uint16_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->begin_id_)), sizeof(uint32_t));
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(this->end_tid_)), sizeof(uint32_t));
            uint64_t tp_slot_ptr = reinterpret_cast<uint64_t>(this->tuple_slot_);
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(tp_slot_ptr)), sizeof(uint64_t));
            uint64_t tp_catlog_ptr = reinterpret_cast<uint64_t>(this->catalog_);
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(tp_catlog_ptr)), sizeof(uint64_t));
            //indirect_lsn
            uint64_t lsn_ptr = reinterpret_cast<uint64_t>(this->indirect_lsn_);
            ed_size += WriteEntry(data_ptr+ed_size, reinterpret_cast<const char *>(&(lsn_ptr)), sizeof(uint64_t));

            PELOTON_ASSERT(ed_size == sz,"Log write data delete fail.");
        }
    }

    inline size_t Size() const{
        if(type_ == LogRecordType::BEGIN_TXN || type_ == LogRecordType::COMMIT_TXN ||
           type_ == LogRecordType::ABORT_TXN){
            return sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t);
        } else if(type_ == LogRecordType::UPDATE){
            uint32_t bit_sz =0;
            uint32_t sz = 0;
            if (this->bits_ != nullptr){
                uint16_t filed_count = catalog_->field_cnt;
                bit_sz = sizeof(uint8_t)*filed_count;
                sz = sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint64_t)
                     +sizeof(uint16_t) +len_+ bit_sz +sizeof(uint64_t)
                     +sizeof(uint64_t)+sizeof(uint64_t);
            }else{
                sz = sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)
                     +len_
                     +sizeof(uint64_t)+sizeof(uint64_t);
            }
            return sz;
        } else if(type_ == LogRecordType::INSERT){
            return sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)+ sizeof(uint16_t)+
                   this->len_ + sizeof(uint64_t)
                   +sizeof(uint64_t);
        } else if(type_ == LogRecordType::DELETE){
            return sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint64_t)+sizeof(uint64_t)
                   +sizeof(uint64_t);
        }
        return 0;
    }

    // insert
    void init(LogRecordType type, const LSN_T *indirect_lsn, const char *delta,
              cid_t begin_id, uint16_t len, Catalog *catalog){
        this->type_ = type;
        this->indirect_lsn_ = indirect_lsn;
//        this->tid_ = tid;
        this->delta_ = delta;
        this->len_ = len;
        this->catalog_ = catalog;
        this->begin_id_ = begin_id;
        this->end_tid_ = begin_id;

//        this->cur_wr_size = 0;
//        this->payload_location_ = payload_loation;
    }
    //update
    void init(LogRecordType type, uint64_t pre_lsn, const LSN_T *indirect_lsn,
              char *payload_loation,
              cid_t begin_id, char *tuple_slot,
              const char *delta, uint16_t len, uint8_t *bits,
              Catalog *catalog){
        this->type_ = type;
        this->pre_lsn_ = pre_lsn;
        this->indirect_lsn_ = indirect_lsn;
//        this->tid_ = tid;
        this->tuple_slot_ = tuple_slot;
        this->delta_ = delta;
        this->len_ = len;
        this->bits_ = bits;
        this->catalog_ = catalog;
        this->payload_location_ = payload_loation;
        this->begin_id_ = begin_id;
        this->end_tid_ = begin_id;
//        this->cur_wr_size = 0;
    }
    //delete
    void init(LogRecordType type, const LSN_T *indirect_lsn, char *tuple_slot,
              Catalog *catalog,  cid_t begin_id ){
        this->type_ = type;
        this->indirect_lsn_ = indirect_lsn;
//        this->tid_ = tid;
        this->tuple_slot_ = tuple_slot;
        this->catalog_ = catalog;
        this->begin_id_ = begin_id;
        this->end_tid_ = begin_id;
//        this->cur_wr_size = 0;
    }
    //begin/commit/abort
    void init(LogRecordType type, const LSN_T *indirect_lsn, cid_t begin_id, cid_t end_id){
        this->type_ = type;
        this->indirect_lsn_ = indirect_lsn;
        this->begin_id_ = begin_id;
        this->end_tid_ = end_id;
//        this->cur_wr_size = 0;
    }

    size_t WriteEntry(char *data_ptr, const char *sr_ptr, size_t size_){
        if(nvm_emulate){
            std::memcpy(data_ptr, sr_ptr, size_);
        }else{
            memcpy(data_ptr, sr_ptr, size_);
        }

        return size_;
    }

    LogRecord(LogRecordType typ) :  type_(typ),indirect_lsn_(nullptr),
                                    begin_id_(INVALID_CID),end_tid_(INVALID_CID),tuple_slot_(nullptr),
                                    delta_(nullptr),payload_location_(nullptr),len_(0),bits_(nullptr),
                                    catalog_(nullptr){  }

    LogRecord():  type_(LogRecordType::INVALID),indirect_lsn_(nullptr),
                  begin_id_(INVALID_CID),end_tid_(INVALID_CID),tuple_slot_(nullptr),
                  delta_(nullptr),payload_location_(nullptr),len_(0),bits_(nullptr),
                  catalog_(nullptr){}

private:
//    std::mutex latch;
//    size_t cur_wr_size = 0;
    //|begin|abort|commit|update|insert|delete|
    LogRecordType type_;
    //indirect array offset
    uint64_t pre_lsn_;
    const LSN_T *indirect_lsn_;
    cid_t begin_id_;
    //(insert/update/delete)end commit id,
    cid_t end_tid_;
    //install version tuple slot pointer
    char *tuple_slot_;
    //delta varlen data
    const char *delta_;
    //insert/update payload location
    char *payload_location_;
    //delta varlen length
    uint16_t len_;
    //delta schema
    uint8_t *bits_;
    //schema pointer of the related tuple version pointer
    const Catalog *catalog_;

};

}

#endif
