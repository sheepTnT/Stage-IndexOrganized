//
// Created by zhangqian on 2021/10/29.
//

#ifndef MVSTORE_STORAGE_MANAGER_H
#define MVSTORE_STORAGE_MANAGER_H

#include <unordered_map>
#include <unordered_set>
#include "../common/status.h"
#include "../common/spin_latch.h"
#include "../common/concurrent_queue.h"
#include "../common/bitmap.h"
#include "../common/bitmaps.h"
#include "../common/catalog.h"
#include "../common/object_pool.h"
#include "../common/manager.h"
#include "../common/logger.h"
#include "../common/ephemeral_pool.h"
#include "execute/txn_context.h"
#include "tbb/concurrent_hash_map.h"

namespace mvstore{

typedef CuckooMap<cid_t, std::vector<TxnVersionInfo>> GCSet;

struct TupleHeader {
    //current header (index)offset in this block
    oid_t header_id_;
    //transaction id who create it
    cid_t begin_id_;
    //transaction id who overwrite it
    cid_t comm_id_;
    // next version
    uint64_t next_;
    // previous version
    uint64_t pre_;
    // log record entry, which store the delta update
    uint64_t log_entry_;
    // tuple offset in the block
    uint64_t tuple_slot_;
    // gc will get latch, and update the commi_id = INVALID_CID
//    SpinLatch latch;

    TupleHeader() : header_id_(INVALID_OID), begin_id_(INVALID_CID), comm_id_(INVALID_CID),
                    next_(0), pre_(0), log_entry_(0), tuple_slot_(0) { }

    static inline TupleHeader *New(char *data) {
        uint32_t thd_size = sizeof(TupleHeader);
        char *n_hd = new char[thd_size];
        VSTORE_MEMSET(n_hd, 0 , thd_size);
        VSTORE_MEMCPY(n_hd, data, thd_size);
        return reinterpret_cast<TupleHeader *>(reinterpret_cast<void *>(n_hd));
    }

    ~TupleHeader() = default;

    inline void free(uint32_t size_) {
//        VSTORE_MEMSET(this, 0 , size_);
    }
    inline void init(TupleHeader *tp_hd, uint64_t tp_slot, oid_t offset) {
        tp_hd->SetHdId(offset);
        tp_hd->SetBeginId(INVALID_CID);
        tp_hd->SetCommitId(INVALID_CID);
        tp_hd->SetNextHeaderPointer(0);
        tp_hd->SetPreHeaderPointer(0);
        tp_hd->SetLogEntryHeaderPointer(0);
        tp_hd->SetTupleSlot(tp_slot);
    }
    inline void reset() {
        this->header_id_= INVALID_OID;
        this->begin_id_ = INVALID_CID;
        this->comm_id_ = INVALID_CID;
        this->next_ = 0;
        this->pre_ = 0;
        this->log_entry_ = 0;
        this->tuple_slot_ = 0;
    }

    inline void SetHdId(oid_t tuple_header_offset){
        header_id_ = tuple_header_offset;
    }
    //index(offset) of the current tuple header
    inline oid_t GetHdId(){
        return header_id_;
    }
    inline void SetBeginId(cid_t  begin_)    {
        begin_id_ = begin_;
    }

    inline void SetCommitId(cid_t  end_)    {
        comm_id_ = end_;
    }
    inline void SetNextHeaderPointer(uint64_t next__) {
        next_ = next__;
    }
    inline void SetPreHeaderPointer(uint64_t pre__) {
        pre_ = pre__;
    }
    inline void SetLogEntryHeaderPointer(uint64_t log_entry) {
        log_entry_ = log_entry;
    }
    inline void SetTupleSlot(uint64_t tuple_slot) {
        tuple_slot_ = tuple_slot;
    }

//    SpinLatch &GetSpinLatch()   {
//        return latch;
//    }

    inline cid_t GetBeginId() {
        return begin_id_;
    }

    inline cid_t GetEndCommitId() {
        return comm_id_;
    }

    inline uint64_t GetNextHeaderPointer() {

        return  next_;
    }

    inline uint64_t GetPreHeaderPointer() {

        return  pre_;
    }

    inline void *GetLogEntryHeaderPointer() {

        return reinterpret_cast<void *>(log_entry_);
    }

    inline uint64_t GetTupleSlot() {

        return  tuple_slot_;
    }

};


struct NodeHeader {
    // Header:
    // |-----64 bits----|---32 bits---|---32 bits---|
    // |   status word  |     size    | sorted count|
    //
    // Sorted count is actually the index into the first metadata entry for
    // unsorted records. Following the header is a growing array of record metadata
    // entries.

    // 64-bit status word subdivided into five fields. Internal nodes only use the
    // first two (control and frozen) while leaf nodes use all the five.
    struct StatusWord {
        uint64_t word;

        StatusWord() : word(0) {}

        explicit StatusWord(uint64_t word) : word(word) {}

        static const uint64_t kControlMask = uint64_t{0x7} << 61;           // Bits 64-62
        static const uint64_t kFrozenMask = uint64_t{0x1} << 60;            // Bit 61
        static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44;    // Bits 60-45
        static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22;    // Bits 44-23
        static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0;    // Bits 22-1

        inline StatusWord Freeze() {
            return StatusWord{word | kFrozenMask};
        }

        inline bool IsFrozen() {
            bool is_frozen = (word & kFrozenMask) > 0;
            return is_frozen;
        }

        inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 44); }

        inline void SetRecordCount(uint16_t count) {
            word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44);
        }

        inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 22); }

        inline void SetBlockSize(uint32_t sz) {
            word = (word & (~kBlockSizeMask)) | (uint64_t{sz} << 22);
        }

        inline uint32_t GetDeletedSize() { return (uint32_t) (word & kDeleteSizeMask); }

        inline void SetDeleteSize(uint32_t sz) {
            word = (word & (~kDeleteSizeMask)) | uint64_t{sz};
        }

        inline void PrepareForInsert(uint32_t sz) {
            PELOTON_ASSERT(sz > 0, "PrepareForInsert fail, sz <=0. ");
            // Increment [record count] by one and [block size] by payload size(total size)
            word += ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
        inline void FailForInsert(uint32_t sz) {
            // decrease [record count] by one and [block size] by payload size
            word -= ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
    };

    uint32_t size;
    uint32_t sorted_count;
    //record insert from the foot of the node
    std::atomic<oid_t> next_record_slot = ATOMIC_VAR_INIT(size - 1);
    StatusWord status;

    NodeHeader() : size(0), sorted_count(0) {}

    inline StatusWord GetStatus() {
        return status;
    }
};


struct BlockMeta{
    oid_t table_id_;
    oid_t block_id_;
    uint32_t total_tuple_count_;
    uint32_t tuple_length_;
    uint32_t block_size;
    // next free tuple slot
    std::atomic<oid_t> next_tuple_slot;

    BlockMeta():block_size(0){}
    BlockMeta(uint32_t table_id,uint32_t block_id,
              uint32_t total_tuple_count, uint32_t tuple_length,
              uint32_t size): table_id_(table_id),block_id_(block_id),total_tuple_count_(total_tuple_count),
              next_tuple_slot(0),block_size(size){ }

    ~BlockMeta(){ }


    inline uint32_t GetTableId(){
        return table_id_;
    }
    inline uint32_t GetBlockId(){
        return block_id_;
    }
    inline uint32_t GetTotalTupleCount(){
        return total_tuple_count_;
    }
    inline uint32_t GetActiveTupleCount(){
        return next_tuple_slot;
    }
    inline uint32_t GetTupleLength(){
        return tuple_length_;
    }

    inline oid_t GetNextEmptyTupleSlot() {
        if (next_tuple_slot >= total_tuple_count_) {
            return INVALID_OID;
        }

        oid_t tuple_slot_id =
                next_tuple_slot.fetch_add(1, std::memory_order_relaxed);

        if (tuple_slot_id >= total_tuple_count_) {
            return INVALID_OID;
        } else {
            return tuple_slot_id;
        }
    }

};


/**
 * BaseTuple: all tuple derive from this base tuple
 * all value are inlined.
 * get a value by computing the offset and length
 */
class BaseTuple {
public:
    BaseTuple() {} ;
    // Setup the tuple given a schema
    BaseTuple(Catalog *schema)
            : tuple_schema_(schema), tuple_data_(nullptr) {
        assert(tuple_schema_);
        tuple_data_ = new char[schema->tuple_size];
    }
    BaseTuple(char *data)
            :  tuple_data_(data) {
    }

    ~BaseTuple() = default;
    Catalog *GetSchema() const { return tuple_schema_; }

    // Get the address of this tuple in the table's backing store
    const char *GetData() const { return tuple_data_; }

    /**
     * Set the tuple to point toward a given address in a table's
     * backing store
     */
    void Move(void *address) {
        tuple_data_ = reinterpret_cast<char *>(address);
    }

    // For an insert, the copy do copy from the source location
    void Copy(const void *source) {
        memcpy(tuple_data_, source, tuple_schema_->tuple_size);
    }

    // Get the value of a specified column (const)
    // input: column id
    // return: the position (index)
    char *GetValue(oid_t column_id) const;

    void SetValue(const oid_t column_id, const char *value) const;

    bool operator==(const BaseTuple &other) const;
    bool operator!=(const BaseTuple &other) const;

protected:
    char *tuple_data_;
    Catalog *tuple_schema_;
};

/**
 * the record is written to stable storage conforming the following format:
 */
struct LogRecordInsert{
public:

    LogRecordInsert(){}

    // insert
    LogRecordInsert(LogRecordType type, LSN_T indirect_lsn, uint64_t delta,
              cid_t begin_id, uint16_t len, uint64_t catalog){
        this->type_ = type;
        this->indirect_lsn_ = indirect_lsn;
//        this->tid_ = tid;
        this->delta_ptr_ = delta;
        this->len_ = len;
        this->table_catalog_ptr_ = catalog;
        this->begin_id_ = begin_id;
        this->end_tid_ = begin_id;
//        this->payload_location_ = payload_loation;
    }

    ~LogRecordInsert() = default;

    LSN_T GetIndirectLSN() const { return indirect_lsn_; }
    void SetIndirectLSN(LSN_T lsn) { this->indirect_lsn_ = lsn;}

    LogRecordType GetType() const { return type_; }
    void SetType(LogRecordType type){
        this->type_ = type;
    }

    cid_t GetBeginID() const { return begin_id_; }
    void SetBegin(cid_t begin_id){
        this->begin_id_ = begin_id;
    }

    const char *GetDeltaData() const{
        return reinterpret_cast<const char *>(delta_ptr_);
    }
    void SetDeltaPtr(uint64_t delta_ptr){
        this->delta_ptr_ = delta_ptr;
    }

    uint32_t GetLength() const{ return len_;}
    void SetLength(uint16_t length){
        this->len_ = length;
    }

    Catalog *GetTableCatalog() const{
        return reinterpret_cast<Catalog *>(table_catalog_ptr_);
    }
    void SetTableCatalog(uint64_t table_schema){
        this->table_catalog_ptr_ = table_schema;
    }

private:
    //|begin|abort|commit|update|insert|delete|
    LogRecordType type_;
    //indirect array offset
    LSN_T indirect_lsn_;
    cid_t begin_id_;
    //(insert/update/delete)end commit id,
    cid_t end_tid_;
    //delta varlen data
    uint64_t delta_ptr_;
    //insert/update payload location
    //delta varlen length
    uint16_t len_;
    //schema pointer of the related tuple version pointer
    uint64_t table_catalog_ptr_;

};

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * this only used by initializing a nodebase
 */
class alignas(DRAM_BLOCK_SIZE) DramBlock {
//class  DramBlock {
public:

    /**
     * Contents of the dram block.
     */
    char content_[DRAM_BLOCK_SIZE];

};

/**
* Custom allocator used for the object pool of buffer segments
*/
class DramBlockAllocator {
public:
    /**
     * Allocates a new object by calling its constructor.
     * @return a pointer to the allocated object.
     */
    DramBlock *New( ) { return new DramBlock() ; }

    /**
     * Reuse a reused chunk of memory to be handed out again
     * @param reused memory location, possibly filled with junk bytes
     */
    void Reuse(DramBlock *const reused) {
        /* no operation required */
    }

    /**
     * Deletes the object by calling its destructor.
     * @param ptr a pointer to the object to be deleted.
     */
    void Delete(DramBlock *const ptr) { delete ptr; }
};

using DramBlockPool = ObjectPool<DramBlock, DramBlockAllocator>;

/**
 * Version block is defined size
 * Get empty tuple slot
 * Insert tuple
 * Get tuple value
 * Get a specified value
 */
class VersionBlock {

protected:
    oid_t block_id_;
    //when gc the block, must freeze it
    bool freeze = false;
    //catalog holds the column number of the tuple
    const Catalog *catalog_;
    std::atomic<oid_t> free_pos = ATOMIC_VAR_INIT(0);
    char *block_data;

public:
    /**
     * version block size is defined 1MB
     */
    explicit VersionBlock(char **block, Catalog *schema){
        // initialization version block
        block_data = new char[NVM_BLOCK_SIZE];
        *block = block_data;
        // zero out the data
        memset(*block, 0, NVM_BLOCK_SIZE);

        catalog_ = schema;
    }

    // for the version tuples
    VersionBlock(oid_t table_id,
                 oid_t block_id, char **block,
                 const Catalog *schema): block_id_(block_id), catalog_(schema){
        // initialization version block
        block_data = *block;
        // zero out the data
        memset(block_data, 0, NVM_BLOCK_SIZE);
        // block layout = |block_meta|tuple_header|tuple0,tuple1,,,,|
        // header = |begin_id 4|comm_id 4|next_ 8|pre_ 8|log_entry 8|slot 8|
        uint32_t tuple_length = schema->tuple_size;
        uint32_t tuple_hd_length = sizeof(TupleHeader);
        uint32_t total_tuple_length = tuple_length + tuple_hd_length;
        uint32_t block_meta_size = sizeof(BlockMeta) ;
        uint32_t total_tuple_count = (NVM_BLOCK_SIZE - block_meta_size) / total_tuple_length;

        //block metadata
        BlockMeta *block_meta_ = reinterpret_cast<BlockMeta *>(block_data);
        block_meta_->table_id_ = table_id;
        block_meta_->block_id_ = block_id;
        block_meta_->total_tuple_count_ = total_tuple_count;
        block_meta_->tuple_length_ = tuple_length;
        block_meta_->block_size = NVM_BLOCK_SIZE;

        //block tuple headers
        for(int i=0; i<total_tuple_count; ++i){
            char *tuple_hd_location = &block_data[block_meta_size + i * tuple_hd_length];
            TupleHeader *tuple_header = reinterpret_cast<TupleHeader *>(tuple_hd_location);
            char *indire_ = &block_data[block_meta_size +
                                        total_tuple_count * tuple_hd_length +
                                        i * tuple_length] ;
//            LOG_DEBUG("version block tuple slot: %p", indire_);
            tuple_header->init(tuple_header, reinterpret_cast<uint64_t>(indire_), i);
        }

        this->catalog_ = schema;
        this->block_id_ = block_id;
    }

    // for the log records, log table catalog
    VersionBlock(oid_t block_id, char **block, const Catalog *schema):
                                        block_id_(block_id), catalog_(schema) {
        // initialization version block
        block_data = *block;
        // zero out the data
        memset(block_data, 0, NVM_BLOCK_SIZE);
        // version block size = 1MB
        // block layout = |block_id|log_table_catalog|size,log_entry0|size,log_entry1,,,,|
        memset(block_data, block_id, sizeof(uint32_t));
        memset(block_data+sizeof(uint32_t), reinterpret_cast<uint64_t>(schema), sizeof(uint64_t));

        this->catalog_ = schema;
        this->block_id_ = block_id;
        this->free_pos = sizeof(uint32_t) + sizeof(uint64_t);
    }

    ~VersionBlock() = default;

    inline uint32_t GetBlockId() const{
        return block_id_;
    }

    inline char *GetBlockData() const{
        return block_data;
    }

    inline uint32_t GetPos() const{
        return free_pos;
    }

    bool IsFrozen(){
        return freeze;
    }
    void Freeze(){
        freeze = true;
    }
    /**
     * Insert tuple at slot
     * NOTE : No checks, must be at valid slot.
     * return: the tuple offset
     */
    TupleHeader *InsertTuple(const char *tuple);
    static bool InsertLogEntry(LogRecord *log_entry, char *location);

    /**
     *
     * @param tuple_offset, the index of the tuple in this block
     * @param column_id, requesred column
     * @return , value of the column, char []
     */
    void GetValue(const oid_t tuple_offset, char *column_type,
                  uint32_t column_offset,
                  uint32_t column_length,   char **value,
                  TupleHeader &tuple_header);
    /**
     * Returns a pointer to the tuple requested. No checks are done that the index
     * is valid.
//     */
//    char *GetTupleLocation(const oid_t tuple_offset) const;
    void CopyTuple(const BaseTuple *tuple, const oid_t &tuple_slot_id);
    inline BlockMeta *GetBlockMeta() {
        return reinterpret_cast<BlockMeta *>(block_data);
    }
    /**
     * Get the header of the tuple offset
     * @param tuple_offset
     * @return
     */
    std::unique_ptr<TupleHeader> GetTupleHeader(oid_t offset){
        char *header_location = block_data + sizeof(BlockMeta) + offset*sizeof(TupleHeader);
        return std::unique_ptr<TupleHeader>(TupleHeader::New(header_location));
    }
    inline char *GetEmptyTupleHeader(oid_t offset){
        char *header_location = block_data + sizeof(BlockMeta);
        return (header_location + offset*sizeof(TupleHeader));
    }

    void CleanTupleHeader(oid_t offset, uint32_t tuple_size){
        char *header_location = block_data + sizeof(BlockMeta) + offset*sizeof(TupleHeader);
        auto header = reinterpret_cast<TupleHeader *>(header_location);
        header->free(tuple_size);
    }
    //used by log record write
    inline char *GetNextSlot(size_t entry_size, LSN_T &last_lsn){
        oid_t pos = free_pos.load(std::memory_order_relaxed);

        if(entry_size > (NVM_BLOCK_SIZE - pos)){
            return nullptr;
        }

        //fetch_add , return the previous value
        oid_t tuple_slot_ = free_pos.fetch_add((entry_size+sizeof(uint32_t)),
                                               std::memory_order_relaxed);

        if(tuple_slot_ > NVM_BLOCK_SIZE){
            return nullptr;
        }else{
            last_lsn = block_id_;
            last_lsn = (last_lsn <<32 ) | tuple_slot_;
//            LOG_DEBUG("log record write lsn: %lu, block id : %u, off : %u",last_lsn, block_id_, tuple_slot_);
            return block_data+tuple_slot_;
        }
    }

 };

using VersionBlockElem = std::pair<oid_t, char *>;
using FreeBlockSlot = std::shared_ptr<TxnVersionInfo>;

class VersionBlockManager {
/**
*  memory heap style
*/
public:
    // Global Singleton
    static VersionBlockManager *GetInstance();

    // Destroy all related db files under `db_path` but not deleting the directory.
    ~VersionBlockManager();

    Status Init();

    // add the allocted block to the cuckoo map
    void AddVersionBlock(std::shared_ptr<VersionBlock> location);

    // delete the allocted block from the cuckoo map
    void DropVersionBlock(std::shared_ptr<VersionBlock> location) ;

    // get the physical location of the version block
    VersionBlock *GetVersionBlock(oid_t block_id);

    // used for logging test
    void ClearVersionBlock() ;

    // use this function to allocate a nvm block
    VersionBlockElem AllocateBlock();
    VersionBlockElem AllocateLogBlock();

    // use this function to deallocate a block
    void DeallocateBlock(void *p);
    oid_t GetNextVersionBlockId() { return ++version_block_oid_; }
    oid_t GetCurrentVersionBlockId() { return version_block_oid_; }
//    void RegisterThread(const size_t thread_id)   {
//        lock_.Lock();
//
//        local_clean_set_->operator[](thread_id) = {};
//
//        lock_.Unlock();
//    }
//
//    void DeregisterThread(const size_t thread_id)   {
//        lock_.Lock();
//
//        local_clean_set_->erase(thread_id);
//
//        lock_.Unlock();
//    }

    /**
     * A transaction enters local cleaner with end_commit_id
     * @param thread_id, local thread id
     * @param end_commit_id, txn end commit id
     * @param gc_type, garbage version produced by txn type
     * @param physical_location, old version physical location NVM
     * @return
     */
    void EnterCleaner(const cid_t end_commit_id,
                       const GCVersionType gc_type, void *physical_locatioin,
                       uint32_t tuple_header_size, oid_t table_id, uint64_t block_ptr) ;

    /**
     * A transaction exits local cleaner with end_commit_id
     * @param thread_id
     * @param end_commit_id
     */
    void ExitCleaner(const cid_t end_commit_id) ;

    FreeBlockSlot GetFreeSlot(oid_t table_id){
        if (local_available_.size() <=0 ){return nullptr;}
        tbb::concurrent_queue<FreeBlockSlot> f_b_ss = local_available_[table_id];
        if (f_b_ss.empty()){return nullptr;}
        FreeBlockSlot f_b_s;

        f_b_ss.try_pop(f_b_s);
        return f_b_s;
    }

    int CollectOldVersionSet( const cid_t min_active_id);

    int CleanCandidateSlot(const int thread_id);

    void DeallocateNVMSpace();

private:
    VersionBlockManager();

    std::atomic<oid_t> version_block_oid_ = ATOMIC_VAR_INIT(START_OID);
    // hold the block id and block location
    std::vector<std::shared_ptr<VersionBlock>> version_block_array_;
    std::vector<std::shared_ptr<VersionBlock>>::iterator version_block_iterator_;
    static std::shared_ptr<VersionBlock> empty_version_block_;
    //allocate a large heap for the retiring versions
    //this large memory will be segment multiple fixed size chunks
    void *mmap_start_addr_ = NULL;
    //allocate a large heap for the redo log records
    //this large memory will be segment multiple fixed size chunks
    //if real pmm, use the fsdax mode, to mmap a large nvm space
    char *log_mmap_start_addr_ = NULL;
    //versions block queue
    ConcurrentQueue<VersionBlockElem,std::allocator<VersionBlockElem>> blocks_queue;
    //log block queue
    ConcurrentQueue<VersionBlockElem,std::allocator<VersionBlockElem>> log_blocks_queue;
    // break the NVM File into nvm_num_block
    AtomicBitmap bitmaps;
    // offset of the current location in NVM File
    std::atomic<int> last_pos{};
    // Each thread holds a pointer to a local garbage collection set.
    // It updates the local clean to report their local time.
    SpinLatch lock_;
    //thread id, commit id, version gc
//    std::unordered_map<int, std::unordered_map<cid_t, std::vector<TxnVersionInfo>>> local_clean_set_;
    std::shared_ptr<GCSet> local_clean_set_;
    // free slot: tuple header physical location
    typedef std::queue<FreeBlockSlot> deallocated_queue;
    std::unordered_map<oid_t, tbb::concurrent_queue<FreeBlockSlot>> local_available_;
    std::unordered_map<int, std::unique_ptr<deallocated_queue>> local_candidate_available_;

};

/**
 * log record
 * log buffer (version block)
 */
class LogManager{

public:
    static LogManager *GetInstance();

    Status Init();

    ~LogManager();

    LogRecord *LogBeginTxn(cid_t begin_tid );

    LogRecord *LogUpdate(cid_t tid, char *tuple_slot, char *payload_loction,
                         const char *delta, uint16_t len, uint8_t *bits, Catalog *record_catalog);
    LogRecord *LogDelta(cid_t begin_tid, uint64_t pre_lsn, const char *delta);
    LogRecord *LogInsert(cid_t begin_tid, const char *delta, uint16_t len,
                         Catalog *record_catalog);

    LogRecord *LogDelete(cid_t tid, char *tuple_slot, Catalog *record_catalog);

    LogRecord *LogCommitTxn(cid_t begin_tid, cid_t end_tid);

    LogRecord *LogAbortTxn(cid_t begin_tid, cid_t end_tid);

    static void LogWrite(TransactionContext *current_txn,
                         cid_t end_comm_id,
                         std::function<void(bool, TransactionContext *, std::vector<LSN_T> &&)> on_complete);

    oid_t AddDefaultIndirectionArray();

    void IndirectLocation(const LSN_T **lsn);

    void LogStart(){ starting = true ;}

    bool IsLogStart(){return starting;}

    void LogCatalog(Catalog *catalog){log_catalog = catalog;}

    Catalog *GetLogCatalog(){return log_catalog;}
//    std::multimap<cid_t, std::shared_ptr<LogRecord>> GetLogRecordBuffer() {
//        return log_record_buffer;
//    }
//
//    uint32_t GetLogEntrySize(){
//        return log_record_buffer.size();
//    }

private:
    LogManager();
    SpinLatch latch_;
    Catalog *log_catalog;
    //indirect array, to produce (log serial number)lsn
    std::shared_ptr<IndirectionArray> lsn_indirect_array;
    bool starting = false;
//    std::unordered_map<cid_t, std::vector<std::unique_ptr<LogRecord>>>::iterator log_record_buffer_itr;
//    std::unordered_map<cid_t, std::vector<std::unique_ptr<LogRecord>>> log_record_buffer;
//    std::multimap<cid_t, std::shared_ptr<LogRecord>>::iterator log_record_buffer_itr;
//    std::multimap<cid_t, std::shared_ptr<LogRecord>> log_record_buffer;
//    std::list<std::shared_ptr<LogRecord>> log_record_buffer;
//    tbb::concurrent_hash_map<cid_t, std::vector<LogRecord>>::accessor log_record_buffer_itr;
//    tbb::concurrent_hash_map<cid_t, std::vector<LogRecord>> log_record_buffer;
};

class VersionStore{
public:
    static VersionStore *GetInstance();

    Status Init(std::vector<Catalog *> catalog_list);

    ~VersionStore();

    void AddDefaultVersionBlock(const Catalog *catalog);
    oid_t AddDefaultBlockToManager(const Catalog *catalog, oid_t active_block_id, bool is_log);
    // return new version location
    std::pair<oid_t, TupleHeader *> AcquireVersion(Catalog *catalog);
    // return the insert location
    TupleHeader *InsertTuple(const BaseTuple *tuple);
    // return a empty tuple location
    std::pair<oid_t, TupleHeader *> GetEmptyTupleSlot(Catalog *catalog, const BaseTuple *tuple);
    // increase the tuple count
    void IncreaseTupleCount(const size_t &amount){
        total_count.fetch_add(1);
    }
    uint32_t GetTotalCount(){
        return total_count;
    }
    uint32_t GetCurrentVersionBlockId();
    // log write record
    LSN_T LogRecordPersist(LogRecord *entry, char **data_ptr);

    LogManager* GetLogManager() { return log_manager; }
    void CleanVersionStore();

private:
    VersionStore();
    std::atomic<uint32_t> total_count = ATOMIC_VAR_INIT(0);
    //log table id 0, user table id from 1 to n
    std::unordered_map<oid_t, std::vector<VersionBlock *>>::iterator active_v_b_itr;
    //active retiring version chunks
    std::unordered_map<oid_t, std::vector<VersionBlock *>> active_version_blocks_;
//    tbb::concurrent_unordered_map<oid_t, std::vector<VersionBlock *>> active_version_blocks_;
//    std::vector<std::vector<VersionBlock *>> active_log_blocks_;
    LogManager *log_manager{};

};

/**
 * Asynchronous persist the log records of the Transaction
 * when txn commit.
 */
class AsyncPersistentLogManager{
public:
    AsyncPersistentLogManager(LogManager * log_mgr,
                               size_t buf_size = 2 * 1024 * 1024);

    ~AsyncPersistentLogManager() {}

    Status WriteRecord(LogRecord * rec) ;

private:
    char *buf;
    size_t buf_size;
    LogManager *log_mgr;
};
/**
 * version cleaner
 * 1. flush to SSD for recovery, release the location of the log chunks
 *    compute the current min active txn id,
 *    traverse the log records, and filter the visible records
 *    and persist those records into log files.
 * 2. clear the retired versions, free the memory for other versions
 *    when txn commit, update/delete will insert to the release
 *    space list, we will recycle those memory space by transaction level,
 *    because we can easily track the location using physical pointers.
 * 3. clear the overwritten buffer pool, refresh the location
 * 4. clear the transaction info
 * @param cleaner
 */
class VersionCleaner {
public:
    VersionCleaner(VersionStore *buf_mgr): buf_mgr(buf_mgr) {}

    void StartClean();
    void StopClean();
    static void ClearCleans(int thread_id);
//    int StartCleanerThread();
//
//    int EndCleanerThread();
protected:
    void CleanVersionsProcessing(const int &thread_id);

private:
    VersionStore *buf_mgr;
    volatile bool is_running_;
    static int clean_thread_count_;
//    std::atomic<bool> stopped{false};
//    std::unique_ptr<std::thread> clean_thread;
};


}

#endif
