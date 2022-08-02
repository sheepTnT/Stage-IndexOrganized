//
// Created by zhangqian on 2021/10/29.
//

#include <sys/mman.h>
#include <memory>
#include <execute/txn.h>
#include "../include/vstore/version_store.h"
#include "../include/common/env.h"
#include "../include/common/mono_queue_pool.h"

namespace mvstore{

// 20*1024 * 64KB = 20GB
size_t dram_block_cap_in_bytes = default_blocks * DRAM_BLOCK_SIZE;
size_t nvm_block_cap_in_bytes = default_nvm_blocks * NVM_BLOCK_SIZE;

static const std::string heapfile_path_ = "/mnt/pmem5/heapfile";

size_t num_nvm_blocks_ = (nvm_block_cap_in_bytes/NVM_BLOCK_SIZE) * 1;

static int clean_thread_count_ = 1;
static std::unordered_map<oid_t, tbb::concurrent_queue<FreeBlockSlot>> local_available_;

void BaseTuple::SetValue(oid_t column_id, const char *value) const {
    uint32_t value_size = tuple_schema_->_columns[column_id].size;
    std::string value_type = tuple_schema_->_columns[column_id].type;
    int pos = tuple_schema_->_columns[column_id].index;
    char *location = &tuple_data_[pos];

    if(value_type == "INTEGER" || value_type == "LONG INTEGER"){
        memcpy(location, &value, value_size);
    }else{
        memcpy(location, value, value_size);
    }
}
char *BaseTuple::GetValue(const oid_t column_id) const {
    int pos = tuple_schema_->_columns[column_id].index;
    return &tuple_data_[pos];
}
bool BaseTuple::operator==(const BaseTuple &other) const {
    if (tuple_data_ != other.tuple_data_) {
        return false;
    }

    return true;
}
bool BaseTuple::operator!=(const BaseTuple &other) const {
    return !(*this == other);
}

//==========================================================
//----------------------------VersionBlock------------------
//==========================================================
/**
 * Insert tuple at slot
 * NOTE : No checks, must be at valid slot.
 * return: the tuple offset
 */
TupleHeader *VersionBlock::InsertTuple(const char *tuple_data) {
    auto block_meta = GetBlockMeta();
    if (block_meta == nullptr) {
        return nullptr;
    }

    oid_t tuple_slot_id = block_meta->GetNextEmptyTupleSlot();

    LOG_TRACE("version block Id :: %u status :: %u out of %u slots ", block_id_,
              tuple_slot_id, allocated_tuple_slots);

    // No more slots
    if (tuple_slot_id == INVALID_OID) {
        LOG_DEBUG("Failed to get next empty tuple slot within current version block.");
        return nullptr;
    }

    // copy tuple to the slot
    char *tuple_header_loc = nullptr;
    tuple_header_loc = GetEmptyTupleHeader(tuple_slot_id);
    if(tuple_header_loc == nullptr){
        LOG_DEBUG("Failed to get tuple header within current version block.");
        return nullptr;
    }
    uint32_t tuple_length = block_meta->GetTupleLength();
    auto tuple_header = reinterpret_cast<TupleHeader *>(tuple_header_loc);
    uint64_t tuple_slot_ptr = tuple_header->GetTupleSlot();

    if(tuple_slot_ptr == 0 || tuple_slot_ptr == std::numeric_limits<uint64_t>::min() ||
            tuple_slot_ptr == std::numeric_limits<uint64_t>::max()){
        LOG_INFO("Failed to get tuple slot location within current version block.");
        return nullptr;
    }

    auto tuple_slot_loc = reinterpret_cast<char *>(tuple_slot_ptr);
    if(tuple_data != nullptr){
        VSTORE_MEMCPY(tuple_slot_loc, tuple_data, tuple_length);
    }else{
        VSTORE_MEMSET(tuple_slot_loc, 0, tuple_length);
    }

    return tuple_header;
}

/**
 *
 * @param tuple_offset, the index of the tuple in this block
 * @param column_id, requesred column, out value
 * @return , value of the column, char []
 */
void VersionBlock::GetValue(const oid_t tuple_offset, char *column_type,
                            uint32_t column_offset,
                            uint32_t column_length, char **value,
                            TupleHeader &tuple_header) {
    std::string value_type = column_type;
    auto block_meta = GetBlockMeta();
    auto allocated_tuple_slots = block_meta->GetTotalTupleCount();
    assert(tuple_offset <= allocated_tuple_slots);

    // |field0|field1|field2|....
    char *tuple_location_rel = reinterpret_cast<char *>(tuple_header.GetTupleSlot());
    const char *column_location = tuple_location_rel + column_offset;

    if(value_type == "INTEGER"){
        uint32_t val = *reinterpret_cast<const uint32_t *>(column_location);
        *value = reinterpret_cast<char *>(&val);
    }else{
        char *ret = new char[column_length];
        memcpy(ret, column_location, column_length);
        *value = ret;
    }
}
/**
 * Copy from tuple.
 */
void VersionBlock::CopyTuple(const BaseTuple *tuple, const oid_t &tuple_slot_id) {
    LOG_TRACE("version block Id :: %u status :: %u out of %u slots ", this.block_id_,
              tuple_slot_id, allocated_tuple_slots);

    // copy tuple to the slot
    auto block_meta = GetBlockMeta();
    auto tuple_length = block_meta->GetTupleLength();
    std::unique_ptr<TupleHeader> tuple_header = GetTupleHeader(tuple_slot_id);
    char *tuple_slot_location = reinterpret_cast<char *>(tuple_header->GetTupleSlot());
    memcpy(tuple_slot_location,tuple->GetData(),tuple_length);
}

//==========================================================
//----------------------------VersionBlockManager------------
//==========================================================
std::shared_ptr<VersionBlock> VersionBlockManager::empty_version_block_;

VersionBlockManager::VersionBlockManager() = default;

VersionBlockManager::~VersionBlockManager() {
    blocks_queue.Clear();
};

// Get instance of the global version storage manager
VersionBlockManager *VersionBlockManager::GetInstance() {
    static VersionBlockManager global_version_store_manager;
    return &global_version_store_manager;
}

/**
 * database start, initialize the version block manager
 * @return
 */
Status VersionBlockManager::Init() {
    last_pos = 0;
    size_t num_block =  ((num_nvm_blocks_ + 63) / 64) * 64;
    bitmaps = AtomicBitmap(num_block);
    size_t filesize = num_nvm_blocks_ * NVM_BLOCK_SIZE;
    version_block_oid_ = 0;

    if(nvm_emulate){
        LOG_DEBUG("DRAM emulate log file.");
        //mmap version blocks and log blocks
        mmap_start_addr_ = mmap(NULL, filesize, PROT_READ | PROT_WRITE,
                                MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        // If allocation fails, we throw an error because this is uncoverable
        // The upper level functions should either catch this exception
        // and then use another index instead, or simply kill the system
        PELOTON_ASSERT((mmap_start_addr_ != (void *)-1),
                       "mmap() failed to initialize mapping table for Version Block Manager.");
        LOG_TRACE("Mapping version block allocated via dram mmap().");

        while(true){
            int p = bitmaps.TakeFirstNotSet(last_pos);
            if (p == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                break;
            }
            last_pos.store(p);
            char *alloc_ = static_cast<char *>(mmap_start_addr_) + p * NVM_BLOCK_SIZE;

            PELOTON_ASSERT(alloc_, "bad allocate for version block.");

            char *block = alloc_;
            oid_t block_id = GetNextVersionBlockId();
            blocks_queue.Enqueue(std::make_pair(block_id, block));
        }
        auto block_count =  blocks_queue.UnsafeSize();
        PELOTON_ASSERT(num_block == block_count, "version block allocate fail.");
        LOG_DEBUG("alloc block count: %lu", block_count);

    }else{
        LOG_DEBUG("NVM(PMM) log file.");
        //1.mmap retiring version blocks
        mmap_start_addr_ = mmap(NULL, filesize, PROT_READ | PROT_WRITE,
                                MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        // If allocation fails, we throw an error because this is uncoverable
        // The upper level functions should either catch this exception
        // and then use another index instead, or simply kill the system
        PELOTON_ASSERT((mmap_start_addr_ != (void *)-1),
                       "mmap() failed to initialize mapping table for Version Block Manager.");
        LOG_TRACE("Mapping version block allocated via dram mmap().");

        //2.mmap log blocks
        filesize = num_nvm_blocks_ * NVM_BLOCK_SIZE;
        void *mmap_addr_tmp;
        PosixEnv::DeleteFile(heapfile_path_);
        Status st = PosixEnv::MMapNVMFile(heapfile_path_,mmap_addr_tmp, filesize);
        if (!st.ok()){
            LOG_INFO("PosixEnv NVMLogFile fail, %s",st.ToString().c_str());
        }
        PELOTON_ASSERT(st.ok(),"PosixEnv NVMLogFile fail.");
        log_mmap_start_addr_ = (char *) mmap_addr_tmp;

        while(true){
            int p = bitmaps.TakeFirstNotSet(last_pos);
            if (p == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                break;
            }
            last_pos.store(p);
            char *alloc_ = static_cast<char *>(mmap_start_addr_) + p * NVM_BLOCK_SIZE;

            PELOTON_ASSERT(alloc_, "bad allocate for version block.");

            char *block = alloc_;
            oid_t block_id = GetNextVersionBlockId();
            blocks_queue.Enqueue(std::make_pair(block_id, block));
        }
        auto block_count =  blocks_queue.UnsafeSize();
        PELOTON_ASSERT(num_block == block_count, "version block allocate fail.");
        LOG_DEBUG("alloc block count: %lu", block_count);

        last_pos = 0;
        bitmaps = AtomicBitmap(num_block);
        while(true){
            int p = bitmaps.TakeFirstNotSet(last_pos);
            if (p == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                break;
            }
            last_pos.store(p);
            char *alloc_ =  log_mmap_start_addr_ + p * NVM_BLOCK_SIZE;

            PELOTON_ASSERT(alloc_, "bad allocate for version block.");

            char *block = alloc_;
            oid_t block_id = GetNextVersionBlockId();
            LOG_DEBUG("log blocks, block id %u, block location %p",block_id, block);
            log_blocks_queue.Enqueue(std::make_pair(block_id, block));
        }


        auto log_block_count =  log_blocks_queue.UnsafeSize();
        if (num_block != log_block_count){
            LOG_INFO("log block allocate fail, %zu, %zu", num_block, log_block_count);
        }
        PELOTON_ASSERT(num_block == log_block_count, "log block allocate fail.");
        LOG_DEBUG("alloc log block count: %lu", log_block_count);
    }


    //clean the version blocks
    local_retired_set_ = std::make_shared<VCSet>();

    return Status::OK();
}
/**
 *
 */
void VersionBlockManager::DeallocateNVMSpace(){
    size_t filesize = num_nvm_blocks_ * NVM_BLOCK_SIZE;
    // NOTE: Only unmap memory here because we need to access the mapping
    // table in the above routine.
    int munmap_ret = munmap(static_cast<char *>(mmap_start_addr_), filesize);

    // Although failure of munmap is not fatal, we still print out
    // an error log entry
    // Otherwise just trace log
    if(munmap_ret != 0) {
        LOG_ERROR("munmap() returns with %d", munmap_ret);
    } else {
        LOG_TRACE("Mapping table is unmapped for Version Block Manager");
    }

}
/**
 *
 * @return
 */
VersionBlockElem VersionBlockManager::AllocateBlock() {
    VersionBlockElem version_block_elem;
//    LOG_DEBUG("current block array size:%lu",version_block_array_.size() );
    lock_.Lock();
    if (blocks_queue.UnsafeSize() > 0){
        blocks_queue.Dequeue(&version_block_elem);
    }else{
        LOG_DEBUG("there is no enough block allocated, current block size: %lu.",version_block_array_.size());
    }
    lock_.Unlock();
    PELOTON_ASSERT(version_block_elem.second,"there is no enough block allocated.");

    return version_block_elem;
}
/**
 *
 * @return
 */
VersionBlockElem VersionBlockManager::AllocateLogBlock() {
    VersionBlockElem version_block_elem;
//    LOG_DEBUG("current block array size:%lu",version_block_array_.size() );
    if (nvm_emulate){
        lock_.Lock();
        if (blocks_queue.UnsafeSize() > 0){
            blocks_queue.Dequeue(&version_block_elem);
        }else{
            LOG_DEBUG("there is no enough block allocated, current block size: %lu.",version_block_array_.size());
        }
        lock_.Unlock();
        PELOTON_ASSERT(version_block_elem.second,"there is no enough block allocated.");
    }else{
        LOG_DEBUG("allocate log block.");
        lock_.Lock();
        if (log_blocks_queue.UnsafeSize() > 0){
            log_blocks_queue.Dequeue(&version_block_elem);
        }else{
            LOG_DEBUG("there is no enough block allocated, current log block size: %lu.",version_block_array_.size());
        }
        lock_.Unlock();
        PELOTON_ASSERT(version_block_elem.second,"there is no enough log block allocated.");
    }

    return version_block_elem;
}
/**
 *
 * @param location
 */
void VersionBlockManager::AddVersionBlock(std::shared_ptr<VersionBlock> location) {
    lock_.Lock();
    version_block_array_.push_back(location);
    lock_.Unlock();
}
/**
 *
 * @param location
 */
void VersionBlockManager::DropVersionBlock(std::shared_ptr<VersionBlock> location) {
    for (version_block_iterator_ = version_block_array_.begin();
                version_block_iterator_ != version_block_array_.end();){
        if(*version_block_iterator_ == location){
            version_block_array_.erase(version_block_iterator_);
        }
    }
}
/**
 * Get version block by version block id
 * @param oid
 * @return
 */
VersionBlock *VersionBlockManager::GetVersionBlock(oid_t block_id) {
    std::shared_ptr<VersionBlock> location;
    for(auto &nv_b : version_block_array_){
        auto block_id_ = nv_b->GetBlockId();
        if(block_id == block_id_){
            location = nv_b;
            break;
        }
    }

    return location.get();
}

// used for logging test
void VersionBlockManager::ClearVersionBlock() {
    DeallocateNVMSpace();
    version_block_array_.clear();
    blocks_queue.Clear();
    log_blocks_queue.Clear();
}

void VersionBlockManager::DeallocateBlock(void *p) {
    auto pointer_diff = static_cast<size_t>(static_cast<char *>(p)
                                            - static_cast<char *>(mmap_start_addr_));
    assert(pointer_diff % NVM_BLOCK_SIZE == 0);
    int pos = pointer_diff / NVM_BLOCK_SIZE;
    bitmaps.Clear(pos);
}
/**
 * one backend thread pin to core to collect the candidate tuple slot
 * these tuple slot are the retired versions
 * @param min_active_id
 * @return
 */
int VersionBlockManager::CollectRetiredVersionSet(const cid_t min_active_id){
    std::vector<TxnVersionInfo> gc_set_txn;
    int collect_count = 0;
    auto gc_set_itr = local_retired_set_->GetIterator();
    for(auto &cuck_itr : gc_set_itr){
        //thread id, has the txn id < min active id
        oid_t txn_commit_id = cuck_itr.first;
        if(txn_commit_id < min_active_id){
            //1.traverse the txn gc set(retired version slots)
            gc_set_txn = cuck_itr.second;
            if(!gc_set_txn.empty()){
                for(const auto &iter : gc_set_txn){
                    FreeBlockSlot f_s_l = std::make_shared<TxnVersionInfo>(TxnVersionInfo{iter.table_id,
                              iter.tuple_header, iter.tuple_header_size, iter.version_type});

                    collect_count++;

                    local_candidate_available_.push(std::move(f_s_l));
                }
            }

            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto txn_info = transaction_manager->GetTransactionContext(txn_commit_id);
            //2.erase the overwritten buffer item and free the location
            transaction_manager->CleanTxnOverwrittenBuffer(txn_info);
            //3.delete the txn context info of the commit id
            delete txn_info;
            txn_info = nullptr;
            //4.delete item from the active id sets
            bool rt = transaction_manager->EraseTid(txn_commit_id);
            LOG_DEBUG("delete from the active id sets result: %d", rt);
            //5.after collect, clear the slot set of the commit id
            gc_set_txn.clear();
            //current transaction exits from local cleaner
            local_retired_set_->Erase(txn_commit_id);
        }
    }

    return collect_count;
}
/**
 * work thread pool to free the tuple slot of version store
 * this pool has backend number of work threads
 * @param thread_id
 * @return
 */
void VersionBlockManager::CleanCandidateSlot(std::shared_ptr<TxnVersionInfo> &elem,
                                       const std::function<void(int count)>& on_complete){
    int clean_count = 0 ;
    // reset the tuple header of the slot,
    auto tpl_hd = reinterpret_cast<TupleHeader *>(elem->tuple_header);

    tpl_hd->reset();

    local_available_[elem->table_id].push(elem);

    clean_count++;

    on_complete(clean_count);
}

FreeBlockSlot VersionBlockManager::GetFreeSlot(oid_t table_id){
    if (local_available_.size() <=0 ){return nullptr;}
    tbb::concurrent_queue<FreeBlockSlot> f_b_ss = local_available_[table_id];
    if (f_b_ss.empty()){return nullptr;}
    FreeBlockSlot f_b_s;

    f_b_ss.try_pop(f_b_s);
    return f_b_s;
}

//==========================================================
//----------------------------VersionStore------------------
//==========================================================
VersionStore::VersionStore() = default;

VersionStore::~VersionStore() {
    for (int i = 0; i < retir_active_version_blocks_.size(); ++i) {
        retir_active_version_blocks_.erase(i);
    }
};

VersionStore *VersionStore::GetInstance() {
    static VersionStore global_version_store_;
    return &global_version_store_;
}
/**
 * initialize the version store with catalog list
 * @param catalog_list
 */
Status VersionStore::Init(std::vector<Catalog *> &catalog_list) {
    //inialize the user table, log table
    for (int i=0; i<catalog_list.size(); ++i) {
        const auto catalog_ = catalog_list[i];
        auto table_id = catalog_->table_id;
        LOG_DEBUG("initialize version store, table id: %u",table_id);

        if (catalog_->is_log){
            log_active_version_blocks_.reserve(default_active_block_count_);
            log_manager = LogManager::GetInstance();
            log_manager->LogCatalog(catalog_);
            for(size_t j = 0; j < default_active_block_count_; ++j){
                //initialize the default version block for log record
                AddDefaultBlockToManager(catalog_, j, true);
            }
        }else{
            for(size_t j = 0; j < default_active_block_count_; ++j){
                //initialize the default version block for tuple version record
                AddDefaultBlockToManager(catalog_, j, false);
            }
            total_count[table_id] = 0;
        }
    }
//    total_count = 0;

    return Status::OK();
}
uint32_t VersionStore::GetCurrentVersionBlockId() {
    auto v_b_mng = VersionBlockManager::GetInstance();
    auto curr_v_b_id = v_b_mng->GetCurrentVersionBlockId();

    return curr_v_b_id;
}
/**
 * @brief create default version block for the catalog
 */
void VersionStore::AddDefaultVersionBlock(const Catalog *catalog) {
    // random a active block id in [0,1]
    size_t active_block_id = rand() % default_active_block_count_;
    AddDefaultBlockToManager(catalog, active_block_id, false);
}
/**
 * Get a fixed block from version block manager
 * Initialize it
 * Add it to the version block manager locator
 * @param catalog
 * @param active_block_id
 * @return
 */
oid_t VersionStore::AddDefaultBlockToManager(const Catalog *catalog,
                                             oid_t active_block_id, bool is_log) {
    oid_t version_block_id = INVALID_OID;

    std::shared_ptr<VersionBlock> version_block_ = nullptr;
    auto table_id = catalog->table_id;
    if(is_log){
        assert(table_id == 0);
       //first: Create a version block with NVM mmap File
       auto nvm_block_ = VersionBlockManager::GetInstance()->AllocateLogBlock();
       version_block_id = nvm_block_.first;
       char *block = nvm_block_.second;
       version_block_.reset(new VersionBlock(version_block_id, &block, catalog));
    }else{
        //first: Create a version block with dram mmap File
        auto nvm_block_ = VersionBlockManager::GetInstance()->AllocateBlock();
        version_block_id = nvm_block_.first;
        char *block = nvm_block_.second;
        version_block_.reset(new VersionBlock(table_id, version_block_id, &block, catalog)) ;
    }

    assert(version_block_);

    LOG_TRACE("Added a version block to version block manager ");
    VersionBlockManager::GetInstance()->AddVersionBlock(version_block_);

    COMPILER_MEMORY_FENCE;

    //second: Add it to the current catalog active version blocks
    if(is_log){
        log_active_version_blocks_[active_block_id] = version_block_.get();
    }else{
        retir_active_version_blocks_[table_id][active_block_id] = version_block_.get();
//        retir_active_version_blocks_[table_id].emplace_back(version_block_.get());
    }

    // we must guarantee that the compiler always add tile group before adding
    COMPILER_MEMORY_FENCE;

    LOG_TRACE("Recording version block : %u ", version_block_id);

    return version_block_id;
}

/**
 * insert into a tuple with the tuple and catalog
 * version store does not matter the index
 * @param tuple
 * @return
 */
TupleHeader *VersionStore::InsertTuple(const BaseTuple *tuple) {
    Catalog *catalog = tuple->GetSchema();
    oid_t table_id = catalog->table_id;
    auto tuple_location = GetEmptyTupleSlot(catalog,tuple);
    if (tuple_location.second == nullptr) {
        LOG_TRACE("Failed to get tuple slot.");
        return nullptr;
    }

    LOG_TRACE("tuple location, header: %p, %p ", tuple_location,  tuple_location->GetSlot());

    IncreaseTupleCount(table_id,1);

    LOG_DEBUG("version store total tuple count of table id: %u , %d", GetTotalCount(table_id), table_id);

    return tuple_location.second;
}
/**
 * acquire a empty version tuple slot
 * @param catalog
 * @return
 */
std::pair<oid_t, TupleHeader *> VersionStore::AcquireVersion(Catalog *catalog) {
    // First, claim a slot
    oid_t table_id = catalog->table_id;
    auto location = GetEmptyTupleSlot(catalog,nullptr);
    if (location.second  == nullptr) {
        LOG_INFO("Failed to get tuple slot.");
        return {};
    }

    IncreaseTupleCount(table_id,1);

    LOG_TRACE("tuple location, header: %p, %p ", tuple_location,  tuple_location->GetSlot());

    return location;
}
/**
 * get the empty slot from GC
 * or create a new slot
 * for the version record
 * @param version record of the tuple
 * @return the version record slot
 */
std::pair<oid_t,TupleHeader *> VersionStore::GetEmptyTupleSlot(Catalog *catalog,
                                                               const BaseTuple *tuple) {
    assert(catalog->is_log==false);

    //=============== garbage collection==================
    // check if there are recycled tuple slots
    auto version_block_mng = VersionBlockManager::GetInstance();
    auto free_slot = version_block_mng->GetFreeSlot(catalog->table_id);
    if(free_slot != nullptr){
        auto *free_tuple_hd(reinterpret_cast<TupleHeader *>(free_slot->tuple_header));
        char *tuple_slot_location = reinterpret_cast<char *>(free_tuple_hd->GetTupleSlot());
        memcpy(tuple_slot_location, tuple->GetData(), catalog->tuple_size);
        return std::make_pair(0,free_tuple_hd);
    }

    //====================================================
    oid_t table_id = catalog->table_id;
    uint64_t total_tuple_count = this->GetTotalCount(table_id);
    size_t active_block_id = total_tuple_count % default_active_block_count_;
    VersionBlock *version_block = nullptr;
    oid_t tuple_slot = INVALID_OID;
    oid_t version_block_id = INVALID_OID;
    TupleHeader *tupleHeader = nullptr;
    const char *tuple_data;

    // get valid tuple.
    while (true) {
        // get the last tile group.
        auto retir_active_version_blocks = retir_active_version_blocks_[table_id];
        version_block = retir_active_version_blocks[active_block_id];
//        LOG_DEBUG("active_block_id: %zu,%zu,%zu,%u",
//                  active_block_id,total_tuple_count,default_active_block_count_,table_id);

        tuple == nullptr ? tuple_data = nullptr : tuple_data = tuple->GetData();
        tupleHeader = version_block->InsertTuple(tuple_data);

        // now we have already obtained a new tuple slot.
        if (tupleHeader != nullptr) {
            tuple_slot = tupleHeader->GetHdId();
            version_block_id = version_block->GetBlockId();
            break;
        }
    }

    // if this is the last tuple slot we can get
    // then create a new active version block from manager
    auto block_meta = version_block->GetBlockMeta();
    auto alloc_tuple_count = block_meta->GetTotalTupleCount();
    if (tuple_slot ==  (alloc_tuple_count - 1)) {
        AddDefaultBlockToManager(catalog,active_block_id, false);
    }

    LOG_TRACE(" version block id: %u, address: %p, tuple header address:%p",
              version_block_id, version_block.get(), tupleHeader);

    return std::make_pair(version_block_id,tupleHeader);
}

void VersionStore::CleanVersionStore(){
//    auto nvm_block_mng_ = VersionBlockManager::GetInstance();
//    nvm_block_mng_->DeallocateNVMSpace();

    log_active_version_blocks_.clear();
    retir_active_version_blocks_.clear();
}
/**
 * @param rec
 * @return
 */
LSN_T VersionStore::LogRecordPersist(LogRecord *entry, char **data_ptr) {
    size_t size = entry->Size();
    LSN_T last_lsn;
    size_t active_block_id = rand() % default_active_block_count_;
    VersionBlock *version_block = nullptr;
    oid_t version_block_id = INVALID_OID;
    auto log_table_catalog = LogManager::GetInstance()->GetLogCatalog();
    oid_t table_id = log_table_catalog->table_id;
//    LOG_DEBUG("log record persist table id: %u , %u", table_id, active_block_id);

    while (true) {
        // get the last tile group.
        LOG_DEBUG("log record persist table id: %u , %zu", table_id, active_block_id);
        version_block = log_active_version_blocks_[active_block_id];

        char *buf_start = version_block->GetNextSlot(size,last_lsn);

        // now we have already obtained a new tuple slot.
        if (buf_start != nullptr) {
            version_block_id = version_block->GetBlockId();
            *data_ptr = buf_start;
            break;
        }
        else{
            LOG_DEBUG("block has no enough space, add new log block. ");
            AddDefaultBlockToManager(log_table_catalog, active_block_id, true);
        }
    }

    LOG_DEBUG("version block id: %u, address: %p, tuple header address: %p",
              version_block_id, version_block, *data_ptr);

    return last_lsn;
}

//==========================================================
//----------------------------LogManager------------
//==========================================================
LogManager::LogManager() = default;
LogManager::~LogManager() = default;
LogManager *LogManager::GetInstance() {
    static LogManager log_manager;
    return &log_manager;
}
Status LogManager::Init(){
    AddDefaultIndirectionArray();
    starting = false;
    return Status::OK();
}
LogRecord *LogManager::LogBeginTxn(cid_t begin_id) {
    LogRecord *lg_begn = new LogRecord(BEGIN_TXN);
    assert(lg_begn != nullptr);
    //get an empty indirect array offset
    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_begn->init(LogRecordType::BEGIN_TXN, indirect_lsn , begin_id, begin_id);

    return lg_begn;
}
/**
 *
 * @param type, log record type
 * @param tid, transaction begin id
 * @param tuple_slot, install version slot
 * @param payload_loction, undo buffer pool, copy location(key/payload)
 * @param delta, delta data
 * @param len, delta length
 * @param bits, delta schema
 * @param catalog, user table record schema catalog
 */
LogRecord *LogManager::LogUpdate(cid_t begin_id, char *tuple_slot,
                           char *payload_loction, const char *delta, uint16_t len,
                           uint8_t *bits, Catalog *record_catalog) {
    LogRecord *lg_upt = new LogRecord(UPDATE);
    assert(lg_upt != nullptr);
    //get an empty indirect array offset
    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_upt->init(LogRecordType::UPDATE, 0, indirect_lsn,
          payload_loction, begin_id, tuple_slot, delta,
          len, bits, record_catalog);

    return lg_upt;
}
LogRecord *LogManager::LogDelta(cid_t begin_id, uint64_t pre_lsn, const char *delta){
    LogRecord *lg_upt_delta = new LogRecord(UPDATE);
    assert(lg_upt_delta != nullptr);
    //get an empty indirect array offset
    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_upt_delta->init(LogRecordType::UPDATE, pre_lsn, indirect_lsn,
                 nullptr, begin_id, nullptr, delta,
                 0, nullptr, nullptr);
    return lg_upt_delta;
}
LogRecord *LogManager::LogInsert(cid_t begin_id, const char *delta, uint16_t len,
                           Catalog *record_catalog) {
    LogRecord *lg_inst = new LogRecord(INSERT);
    assert(lg_inst != nullptr);
    //get an empty indirect array offset
    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_inst->init(LogRecordType::INSERT, indirect_lsn, delta, begin_id, len, record_catalog);

    return lg_inst;
}
LogRecord *LogManager::LogDelete(cid_t begin_id, char *tuple_slot, Catalog *record_catalog) {
    LogRecord *lg_dele = new LogRecord(DELETE);
    assert(lg_dele != nullptr);

    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_dele->init(LogRecordType::DELETE, indirect_lsn, tuple_slot, record_catalog, begin_id);

    return lg_dele;
}
LogRecord *LogManager::LogCommitTxn(cid_t begin_id , cid_t end_commit_id) {
    LogRecord *lg_comm = new LogRecord(COMMIT_TXN);
    assert(lg_comm != nullptr);
    //get an empty indirect array offset
    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_comm->init(LogRecordType::COMMIT_TXN, indirect_lsn, begin_id, end_commit_id);

    return lg_comm;
}
LogRecord *LogManager::LogAbortTxn(cid_t begin_id , cid_t end_commit_id) {
    LogRecord *lg_abt = new LogRecord(ABORT_TXN);
    assert(lg_abt != nullptr);
    //get an empty indirect array offset
    const LSN_T *indirect_lsn = nullptr;
    IndirectLocation(&indirect_lsn);
    lg_abt->init(LogRecordType::ABORT_TXN, indirect_lsn ,begin_id , end_commit_id);

    return lg_abt;
}

void LogManager::IndirectLocation(const LSN_T **lsn_t){
    //the pointer point to the reference of the offset
    size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

    while (true) {
        auto active_indirection_array = lsn_indirect_array;
        indirection_offset = active_indirection_array->AllocateIndirection();

        if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
            *lsn_t = active_indirection_array->GetIndirectionByOffset(indirection_offset);
            break;
        }
    }

    if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
        AddDefaultIndirectionArray();
    }
}
oid_t LogManager::AddDefaultIndirectionArray() {
    auto &manager = Manager::GetInstance();
    oid_t indirection_array_id = manager.GetNextIndirectionArrayId();

    std::shared_ptr<IndirectionArray> indirection_array(
                                new IndirectionArray(indirection_array_id));
    manager.AddIndirectionArray(indirection_array_id, indirection_array);

    lsn_indirect_array = indirection_array;

    return indirection_array_id;
}

/**
 * 1. copy and persist the log record(delta) to the version block
 * 2. copy and persist the old version to the tuple slot
 * @param begin_tid
 * @param end_commit_id
 * @return
 */
void LogManager::LogWrite(TransactionContext *current_txn,
                          cid_t end_comm_id,
                          std::function<void(bool,
                                             TransactionContext *txn_,
                                             std::vector<LSN_T> &&)> on_complete) {
    auto log_rcds = current_txn->GetLogRecords();
    std::vector<LSN_T> rt_lsn;
    auto version_store = VersionStore::GetInstance();

    for (int i=0; i < log_rcds.size(); ++i) {
        auto lg_rd = log_rcds[i];
        if (lg_rd == nullptr){
            continue;
        }else{
            if (lg_rd->GetType() == LogRecordType::BEGIN_TXN || lg_rd->GetType() == COMMIT_TXN
                || lg_rd->GetType() == LogRecordType::INSERT || lg_rd->GetType() == LogRecordType::UPDATE ||
                lg_rd->GetType() == LogRecordType::DELETE || lg_rd->GetType() == LogRecordType::ABORT_TXN){
                lg_rd->SetCommitId(end_comm_id);

                char *data_ptr = nullptr;
                //write log record to persist store
                //allocate location
//                LOG_INFO("LogRecordPersist get next_lsn.");
                LSN_T next_lsn = version_store->LogRecordPersist(lg_rd,
                                                                 &data_ptr);
//                LOG_INFO("LogRecordPersist next_lsn: %lu, data location: %p.", next_lsn, data_ptr);
                rt_lsn.push_back(next_lsn);

                //serialize the log record
                lg_rd->WriteLogData(data_ptr);

                //reset the indirect lsn's content = next_lsn
                LSN_T *indir_lsn = lg_rd->GetIndirectLSN();
                (*indir_lsn) = next_lsn;

            }else{
                LOG_DEBUG("current log record has nothing write.");
            }
        }

    }

    on_complete(true, current_txn, std::move(rt_lsn));
}


VersionCleaner::VersionCleaner() = default;
VersionCleaner::~VersionCleaner() = default;

VersionCleaner *VersionCleaner::GetInstance() {
    static VersionCleaner version_cleaner;
    return &version_cleaner;
}
/**
 * retired version cleaner
 * 1. release the memory for overwritten version buffer
 * 2. clear the retired versions, free the memory for other versions
 * @param cleaner
 */
void VersionCleaner::CleanVersionsProcessing(double period_duration) {
    LOG_DEBUG("Starting version(overwritten/retired) cleaning.");

    constexpr float CYCLES_PER_SEC = 3 * 1024ULL * 1024 * 1024;
    uint32_t backoff_shifts = 0;
    size_t init_pround = 0;
    size_t profile_round = (size_t) (period_duration);


    while (init_pround < profile_round) {
        ++init_pround;
        int collect_count = 0;
        int clean_count = 0;
        long long cycles = 0;
        {
            ScopedTimer timer([&cycles](unsigned long long c) {cycles += c;});

            VersionBlockManager *block_mng = VersionBlockManager::GetInstance();
            //get the transaction minactive id
            auto transaction_manager = SSNTransactionManager::GetInstance();
            auto min_active_tid = transaction_manager->MinActiveTID();

            //collect the candidate retired version slot
            //and erase the txn info, overwritten buffered versions
            collect_count = block_mng->CollectRetiredVersionSet(min_active_tid);

            //work pool, clean the retired slot and free the memory space
            auto &pool = MonoQueuePool::GetInstance();
            auto txn_version_infos = block_mng->GetDeallocatedQueue();

            while(!txn_version_infos.empty()){
                auto on_complete = [&clean_count](int count) {
                    clean_count = clean_count + count;
                };

                std::shared_ptr<mvstore::TxnVersionInfo> elem;
                txn_version_infos.try_pop(elem);
                pool.SubmitTask([&elem, on_complete] {
                    VersionBlockManager::CleanCandidateSlot(elem, on_complete);
                });
            }

        }
        LOG_INFO("Clean %d retired version slots, took %lf secs\n", clean_count, cycles / (CYCLES_PER_SEC + 0.0));

        if (collect_count == 0 && clean_count == 0) {
            // sleep at most 0.8192 s
            if (backoff_shifts < 13) {
                ++backoff_shifts;
            }
            uint64_t sleep_duration = 1UL << backoff_shifts;
            sleep_duration *= 100;
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
        } else {
            backoff_shifts >>= 1;
        }
    }

    LOG_DEBUG("Stop version(overwritten/retired) cleaning.");
}

}
