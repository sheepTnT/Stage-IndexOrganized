//
// Created by zhangqian on 2021/11/1.
//

#ifndef MVSTORE_CONSTANTS_H
#define MVSTORE_CONSTANTS_H

#include <limits>
#include <functional>
#include <queue>
#include <boost/functional/hash.hpp>

namespace mvstore {


typedef uint32_t oid_t;
static const oid_t START_OID = 0;
static const oid_t INVALID_OID = std::numeric_limits<oid_t>::max();
static const oid_t MAX_OID = std::numeric_limits<oid_t>::max() - 1;
#define NULL_OID MAX_OID

// For transaction id
typedef uint64_t txn_id_t;
static const txn_id_t INVALID_TXN_ID = 0;
static const txn_id_t INITIAL_TXN_ID = 1;
static const txn_id_t MAX_TXN_ID = std::numeric_limits<txn_id_t>::max();

// For commit id
//typedef uint64_t cid_t;
typedef uint32_t cid_t;
static const cid_t INVALID_CID = 0;
static const cid_t MAX_CID = std::numeric_limits<cid_t>::max();

// For epoch id
typedef uint64_t eid_t;
static const eid_t INVALID_EID = 0;
static const eid_t MAX_EID = std::numeric_limits<eid_t>::max();

// For epoch
static const size_t EPOCH_LENGTH = 40;

constexpr oid_t InvalidBlockID = std::numeric_limits<oid_t>::max();
constexpr oid_t BlockSizeBits = 14;

/**
 * Block/NVMBlock size, in bytes. Must be a power of 2.
 * 1MB
 * 64KB
 */
static const uint32_t NVM_BLOCK_SIZE = 1024*1024;
static const uint32_t DRAM_BLOCK_SIZE = 64*1024;
static const uint32_t BUFFER_SEGMENT_SIZE = 1024*1024;
//static const uint32_t BLOCK_SIZE = 256;
//static const uint32_t DRAM_BLOCK_SIZE = 256;

#define CL_SIZE	64
typedef uint64_t LSN_T;
static constexpr LSN_T InvalidLSN = std::numeric_limits<LSN_T>::min();

// For all of the enums defined in this header, we will
// use this value to indicate that it is an invalid value
// I don't think it matters whether this is 0 or -1
#define INVALID_TYPE_ID  0
#define CACHE_LINE_SIZE	 64

static const size_t default_active_block_count_ = 10;

static size_t default_blocks = 40*1024;
static size_t default_nvm_blocks = 2*1024;

//64KB*1024*1024/64 =64GB/64=1GB
//static size_t default_blocks = 512*1024;
//static size_t default_nvm_blocks = 30*1024;


static bool nvm_emulate = true;
static bool enable_pcm = false;

static int monoqueue_task_queue_size = 10;
static int monoqueue_worker_pool_size = 3 ;

static void *block_aligned_alloc(std::size_t sz) {
    void *ptr;
    auto res = posix_memalign(&ptr, 512, sz);
    if (res != 0) {
        return nullptr;
    }
    return ptr;
}

enum class BTreeState {
    FAILED,
    UPDATED,
    SUCCESS,
    NOT_FOUND,
    PRED_TRUE,
    PRED_FALSE,
    RETRY_SCAN,
    END_SCAN,
    CONTINUE_SCAN
};

//===--------------------------------------------------------------------===//
// Result Types
//===--------------------------------------------------------------------===//

enum class ResultType {
    INVALID = 0,  // invalid result type
    SUCCESS = 1,
    FAILURE = 2,
    ABORTED = 3,  // aborted
    NOOP = 4,     // no op
    UNKNOWN = 5,
    QUEUING = 6,
    TO_ABORT = 7,
};

//===--------------------------------------------------------------------===//
// Isolation Levels
//===--------------------------------------------------------------------===//

enum class IsolationLevelType {
    INVALID = 0,
    SERIALIZABLE = 1,      // serializable
    SNAPSHOT = 2,          // snapshot isolation
    REPEATABLE_READS = 3,  // repeatable reads
    READ_COMMITTED = 4,    // read committed
    READ_ONLY = 5          // read only
};

//===--------------------------------------------------------------------===//
// Visibility Types
//===--------------------------------------------------------------------===//

enum class VisibilityType {
    INVALID = 0,
    INVISIBLE = 1,
    DELETED = 2,
    OK = 3
};

enum class VisibilityIdType {
    INVALID = 0,
    READ_ID = 1,
    COMMIT_ID = 2
};

//===--------------------------------------------------------------------===//
// read-write set
//===--------------------------------------------------------------------===//

// this enum is to identify the operations performed by the transaction.
enum class RWType {
    INVALID,
    READ,
    READ_OWN,  // select for update
    UPDATE,
    INSERT,
    DELETE,
    INS_DEL,  // delete after insert.
};
enum class ColumnFieldType{
    INVALID,

};

//===--------------------------------------------------------------------===//
// gc set
//===--------------------------------------------------------------------===//
// this enum is to identify why the version should be GC'd.
enum class GCVersionType {
    INVALID,
    COMMIT_UPDATE,   // a version that is updated during txn commit.
    COMMIT_DELETE,   // a version that is deleted during txn commit.
    COMMIT_INS_DEL,  // a version that is inserted and deleted during txn commit.
    ABORT_UPDATE,    // a version that is updated during txn abort.
    ABORT_DELETE,    // a version that is deleted during txn abort.
    ABORT_INSERT,    // a version that is inserted during txn abort.
    ABORT_INS_DEL,   // a version that is inserted and deleted during txn commit.
};
// <block physical location, offset> -> gc version type -> end commit id
//typedef std::unordered_map<void *, std::pair<GCVersionType, txn_id_t>> GCSet;
//txn commit id, old version type, table id, version tuple header physical location, header size
struct TxnVersionInfo{
  uint32_t table_id;
//  uint64_t block_ptr;
  void *tuple_header;
  uint32_t tuple_header_size;
  GCVersionType version_type;

    bool operator==(const TxnVersionInfo &rhs) const {
        return table_id == rhs.table_id &&
                tuple_header == rhs.tuple_header && version_type == rhs.version_type;
    }

    bool operator!=(const TxnVersionInfo &rhs) const {
        return !operator==(rhs);
    }
};
struct TxnVersionInfoHasher {
    size_t operator()(const TxnVersionInfo &item) const {
        // This constant is found in the CityHash code
        // [Source libcuckoo/default_hasher.hh]
        // std::hash returns the same number for unsigned int which causes
        // too many collisions in the Cuckoohash leading to too many collisions
        size_t seed = 0UL;
        boost::hash_combine(seed, item.tuple_header_size);
        boost::hash_combine(seed, item.tuple_header);
        boost::hash_combine(seed, item.table_id);
//        boost::hash_combine(seed, item.block_ptr);
        boost::hash_combine(seed, item.version_type);

        return seed;
    }
};



const size_t INDIRECTION_ARRAY_MAX_SIZE = 64*1024 / 8;
const size_t INVALID_INDIRECTION_OFFSET = std::numeric_limits<size_t>::max();

}
#endif
