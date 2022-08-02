
#pragma once

#include <cstdint>
#include <cstdlib>
#include "tbb/concurrent_unordered_map.h"
#include "../include/common/cuckoo_map.h"
#include "macros.h"
#include "spin_latch.h"
#include "constants.h"
#include "../include/vstore/valen_buffer.h"

namespace mvstore {

//===----------------------------------------------------------------------===//
//
// A memory pool that can quickly allocate chunks of memory to clients.
// For each tuple in BTree, we allocate the space to store the tmp version
// By using this method, we can deal the transaction roll back(used undo) and
//    process the read/write transaction concurrency on the tuple
//===----------------------------------------------------------------------===//

class EphemeralPool {

public:
    struct OverwriteVersionHeader{
        OverwriteVersionHeader();
        OverwriteVersionHeader(cid_t cstamp_, cid_t pstamp_, cid_t sstamp_ ) : cstamp(cstamp_),
                             pstamp(pstamp_),sstamp(sstamp_),count(0),next_ptr(0),
                             pre_ptr(0),node_header_ptr(nullptr),buffer_index_(0),key_len(0),
                             payload_size(0),waiting_free(false){
            readers.resize(0);
        }
        //the create timestamp of the version
        //writer
        cid_t cstamp;
        //the latest read/write timestamp of the version
        cid_t pstamp;
        //the end timestamp of the version
        cid_t sstamp;
        //old version tuple header
        uint64_t next_ptr;
        //new version tuple header
        uint64_t pre_ptr;
        //readers
        std::vector<cid_t> readers;
        char *node_header_ptr;
        uint32_t payload_size;
        uint16_t key_len;
        //the index of the valen buffer
        uint32_t buffer_index_;
        bool waiting_free;
        //read dependency transaction count
        uint16_t count;
        SpinLatch latch_;

        inline SpinLatch &Getlatch(){
            return this->latch_;
        }
        inline void AddReader(cid_t read_id) {
            latch_.Lock();
            readers.emplace_back(read_id);
            latch_.Unlock();
        }
        inline cid_t GetReaders(int num) {
            cid_t reader_id = INVALID_CID;
            if (readers.size() >= num){
                reader_id = readers[num];
            }
            return reader_id;
        }
        inline int GetReadersNum() const{
            return readers.size();
        }
        inline cid_t GetPstamp(){
            return pstamp;
        }
        inline cid_t GetSstamp(){
            return sstamp;
        }
        inline cid_t GetCstamp(){
            return cstamp;
        }
        inline char *GetNodeHeader(){
            return reinterpret_cast<char *>(node_header_ptr);
        }
        inline void SetNodeHeader(char *header){
            this->node_header_ptr = header;
        }
        inline uint32_t GetPayloadSize(){
            return payload_size;
        }
        inline void SetPayloadSize(uint32_t payload_sz){
            this->payload_size = payload_sz;
        }
        inline uint16_t GetKeyLen(){
            return key_len;
        }
        inline void SetKeyLen(uint16_t key_sz){
            this->key_len = key_sz;
        }
        inline void SetPstamp(cid_t ps){
            this->pstamp = ps;
        }
        inline void SetSstamp(cid_t ss){
            this->sstamp = ss;
        }
        inline void SetNext(uint64_t next_){
            this->next_ptr = next_;
        }
        inline void SetPre(uint64_t pre_){
            this->pre_ptr = pre_;
        }
        inline void SetBufferIndex(uint32_t index_){
            this->buffer_index_ = index_;
        }
        inline uint32_t GetBufferIndex(){
            return this->buffer_index_ ;
        }
        inline uint64_t GetNext(){
            return  this->next_ptr;
        }
        inline  uint64_t GetPre(){
            return  this->pre_ptr;
        }
        inline void SetWaiting(bool is_wait){
            waiting_free = is_wait;
        }
        inline bool GetWaiting(){
            return waiting_free;
        }
        inline bool AddCount(){
            if(!waiting_free){
                count++;
                return  true;
            }
            return false;
        }
        inline bool SubCount(){
            count--;
            return count;
        }
        inline uint16_t Count(){
            return count;
        }
    };

/**
* @brief A hashing functor for the OverwriteVersionHeader abstraction
*/
    class OverwriteVersionHeaderHasher {
        size_t operator()(const OverwriteVersionHeader &header) const {
            std::size_t seed = 0;
            boost::hash_combine(seed,header.cstamp);
            boost::hash_combine(seed,header.pstamp);
            boost::hash_combine(seed,header.sstamp);
            return seed;
        }
    };

/**
 * @brief Comparator functor for the OverwriteVersionHeader abstraction
 */
    class OverwriteVersionHeaderComparator {
    public:
        bool operator()(const OverwriteVersionHeader &lhs,
                        const OverwriteVersionHeader &rhs) const {
            return (lhs.cstamp == rhs.cstamp) && (lhs.pstamp == rhs.pstamp)
                   && (lhs.sstamp == rhs.sstamp) ;
        }
    };

  EphemeralPool(UndoBuffer *buffer): undo_buffer_pool(buffer) { };

  ~EphemeralPool();

  std::pair<uint32_t, uint64_t>  Allocate(char *header, char *src_rcd, uint64_t next_tuple,
                    uint16_t padd_key_len,
                    uint32_t payload_size, const cid_t cstamp, const cid_t pstamp);

  bool Free( uint32_t buffer_index, uint64_t ptr);

  cid_t GetPs(uint64_t ptr);
  std::shared_ptr<OverwriteVersionHeader> GetOversionHeader(uint64_t ptr);
  std::shared_ptr<OverwriteVersionHeader> GetOversionHeaderComm(uint64_t ptr);
  void DecreaseWRCount(uint64_t ptr);
  bool IncreaseWRCount(uint64_t ptr) const;
  bool UpdatePs(uint64_t ptr,  cid_t pstamp);
  bool UpdateSs(uint64_t ptr,  cid_t sstamp);
  void SetNext(uint64_t ptr, uint64_t next_);
  uint64_t GetNext(uint64_t ptr)  ;
  void SetPre(uint64_t ptr,uint64_t pre_);
  uint64_t GetPre(uint64_t ptr)  ;
//  void AddWriters(uint64_t metaptr_val, cid_t overwriter, cid_t curr);
//  cid_t GetOveriter(uint64_t metaptr_val, cid_t curr);


 public:
//  tbb::concurrent_unordered_map<uint64_t, std::shared_ptr<OverwriteVersionHeader>> locations_;
  CuckooMap<uint64_t, std::shared_ptr<OverwriteVersionHeader>> locations_;
  //meta ptr, meta cstamp, overwriter version
  CuckooMap<uint64_t, std::vector<std::pair<cid_t, cid_t>>> writers_;
  //this is a global transaction undobuffer, which holds the tmp records
  //gc will iterate the valenbuffers of the undobuffer,
  //if the valenbuffer entris count =0, then the valenbuffer will be release
  UndoBuffer *undo_buffer_pool;
  //spin lock protecting location list
  SpinLatch pool_lock_;
};


}
