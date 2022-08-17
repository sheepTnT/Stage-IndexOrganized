//
// Created by zhangqian on 2022/1/12.
//
#include "../include/common/ephemeral_pool.h"

namespace mvstore {

EphemeralPool::~EphemeralPool() {
    auto iterator = locations_.GetIterator();
    for (auto &iter : iterator) {
        locations_.Erase(iter.first);
    }
    locations_.Clear();
    undo_buffer_pool->Empty();
}

std::pair<uint32_t, uint64_t> EphemeralPool::Allocate(char *hdr_,
                                 char *src_rcd, uint64_t next_tuple,
                                 uint16_t key_len,
                                 uint32_t payload_size,
                                 const cid_t cstamp,
                                 const cid_t rstamp) {
    auto size = payload_size + key_len;
    std::pair<uint32_t, char *> location = undo_buffer_pool->NewEntry(size);

    //copy the record to the new location
    char *new_loc = location.second;

    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header
            (new EphemeralPool::OverwriteVersionHeader(cstamp, rstamp, MAX_CID));
    header->SetNodeHeader(hdr_);
//    std::cout<< "GetNextTupleHeader %zu:" << next_tuple << std::endl;
    header->SetNext(next_tuple);
    header->SetBufferIndex(location.first);
    header->SetWaiting(false);
    header->SetPayloadSize(payload_size);
    header->SetKeyLen(key_len);

    auto loc_ptr = reinterpret_cast<uint64_t>(new_loc);
    locations_.Upsert(loc_ptr,header);

    return std::make_pair(location.first,loc_ptr);
}
/**
 * free the location
 * @param ptr
 */
bool EphemeralPool::Free(uint32_t buffer_index, uint64_t ptr) {

    auto ret = locations_.Erase(ptr);
    assert(ret);
    undo_buffer_pool->Erase(buffer_index);
    return true;

//    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
//    auto ret = locations_.Find(ptr,header);
//    //undo buffer pool block's entry count --
//    if(ret){
//        if(header->count <= 0){
//            undo_buffer_pool->Erase(ptr);
//        }
//    }
}
/**
 * decrease the dependency on the location
 * @param ptr
 */
void EphemeralPool::DecreaseWRCount(uint64_t ptr) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];

    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        auto &latch = header->Getlatch();
        latch.Lock();
        UNUSED_ATTRIBUTE uint16_t count = header->SubCount();
        latch.Unlock();
//        if (count <= 0) {
//            Free(header->GetBufferIndex(), ptr);
//        }
    }

}
/**
 * increase the dependency on the location
 * @param ptr
 */
bool EphemeralPool::IncreaseWRCount(uint64_t ptr) const {
//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        auto &latch = header->Getlatch();
        latch.Lock();
        auto ret_add = header->AddCount();
        latch.Unlock();
        return ret_add;
    }
    return ret;
}
/**
 *
 * @param ptr copy record location
 * @return null: has been old version or waiting free
 *         not null: has been writting
 */
std::shared_ptr<EphemeralPool::OverwriteVersionHeader>
                    EphemeralPool::GetOversionHeader(uint64_t ptr) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_.Find()find(ptr) ;
    if (ptr == 0){
        return nullptr;
    }
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header ;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        return header;
    }
    return nullptr;
}
/**
 * get the version info of the location
 * @param ptr
 * @return
 */
std::shared_ptr<EphemeralPool::OverwriteVersionHeader>
                EphemeralPool::GetOversionHeaderComm(uint64_t ptr) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_.Find()find(ptr) ;
    if (ptr == 0){return nullptr;}
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header ;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        //true:waiting free, waiting state
        //false:in writing, active state
        return header;
    }
    return nullptr;
}
///**
// * track the writer of the record meta
// * old->new
// * @param metaptr_val key, recoord meta
// * @param meta old_meta
// * @param overwriter new_meta(overwriter)
// */
//void EphemeralPool::AddWriters(uint64_t metaptr_val, cid_t meta, cid_t overwriter){
//    std::vector<std::pair<cid_t, cid_t>> writers;
//    auto ret = writers_.Find(metaptr_val, writers);
//    if (ret){
//        writers.emplace_back(std::make_pair(meta, overwriter));
//    }else{
//        writers.emplace_back(std::make_pair(meta, overwriter));
//        writers_.Upsert(metaptr_val, writers);
//    }
//}
///**
// * get old by cstamp of the current new meta
// * @param metaptr_val
// * @param meta
// * @return
// */
//cid_t EphemeralPool::GetOveriter(uint64_t metaptr_val, cid_t meta){
//    cid_t overwr = INVALID_CID;
//    std::vector<std::pair<cid_t, cid_t>> writers;
//    auto ret = writers_.Find(metaptr_val, writers);
//    if (ret){
//        for (int i = 0; i < writers.size(); ++i) {
//            if (writers[i].first == meta){
//                overwr = writers[i].second;
//                break;
//            }
//        }
//    }
//
//    return overwr;
//}
cid_t EphemeralPool::GetPs(uint64_t ptr) {
    cid_t ps = INVALID_CID;

//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        ps = header->GetPstamp();
    }
    return ps;
}

bool EphemeralPool::UpdatePs(uint64_t ptr, cid_t psp) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        header->SetPstamp(psp);
        return true;
    }

    return false;
}

bool EphemeralPool::UpdateSs(uint64_t ptr, cid_t ssp) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        header->SetSstamp(ssp);
        return true;
    }

    return false;
}

void EphemeralPool::SetNext(uint64_t ptr, uint64_t next_) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        header->SetNext(next_);
    }
}

uint64_t EphemeralPool::GetNext(uint64_t ptr)  {
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    uint64_t next = 0;
    if (ret) {
        next = header->GetNext();
    }
    return next;
}
void EphemeralPool::SetPre(uint64_t ptr,  uint64_t pre_) {
//    auto *cptr = (char *) ptr;
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    if (ret) {
        header->SetPre(pre_);
    }
}

uint64_t EphemeralPool::GetPre(uint64_t ptr)  {
//    auto header = locations_[ptr];
    std::shared_ptr<EphemeralPool::OverwriteVersionHeader> header;
    auto ret = locations_.Find(ptr,header);
    uint64_t pre;
    if (ret) {
        pre = header->GetPre();
        return pre;
    }
    return 0;
}
}