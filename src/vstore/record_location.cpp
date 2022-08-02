
#include "../include/vstore/record_location.h"
#include "../include/common/raw_atomics.h"

namespace mvstore {


RecordLocation INVALID_RECORDLOCATION;

bool AtomicUpdateRecordLocation(void *meta_ptr_, const RecordLocation& value) {
//    PELOTON_ASSERT(sizeof(RecordLocation) == sizeof(int64_t),"atomic update recordlocation fail.");
//    int64_t* cast_src_ptr = reinterpret_cast<int64_t*>((void*)src_ptr);
//    int64_t* cast_value_ptr = reinterpret_cast<int64_t*>((void*)&value);
//    auto ret1 = __sync_bool_compare_and_swap(cast_src_ptr, *cast_src_ptr,
//                                        *cast_value_ptr);
    RecordMetadata *meta_ptr = (reinterpret_cast<RecordMetadata *>(meta_ptr_));
    RecordLocation *src_ptr = meta_ptr->GetLocationPtr();
    RecordMetadata expected_meta = *meta_ptr;
    RecordMetadata finl_meta = expected_meta;
    finl_meta.FinalizeForUpdate();
    bool ret_1 = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &meta_ptr->meta,
            &(expected_meta.meta),
            finl_meta.meta);
    src_ptr->record_meta_ptr = value.record_meta_ptr;
    src_ptr->node_header_ptr = value.node_header_ptr;
    finl_meta.FinalizeForUpdate();
    expected_meta = *meta_ptr;
    bool ret_2 = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &meta_ptr->meta,
            &(expected_meta.meta),
            finl_meta.meta);

    if (!ret_1 || !ret_2){
        LOG_DEBUG("atomic update fail, %d, %d", ret_1, ret_2);
    }
    assert(ret_1 && ret_2);

    return ret_1 && ret_2;
}

}
