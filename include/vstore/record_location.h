//
// Created by zhangqian on 2022/7/26.
//

#ifndef MVSTORE_RECORD_LOCATION_H
#define MVSTORE_RECORD_LOCATION_H

#include "../common/constants.h"

namespace mvstore{

// record meta physical location
class RecordLocation {
public:
    // node header location
    uint64_t node_header_ptr;

    // record meta location
    uint64_t record_meta_ptr;

    RecordLocation() : node_header_ptr(0), record_meta_ptr(0) {}

    RecordLocation(uint64_t n_d_ptr, uint64_t r_m_ptr) : node_header_ptr(n_d_ptr), record_meta_ptr(r_m_ptr) {}

    bool IsNull() const {
        return (node_header_ptr ==0 && record_meta_ptr == 0);
    }

    bool operator<(const RecordLocation &rhs) const {
        if (node_header_ptr != rhs.node_header_ptr) {
            return node_header_ptr < rhs.node_header_ptr;
        } else {
            return record_meta_ptr < rhs.record_meta_ptr;
        }
    }

    bool operator==(const RecordLocation &rhs) const {
        return ( node_header_ptr == rhs.node_header_ptr && record_meta_ptr == rhs.record_meta_ptr);
    }


} __attribute__((__aligned__(8))) __attribute__((__packed__));

extern RecordLocation INVALID_RECORDLOCATION;

class RecordLocationComparator {
public:
    bool operator()(RecordLocation *const &p1, RecordLocation *const &p2) const {
        return  (p1->node_header_ptr == p2->node_header_ptr && p1->record_meta_ptr == p2->record_meta_ptr);
    }

    bool operator()(RecordLocation const &p1, RecordLocation const &p2) const {
        return  (p1.node_header_ptr == p2.node_header_ptr && p1.record_meta_ptr == p2.record_meta_ptr);
    }

    RecordLocationComparator(const RecordLocationComparator &) {}
    RecordLocationComparator() {}
};

struct RecordLocationHasher {
    size_t operator()(const RecordLocation &item) const {
        // This constant is found in the CityHash code
        // [Source libcuckoo/default_hasher.hh]
        // std::hash returns the same number for unsigned int which causes
        // too many collisions in the Cuckoohash leading to too many collisions
        return (std::hash<uint64_t>()(item.node_header_ptr)*0x9ddfea08eb382d69ULL) ^
               std::hash<uint64_t>()(item.record_meta_ptr);

    }
};

class RecordLocationHashFunc {
public:
    size_t operator()(RecordLocation *const &p) const {
        return (std::hash<uint64_t>()(p->node_header_ptr)*0x9ddfea08eb382d69ULL) ^
               std::hash<uint64_t>()(p->record_meta_ptr);
    }

    RecordLocationHashFunc(const RecordLocationHashFunc &) {}
    RecordLocationHashFunc() {}
};

bool AtomicUpdateRecordLocation(void *meta_ptr, const RecordLocation &value);

}

#endif
