//
// Created by zhangqian on 2022/2/10.
//
#pragma once

#include "../../include/execute/txn.h"
#include "../../include/common/logger.h"
#include "../../include/common/sync.h"
#include "../../include/execute/executor.h"
#include "../../include/vstore/b_tree.h"
#include "ycsb_configuration.h"

namespace mvstore {
namespace benchmark {
namespace ycsb {

typedef BTree YCSBTable;

extern configuration state;

extern YCSBTable *user_table;

extern std::vector<std::string> eml_keys;

void CreateYCSBDatabase(ParameterSet param, VersionStore *buf_mgr = nullptr,
                        LogManager *log_mng = nullptr,
                        DramBlockPool *leaf_node_pool= nullptr,
                        InnerNodeBuffer *inner_node_pool= nullptr,
                        EphemeralPool *conflict_buffer= nullptr);

void LoadYCSBDatabase(VersionStore *version_store);

void LoadYCSBRows(VersionStore *version_store, const int begin_rowid, const int end_rowid, int thread_id);

void DestroyYCSBDatabase(VersionBlockManager *version_block_mng,
                         VersionStore *version_store,
                         SSNTransactionManager *txn_mng);
}
}
}
