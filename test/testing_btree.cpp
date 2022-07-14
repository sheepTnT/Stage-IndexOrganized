//
// Created by zhangqian on 2022-1-7.
//

#include <iostream>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>
#include "../include/common/bitmap.h"
#include "../include/common/cuckoo_map.h"
#include "../include/common/random.h"
#include "../include/common/crc32c.h"
#include "../include/vstore/b_tree.h"
#include "../include/execute/txn.h"

namespace mvstore{

namespace test{

//---------------------Test LeafNode insert/read/delete/update---------------------//
//---------------------------------------------------------------------------------//
//class LeafNodeFixtures : public ::testing::Test{
//public:
//    const uint32_t node_size = 4096;
//    const uint32_t split_threshold = 3072;
//
//    void EmptyNode() {
//        delete leaf_node;
//        leaf_node = (LeafNode *) (new char[node_size]);
//        memset(leaf_node, 0, leaf_node->GetHeader()->size);
//        new(leaf_node) LeafNode(node_size);
//    }
//
//    // Dummy value:
//    // sorted -> 0:10:100
//    // unsorted -> 200:10:300
////    TransactionContext *txn;
//    void InsertDummy() {
//        for (uint32_t i = 0; i < 100; i += 10) {
//            RecordMetadata *meta = nullptr;
//            auto str = std::to_string(i);
//            txn_conxt->SetCommitId(0);
//            leaf_node->Insert(str.c_str(), (uint16_t) str.length(), reinterpret_cast<const char *>(&i),
//                         sizeof(uint64_t), &meta, txn_conxt->GetCommitId(), split_threshold);
//
//            leaf_node->FinalizeInsert(meta, txn_conxt->GetCommitId());
//        }
//
//        LeafNode *new_leaf = (LeafNode *) (new char[node_size]);
//        //merging and sorting
//        leaf_node->Consolidate(sizeof(uint64_t), txn_conxt->GetCommitId(), &new_leaf, node_size,
//                               dram_block_pool);
//
//        for (uint32_t i = 200; i < 300; i += 10) {
//            RecordMetadata *meta = nullptr;
//            auto str = std::to_string(i);
//            new_leaf->Insert(str.c_str(), (uint16_t) str.length(), reinterpret_cast<const char *>(&i),
//                             sizeof(uint64_t), &meta, txn_conxt->GetCommitId(), split_threshold);
//
//            new_leaf->FinalizeInsert(meta, txn_conxt->GetCommitId());
//        }
//
//        delete leaf_node;
//        leaf_node = new_leaf;
//    }
//
//    void ASSERT_READ(LeafNode *node, const char *key, uint16_t key_size, uint64_t expected) {
//        RecordMetadata *meta = nullptr;
//        node->Read(key, key_size, sizeof(uint64_t), &meta, false);
//
//        RecordMeta record_meta{reinterpret_cast<uint64_t>(node),0,*meta};
//
//        Record *record = Record::New(record_meta, node, sizeof(uint64_t));
//        uint64_t payload_ = *reinterpret_cast<const uint64_t *>(record->GetPayload());
//        assert(payload_ == expected);
//
//    }
//
//protected:
//    DramBlockPool *dram_block_pool ;
//    EphemeralPool *ephm_pool ;
//    LeafNode *leaf_node = nullptr;
//    TupleHeader *tuple_hdr = nullptr ;
//    TransactionContext *txn_conxt ;
//    Catalog schema;
//    InnerNodeBuffer *inner_node_buffer;
//
//
//    void SetUp() override{
//        dram_block_pool = new DramBlockPool(default_blocks, default_blocks);
//
//        RecordBufferPool *buffer_pool = new RecordBufferPool(5000,5000);
//        UndoBuffer *undo_buffer_pool = new UndoBuffer(buffer_pool);
//        ephm_pool = new EphemeralPool(undo_buffer_pool);
//
//        RecordBufferPool *inner_buffer_pool = new RecordBufferPool(5000,5000);
//        inner_node_buffer = new InnerNodeBuffer(inner_buffer_pool);
//
//        leaf_node = (LeafNode *) (new char[node_size]);
//        LeafNode::New(&leaf_node, node_size, dram_block_pool);
//        //thread_id = 0, read_id = 0, commit_id = 0
//        txn_conxt = new TransactionContext(0,IsolationLevelType::SERIALIZABLE,
//                                           0,0);
//
//        uint8_t *key_col_bits = new uint8_t[2];
//        key_col_bits[0] = 1;
//        key_col_bits[1] = 0;
//        schema.init("test",2,key_col_bits, 8,0,false);
//        schema.add_col("key",8,"VARCHAR");
//        schema.add_col("payload",8,"VARCHAR");
//    }
//
//    void TearDown() override{
//        delete dram_block_pool;
//        delete tuple_hdr;
//        delete txn_conxt;
//    }
//};
//
//TEST_F(LeafNodeFixtures, Read) {
//    InsertDummy();
//    RecordMetadata *meta_read = nullptr;
//
//    ASSERT_READ(leaf_node, "0", 1, 0);
//    ASSERT_READ(leaf_node, "10", 2, 10);
//    ASSERT_TRUE(leaf_node->Read("200", 3, sizeof(uint64_t), &meta_read,false).IsOk());
//    ASSERT_TRUE(leaf_node->Read("20", 2, sizeof(uint64_t), &meta_read,false).IsOk());
//    ASSERT_TRUE(leaf_node->Read("100", 3, sizeof(uint64_t), &meta_read,false).IsNotFound());
//
//    ASSERT_READ(leaf_node, "200", 3, 200);
//    ASSERT_READ(leaf_node, "210", 3, 210);
//    ASSERT_READ(leaf_node, "280", 3, 280);
//
//}
//
//TEST_F(LeafNodeFixtures, Insert) {
//    EmptyNode();
//
//    RecordMetadata *meta_inrt_def = nullptr;
//    ASSERT_TRUE(leaf_node->Insert("def", 3, reinterpret_cast<char *>(100),
//                             sizeof(uint64_t), &meta_inrt_def, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    leaf_node->FinalizeInsert(meta_inrt_def, 2);
//
//    RecordMetadata *meta_inrt_bdef = nullptr;
//    ASSERT_TRUE(leaf_node->Insert("bdef", 4, reinterpret_cast<char *>(101),
//                             sizeof(uint64_t), &meta_inrt_bdef, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    leaf_node->FinalizeInsert(meta_inrt_bdef, 2);
//
//    RecordMetadata *meta_inrt_abc = nullptr;
//    ASSERT_TRUE(leaf_node->Insert("abc", 3, reinterpret_cast<char *>(102),
//                             sizeof(uint64_t), &meta_inrt_abc, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    leaf_node->FinalizeInsert(meta_inrt_abc, 2);
//
//    ASSERT_READ(leaf_node, "def", 3, 100);
//    ASSERT_READ(leaf_node, "abc", 3, 102);
//
//    LeafNode *new_leaf = nullptr;
//    char *new_node = new char[node_size];
//    new_leaf = (LeafNode *) (new_node);
//    memset(new_leaf, 0, node_size);
//    leaf_node->Consolidate(sizeof(uint64_t), txn_conxt->GetCommitId(), &new_leaf, node_size, dram_block_pool);
//
//    RecordMetadata *meta_inrt_apple = nullptr;
//    ASSERT_TRUE(new_leaf->Insert("apple", 5, reinterpret_cast<char *>(106),
//                                 sizeof(uint64_t), &meta_inrt_apple, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    new_leaf->FinalizeInsert(meta_inrt_apple, 2);
//
//    ASSERT_READ(leaf_node, "bdef", 4, 101);
//    ASSERT_READ(new_leaf, "apple", 5, 106);
//
//    delete  new_leaf;
//}
//
//TEST_F(LeafNodeFixtures, DuplicateInsert) {
//    EmptyNode();
//    InsertDummy();
//
//    RecordMetadata *meta_inrt_10 = nullptr;
//    ASSERT_TRUE(leaf_node->Insert("10", 2, reinterpret_cast<char *>(111),
//                             sizeof(uint64_t), &meta_inrt_10, txn_conxt->GetCommitId(), split_threshold).IsKeyExists());
//
//    RecordMetadata *meta_inrt_11 = nullptr;
//    ASSERT_TRUE(leaf_node->Insert("11", 2, reinterpret_cast<char *>(1212),
//                             sizeof(uint64_t), &meta_inrt_11, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    leaf_node->FinalizeInsert(meta_inrt_11, 2);
//
//    ASSERT_READ(leaf_node, "10", 2, 10);
//    ASSERT_READ(leaf_node, "11", 2, 1212);
//
//    LeafNode *new_leaf = nullptr;
//    new_leaf = (LeafNode *) (new char[node_size]);
//    memset(new_leaf, 0, node_size);
//    leaf_node->Consolidate(sizeof(uint64_t), txn_conxt->GetCommitId(), &new_leaf, node_size, dram_block_pool);
//
//    RecordMetadata *meta_inrt_13 = nullptr;
//    ASSERT_TRUE(new_leaf->Insert("11", 2, reinterpret_cast<char *>(1213),
//                                 sizeof(uint64_t), &meta_inrt_13, txn_conxt->GetCommitId(), split_threshold).IsKeyExists());
//    ASSERT_READ(new_leaf, "11", 2, 1212);
//
//    RecordMetadata *meta_inrt_201 = nullptr;
//    ASSERT_TRUE(new_leaf->Insert("201", 3, reinterpret_cast<char *>(201),
//                                 sizeof(uint64_t), &meta_inrt_201, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    new_leaf->FinalizeInsert(meta_inrt_201, 2);
//    ASSERT_READ(new_leaf, "201", 3, 201);
//
//    delete  new_leaf;
//}
//
//TEST_F(LeafNodeFixtures, Delete) {
//    EmptyNode();
//    InsertDummy();
//
//    ASSERT_READ(leaf_node, "40", 2, 40);
//
//    RecordMetadata *meta_del = nullptr;
//    ASSERT_TRUE(leaf_node->Read("20", 2, sizeof(uint64_t), &meta_del,false).IsOk());
//    ASSERT_TRUE(leaf_node->Read("200", 3, sizeof(uint64_t), &meta_del,false).IsOk());
//    ASSERT_TRUE(leaf_node->Read("20", 2, sizeof(uint64_t), &meta_del,false).IsOk());
//    ASSERT_TRUE(leaf_node->Delete("40", 2, sizeof(uint64_t), &meta_del,
//                                  tuple_hdr,false, txn_conxt->GetCommitId(), ephm_pool).IsOk());
//    leaf_node->FinalizeDelete(meta_del, 2, ephm_pool);
//
//    RecordMetadata *meta_read = nullptr;
//    ASSERT_TRUE(leaf_node->Read("40", 2, sizeof(uint64_t), &meta_read,false).IsNotFound());
//
//    LeafNode *new_leaf = nullptr;
//    new_leaf = (LeafNode *) (new char[node_size]);
//    memset(new_leaf, 0, node_size);
//    leaf_node->Consolidate(sizeof(uint64_t), txn_conxt->GetCommitId(), &new_leaf, node_size,dram_block_pool);
//
//    ASSERT_READ(new_leaf, "200", 3, 200);
//    RecordMetadata *meta_del_new = nullptr;
//    ASSERT_TRUE(new_leaf->Delete("200", 3, sizeof(uint64_t), &meta_del_new,
//                                 tuple_hdr,false, txn_conxt->GetCommitId(), ephm_pool).IsOk());
//    new_leaf->FinalizeDelete(meta_del_new, 2, ephm_pool);
//
//    RecordMetadata *meta_read_new = nullptr;
//    ASSERT_TRUE(new_leaf->Read("200", 3,sizeof(uint64_t), &meta_read_new,false).IsNotFound());
//
//    delete  new_leaf;
//}
//
//TEST_F(LeafNodeFixtures, SplitPrep) {
//    EmptyNode();
//    InsertDummy();
//    RecordMetadata *meta_inrt;
//
//    ASSERT_TRUE(leaf_node->Insert("abc", 3, reinterpret_cast<char *>(100),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    ASSERT_TRUE(leaf_node->Insert("bdef", 4, reinterpret_cast<char *>(101),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    ASSERT_TRUE(leaf_node->Insert("abcd", 4, reinterpret_cast<char *>(102),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    ASSERT_TRUE(leaf_node->Insert("deadbeef", 8, reinterpret_cast<char *>(103),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    ASSERT_TRUE(leaf_node->Insert("parker", 6, reinterpret_cast<char *>(104),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    ASSERT_TRUE(leaf_node->Insert("deadpork", 8, reinterpret_cast<char *>(105),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//    ASSERT_TRUE(leaf_node->Insert("toronto", 7, reinterpret_cast<char *>(106),
//                             sizeof(uint64_t), &meta_inrt, txn_conxt->GetCommitId(), split_threshold).IsOk());
//
//    Stack stack;
//    LeafNode *left = (LeafNode *) (new char[node_size]);
//    memset(left, 0, node_size);
//    LeafNode *right = (LeafNode *) (new char[node_size]);
//    memset(right, 0, node_size);
//    leaf_node->Freeze();
//    InternalNode *parent;
//    leaf_node->PrepareForSplit(stack, 3000, sizeof(uint64_t),
//                               txn_conxt->GetCommitId(), &left, &right, &parent, true,
//                                dram_block_pool, inner_node_buffer);
//    ASSERT_NE(parent, nullptr);
//    ASSERT_NE(left, nullptr);
//    ASSERT_NE(right, nullptr);
//}
//TEST_F(LeafNodeFixtures, Update) {
//    EmptyNode();
//    InsertDummy();
//    ASSERT_READ(leaf_node, "10", 2, 10);
//    std::vector<mvstore::oid_t> columns = {1};
//    RecordMetadata *meta_upt = nullptr;
//    Catalog schema;
//    uint8_t *key_col_bits = new uint8_t[2];
//    key_col_bits[0] = 1;
//    key_col_bits[1] = 0;
//    schema.init("test_up",2,key_col_bits,
//                            8,1,false);
//    schema.add_col("key",8,"VARCHAR");
//    schema.add_col("payload",8,"VARCHAR");
//    txn_conxt->SetCommitId(100);
//    ASSERT_TRUE(leaf_node->Update("10", 2, reinterpret_cast<char *>(11), columns,
//                             sizeof(uint64_t), &meta_upt, txn_conxt->GetCommitId(),
//                             false, ephm_pool, schema).IsOk());
//    leaf_node->FinalizeUpdate(meta_upt, 200, ephm_pool);
//    ASSERT_READ(leaf_node, "10", 2, 11);
//
//    ASSERT_READ(leaf_node, "200", 3, 200);
//    ASSERT_TRUE(leaf_node->Update("200", 3, reinterpret_cast<char *>(201), columns,
//                             sizeof(uint64_t), &meta_upt, txn_conxt->GetCommitId(),
//                             false, ephm_pool, schema).IsOk());
//    leaf_node->FinalizeUpdate(meta_upt, 201, ephm_pool);
//    ASSERT_READ(leaf_node, "200", 3, 201);
//
//    columns.clear();
//}

//TEST_F(LeafNodeFixtures, RangeScanByKey) {
//    EmptyNode();
//    InsertDummy();
//    std::vector<Record *> result;
//    ASSERT_TRUE(leaf_node->RangeScanByKey("10", 2, "40", 2, &result,
//                                                    sizeof(uint64_t)).IsOk());
//    ASSERT_EQ(result.size(), 14);
//    ASSERT_EQ(result[0]->GetPayload(), std::string("10"));
//    ASSERT_EQ(result[2]->GetPayload(), std::string("200"));
//    ASSERT_EQ(result[13]->GetPayload(), std::string("40"));
//    ASSERT_EQ(std::string(result[0]->GetKey(), result[0]->meta.GetKeyLength()),
//              std::string("10"));
//    ASSERT_EQ(std::string(result[2]->GetKey(), result[2]->meta.GetKeyLength()),
//              std::string("200"));
//    ASSERT_EQ(std::string(result[13]->GetKey(), result[13]->meta.GetKeyLength()),
//              std::string("40"));
//
//
//    result.clear();
//}

//---------------------Test BTree insert/read/delete/update---------------------//
//---------------------------------------------------------------------------------//
class BTreeTest : public ::testing::Test {
protected:
    BTree *btree;
    Catalog *table_catalog;
    DramBlockPool *leaf_node_pool;
    TransactionContext *txn_conxt;
    EphemeralPool *conflict_buffer;
    InnerNodeBuffer *inner_node_pool;

    void InsertDummy() {
        RecordMeta inrt_meta;
        for (uint64_t i = 0; i < 100; i += 10) {
            std::string key = std::to_string(i);
            uint64_t delta = i;
            btree->Insert(key.c_str(), key.length(), reinterpret_cast<const char *>(&delta),
                          &inrt_meta,
                          txn_conxt->GetCommitId() );
            btree->FinalizeInsert(inrt_meta.GetMetaPtr(), txn_conxt->GetCommitId());
        }

    }

    void SetUp() override {

        txn_conxt = new TransactionContext(0,IsolationLevelType::SERIALIZABLE,
                                           0,0);

        table_catalog = new Catalog();
        uint8_t *keys_cl = new uint8_t[2];
        keys_cl[0] = 1;
        keys_cl[1] = 0;
        table_catalog->init("test_tree",2, keys_cl, 8,3,false);
        table_catalog->add_col("key",8,"INTEGER");
        table_catalog->add_col("payload",8,"VARCHAR");

//        ParameterSet param(256, 128,256 , 8);
        ParameterSet param(3072, 1024,4096 , 8);

        //for leaf node
        leaf_node_pool = new DramBlockPool(default_blocks, default_blocks);
        //for inner node
        RecordBufferPool *_pool = new RecordBufferPool(10000000,10000000);
        inner_node_pool = new InnerNodeBuffer(_pool);
        //for undo buffer
        RecordBufferPool *buffer_pool = new RecordBufferPool(10000000,10000000);
        UndoBuffer *undo_buffer_pool = new UndoBuffer(buffer_pool);
        conflict_buffer = new EphemeralPool(undo_buffer_pool);

        btree =  new BTree(param, *table_catalog, leaf_node_pool, inner_node_pool, conflict_buffer);
    }

    void TearDown() override {
        delete btree;
        delete table_catalog;
//        delete leaf_node_pool;
//        delete txn_conxt;
//        delete inner_node_pool;
//        delete conflict_buffer;
    }
};

TEST_F(BTreeTest, Insert) {
    static const uint32_t kMaxKey = 1000*100;
//    char *payload_location;
    std::unique_ptr<Record> rcd = nullptr ;
    RecordMeta inrt_meta;
    txn_conxt->SetCommitId(1005);

    for (uint32_t i = 0; i < kMaxKey; ++i) {
        std::string key = std::to_string(i);
        uint64_t delta = i;
        auto rc = btree->Insert(key.c_str(), key.length(), reinterpret_cast<const char *>(&delta),
                                &inrt_meta,
                                txn_conxt->GetCommitId() );

        ASSERT_TRUE(rc.IsOk());
        btree->FinalizeInsert(inrt_meta.GetMetaPtr(), txn_conxt->GetCommitId());

        uint64_t payload = 0;
        rcd = btree->Read(key.c_str(), (uint16_t)key.length(),
                          txn_conxt->GetCommitId(),false);

        ASSERT_TRUE(rc.IsOk());
        ASSERT_TRUE(rcd != nullptr);
        payload = *reinterpret_cast<const uint64_t *>(rcd->GetPayload());
        ASSERT_TRUE(payload == i);
    }

    btree->Dump();

    txn_conxt->SetCommitId(1007);
    // Read everything back
    for (uint32_t i = 0; i < kMaxKey; ++i) {
        std::string key = std::to_string(i);
        uint64_t payload = 0;

        rcd = btree->Read(key.c_str(), (uint16_t)key.length(),
                          txn_conxt->GetCommitId(),false);
        payload = *reinterpret_cast<const uint64_t *>(rcd->GetPayload());
        ASSERT_TRUE(rcd != nullptr);
        ASSERT_TRUE(payload == i);
        ASSERT_TRUE(payload == i);
    }

    LOG_INFO("BTreeTest Insert have done!");
//    auto rel = rcd.release();
//    delete rel;
}

TEST_F(BTreeTest, Read) {
    uint64_t payload;

    txn_conxt->SetCommitId(2007);
    std::string key = std::to_string(10);
    std::unique_ptr<Record> rcd = btree->Read(key.c_str(), 2,
                                              txn_conxt->GetCommitId(),false);
    ASSERT_TRUE(rcd == nullptr);
//    auto rel_0 = rcd.release();
//    delete [] rel_0;

    InsertDummy();
    std::unique_ptr<Record> rcd_1 = btree->Read(key.c_str(), 2,
                                                txn_conxt->GetCommitId(),false);
    ASSERT_TRUE(rcd_1.get() != nullptr);
    payload = *reinterpret_cast<uint64_t *>(rcd_1.get()->GetPayload());
    ASSERT_EQ(payload, 10);
//    auto rel_1 = rcd_1.release();
//    delete [] rel_1;

    std::string key2 = std::to_string(11);
    std::unique_ptr<Record> rcd_2 = btree->Read(key2.c_str(), 2,
                                                txn_conxt->GetCommitId(),false);
    ASSERT_TRUE(rcd_2 == nullptr);
//    auto rel_2 = rcd_2.release();
//    delete [] rel_2;

    LOG_INFO("BTreeTest Read have done!");
}

TEST_F(BTreeTest, Update) {
    uint64_t payload;
    InsertDummy();
    RecordMeta meta_upt;

    txn_conxt->SetCommitId(5000);
    std::unique_ptr<Record> rcd = btree->Read("20", 2,
                                              txn_conxt->GetCommitId(),false);
    payload = *reinterpret_cast<uint64_t *>(rcd.get()->GetPayload());
    assert(payload == 20);
//    auto rel_0 = rcd.release();
//    delete [] rel_0;

    txn_conxt->SetCommitId(5000);
    std::vector<oid_t> colms = {1};
    uint64_t delta = 21;
    std::string key = std::to_string(20);
    ASSERT_TRUE(btree->Update(key.c_str(), 2, reinterpret_cast<const char *>(&delta),
                              colms, &meta_upt,
                              false,
                              txn_conxt->GetCommitId()).IsOk());
    //in-inserting
    txn_conxt->SetCommitId(5005);
    std::unique_ptr<Record> rcd_1;
    key = std::to_string(20);
    rcd_1 = btree->Read(key.c_str(), 2,
                        txn_conxt->GetCommitId(),false);
    payload = *reinterpret_cast<const uint64_t *>(rcd_1->GetPayload());
    assert(payload == 20);

    //not-in-inserting
    btree->FinalizeUpdate(meta_upt.GetMetaPtr(), txn_conxt->GetCommitId());
    txn_conxt->SetCommitId(5006);
    key = std::to_string(20);
    rcd_1 = btree->Read(key.c_str(), 2,
                        txn_conxt->GetCommitId(),false);
    payload = *reinterpret_cast<uint64_t *>(rcd_1->GetPayload());
    assert(payload == 21);

//    auto rel_1 = rcd_1.release();
//    delete [] rel_1;
//    colms.clear();
    LOG_INFO("BTreeTest Update have done!");
}

TEST_F(BTreeTest, Upsert) {
    InsertDummy();
    RecordMeta meta_upti;

    txn_conxt->SetCommitId(6000);
    std::unique_ptr<Record> rcd_upinr_0;
    std::string key = "abc";
    rcd_upinr_0 = btree->Read(key.c_str(), 3,
                              txn_conxt->GetCommitId(),false);
    assert(rcd_upinr_0 == nullptr);

    txn_conxt->SetCommitId(6000);
    std::vector<oid_t> colms = {1};
    key = "abc";
    uint64_t delta = 42;
    btree->Upsert(key.c_str(), 3, reinterpret_cast<const char *>(&delta),
                  colms, &meta_upti,
                  false,
                              txn_conxt->GetCommitId() );
    btree->FinalizeInsert(meta_upti.GetMetaPtr(), txn_conxt->GetCommitId());

    key = "abc";
    rcd_upinr_0 = btree->Read(key.c_str(), 3,
                              txn_conxt->GetCommitId(),false);
    assert(rcd_upinr_0 != nullptr);
    uint64_t payload_0 = *reinterpret_cast<const uint64_t *>(rcd_upinr_0->GetPayload());
    assert(payload_0 == 42);

//    auto rel = rcd_upinr_0.release();
//    delete [] rel;

    txn_conxt->SetCommitId(6001);
    key = std::to_string(20);
    delta = 21;
    ASSERT_TRUE(btree->Upsert(key.c_str(), 2, reinterpret_cast<const char *>(&delta),
                              colms, &meta_upti,
                              false,
                              txn_conxt->GetCommitId() ).IsOk());
    std::unique_ptr<Record> rcd_upinr_1 ;
    //in-inserting
    key = std::to_string(20);
    rcd_upinr_1 = btree->Read(key.c_str(), 2,
                              txn_conxt->GetCommitId(),false);
    assert(rcd_upinr_1 != nullptr);
    uint64_t payload_1;
    payload_1 = *reinterpret_cast<uint64_t *>(rcd_upinr_1->GetPayload());
    assert(payload_1 == 20);
    //not-in-inserting
    btree->FinalizeUpdate(meta_upti.GetMetaPtr(), txn_conxt->GetCommitId());
    txn_conxt->SetCommitId(6002);
    key = std::to_string(20);
    rcd_upinr_1 = btree->Read(key.c_str(), 2,
                              txn_conxt->GetCommitId(),false);
    assert(rcd_upinr_1 != nullptr);
    payload_1 = *reinterpret_cast<const uint64_t *>(rcd_upinr_1->GetPayload());
    assert(payload_1 == 21);

//    auto rel_1 = rcd_upinr_1.release();
//    delete rel_1;
//    colms.clear();

    LOG_INFO("BTreeTest Upsert have done!");
}

TEST_F(BTreeTest, Delete) {
//    char *payload_location = nullptr;
    RecordMeta inrt_meta ;
    TupleHeader *tph_hd = nullptr;

    for (uint64_t i = 0; i < 50; i++) {
        std::string key = std::to_string(i);
        txn_conxt->SetCommitId(7001);
        uint64_t delta = i;
        auto ret = btree->Insert(key.c_str(), key.length(), reinterpret_cast<const char *>(&delta),
                                 &inrt_meta,
                                 txn_conxt->GetCommitId() );
        ASSERT_TRUE(ret.IsOk());
        btree->FinalizeInsert(inrt_meta.GetMetaPtr(), txn_conxt->GetCommitId());
    }

    RecordMeta del_meta;
    for (uint64_t i = 0; i < 40; i++) {
        std::string key = std::to_string(i);
        txn_conxt->SetCommitId(7002);
        ReturnCode rc = btree->Delete(key.c_str(), key.length(), &del_meta, tph_hd,
                                      false,
                                      txn_conxt->GetCommitId() );
        ASSERT_TRUE(rc.IsOk());
        btree->FinalizeDelete(del_meta.GetMetaPtr(), txn_conxt->GetCommitId());
        //not-inserting
        txn_conxt->SetCommitId(7003);
        std::unique_ptr<Record> rcd =  btree->Read(key.c_str(), key.length(),
                                                   txn_conxt->GetCommitId(),false);
        ASSERT_TRUE(rcd.get() == nullptr);

//        auto rel_0 = rcd.release();
//        delete [] rel_0;
    }

    btree->Dump();

    LOG_INFO("BTreeTest Delete have done!");
}


TEST_F(BTreeTest, RangeScanBySize) {

    static const uint32_t kMaxKey = 9999;
    for (uint32_t i = 1000; i <= kMaxKey; i++) {
        auto key = std::to_string(i);
        RecordMeta record_meta ;
        txn_conxt->SetCommitId(8001);
        uint64_t delta = i;
        auto ret = btree->Insert(key.c_str(), static_cast<uint16_t>(key.length()),
                                 reinterpret_cast<const char *>(&delta),
                     &record_meta, txn_conxt->GetCommitId() );
        ASSERT_TRUE(ret.IsOk());
        btree->FinalizeInsert(record_meta.GetMetaPtr(), txn_conxt->GetCommitId());
    }

    auto iter = btree->RangeScanBySize("9000", 4, 100);
    int count = 0;
    while (true) {
        auto r = iter->GetNext();
        if (!r) {
            break;
        }
        ++count;
        std::string key_str(r->GetKey(), 4);
        LOG_DEBUG("0 scan current count= %u, get key= %s", count, key_str.c_str());
    }
    assert(count ==100);
    LOG_INFO("BTreeTest scan count 100 have done!");

    iter = btree->RangeScanBySize("9000", 4, 1000);
    count = 0;
    while (true) {
        auto r = iter->GetNext();
        if (!r) {
            break;
        }
        ++count;
        std::string key_str(r->GetKey(), 4);
        LOG_DEBUG("1 scan current count= %u, get key= %s", count, key_str.c_str());
    }
    assert(count ==1000);
    LOG_INFO("BTreeTest scan count 1000 have done!");

}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


}
}
