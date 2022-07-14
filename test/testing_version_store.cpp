//
// Created by zhangqian on 2022-1-7.
//

#include <iostream>
#include <unordered_map>
#include <vector>
#include <map>
#include "harness.h"

namespace mvstore {
namespace test {

//===--------------------------------------------------------------------===//
// version store Tests
//===--------------------------------------------------------------------===//
class VersionStoreTest : public VStoreTest{
    void SetUp() override{
        //Version Block Manager
        version_block_mng = VersionBlockManager::GetInstance();
        Status v_st = version_block_mng->Init();
        PELOTON_ASSERT(v_st.ok(),"VersionBlockManager initialization fail.");
        //Version store
        version_store = VersionStore::GetInstance();
//        log_record_buffer = GetLogRecordBuffer();

    }
    void TearDown() override{
        version_block_mng->ClearVersionBlock();
        version_store->CleanVersionStore();
//        log_mng->CleanLogIndirectEntry();
//        log_record_buffer.clear();
//        delete version_block_mng;
    }


protected:
    VersionBlockManager *version_block_mng;
    VersionStore *version_store;
//    LogManager *log_mng;
//    Catalog *log_catalog;
//    std::unordered_map<cid_t, std::queue<LogRecord *>> log_record_buffer;
};

TEST_F(VersionStoreTest, VersionBlockTest) {
    std::vector<Column> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string>> column_names;
    std::vector<Catalog> schemas;
    //allocate a fixed memory space,
    //segment it into many version blocks, enqueue block_queue
//    version_block_mng->Init();

    //SCHEMA
    std::unique_ptr<Catalog> schema1(new Catalog());
    uint8_t *key_bitmaps = new uint8_t[4];
    key_bitmaps[0]=1;
    key_bitmaps[1]=0;
    key_bitmaps[2]=0;
    key_bitmaps[3]=0;
    schema1->init("table0",4,key_bitmaps,4,1,false);
    schema1->add_col("A", 4, "INTEGER");
    schema1->add_col("B", 4, "INTEGER");
    schema1->add_col("C", 4, "INTEGER");
    schema1->add_col("D", 25, "VARCHAR");
    schemas.push_back(*schema1);

    //Version Block
    VersionBlockElem v_b_elm = version_block_mng->AllocateBlock();
    char *block = v_b_elm.second;
    std::shared_ptr<VersionBlock> version_block(
                new VersionBlock(schema1->table_id,0,&block,schema1.get()));
    version_block_mng->AddVersionBlock(version_block);
    auto version_block_id = version_block->GetBlockId();
    EXPECT_EQ(0, version_block_id);
    auto block_meta = version_block->GetBlockMeta();
    auto tuple_header0 = version_block->GetEmptyTupleHeader(1);
    auto alloc_tuple_count = block_meta->GetTotalTupleCount();
    LOG_DEBUG("version block alloc tuple count: %u", alloc_tuple_count);

    //TUPLES
    std::unique_ptr<BaseTuple> tuple1(new BaseTuple(schema1.get()));
    std::unique_ptr<BaseTuple> tuple2(new BaseTuple(schema1.get()));

    uint32_t i=1;
    tuple1->SetValue(0,reinterpret_cast<const char *>(i));
    tuple1->SetValue(1,reinterpret_cast<const char *>(i));
    tuple1->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t1_cl3 = "tuple 1";
    tuple1->SetValue(3, t1_cl3.c_str());

    i=2;
    tuple2->SetValue(0,reinterpret_cast<const char *>(i));
    tuple2->SetValue(1,reinterpret_cast<const char *>(i));
    tuple2->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t2_cl3 = "tuple 2";
    tuple2->SetValue(3, t2_cl3.c_str());


    //Version Block Insert
    auto active_tuple_count = block_meta->GetActiveTupleCount();
    EXPECT_EQ(0, active_tuple_count);

    TupleHeader *tuple_header_insrt_0 = nullptr;
    tuple_header_insrt_0 = version_block->InsertTuple(tuple1->GetData());
    tuple_header_insrt_0 = version_block->InsertTuple(tuple2->GetData());
    tuple_header_insrt_0 = version_block->InsertTuple(tuple1->GetData());
    auto  get_hd_elm_0 = tuple_header_insrt_0->GetBeginId();
    auto  get_hd_elm_1 = tuple_header_insrt_0->GetEndCommitId();
    auto  get_hd_elm_2 = tuple_header_insrt_0->GetNextHeaderPointer();
    auto  get_hd_elm_3 = tuple_header_insrt_0->GetPreHeaderPointer();
    auto  get_hd_elm_4 = tuple_header_insrt_0->GetLogEntryHeaderPointer();
    auto  get_hd_elm_5 = tuple_header_insrt_0->GetTupleSlot();
    auto  get_hd_elm_8 = tuple_header_insrt_0->GetHdId( );
    block_meta = version_block->GetBlockMeta();
    active_tuple_count  = block_meta->GetActiveTupleCount();
    ASSERT_TRUE(3 == active_tuple_count);

    //Version Block GetValue
    std::unique_ptr<TupleHeader> tuple_header = nullptr;
    tuple_header = version_block->GetTupleHeader(0);
    char *value_0;
    version_block->GetValue(0,schema1->_columns[0].type, schema1->_columns[0].index,4,&value_0,  *tuple_header);
    uint32_t val_0 = *reinterpret_cast<const uint32_t *>(value_0);
    ASSERT_TRUE(1 == val_0);
    char *value_1;
    version_block->GetValue(0,schema1->_columns[1].type, schema1->_columns[1].index,4,&value_1,  *tuple_header);
    uint32_t val_1 = *reinterpret_cast<uint32_t *>(value_1);
    ASSERT_TRUE(1 == val_1);
    char *value_2;
    version_block->GetValue(0,schema1->_columns[2].type, schema1->_columns[2].index,4,&value_2,  *tuple_header);
    uint32_t val_2 = *reinterpret_cast<uint32_t *>(value_2);
    ASSERT_TRUE(1 == val_2);
    char *value_3;
    version_block->GetValue(0,schema1->_columns[3].type, schema1->_columns[3].index,25,&value_3, *tuple_header);
    char *val_3 = new char[25];
    memcpy(val_3, value_3, 25);
    std::string vl_3 = val_3;
    ASSERT_TRUE( t1_cl3 == vl_3);
    auto tpl = tuple_header.release();
    delete [] tpl;

    std::unique_ptr<TupleHeader> tuple_header_1 = nullptr;
    tuple_header_1 = version_block->GetTupleHeader(1) ;
    version_block->GetValue(1,schema1->_columns[0].type, schema1->_columns[0].index,4,&value_0,  *tuple_header_1);
    val_0 = *reinterpret_cast<const uint32_t *>(value_0);
    ASSERT_TRUE(2 == val_0);
    version_block->GetValue(1,schema1->_columns[1].type, schema1->_columns[1].index,4,&value_1,  *tuple_header_1);
    val_1 = *reinterpret_cast<uint32_t *>(value_1);
    ASSERT_TRUE(2 == val_1);
    version_block->GetValue(1,schema1->_columns[2].type, schema1->_columns[2].index,4,&value_2,  *tuple_header_1);
    val_2 = *reinterpret_cast<uint32_t *>(value_2);
    ASSERT_TRUE(2 == val_2);
    version_block->GetValue(1,schema1->_columns[3].type, schema1->_columns[3].index,25,&value_3, *tuple_header_1);
    memcpy(val_3, value_3, 25);
    vl_3 = val_3;
    ASSERT_TRUE( t2_cl3 == vl_3);

    auto hd = tuple_header_1.release();
    delete [] hd;

}
TEST_F(VersionStoreTest, VersionSStoreTest) {
    std::vector<Column> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string>> column_names;
    std::vector<Catalog *> schemas;

    //SCHEMA
    std::unique_ptr<Catalog> schema1(new Catalog());
    uint8_t *key_bitmaps = new uint8_t[4];
    key_bitmaps[0]=1;
    key_bitmaps[1]=0;
    key_bitmaps[2]=0;
    key_bitmaps[3]=0;
    schema1->init("table0",4,key_bitmaps,4,1,false);
    schema1->add_col("A", 4, "INTEGER");
    schema1->add_col("B", 4, "INTEGER");
    schema1->add_col("C", 4, "INTEGER");
    schema1->add_col("D", 25, "VARCHAR");
    schemas.push_back(schema1.get());

    //initialize a default version block
    version_store->Init(schemas);

    //Tuples
    std::unique_ptr<BaseTuple> tuple1(new BaseTuple(schema1.get()));
    std::unique_ptr<BaseTuple> tuple2(new BaseTuple(schema1.get()));
    uint32_t i=1;
    tuple1->SetValue(0,reinterpret_cast<const char *>(i));
    tuple1->SetValue(1,reinterpret_cast<const char *>(i));
    tuple1->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t1_cl3 = "tuple 1";
    tuple1->SetValue(3, t1_cl3.c_str() );

    i=2;
    tuple2->SetValue(0,reinterpret_cast<const char *>(i));
    tuple2->SetValue(1,reinterpret_cast<const char *>(i));
    tuple2->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t2_cl3 = "tuple 2";
    tuple2->SetValue(3, t2_cl3.c_str() );

    auto tuple1_header = version_store->InsertTuple(tuple1.get());
    auto tuple2_header = version_store->InsertTuple(tuple2.get());
    auto tuple3_header = version_store->InsertTuple(tuple1.get());
    auto  get_hd_elm_0 = tuple1_header->GetBeginId();
    auto  get_hd_elm_1 = tuple1_header->GetEndCommitId();
    auto  get_hd_elm_2 = tuple1_header->GetNextHeaderPointer();
    auto  get_hd_elm_3 = tuple1_header->GetPreHeaderPointer();
    auto  get_hd_elm_4 = tuple1_header->GetLogEntryHeaderPointer();
    auto  get_hd_elm_5 = tuple1_header->GetTupleSlot();
    auto  get_hd_elm_8 = tuple1_header->GetHdId( );

    auto total_count_ = version_store->GetTotalCount();
    EXPECT_EQ(3, total_count_);
}
void VersionStoreInsert(VersionStore *version_store,
                        Catalog *schema, UNUSED_ATTRIBUTE uint64_t thread_itr) {
//    uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

    std::unique_ptr<BaseTuple> tuple(new BaseTuple(schema));
    uint32_t  i =1;
    tuple->SetValue(0,reinterpret_cast<const char *>(i));
    tuple->SetValue(1,reinterpret_cast<const char *>(i));
    tuple->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t_cl = "tuple";
    tuple->SetValue(3, t_cl.c_str());

    for (int insert_itr = 0; insert_itr < 1000; insert_itr++) {
        auto tuple1_header = version_store->InsertTuple(tuple.get());
    }
}

TEST_F(VersionStoreTest, StressTest) {
    std::vector<Column> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string>> column_names;
    std::vector<Catalog *> schemas;

    //SCHEMA
    std::unique_ptr<Catalog> schema1(new Catalog());
    uint8_t *key_bitmaps = new uint8_t[4];
    key_bitmaps[0]=1;
    key_bitmaps[1]=0;
    key_bitmaps[2]=0;
    key_bitmaps[3]=0;
    schema1->init("table0",4,key_bitmaps,4,1,false);
    schema1->add_col("A", 4, "INTEGER");
    schema1->add_col("B", 4, "INTEGER");
    schema1->add_col("C", 4, "INTEGER");
    schema1->add_col("D", 25, "VARCHAR");
    schemas.push_back(schema1.get());
    //initialize a default version block
    version_store->Init(schemas);

    LaunchParallelTest(6, VersionStoreInsert, version_store, schema1.get());

    auto total_count_ = version_store->GetTotalCount();
    EXPECT_EQ(6000, total_count_);
}

TEST_F(VersionStoreTest, LogRecordTest){
    std::vector<Column> columns;
    std::vector<std::string> tile_column_names;
    std::vector<std::vector<std::string>> column_names;
    std::vector<Catalog *> schemas;

    //SCHEMA
    std::unique_ptr<Catalog> schema1(new Catalog());
    uint8_t *key_bitmaps = new uint8_t[4];
    key_bitmaps[0]=1;
    key_bitmaps[1]=0;
    key_bitmaps[2]=0;
    key_bitmaps[3]=0;
    schema1->init("table0",4,key_bitmaps,4,1,false);
    schema1->add_col("A", 4, "INTEGER");
    schema1->add_col("B", 4, "INTEGER");
    schema1->add_col("C", 4, "INTEGER");
    schema1->add_col("D", 25, "VARCHAR");
    //Log manager
    Catalog *log_catalog = new Catalog();
    log_catalog->table_id = 0;
    log_catalog->is_log = true;
    schemas.push_back(log_catalog);
    schemas.push_back(schema1.get());
    //initialize a default version block
    version_store->Init(schemas);

    std::unique_ptr<BaseTuple> tuple(new BaseTuple(schema1.get()));
    std::unique_ptr<BaseTuple> tuple1(new BaseTuple(schema1.get()));

    uint32_t i=5;
    tuple->SetValue(0,reinterpret_cast<const char *>(i));
    tuple->SetValue(1,reinterpret_cast<const char *>(i));
    tuple->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t_cl = "tuple5";
    tuple->SetValue(3, t_cl.c_str());

    i=6;
    tuple1->SetValue(0,reinterpret_cast<const char *>(i));
    tuple1->SetValue(1,reinterpret_cast<const char *>(i));
    tuple1->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t_cl_1 = "tuple6";
    tuple1->SetValue(3, t_cl_1.c_str());
    //log begin insert update delete commit
    auto tuple_header = version_store->InsertTuple(tuple.get());
    auto tuple1_header = version_store->InsertTuple(tuple1.get());

    LogManager *log_mng = version_store->GetLogManager();
    cid_t begin_id = 0;
    cid_t comm_id = 1;
    log_mng->LogBeginTxn(begin_id);
    log_mng->LogInsert(begin_id,tuple->GetData(),schema1->tuple_size,schema1.get());
    log_mng->LogInsert(begin_id,tuple1->GetData(),schema1->tuple_size,schema1.get());
    log_mng->LogCommitTxn(begin_id,comm_id);

    begin_id = 2;
    comm_id = 3;
    log_mng->LogBeginTxn(begin_id);
    std::string t_cl_up = "tuple50";
    uint8_t *bit_up = new uint8_t[schema1->field_cnt];
    bit_up[0] = 0;
    bit_up[1] = 0;
    bit_up[2] = 0;
    bit_up[3] = 1;
    //update tuple
    char *old_tuple_location = reinterpret_cast<char *>((tuple_header->GetTupleSlot()));
    auto new_tuple_header = version_store->AcquireVersion(schema1.get());
    oid_t version_block_id = new_tuple_header.first;
    char *new_tuple_location = reinterpret_cast<char *>(new_tuple_header.second->GetTupleSlot());
    log_mng->LogUpdate(begin_id, new_tuple_location, const_cast<char *>(old_tuple_location),
                       t_cl_up.c_str(), 25, bit_up, schema1.get());
    //delete tuple1
    char *del_tuple_location = reinterpret_cast<char *>(tuple1_header->GetTupleSlot());
    log_mng->LogDelete(begin_id,del_tuple_location,schema1.get());
    //commit
    log_mng->LogCommitTxn(begin_id,comm_id);

//    uint32_t log_r_s = log_mng->();
//    ASSERT_TRUE(2 == log_r_s);

    //log begin insert update delete abort
    std::unique_ptr<BaseTuple> tuple10(new BaseTuple(schema1.get()));
    std::unique_ptr<BaseTuple> tuple11(new BaseTuple(schema1.get()));
    std::unique_ptr<BaseTuple> tuple12(new BaseTuple(schema1.get()));
    i=10;
    tuple10->SetValue(0,reinterpret_cast<const char *>(i));
    tuple10->SetValue(1,reinterpret_cast<const char *>(i));
    tuple10->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t_cl_10 = "tuple10";
    tuple10->SetValue(3, t_cl_10.c_str());
    i=11;
    tuple11->SetValue(0,reinterpret_cast<const char *>(i));
    tuple11->SetValue(1,reinterpret_cast<const char *>(i));
    tuple11->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t_cl_11 = "tuple11";
    tuple11->SetValue(3, t_cl_11.c_str());
    i=12;
    tuple12->SetValue(0,reinterpret_cast<const char *>(i));
    tuple12->SetValue(1,reinterpret_cast<const char *>(i));
    tuple12->SetValue(2,reinterpret_cast<const char *>(i));
    std::string t_cl_12 = "tuple12";
    tuple12->SetValue(3, t_cl_12.c_str());
    auto tuple10_header = version_store->InsertTuple(tuple10.get());
    auto tuple11_header = version_store->InsertTuple(tuple11.get());

    begin_id = 10;
    comm_id = 11;
    log_mng->LogBeginTxn(begin_id);
    log_mng->LogInsert(begin_id,tuple10->GetData(),schema1->tuple_size,schema1.get());
    log_mng->LogInsert(begin_id,tuple11->GetData(),schema1->tuple_size,schema1.get());
    log_mng->LogCommitTxn(begin_id,comm_id);

    begin_id = 12;
    comm_id = 13;
    log_mng->LogBeginTxn(begin_id);
    //insert 12
    log_mng->LogInsert(begin_id,tuple12->GetData(),schema1->tuple_size,schema1.get());
    //update tuple 10
    std::string t_cl_up_10 = "tuple100";
    uint8_t *bit_up_10 = new uint8_t[schema1->field_cnt];
    bit_up[0] = 0;
    bit_up[1] = 0;
    bit_up[2] = 0;
    bit_up[3] = 1;
//    oid_t version_block_id = 0;
    old_tuple_location = reinterpret_cast<char *>(tuple10_header->GetTupleSlot());
//    old_tuple_location = t_cl_10.c_str();
    new_tuple_header = version_store->AcquireVersion(schema1.get());
    version_block_id = new_tuple_header.first;
    new_tuple_location = reinterpret_cast<char *>(new_tuple_header.second->GetTupleSlot());
//    char *payload_location = new_tuple_location + sizeof(uint32_t)*3;
    log_mng->LogUpdate(begin_id, new_tuple_location, old_tuple_location,
                       t_cl_up_10.c_str(), 25, bit_up_10, schema1.get());
    //delete tuple11
    char *del_tuple_location_11 = reinterpret_cast<char *>(tuple11_header->GetTupleSlot());
    log_mng->LogDelete(begin_id,del_tuple_location_11,schema1.get());
    //commit
    log_mng->LogAbortTxn(begin_id,comm_id);

//    uint32_t log_r_s_a = log_mng->GetLogEntrySize();
//    ASSERT_TRUE(4 == log_r_s_a);


    //log write and recovery test
    //TODO: doing....
//    auto ret_0 = log_mng->LogWrite(0,1, nullptr);
//    auto ret_2 = log_mng->LogWrite(2,3);
//    auto ret_10 = log_mng->LogWrite(10,11);
//    auto ret_12 = log_mng->LogWrite(12,13);
//    ASSERT_TRUE(!ret_0.empty());
//    ASSERT_TRUE(!ret_2.empty());
//    ASSERT_TRUE(!ret_10.empty());
//    ASSERT_TRUE(!ret_12.empty());
//
//    char *value_10;
//    VersionBlock *v_bk = version_block_mng->GetVersionBlock(version_block_id);
//    std::unique_ptr<TupleHeader> tuple_header_10 = v_bk->GetTupleHeader(2);
//    v_bk->GetValue(2,schema1->_columns[3].type, schema1->_columns[3].index,25,
//                   &value_10, *tuple_header_10);
//    char *val_10 = new char[25];
//    memcpy(val_10, value_10, 25);
//    std::string vl_10 = val_10;
//    ASSERT_TRUE( t_cl_10 == vl_10);//old value copy "tuple10"
//
//    LSN_T lsn_up =  ret_12[2];
//    LSN_T lsn_up_0 =  ret_12[2];
//    uint64_t block_id_l = lsn_up >> 32;
//    uint32_t pos = lsn_up & uint64_t{0xFFFFFFFF};
//    VersionBlock *v_bk_l = version_block_mng->GetVersionBlock(block_id_l);
//    char *block_data_ptr = v_bk_l->GetBlockData();
//    uint32_t cur_off = v_bk_l->GetPos();
//    auto block_up_ptr = block_data_ptr + pos;
//    auto delta_ptr = block_up_ptr + sizeof(LogRecordType)+sizeof(cid_t)+sizeof(cid_t)+sizeof(uint64_t)+
//                                                sizeof(uint16_t);
//    char *val_100 = new char[25];
//    memcpy(val_100,delta_ptr,25);
//    std::string vl_100 = val_100;
//    ASSERT_TRUE( t_cl_up_10 == vl_100);//new value "tuple100"
}

TEST_F(VersionStoreTest, GCTest){
    version_block_mng->ClearVersionBlock();
    version_store->CleanVersionStore();
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

}
}