//
// Created by zhangqian on 2022-1-7.
//

#include <iostream>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <map>
#include <random>
#include <gtest/gtest.h>
//#include "../include/common/ephemeral_pool.h"
#include "../include/common/bitmap.h"
#include "../include/common/cuckoo_map.h"
#include "../include/common/random.h"
#include "../include/common/crc32c.h"
#include "../include/common/sync.h"
#include "../include/common/tools.h"
#include "../include/vstore/b_tree.h"
#include "../include/execute/txn.h"
//uint32_t descriptor_pool_size = 500000;
namespace mvstore {
namespace test {

class BtreeMultithreadTest : public ::testing::Test{
    void SetUp() override{



    }
    void TearDown() override{

    }



protected:
    VersionBlockManager *version_block_mng;
    VersionStore *buf_mgr;
    DramBlockPool *leaf_node_pool;
    InnerNodeBuffer *inner_node_pool;
    EphemeralPool *conflict_buffer;

};
template<class T>
void ReportStatsAboutWorkload(const std::vector<T> &workload, int first_n_most_freq) {
    std::unordered_map<T, int> cnt;

    int max_cnt = 0;
    for (int i = 0; i < workload.size(); ++i) {
        cnt[workload[i]]++;
    }
    std::vector<std::pair<int, T>> pairs;
    for (auto kv : cnt) {
        pairs.push_back(std::make_pair(kv.second, kv.first));
        max_cnt = std::max(max_cnt, kv.second);
    }
    std::sort(pairs.begin(), pairs.end(), [](const std::pair<int, T> &p1, const std::pair<int, T> &p2) {
        return p1.first > p2.first;
    });

    std::cout << "1 -> " << cnt[1] << std::endl;
    std::cout << "Max count " << max_cnt << std::endl;
    int current_count = 0;
    for (int i = 0; i < std::min(first_n_most_freq, (int) pairs.size()); ++i) {
        current_count += pairs[i].first;
    }
    std::cout << "Top " << std::min(first_n_most_freq, (int) pairs.size()) << " items take up "
              << current_count / (workload.size() + 0.0) * 100 << "% of the entire workload" << std::endl;
}

static int IntRand() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, std::numeric_limits<int>::max());
    return distribution(generator);
}


std::vector<uint64_t> GetWorkload(int n_ops, int n_kvs, const std::string &workload_filepath) {
    std::vector<uint64_t> workload;
    workload.reserve(n_ops);
    if (workload_filepath.empty()) {
        for (int i = 0; i < n_ops; ++i) {
            workload.push_back(IntRand() % n_kvs);
        }
    } else {
        std::ifstream infile(workload_filepath);
        while (!infile.eof()) {
            uint64_t idx;
            infile >> idx;
            workload.push_back(idx);
        }
    }
    return workload;
}
struct Tuple  {
    uint64_t key;
    uint64_t values[10];

    uint64_t Key() const { return key; }
    char *GetData() const{
        char *data = new char[sizeof(uint64_t)*10];
//        memset(data,key,sizeof(uint64_t));
        uint32_t sz = 0;
        for(int i=0; i<10; ++i){
            memset(data + sz,values[i],sizeof(uint64_t));
            sz = sz + sizeof(uint64_t);
        }
        return data;
    }

};

void TestConcurrentBTree(const ParameterSet param, const Catalog &table_catalog,
                         DramBlockPool *leaf_node_pool,
                         InnerNodeBuffer *inner_node_pool,
                         EphemeralPool *conflict_buffer,
                         size_t n_threads, sync::ThreadPool *tp,
                         std::vector<uint64_t> &workload) {

    {


        auto btree = new BTree(param, table_catalog,
                                leaf_node_pool, inner_node_pool, conflict_buffer);


        size_t total_ops = workload.size();
        //ReportStatsAboutWorkload(workload, 300);

        {
            std::vector<std::thread> threads;
            sync::CountDownLatch latch(n_threads);
            auto thread_work = [&](int i) {
                size_t ops_per_thread = total_ops / n_threads;
                int start = i * ops_per_thread;
                int end = std::min(start + ops_per_thread, total_ops);
                if (i == n_threads - 1) {
                    end = total_ops;
                }
                RecordMeta inrt_meta;
                // Upsert
                for (int i = start; i < end; ++i) {
                    std::string key = std::to_string(workload[i]);
                    uint64_t delta = workload[i];
//                    btree->Insert(workload[i], workload[i]);
                    auto rc = btree->Insert(key.c_str(), key.length(),
                                            reinterpret_cast<const char *>(&delta),
                                            &inrt_meta,
                                            (1000+i));
//                    ASSERT_TRUE(rc.IsOk());
                    LOG_DEBUG("btree insert, return: %u",rc);
                    if(rc.IsOk())
                        btree->FinalizeInsert(inrt_meta.GetMetaPtr(), (uint32_t)i);
                }

                latch.CountDown();
            };

            TimedThroughputReporter reporter(total_ops);
            for (size_t i = 0; i < n_threads; ++i) {
                tp->enqueue(thread_work, i);
            }
            latch.Await();

            std::cout << "Concurrent random test finished, " << total_ops << " btree writes " << n_threads
                      << " workers." << std::endl;
//            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
//            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
        }

//        buf_mgr.ClearStats();
        sleep(1);

//        std::cout << "Stats before read: \n" << buf_mgr.GetStatsString() << std::endl;
//        std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;

        {
            std::vector<std::thread> threads;
            sync::CountDownLatch latch(n_threads);
            std::unique_ptr<Record> rcd = nullptr ;
            auto thread_work = [&](int i) {
                size_t ops_per_thread = total_ops / n_threads;
                int start = i * ops_per_thread;
                int end = std::min(start + ops_per_thread, total_ops);
                if (i == n_threads - 1) {
                    end = total_ops;
                }
                // Search
                for (int i = start; i < end; ++i) {
                    uint64_t val = 0;
//                    bool res = btree->Lookup(workload[i], val);
                    std::string key = std::to_string(workload[i]);
                    rcd = btree->Read(key.c_str(), (uint16_t)key.length(),
                                      (2000+i),false);
//                    if (res == false) {
//                        res = btree->Lookup(workload[i], val);
//                    }
//                    assert(res);
//                    LOG_DEBUG("btree read, return: %u", (rcd == nullptr));
//                    assert(rcd != nullptr);
                    if (rcd != nullptr){
                        val = *reinterpret_cast<const uint64_t *>(rcd->GetPayload());
                    }

//                    assert(val == workload[i]);
//                    LOG_DEBUG("btree read, return value: %u", val);
                }

                latch.CountDown();
            };

            TimedThroughputReporter reporter(total_ops);
            for (size_t i = 0; i < n_threads; ++i) {
                tp->enqueue(thread_work, i);
            }
            latch.Await();

            std::cout << "Concurrent random test finished, " << total_ops << " btree reads " << n_threads
                      << " workers." << std::endl;
//            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
//            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
        }


//        buf_mgr.ClearStats();


//        sort(workload.begin(), workload.end());
//        workload.erase(std::unique(workload.begin(), workload.end()), workload.end());
//        std::random_shuffle(workload.begin(), workload.end());
//        std::vector<uint64_t> delete_workload;
//        delete_workload.reserve(workload.size() / 2);
//        for (int i = 0; i < workload.size(); i++) {
//            delete_workload.push_back(workload[i]);
//        }
//
//        {
//            std::vector<std::thread> threads;
//            sync::CountDownLatch latch(n_threads);
//            total_ops = delete_workload.size();
//            TupleHeader *tph_hd = nullptr;
//            RecordMeta del_meta;
//            auto thread_work = [&](int i) {
//                size_t ops_per_thread = total_ops / n_threads;
//                int start = i * ops_per_thread;
//                int end = std::min(start + ops_per_thread, total_ops);
//                if (i == n_threads - 1) {
//                    end = total_ops;
//                }
//                // delete
//                for (int i = start; i < end; ++i) {
////                    bool res = btree.Erase(delete_workload[i]);
////                    if (res == false) {
////                        res = btree.Erase(delete_workload[i]);
////                    }
//                    std::string key = std::to_string(i);
//                    ReturnCode rc = btree->Delete(key.c_str(), key.length(),
//                                                  &del_meta, tph_hd,
//                                                  false,
//                                                  (3000+i));
////                    ASSERT_TRUE(rc.IsOk());
//                    if(rc.IsOk()){
//                        btree->FinalizeDelete(del_meta.GetMetaPtr(), 3002);
//                    }
//
//                    LOG_DEBUG("btree delete, return: %u", rc);
////                    assert(rc.IsOk());
//                }
//
//                latch.CountDown();
//            };
//
//            TimedThroughputReporter reporter(total_ops);
//            for (size_t i = 0; i < n_threads; ++i) {
//                tp->enqueue(thread_work, i);
//            }
//            latch.Await();
//
//            std::cout << "Concurrent random test finished, " << total_ops << " btree deletes " << n_threads
//                      << " workers." << std::endl;
////            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
////            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
//        }

//        buf_mgr.ClearStats();
//        sleep(1);
//        {
//            std::vector<std::thread> threads;
//            sync::CountDownLatch latch(n_threads);
//            total_ops = workload.size();
//            std::unique_ptr<Record> rcd = nullptr ;
//            auto thread_work = [&](int i) {
//                size_t ops_per_thread = total_ops / n_threads;
//                int start = i * ops_per_thread;
//                int end = std::min(start + ops_per_thread, total_ops);
//                if (i == n_threads - 1) {
//                    end = total_ops;
//                }
//                // Search
//                for (int i = start; i < end; ++i) {
//                    uint64_t val = 0;
////                    bool res = btree.Lookup(workload[i], val);
////                    if (i % 1 == 0) {
////                        assert(res == false);
////                    } else {
////                        if (res == false) {
////                            res = btree.Lookup(workload[i], val);
////                        }
////                        assert(res);
////                        assert(val == workload[i]);
////                    }
//                    std::string key = std::to_string(workload[i]);
//                    rcd = btree->Read(key.c_str(), (uint16_t)key.length(),
//                                      (4000+i),false);
////                    assert(rcd != nullptr);
//                    LOG_DEBUG("btree read, return: %u", (rcd == nullptr));
////                    assert(rcd != nullptr);
//                    if (rcd != nullptr){
//                        val = *reinterpret_cast<const uint64_t *>(rcd->GetPayload());
//                    }
////                    assert(val == workload[i]);
//                    LOG_DEBUG("btree read, return value: %u", val);
//                }
//
//                latch.CountDown();
//            };
//
//            TimedThroughputReporter reporter(total_ops);
//            for (size_t i = 0; i < n_threads; ++i) {
//                tp->enqueue(thread_work, i);
//            }
//            latch.Await();
//
//            std::cout << "Concurrent random test finished, " << total_ops << " btree ops " << n_threads
//                      << " workers." << std::endl;
////            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
////            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
//        }
    }
}

TEST_F(BtreeMultithreadTest, PoolTest) {
        VersionBlockManager *version_block_mng;
        VersionStore *buf_mgr;
        DramBlockPool *leaf_node_pool;
        InnerNodeBuffer *inner_node_pool;
        EphemeralPool *conflict_buffer;
        //Version Block Manager
        version_block_mng = VersionBlockManager::GetInstance();
        Status v_st = version_block_mng->Init();
        PELOTON_ASSERT(v_st.ok(),"VersionBlockManager initialization fail.");
        //Version store
        buf_mgr = VersionStore::GetInstance();


        //for leaf node
        leaf_node_pool = new DramBlockPool(default_blocks, default_blocks);
        //for inner node
        RecordBufferPool *_pool = new RecordBufferPool(50000,50000);
        inner_node_pool = new InnerNodeBuffer(_pool);
        //for undo buffer
        RecordBufferPool *buffer_pool = new RecordBufferPool(50000,50000);
        UndoBuffer *undo_buffer_pool = new UndoBuffer(buffer_pool);
        conflict_buffer = new EphemeralPool(undo_buffer_pool);

        SSNTransactionManager *transaction_manager = SSNTransactionManager::GetInstance();
        transaction_manager->Init(conflict_buffer);

        const int n_ops = 1024*1024;
        const int n_kvs = n_ops;
        const int n_txns = 10;
        size_t n_threads = 20;
        ParameterSet param(4096, 2048,4096 , 8);

        std::vector<Catalog *> schemas;
        std::unique_ptr<Catalog> table_catalog(new Catalog());
        uint8_t *keys_cl = new uint8_t[2];
        keys_cl[0] = 1;
        keys_cl[1] = 0;
        table_catalog->init("test_tree",2, keys_cl, 8,4,false);
        table_catalog->add_col("key",8,"INTEGER");
        table_catalog->add_col("payload",8,"VARCHAR");

        //Log manager
        Catalog *log_catalog = new Catalog();
        log_catalog->table_id = 0;
        log_catalog->is_log = true;
        schemas.push_back(log_catalog);
        schemas.push_back(table_catalog.get());
        buf_mgr->Init(schemas);

        sync::ThreadPool tp(n_threads);
        {
            auto workload = GetWorkload(n_ops, n_kvs, "");

            for (int i = n_threads; i <= n_threads; ++i) {
                TestConcurrentBTree(param, *table_catalog.get(),
                                    leaf_node_pool,
                                    inner_node_pool,
                                    conflict_buffer, i, &tp, workload);
            }
        }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

}
}