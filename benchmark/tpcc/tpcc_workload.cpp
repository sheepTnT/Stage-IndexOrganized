//
// Created by zhangqian on 2022-03-1.
//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_workload.cpp
//
// Identification: src/main/tpcc/tpcc_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>
#include <sys/utsname.h>
#include <regex>
//#include <papi.h>
#include <fstream>
#include "../benchmark/benchmark_common.h"
#include "tpcc_workload.h"
#include "tpcc_configuration.h"
#include "tpcc_loader.h"
#include "../pcm/cpucounters.h"

namespace mvstore {
namespace benchmark {
namespace tpcc {

//#define STOCK_LEVEL_RATIO     0.04
//#define ORDER_STATUS_RATIO    0.04
//#define PAYMENT_RATIO         0.47
//#define NEW_ORDER_RATIO       0.45

//#define STOCK_LEVEL_RATIO     0
//#define NEW_ORDER_RATIO       1

volatile bool is_running = true;

PadInt *abort_counts;
PadInt *commit_counts;
PadInt *q2_abort_counts;
PadInt *q2_commit_counts;
PadInt *new_abort_counts;
PadInt *new_commit_counts;
PadInt *stock_abort_counts;
PadInt *stock_commit_counts;

//static constexpr int PAPI_EVENT_COUNT = 4;

size_t GenerateWarehouseId(const size_t &thread_id) {
    if (state.affinity) {
        if (state.warehouse_count <= state.backend_count) {
            return thread_id % state.warehouse_count;
        } else {
            int warehouse_per_partition = state.warehouse_count / state.backend_count;
            int start_warehouse = warehouse_per_partition * thread_id;
            int end_warehouse = ((int) thread_id != (state.backend_count - 1)) ?
                                start_warehouse + warehouse_per_partition - 1 : state.warehouse_count - 1;
            return GetRandomInteger(start_warehouse, end_warehouse);
        }
    } else {
        return GetRandomInteger(0, state.warehouse_count - 1);
    }
}

#ifndef __APPLE__

void PinToCore(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#else
    void PinToCore(size_t UNUSED_ATTRIBUTE core) {
// Mac OS X does not export interfaces that identify processors or control thread placement
// explicit thread to processor binding is not supported.
// Reference: https://superuser.com/questions/149312/how-to-set-processor-affinity-on-os-x
#endif
}

static void RunWarmupBackend(const size_t thread_id, VersionStore *version_store,
                             EphemeralPool *conflict_buffer) {

//    PinToCore(thread_id);
//
//    // backoff
//    while (true) {
//        if (is_running == false) {
//            break;
//        }
//        while (RunStockLevel(thread_id, version_store,conflict_buffer) == false) {
//            if (is_running == false) {
//                break;
//            }
//        }
//    }
}

//void InitPAPI() {
//    int retval;
//
//    retval = PAPI_library_init(PAPI_VER_CURRENT);
//    if(retval != PAPI_VER_CURRENT && retval > 0) {
//        fprintf(stderr,"PAPI library version mismatch!\n");
//        exit(1);
//    }
//
//    if (retval < 0) {
//        fprintf(stderr, "PAPI failed to start (1): %s\n", PAPI_strerror(retval));
//        exit(1);
//    }
//
//    retval = PAPI_is_initialized();
//
//
//    if (retval != PAPI_LOW_LEVEL_INITED) {
//        fprintf(stderr, "PAPI failed to start (2): %s\n", PAPI_strerror(retval));
//        exit(1);
//    }
//
//    return;
//}
//void InitInstMonitor() {
//    InitPAPI();
//    return;
//}

void RunBackend(const size_t thread_id, VersionStore *version_store,
                                        EphemeralPool *conflict_buffer) {

    PinToCore(thread_id);

    PadInt &execution_count_ref = abort_counts[thread_id];
    PadInt &transaction_count_ref = commit_counts[thread_id];
//    PadInt &q2_execution_count_ref = q2_abort_counts[thread_id];
//    PadInt &q2_transaction_count_ref = q2_commit_counts[thread_id];
    PadInt &new_execution_count_ref = new_abort_counts[thread_id];
    PadInt &new_transaction_count_ref = new_commit_counts[thread_id];
    PadInt &stock_execution_count_ref = stock_abort_counts[thread_id];
    PadInt &stock_transaction_count_ref = stock_commit_counts[thread_id];

    double STOCK_LEVEL_RATIO_ = state.stock_level_rate;
    double NEW_ORDER_RATIO_= state.new_order_rate;

    uint32_t backoff_shifts = 0;
    FastRandom rng(rand());

    while (true) {
        if (is_running == false) {
            break;
        }

        auto rng_val = rng.NextUniform();

        if (rng_val <= STOCK_LEVEL_RATIO_) {
            while (RunStockLevel(thread_id, version_store, conflict_buffer) == false) {
                if (is_running == false) {
                    break;
                }
                execution_count_ref.data++;
                stock_execution_count_ref.data++;
                // backoff
                if (state.exp_backoff) {
                    if (backoff_shifts < 13) {
                        ++backoff_shifts;
                    }
                    uint64_t sleep_duration = 1UL << backoff_shifts;
                    sleep_duration *= 100;
                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
                }
            }

            stock_transaction_count_ref.data++;
        //      LOG_DEBUG("stocklevel");
        //      LOG_DEBUG("%f, %f",STOCK_LEVEL_RATIO_, NEW_ORDER_RATIO_);
        }else if (rng_val <= STOCK_LEVEL_RATIO_ + NEW_ORDER_RATIO_) {
            while (RunNewOrder(thread_id, version_store, conflict_buffer) == false) {
                if (is_running == false) {
                    break;
                }
                execution_count_ref.data++;
                new_execution_count_ref.data++;
                // backoff
                if (state.exp_backoff) {
                    if (backoff_shifts < 13) {
                        ++backoff_shifts;
                    }
                    uint64_t sleep_duration = 1UL << backoff_shifts;
                    sleep_duration *= 100;
                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
                }
            }

            new_transaction_count_ref.data ++;
            //LOG_DEBUG("neworder");
        }

//        if (is_running == false) {
//            break;
//        }
//        while (RunNewOrder(thread_id, version_store,conflict_buffer) == false) {
//            if (is_running == false) {
//                break;
//            }
//            execution_count_ref.data++;
//            // backoff
//            if (state.exp_backoff) {
//                if (backoff_shifts < 13) {
//                    ++backoff_shifts;
//                }
//                uint64_t sleep_duration = 1UL << backoff_shifts;
//                sleep_duration *= 100;
//                std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//            }
//        }
//        else if (rng_val <= PAYMENT_RATIO + NEW_ORDER_RATIO + STOCK_LEVEL_RATIO) {
//            while (RunPayment(thread_id, version_store,conflict_buffer) == false) {
//                if (is_running == false) {
//                    break;
//                }
//                num_rw_ops_snap = num_rw_ops;
//                execution_count_ref.data++;
//                // backoff
//                if (state.exp_backoff) {
//                    if (backoff_shifts < 13) {
//                        ++backoff_shifts;
//                    }
//                    uint64_t sleep_duration = 1UL << backoff_shifts;
//                    sleep_duration *= 100;
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//                }
//            }
//        } else if (rng_val <= ORDER_STATUS_RATIO + PAYMENT_RATIO + NEW_ORDER_RATIO + STOCK_LEVEL_RATIO) {
//            while (RunOrderStatus(thread_id, version_store,conflict_buffer) == false) {
//                if (is_running == false) {
//                    break;
//                }
//                num_rw_ops_snap = num_rw_ops;
//                execution_count_ref.data++;
//                // backoff
//                if (state.exp_backoff) {
//                    if (backoff_shifts < 13) {
//                        ++backoff_shifts;
//                    }
//                    uint64_t sleep_duration = 1UL << backoff_shifts;
//                    sleep_duration *= 100;
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//                }
//            }
//        } else {
//            while (RunDelivery(thread_id, version_store,conflict_buffer) == false) {
//                if (is_running == false) {
//                    break;
//                }
//                num_rw_ops_snap = num_rw_ops;
//                execution_count_ref.data++;
//                // backoff
//                if (state.exp_backoff) {
//                    if (backoff_shifts < 13) {
//                        ++backoff_shifts;
//                    }
//                    uint64_t sleep_duration = 1UL << backoff_shifts;
//                    sleep_duration *= 100;
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//                }
//            }
//        }

        backoff_shifts >>= 1;
        transaction_count_ref.data ++;
    }
}

void RunScanBackend(size_t thread_id, VersionStore *version_store,
                    EphemeralPool *conflict_buffer,
                    std::vector<std::vector<std::pair<int32_t, int32_t>>> supp_stock_map) {
    PinToCore(thread_id);

    PadInt &execution_count_ref = abort_counts[thread_id];
    PadInt &transaction_count_ref = commit_counts[thread_id];
//    PadInt &q2_execution_count_ref = q2_abort_counts[thread_id];
//    PadInt &q2_transaction_count_ref = q2_commit_counts[thread_id];
//    PadInt &new_execution_count_ref = new_abort_counts[thread_id];
//    PadInt &new_transaction_count_ref = new_commit_counts[thread_id];
//    PadInt &stock_execution_count_ref = stock_abort_counts[thread_id];
//    PadInt &stock_transaction_count_ref = stock_commit_counts[thread_id];

//    double STOCK_LEVEL_RATIO_ = state.stock_level_rate;
//    double NEW_ORDER_RATIO_= state.new_order_rate;
//    double QUERY2_RATIO_ = state.scan_rate;

    // backoff
    uint32_t backoff_shifts = 0;
    FastRandom rng(rand());

    while (true) {
        if (is_running == false) {
            break;
        }

        while (RunQuery2(thread_id, version_store, conflict_buffer,supp_stock_map) == false) {
            if (is_running == false) {
                break;
            }
            execution_count_ref.data++;
            // backoff
            if (state.exp_backoff) {
                if (backoff_shifts < 13) {
                    ++backoff_shifts;
                }
                uint64_t sleep_duration = 1UL << backoff_shifts;
                sleep_duration *= 100;
                std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
            }
        }
//        auto rng_val = rng.NextUniform();
//        if (rng_val <= STOCK_LEVEL_RATIO_) {
//            while (RunStockLevel(thread_id, version_store, conflict_buffer) == false) {
//                if (is_running == false) {
//                    break;
//                }
//                execution_count_ref.data++;
//                stock_execution_count_ref.data++;
//                // backoff
//                if (state.exp_backoff) {
//                    if (backoff_shifts < 13) {
//                        ++backoff_shifts;
//                    }
//                    uint64_t sleep_duration = 1UL << backoff_shifts;
//                    sleep_duration *= 100;
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//                }
//            }
//
//            stock_transaction_count_ref.data++;
////      LOG_DEBUG("stocklevel");
////      LOG_DEBUG("%f, %f, %f",STOCK_LEVEL_RATIO_, NEW_ORDER_RATIO_, QUERY2_RATIO_);
//        }else if (rng_val <= STOCK_LEVEL_RATIO_ + NEW_ORDER_RATIO_) {
//            while (RunNewOrder(thread_id, version_store, conflict_buffer) == false) {
//                if (is_running == false) {
//                    break;
//                }
//                execution_count_ref.data++;
//                new_execution_count_ref.data++;
//                // backoff
//                if (state.exp_backoff) {
//                    if (backoff_shifts < 13) {
//                        ++backoff_shifts;
//                    }
//                    uint64_t sleep_duration = 1UL << backoff_shifts;
//                    sleep_duration *= 100;
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//                }
//            }
//
//            new_transaction_count_ref.data ++;
////      LOG_DEBUG("neworder");
//        }else if (rng_val <= STOCK_LEVEL_RATIO_ + NEW_ORDER_RATIO_ + QUERY2_RATIO_) {
//            while (RunQuery2(thread_id, version_store,conflict_buffer,supp_stock_map) == false) {
//                if (is_running == false) {
//                    break;
//                }
//                execution_count_ref.data++;
//                q2_execution_count_ref.data++;
//                // backoff
//                if (state.exp_backoff) {
//                    if (backoff_shifts < 13) {
//                        ++backoff_shifts;
//                    }
//                    uint64_t sleep_duration = 1UL << backoff_shifts;
//                    sleep_duration *= 100;
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//                }
//            }
//            q2_transaction_count_ref.data ++;
//        }

        backoff_shifts >>= 1;
        transaction_count_ref.data ++;
    }
}

void RunWarmupWorkload(VersionStore *version_store, EphemeralPool *conflict_buffer) {
    // value ranges 0 ~ 9999 ( modulo by 10k )
    std::vector<std::vector<std::pair<int32_t, int32_t>>> supp_stock_map(10000);
    // pre-build supp-stock mapping table to boost tpc-ch queries
    for (uint w = 1; w <= state.warehouse_count; w++)
        for (uint i = 1; i <= state.item_count; i++)
            supp_stock_map[w * i % 10000].push_back(std::make_pair((w-1), (i-1)));

    // Execute the workload to build the log
    size_t num_threads = state.backend_count;
    sync::ThreadPool the_tp(num_threads);

    abort_counts = new PadInt[num_threads];
    memset(abort_counts, 0, sizeof(PadInt) * num_threads);

    commit_counts = new PadInt[num_threads];
    memset(commit_counts, 0, sizeof(PadInt) * num_threads);

    size_t profile_round = (size_t) state.warmup_duration;
    size_t scan_thread = num_threads*state.scan_rate;

//    PCM *pcm_ = PCM::getInstance();
//    auto status = pcm_->program();
//    if (status != PCM::Success)
//    {
//        std::cout << "Error opening PCM: " << status << std::endl;
//        if (status == PCM::PMUBusy)
//            pcm_->resetPMU();
//        else
//            exit(0);
//    }
//    std::unique_ptr<SystemCounterState> before_sstate;
//    before_sstate = std::make_unique<SystemCounterState>();
//    *before_sstate = getSystemCounterState();

    sync::CountDownLatch latch(num_threads);
    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
		if (thread_itr < scan_thread){
            the_tp.enqueue([thread_itr, &latch, version_store, conflict_buffer, &supp_stock_map]() {
                //LOG_INFO("Thread %d started", thread_itr);
                RunScanBackend(thread_itr, version_store, conflict_buffer, supp_stock_map);
                //LOG_INFO("Thread %d ended", thread_itr);
                latch.CountDown();
            });
        }else{
            the_tp.enqueue([thread_itr, &latch, version_store, conflict_buffer]() {
                //LOG_INFO("Thread %d started", thread_itr);
                RunBackend(thread_itr, version_store, conflict_buffer);
                //LOG_INFO("Thread %d ended", thread_itr);
                latch.CountDown();
            });
        }
    }

    //////////////////////////////////////running/////////////
//    int Events[PAPI_EVENT_COUNT] = {PAPI_L1_DCM, PAPI_L2_TCM, PAPI_L3_TCM, PAPI_BR_INS};
//    //int Events[PAPI_EVENT_COUNT] = {PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_BR_MSP};
//    int EventSet = PAPI_NULL;
//    long long values[PAPI_EVENT_COUNT];
//    int retval;
//    if(enable_papi){
//        InitPAPI();
///* Allocate space for the new eventset and do setup */
//        retval = PAPI_create_eventset(&EventSet);
///* Add Flops and total cycles to the eventset */
//        retval = PAPI_add_events(EventSet,Events,PAPI_EVENT_COUNT);
///* Start the counters */
//        retval = PAPI_start(EventSet);
//        assert(retval == PAPI_OK);
//    }

    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        std::this_thread::sleep_for(std::chrono::milliseconds(int(1000)));
    }

    is_running = false;

    // Join the threads with the main thread
    latch.Await();

    LOG_INFO("Warmed up\n");
    is_running = true;

//    std::unique_ptr<SystemCounterState> after_sstate;
//    after_sstate = std::make_unique<SystemCounterState>();
//    *after_sstate = getSystemCounterState();
//    if(enable_papi){
//        /*Stop counters and store results in values */
//        retval = PAPI_stop(EventSet,values);
//        assert(retval == PAPI_OK);
//        std::cout << "L1 miss = " << values[0] << "\n";
//        std::cout << "L2 miss = " << values[1] << "\n";
//        std::cout << "L3 miss = " << values[2] << "\n";
//        std::cout << "Total branch = " << values[3] << "\n";
//        //std::cout << "Total cycle = " << values[0] << "\n";
//        //std::cout << "Total instruction = " << values[1] << "\n";
//        //std::cout << "Total branch misprediction = " << values[2] << "\n";
//        PAPI_shutdown();
//    }
//    std::cout << "PCM Metrics:"
//              << "\n"
//              << "\tL2 HitRatio: " << getL2CacheHitRatio(*before_sstate, *after_sstate) << "\n"
//              << "\tL3 HitRatio: " << getL3CacheHitRatio(*before_sstate, *after_sstate) << "\n"
//              << "\tL2 misses: " << getL2CacheMisses(*before_sstate, *after_sstate) << "\n"
//              << "\tL3 misses: " << getL3CacheMisses(*before_sstate, *after_sstate) << "\n"
//              << "\tDRAM Reads (bytes): " << getBytesReadFromMC(*before_sstate, *after_sstate) << "\n"
//              << "\tDRAM Writes (bytes): " << getBytesWrittenToMC(*before_sstate, *after_sstate) << "\n"
//              << "\tNVM Reads (bytes): " << getBytesReadFromPMM(*before_sstate, *after_sstate) << "\n"
//              << "\tNVM Writes (bytes): " << getBytesWrittenToPMM(*before_sstate, *after_sstate) << "\n"
//              << std::endl;
//    // calculate the throughput and abort rate for the first round.
//    pcm_->cleanup();


    delete[] abort_counts;
    abort_counts = nullptr;
    delete[] commit_counts;
    commit_counts = nullptr;
}

void print_environment()
{
    std::time_t now = std::time(nullptr);
    uint64_t num_cpus = 0;
    std::string cpu_type;
    std::string cache_size;

    std::ifstream cpuinfo("/proc/cpuinfo", std::ifstream::in);

    if(!cpuinfo.good())
    {
        num_cpus = 0;
        cpu_type = "Could not open /proc/cpuinfo";
        cache_size = "Could not open /proc/cpuinfo";
    }
    else
    {
        std::string line;
        while(!getline(cpuinfo, line).eof())
        {
            auto sep_pos = line.find(':');
            if(sep_pos == std::string::npos)
            {
                continue;
            }
            else
            {
                std::string key = std::regex_replace(std::string(line, 0, sep_pos), std::regex("\\t+$"), "");
                std::string val = sep_pos == line.size()-1 ? "" : std::string(line, sep_pos+2, line.size());
                if(key.compare("model name") == 0)
                {
                    ++num_cpus;
                    cpu_type = val;
                }
                else if(key.compare("cache size") == 0)
                {
                    cache_size = val;
                }
            }
        }
    }
    cpuinfo.close();

    std::string kernel_version;
    struct utsname uname_buf;
    if(uname(&uname_buf) == -1)
    {
        kernel_version = "Unknown";
    }
    else
    {
        kernel_version = std::string(uname_buf.sysname) + " " + std::string(uname_buf.release);
    }

    std::cout << "Environment:" << "\n"
              << "\tTime: " << std::asctime(std::localtime(&now))
              << "\tCPU: " << num_cpus << " * " << cpu_type << "\n"
              << "\tCPU Cache: " << cache_size << "\n"
              << "\tKernel: " << kernel_version << std::endl;
}

void RunWorkload(VersionStore *version_store, EphemeralPool *conflict_buffer) {
    // value ranges 0 ~ 9999 ( modulo by 10k )
    std::vector<std::vector<std::pair<int32_t, int32_t>>> supp_stock_map(10000);
    // pre-build supp-stock mapping table to boost tpc-ch queries
    for (uint w = 1; w <= state.warehouse_count; w++)
        for (uint i = 1; i <= state.item_count; i++)
            supp_stock_map[w * i % 10000].push_back(std::make_pair((w-1), (i-1)));

    // Execute the workload to build the log
    size_t num_threads = state.backend_count;
    sync::ThreadPool the_tp(num_threads);
    std::vector<std::unique_ptr<std::thread>> threads_pool(num_threads);

    abort_counts = new PadInt[num_threads];
    memset(abort_counts, 0, sizeof(PadInt) * num_threads);

    commit_counts = new PadInt[num_threads];
    memset(commit_counts, 0, sizeof(PadInt) * num_threads);

//    q2_abort_counts = new PadInt[num_threads];
//    memset(q2_abort_counts, 0, sizeof(PadInt) * num_threads);
//
//    q2_commit_counts = new PadInt[num_threads];
//    memset(q2_commit_counts, 0, sizeof(PadInt) * num_threads);
//
    stock_abort_counts = new PadInt[num_threads];
    memset(stock_abort_counts, 0, sizeof(PadInt) * num_threads);

    stock_commit_counts = new PadInt[num_threads];
    memset(stock_commit_counts, 0, sizeof(PadInt) * num_threads);

    new_abort_counts = new PadInt[num_threads];
    memset(new_abort_counts, 0, sizeof(PadInt) * num_threads);

    new_commit_counts = new PadInt[num_threads];
    memset(new_commit_counts, 0, sizeof(PadInt) * num_threads);

    size_t profile_round = (size_t) (state.duration / state.profile_duration);

    PadInt **abort_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        abort_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PadInt **commit_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        commit_counts_profiles[round_id] = new PadInt[num_threads];
    }

//    PadInt **q2_abort_counts_profiles = new PadInt *[profile_round];
//    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//        q2_abort_counts_profiles[round_id] = new PadInt[num_threads];
//    }
//
//    PadInt **q2_commit_counts_profiles = new PadInt *[profile_round];
//    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//        q2_commit_counts_profiles[round_id] = new PadInt[num_threads];
//    }
    PadInt **new_abort_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        new_abort_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PadInt **new_commit_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        new_commit_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PadInt **stock_abort_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        stock_abort_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PadInt **stock_commit_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        stock_commit_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PCM *pcm_;
    std::unique_ptr<SystemCounterState> before_sstate;
    if (enable_pcm){
        pcm_ = PCM::getInstance();
        auto status = pcm_->program();
        if (status != PCM::Success)
        {
            std::cout << "Error opening PCM: " << status << std::endl;
            if (status == PCM::PMUBusy)
                pcm_->resetPMU();
            else
                exit(0);
        }


        before_sstate = std::make_unique<SystemCounterState>();
        *before_sstate = getSystemCounterState();
    }

    size_t scan_thread = num_threads*state.scan_rate;
//    sync::CountDownLatch latch(num_threads);
//    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
//        if (thread_itr < scan_thread){
//            the_tp.enqueue([thread_itr, &latch, version_store, conflict_buffer, &supp_stock_map]() {
//                //LOG_INFO("Thread %d started", thread_itr);
//                RunScanBackend(thread_itr, version_store, conflict_buffer, supp_stock_map);
//                //LOG_INFO("Thread %d ended", thread_itr);
//                latch.CountDown();
//            });
//        }else{
//            the_tp.enqueue([thread_itr, &latch, version_store, conflict_buffer]() {
//                //LOG_INFO("Thread %d started", thread_itr);
//                RunBackend(thread_itr, version_store, conflict_buffer);
//                //LOG_INFO("Thread %d ended", thread_itr);
//                latch.CountDown();
//            });
//        }
//    }
    for (int thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        if (thread_itr < scan_thread) {
            threads_pool[thread_itr] = std::make_unique<std::thread>(RunScanBackend, thread_itr, version_store,
                                                     conflict_buffer, supp_stock_map);
        }else{
            threads_pool[thread_itr] = std::make_unique<std::thread>(RunBackend, thread_itr, version_store,
                                                                     conflict_buffer);
        }
    }

    oid_t last_block_id = 0;
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(int(state.profile_duration * 1000)));
        memcpy(abort_counts_profiles[round_id], abort_counts,sizeof(PadInt) * num_threads);
        memcpy(commit_counts_profiles[round_id], commit_counts,sizeof(PadInt) * num_threads);
//        memcpy(q2_abort_counts_profiles[round_id], q2_abort_counts, sizeof(PadInt) * num_threads);
//        memcpy(q2_commit_counts_profiles[round_id], q2_commit_counts, sizeof(PadInt) * num_threads);
        memcpy(new_abort_counts_profiles[round_id], new_abort_counts,sizeof(PadInt) * num_threads);
        memcpy(new_commit_counts_profiles[round_id], new_commit_counts,sizeof(PadInt) * num_threads);
        memcpy(stock_abort_counts_profiles[round_id], stock_abort_counts,sizeof(PadInt) * num_threads);
        memcpy(stock_commit_counts_profiles[round_id], stock_commit_counts,sizeof(PadInt) * num_threads);

        auto manager = version_store->GetInstance();
        uint32_t current_block_id = manager->GetCurrentVersionBlockId();
        if (round_id != 0) {
            state.profile_memory.push_back(current_block_id - last_block_id);
        }
        last_block_id = current_block_id;
    }

    state.profile_memory.push_back(state.profile_memory.at(state.profile_memory.size() - 1));
    is_running = false;

    // Join the threads with the main thread
//    latch.Await();
    // Join the threads with the main thread
    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        threads_pool[thread_itr]->join();
    }

    if (enable_pcm){
        std::unique_ptr<SystemCounterState> after_sstate;
        after_sstate = std::make_unique<SystemCounterState>();
        *after_sstate = getSystemCounterState();

        std::cout << "PCM Metrics:"
                  << "\n"
                  << "\tL2 HitRatio: " << getL2CacheHitRatio(*before_sstate, *after_sstate) << "\n"
                  << "\tL3 HitRatio: " << getL3CacheHitRatio(*before_sstate, *after_sstate) << "\n"
                  << "\tL2 misses: " << getL2CacheMisses(*before_sstate, *after_sstate) << "\n"
                  << "\tL3 misses: " << getL3CacheMisses(*before_sstate, *after_sstate) << "\n"
                  << "\tIPC: " << getIPC(*before_sstate, *after_sstate) << "\n"
                  << "\tCycles: " << getCycles(*before_sstate, *after_sstate) << "\n"
                  << "\tinstructions: " << getInstructionsRetired(*before_sstate, *after_sstate) << "\n"
                  << "\tDRAM Reads (bytes): " << getBytesReadFromMC(*before_sstate, *after_sstate) << "\n"
                  << "\tDRAM Writes (bytes): " << getBytesWrittenToMC(*before_sstate, *after_sstate) << "\n"
                  << "\tNVM Reads (bytes): " << getBytesReadFromPMM(*before_sstate, *after_sstate) << "\n"
                  << "\tNVM Writes (bytes): " << getBytesWrittenToPMM(*before_sstate, *after_sstate) << "\n"
                  << std::endl;
        // calculate the throughput and abort rate for the first round.
        pcm_->cleanup();
    }


    // calculate the throughput and abort rate for the first round.
    uint64_t total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_commit_count += commit_counts_profiles[0][i].data;
    }

    uint64_t total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_abort_count += abort_counts_profiles[0][i].data;
    }
    uint64_t q2_total_commit_count = 0;
    for (size_t i = 0; i < scan_thread; ++i) {
        q2_total_commit_count += commit_counts_profiles[0][i].data;
    }

    uint64_t q2_total_abort_count = 0;
    for (size_t i = 0; i < scan_thread; ++i) {
        q2_total_abort_count += abort_counts_profiles[0][i].data;
    }

//    uint64_t new_total_commit_count = 0;
//    for (size_t i = scan_thread; i < num_threads; ++i) {
//        new_total_commit_count += commit_counts_profiles[0][i].data;
//    }
//
//    uint64_t new_total_abort_count = 0;
//    for (size_t i = scan_thread; i < num_threads; ++i) {
//        new_total_abort_count += abort_counts_profiles[0][i].data;
//    }
//    uint64_t q2_total_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        q2_total_commit_count += q2_commit_counts_profiles[0][i].data;
//    }
//
//    uint64_t q2_total_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        q2_total_abort_count += q2_abort_counts_profiles[0][i].data;
//    }

    uint64_t new_total_commit_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        new_total_commit_count += new_commit_counts_profiles[0][i].data;
    }

    uint64_t new_total_abort_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        new_total_abort_count += new_abort_counts_profiles[0][i].data;
    }

    uint64_t stock_total_commit_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        stock_total_commit_count += stock_commit_counts_profiles[0][i].data;
    }

    uint64_t stock_total_abort_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        stock_total_abort_count += stock_abort_counts_profiles[0][i].data;
    }

    state.profile_throughput.push_back(total_commit_count * 1.0 / state.profile_duration);
    state.profile_abort_rate.push_back(total_abort_count * 1.0 / (total_commit_count + total_abort_count));
//    state.profile_q2_throughput.push_back(q2_total_commit_count * 1.0 / state.profile_duration);
//    state.profile_q2_abort_rate.push_back(q2_total_abort_count * 1.0 / q2_total_commit_count);

    // calculate the throughput and abort rate for the remaining rounds.
    for (size_t round_id = 0; round_id < profile_round - 1; ++round_id) {
        total_commit_count = 0;
        for (size_t i = 0; i < num_threads; ++i) {
            total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                                  commit_counts_profiles[round_id][i].data;
        }
        total_abort_count = 0;
        for (size_t i = 0; i < num_threads; ++i) {
            total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                                 abort_counts_profiles[round_id][i].data;
        }
        q2_total_commit_count = 0;
        for (size_t i = 0; i < scan_thread; ++i) {
            q2_total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                                     commit_counts_profiles[round_id][i].data;
        }
        q2_total_abort_count = 0;
        for (size_t i = 0; i < scan_thread; ++i) {
            q2_total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                                    abort_counts_profiles[round_id][i].data;
        }
//        new_total_commit_count = 0;
//        for (size_t i = scan_thread; i < num_threads; ++i) {
//            new_total_commit_count += commit_counts_profiles[round_id + 1][i].data -
//                                      commit_counts_profiles[round_id][i].data;
//        }
//        new_total_abort_count = 0;
//        for (size_t i = scan_thread; i < num_threads; ++i) {
//            new_total_abort_count += abort_counts_profiles[round_id + 1][i].data -
//                                     abort_counts_profiles[round_id][i].data;
//        }
//        q2_total_commit_count = 0;
//        for (size_t i = 0; i < num_threads; ++i) {
//          q2_total_commit_count += q2_commit_counts_profiles[round_id + 1][i].data -
//                                   q2_commit_counts_profiles[round_id][i].data;
//        }
//        q2_total_abort_count = 0;
//        for (size_t i = 0; i < num_threads; ++i) {
//          q2_total_abort_count += q2_abort_counts_profiles[round_id + 1][i].data -
//                                  q2_abort_counts_profiles[round_id][i].data;
//        }
        new_total_commit_count = 0;
        for (size_t i = scan_thread; i < num_threads; ++i) {
            new_total_commit_count += new_commit_counts_profiles[round_id + 1][i].data -
                                      new_commit_counts_profiles[round_id][i].data;
        }
        new_total_abort_count = 0;
        for (size_t i = scan_thread; i < num_threads; ++i) {
            new_total_abort_count += new_abort_counts_profiles[round_id + 1][i].data -
                                     new_abort_counts_profiles[round_id][i].data;
        }
        stock_total_commit_count = 0;
        for (size_t i = scan_thread; i < num_threads; ++i) {
            stock_total_commit_count += stock_commit_counts_profiles[round_id + 1][i].data -
                                        stock_commit_counts_profiles[round_id][i].data;
        }
        stock_total_abort_count = 0;
        for (size_t i = scan_thread; i < num_threads; ++i) {
            stock_total_abort_count += stock_abort_counts_profiles[round_id + 1][i].data -
                                       stock_abort_counts_profiles[round_id][i].data;
        }

        LOG_DEBUG("%f", total_commit_count * 1.0 / state.profile_duration);
        state.profile_throughput.push_back(total_commit_count * 1.0 / state.profile_duration);
        state.profile_abort_rate.push_back(total_abort_count * 1.0 / (total_commit_count + total_abort_count));
    }

    // calculate the aggregated throughput and abort rate.
//    uint64_t warmup_period_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        warmup_period_commit_count += commit_counts_profiles[profile_round / 2][i].data;
//    }
//
//    uint64_t warmup_period_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        warmup_period_abort_count += abort_counts_profiles[profile_round / 2][i].data;
//    }

//    state.warmup_throughput = warmup_period_commit_count * 1.0 / (state.duration / 2);

    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
    }
    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
    }
    q2_total_commit_count = 0;
    for (size_t i = 0; i < scan_thread; ++i) {
        q2_total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
    }
    q2_total_abort_count = 0;
    for (size_t i = 0; i < scan_thread; ++i) {
        q2_total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
    }
//    new_total_commit_count = 0;
//    for (size_t i = scan_thread; i < num_threads; ++i) {
//        new_total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
//    }
//    new_total_abort_count = 0;
//    for (size_t i = scan_thread; i < num_threads; ++i) {
//        new_total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
//    }
//    q2_total_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        q2_total_commit_count += q2_commit_counts_profiles[profile_round - 1][i].data;
//    }
//    q2_total_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        q2_total_abort_count += q2_abort_counts_profiles[profile_round - 1][i].data;
//    }

    new_total_commit_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        new_total_commit_count += new_commit_counts_profiles[profile_round - 1][i].data;
    }
    new_total_abort_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        new_total_abort_count += new_abort_counts_profiles[profile_round - 1][i].data;
    }

    stock_total_commit_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        stock_total_commit_count += stock_commit_counts_profiles[profile_round - 1][i].data;
    }
    stock_total_abort_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
        stock_total_abort_count += stock_abort_counts_profiles[profile_round - 1][i].data;
    }


//  LOG_INFO("scan commit count %d", (int)scan_commit_count);
    std::cout << "overall throughputs:"
              << "\n"
              << "\ttotal transactions: " << (int)(total_commit_count + total_abort_count) << "\n"
              << "\ttotal commit transactions: " << (int)(total_commit_count) << "\n"
              << "\ttotal abort transactions: " << (int)(total_abort_count) << "\n"
              << "\tq2 total commit transactions: " << (int)(q2_total_commit_count) << "\n"
              << "\tq2 total abort transactions: " << (int)(q2_total_abort_count) << "\n"
              << "\tnew total commit transactions: " << (int)(new_total_commit_count) << "\n"
              << "\tnew total abort transactions: " << (int)(new_total_abort_count) << "\n"
              << "\tstock total commit transactions: " << (int)(stock_total_commit_count) << "\n"
              << "\tstock total abort transactions: " << (int)(stock_total_abort_count) << "\n"
              << std::endl;


    state.throughput = total_commit_count * 1.0 / state.duration;// ops/second
    state.abort_rate = total_abort_count * 1.0 / (total_commit_count+total_abort_count);
//  state.q2_throughput = q2_total_commit_count * 1.0 / state.duration;
//  state.q2_abort_rate = q2_total_abort_count * 1.0 / q2_total_commit_count;
    //when q2 is 100%
    if(state.scan_rate == 1){
        state.scan_latency = (state.duration * 1000) / ((q2_total_commit_count + q2_total_abort_count) * 1.0);//millisecond
    }

    // cleanup everything.
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] abort_counts_profiles[round_id];
        abort_counts_profiles[round_id] = nullptr;
    }

    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] commit_counts_profiles[round_id];
        commit_counts_profiles[round_id] = nullptr;
    }
//    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//        delete[] q2_abort_counts_profiles[round_id];
//        q2_abort_counts_profiles[round_id] = nullptr;
//    }
//    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//        delete[] q2_commit_counts_profiles[round_id];
//        q2_commit_counts_profiles[round_id] = nullptr;
//    }

    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] new_abort_counts_profiles[round_id];
        new_abort_counts_profiles[round_id] = nullptr;
    }
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] new_commit_counts_profiles[round_id];
        new_commit_counts_profiles[round_id] = nullptr;
    }

    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] stock_abort_counts_profiles[round_id];
        stock_abort_counts_profiles[round_id] = nullptr;
    }
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] stock_commit_counts_profiles[round_id];
        stock_commit_counts_profiles[round_id] = nullptr;
    }

    delete[] abort_counts_profiles;
    abort_counts_profiles = nullptr;
    delete[] commit_counts_profiles;
    commit_counts_profiles = nullptr;
//    delete[] q2_abort_counts_profiles;
//    q2_abort_counts_profiles = nullptr;
//    delete[] q2_commit_counts_profiles;
//    q2_commit_counts_profiles = nullptr;
    delete[] new_abort_counts_profiles;
    new_abort_counts_profiles = nullptr;
    delete[] new_commit_counts_profiles;
    new_commit_counts_profiles = nullptr;
    delete[] stock_abort_counts_profiles;
    stock_abort_counts_profiles = nullptr;
    delete[] stock_commit_counts_profiles;
    stock_commit_counts_profiles = nullptr;

    delete[] abort_counts;
    abort_counts = nullptr;
    delete[] commit_counts;
    commit_counts = nullptr;
    delete[] q2_abort_counts;
    q2_abort_counts = nullptr;
    delete[] q2_commit_counts;
    q2_commit_counts = nullptr;
    delete[] new_abort_counts;
    new_abort_counts = nullptr;
    delete[] new_commit_counts;
    new_commit_counts = nullptr;
    delete[] stock_abort_counts;
    stock_abort_counts = nullptr;
    delete[] stock_commit_counts;
    stock_commit_counts = nullptr;


}

}
}
}
