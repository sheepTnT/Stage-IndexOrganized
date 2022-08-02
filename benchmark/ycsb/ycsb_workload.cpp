//
// Created by zhangqian on 2022/2/10.
//
#include <string>
#include <vector>
#include <chrono>
#include <ctime>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
//#include <papi.h>
#include <sys/utsname.h>
#include <regex>
#include "ycsb_workload.h"
#include "ycsb_configuration.h"
#include "ycsb_loader.h"
#include "../pcm/cpucounters.h"
//#include "../pcm/pcm-memory.cpp"


namespace mvstore {
namespace benchmark {
namespace ycsb {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;
static constexpr int PAPI_EVENT_COUNT = 7;
PadInt *abort_counts;
PadInt *commit_counts;
thread_local size_t num_rw_ops = 0;
std::atomic<size_t> min_rowid ;
#define PMM_READ 2
#define PMM_WRITE 3

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

void RunWarmupBackend(VersionStore *version_store, const size_t thread_id,
                      std::vector<uint32_t> &keys) {

    PinToCore(thread_id);

    ZipfDistribution zipf((state.scale_factor) - 1, state.zipf_theta);

    FastRandom rng(rand());

    // backoff
    uint32_t backoff_shifts = 0;
    if(state.scan_mode){
        while (true) {
            if (!is_running) {
                break;
            }
            //LOG_INFO("%d working %d", thread_id, cnt);
            while (!RunScan(version_store, thread_id, zipf, rng, keys)) {
                if (!is_running) {
                    break;
                }
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
            backoff_shifts >>= 1;

        }
    }else{
        while (true) {
            if (!is_running) {
                break;
            }
            while (!RunMixed(version_store, thread_id, zipf, rng, keys)) {
                if (!is_running) {
                    break;
                }
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
            backoff_shifts >>= 1;
        }
    }

}
void RunInsertBackend(VersionStore *version_store, const size_t thread_id) {

    PinToCore(thread_id);

    PadInt &execution_count_ref = abort_counts[thread_id];
    PadInt &transaction_count_ref = commit_counts[thread_id];

    // backoff
    uint32_t backoff_shifts = 0;
    while (true) {
        if (!is_running) {
            break;
        }
        //LOG_INFO("%d working %d", thread_id, cnt);
        size_t num_rw_ops_snap = num_rw_ops;

        while (!RunInsert(version_store, thread_id)) {
            if (!is_running) {
                break;
            }
            num_rw_ops_snap = num_rw_ops;
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
        backoff_shifts >>= 1;
        //ycsb statistic the operation nums
        //LOG_INFO("%u num_rw_ops, %d num_rw_ops_snap", num_rw_ops,num_rw_ops_snap);
        transaction_count_ref.data += num_rw_ops - num_rw_ops_snap;
    }
}

void RunBackend(VersionStore *version_store, const size_t thread_id,
                std::vector<uint32_t> keys) {

    PinToCore(thread_id);

    PadInt &execution_count_ref = abort_counts[thread_id];
    PadInt &transaction_count_ref = commit_counts[thread_id];

    ZipfDistribution zipf((state.scale_factor) - 1, state.zipf_theta);

    FastRandom rng(rand());

    // backoff
    uint32_t backoff_shifts = 0;
    if(state.scan_mode){
        while (true) {
            if (!is_running) {
                break;
            }
            //LOG_INFO("%d working %d", thread_id, cnt);

            while (!RunScan(version_store, thread_id, zipf, rng, keys)) {
                if (!is_running) {
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
            backoff_shifts >>= 1;
            //ycsb statistic the operation nums
            //LOG_INFO("%u num_rw_ops, %d num_rw_ops_snap", num_rw_ops,num_rw_ops_snap);
            transaction_count_ref.data ++;
        }
    }else{
        if (state.string_mode){
            while (true) {
                if (!is_running) {
                    break;
                }
                //LOG_INFO("%d working %d", thread_id, cnt);
                size_t num_rw_ops_snap = num_rw_ops;

                while (!RunMixedString(version_store, thread_id, zipf, rng, keys)) {
                    if (!is_running) {
                        break;
                    }
                    num_rw_ops_snap = num_rw_ops;
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
                backoff_shifts >>= 1;
                //ycsb statistic the operation nums
                //LOG_INFO("%u num_rw_ops, %d num_rw_ops_snap", num_rw_ops,num_rw_ops_snap);
                transaction_count_ref.data += num_rw_ops - num_rw_ops_snap;
            }
        }else{
            while (true) {
                if (!is_running) {
                    break;
                }
                //LOG_INFO("%d working %d", thread_id, cnt);
                size_t num_rw_ops_snap = num_rw_ops;

                while (!RunMixed(version_store, thread_id, zipf, rng, keys)) {
                    if (!is_running) {
                        break;
                    }
                    num_rw_ops_snap = num_rw_ops;
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
                backoff_shifts >>= 1;
                //ycsb statistic the operation nums
                //LOG_INFO("%u num_rw_ops, %d num_rw_ops_snap", num_rw_ops,num_rw_ops_snap);
                transaction_count_ref.data += num_rw_ops - num_rw_ops_snap;
            }
        }
    }

    //LOG_INFO("%d Inner Done", thread_id);
}

void RunWarmupWorkload(VersionStore *version_store,  std::vector<uint32_t> &keys) {
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
//
//    pcm_->cleanup();


//    int Events[PAPI_EVENT_COUNT] = {PAPI_L1_DCM, PAPI_L2_DCM, PAPI_L3_DCM, PAPI_BR_INS};
//    //int Events[PAPI_EVENT_COUNT] = {PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_BR_MSP};
//    int EventSet = PAPI_NULL;
//    long long values[PAPI_EVENT_COUNT];
//    int retval;
//    if(enable_papi){
//        InitPAPI();
///* Allocate space for the new eventset and do setup */
//        retval = PAPI_create_eventset(&EventSet);
////        assert(retval == PAPI_OK);
///* Add Flops and total cycles to the eventset */
//        retval = PAPI_add_events(EventSet,Events,PAPI_EVENT_COUNT);
////        assert(retval == PAPI_LOW_LEVEL_INITED);
///* Start the counters */
//        retval = PAPI_start(EventSet);
//        assert(retval == PAPI_OK);
//    }

    //PCM_memory::InitMemoryMonitor();
//    std::unique_ptr<SystemCounterState> before_sstate;
//    before_sstate = std::make_unique<SystemCounterState>();
//    *before_sstate = getSystemCounterState();
//    //PCM_memory::StartMemoryMonitor();
//
//    size_t num_threads = state.backend_count;
//    size_t warmup_duration = state.warmup_duration;

//    sync::ThreadPool the_tp(num_threads);
//    sync::CountDownLatch latch(num_threads);
//
//    // Launch a group of threads
//    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
//        the_tp.enqueue([&latch, thread_itr, version_store, &keys]() {
//            //LOG_INFO("%lu Started", thread_itr);
//            RunWarmupBackend(version_store, thread_itr, keys);
//            //LOG_INFO("%lu Done", thread_itr);
//            latch.CountDown();
//        });
//    }
//
//    for (size_t round_id = 0; round_id < warmup_duration; ++round_id) {
//        std::this_thread::sleep_for(
//                std::chrono::milliseconds(int(1000)));
//    }
//
//    is_running = false;
//
//    // Join the threads with the main thread
//    latch.Await();
//    ZipfDistribution zipf((state.scale_factor) - 1, state.zipf_theta);
//    FastRandom rng(rand());
//    std::chrono::high_resolution_clock::time_point start_ = std::chrono::high_resolution_clock::now();
//    std::chrono::high_resolution_clock::time_point stop_ = std::chrono::high_resolution_clock::now();;
//    std::chrono::duration<float, typename std::chrono::milliseconds::period> elasp = stop_ - start_;
//    auto du =  warmup_duration*1000*1000;
//    int i=0;

//    while (i<10000){
//        RunScan(version_store, 0, zipf, rng, keys);
////        stop_ = std::chrono::high_resolution_clock::now();
////        elasp = stop_ - start_;
//        i++;
//    }

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
//    }

//    //Restore the states
//    is_running = true;

//
//    std::unique_ptr<SystemCounterState> after_sstate;
//    after_sstate = std::make_unique<SystemCounterState>();
//    *after_sstate = getSystemCounterState();
    //PCM_memory::EndMemoryMonitor();

//    print_usage();

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
//
//    LOG_INFO("Warmed up");
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
                std::string key = std::regex_replace(std::string(line, 0, sep_pos), std::regex("\\t+$"),"");
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
void RunWorkload(VersionStore *version_store,  std::vector<uint32_t> &keys) {
    size_t num_threads = state.backend_count;
    min_rowid = 0 ;
    int tuple_count = state.scale_factor;
    min_rowid.fetch_add(tuple_count);
    std::vector<std::thread> thread_group;

    abort_counts = new PadInt[num_threads];
    memset(abort_counts, 0, sizeof(PadInt) * num_threads);

    commit_counts = new PadInt[num_threads];
    memset(commit_counts, 0, sizeof(PadInt) * num_threads);

    size_t profile_round = (size_t) (state.duration / state.profile_duration);

    PadInt **abort_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        abort_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PadInt **commit_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        commit_counts_profiles[round_id] = new PadInt[num_threads];
    }

//    print_environment();
//    print_cpu_details();

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

//    sync::CountDownLatch latch(num_threads);
//    srand(time(0));

//    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
//        thread_group.push_back(std::move(std::thread(RunInsertBackend, version_store, thread_itr)));
//    }
    thread_group.resize(num_threads + state.vc_start);
    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        thread_group.push_back(std::move(std::thread(RunBackend, version_store, thread_itr, keys)));
    }

    //start version cleanning
    if (state.vc_start){
        thread_group.push_back(std::move(std::thread(VersionCleaner::CleanVersionsProcessing, profile_round)));
    }

    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(int(state.profile_duration * 1000)));
        memcpy(abort_counts_profiles[round_id], abort_counts,
               sizeof(PadInt) * num_threads);
        memcpy(commit_counts_profiles[round_id], commit_counts,
               sizeof(PadInt) * num_threads);

        if (round_id != 0) {
            state.profile_memory.push_back(0);
        }
    }

    state.profile_memory.push_back(state.profile_memory.at(state.profile_memory.size() - 1));

    is_running = false;

    // Join the threads with the main thread
    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        thread_group[thread_itr].join();
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


    uint64_t total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_commit_count += commit_counts_profiles[0][i].data;
    }

    uint64_t total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_abort_count += abort_counts_profiles[0][i].data;
    }

    state.profile_throughput.push_back(total_commit_count * 1.0 /
                                       state.profile_duration);
    state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                       total_commit_count);

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
        LOG_INFO("%f", total_commit_count * 1.0 / state.profile_duration);
        state.profile_throughput.push_back(total_commit_count * 1.0 /
                                           state.profile_duration);
        state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                           total_commit_count);
    }

    //////////////////////////////////////////////////
    // calculate the aggregated throughput and abort rate.
//    uint64_t warmup_period_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        warmup_period_commit_count += commit_counts_profiles[(profile_round - 1) / 2][i].data;
//    }
//
//    uint64_t warmup_period_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//        warmup_period_abort_count += abort_counts_profiles[(profile_round - 1) / 2][i].data;
//    }
//
//    state.warmup_throughput = warmup_period_commit_count * 1.0 / (state.duration / 2);

    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
    }

    state.throughput = (total_commit_count) * 1.0 / (state.duration);
    state.abort_rate = (total_abort_count) * 1.0 / (total_commit_count + total_abort_count);
    if (state.scan_mode){
        state.scan_latency = (state.duration * 1000) / (total_commit_count * 1.0);//millisecond
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

    delete[] abort_counts_profiles;
    abort_counts_profiles = nullptr;
    delete[] commit_counts_profiles;
    commit_counts_profiles = nullptr;

    delete[] abort_counts;
    abort_counts = nullptr;
    delete[] commit_counts;
    commit_counts = nullptr;

}


}
}
}
