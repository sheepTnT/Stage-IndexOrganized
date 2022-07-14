
#include <iomanip>
#include <algorithm>
#include <fstream>

#include "tpcc_configuration.h"

namespace mvstore {
namespace benchmark {
namespace tpcc {

void Usage(FILE *out) {
    fprintf(out,
            "Command line options : ycsb <options> \n"
            "   -h --help              :  print help message \n"
//            "   -k --scale_factor      :  # of K tuples \n"
            "   -d --duration          :  execution duration \n"
            "   -p --profile_duration  :  profile duration \n"
            "   -b --backend_count     :  # of backends \n"
            "   -e --exp_backoff       :  enable exponential backoff \n"
            "   -l --loader_count      :  # of loaders \n"
            "   -w --warehouse_count   :  # warehouse counts \n"
            "   -s --hybrid_rate       :  # the percentage of query2 when mixing new-order and query2 \n"
            "   -o --new_order_rate    :  # the percentage of new-order when mixing new-order and stock-level \n"
            "   -r --stock_level_rate  :  # the percentage of stock-level when mixing new-order and stock-level \n"
//            "   -J --wal_path          :  logging directory\n"
//            "   -U --warmup_duration   :  warmup duration(s)\n"
    );
}

static struct option opts[] = {
//        { "scale_factor", optional_argument, NULL, 'k' },
        { "duration", optional_argument, NULL, 'd' },
        { "profile_duration", optional_argument, NULL, 'p' },
        { "backend_count", optional_argument, NULL, 'b' },
        { "exp_backoff", no_argument, NULL, 'e' },
        { "loader_count", optional_argument, NULL, 'l' },
        { "warehouse_count", optional_argument, NULL, 'w' },
        { "hybrid_rate", optional_argument, NULL, 's' },
        { "new_order_rate", optional_argument, NULL, 'o' },
        { "stock_level_rate", optional_argument, NULL, 'r' },
//        { "wal_path", optional_argument, NULL, 'J' },
//        { "warmup_duration", optional_argument, NULL, 'U' },
        { NULL, 0, NULL, 0 }
};

void ValidateScaleFactor(const configuration &state) {
    if (state.scale_factor <= 0) {
        LOG_ERROR("Invalid scale_factor :: %lf", state.scale_factor);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %lf", "scale_factor", state.scale_factor);
}

void ValidateDuration(const configuration &state) {
    if (state.duration <= 0) {
        LOG_ERROR("Invalid duration :: %lf", state.duration);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %lf", "duration", state.duration);
}

void ValidateProfileDuration(const configuration &state) {
    if (state.profile_duration <= 0) {
        LOG_ERROR("Invalid profile_duration :: %lf", state.profile_duration);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %lf", "profile_duration", state.profile_duration);
}

void ValidateBackendCount(const configuration &state) {
    if (state.backend_count <= 0) {
        LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %d", "backend_count", state.backend_count);
}

void ValidateWarehouseCount(const configuration &state) {
    if (state.warehouse_count <= 0) {
        LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %d", "warehouse_count", state.warehouse_count);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
    // Default Values
    state.scale_factor = 1;
    state.duration = 10;
    state.profile_duration = 1;
    state.backend_count = 2;
    state.exp_backoff = false;
    state.affinity = false;
    state.loader_count = 2;
    state.warehouse_count = 2;
    state.warmup_duration = 0;
    state.scan_rate = 0;
    state.new_order_rate = 1;
    state.stock_level_rate = 0;

    // Parse args
    while (1) {
        int idx = 0;
        int c = getopt_long(argc, argv, "theLAMIagi:d:p:b:w:l:y:s:o:r:", opts, &idx);

        if (c == -1) break;

        switch (c) {
//            case 'J':
//                state.wal_path = optarg;
//                break;
//            case 'U':
//                state.warmup_duration = atof(optarg);
//                break;
            case 's':
                state.scan_rate = atof(optarg);
                break;
            case 'o':
                state.new_order_rate = atof(optarg);
                break;
            case 'r':
                state.stock_level_rate = atof(optarg);
                break;
            case 'l':
                state.loader_count = atoi(optarg);
                break;
//            case 'k':
//                state.scale_factor = atof(optarg);
//                break;
            case 'd':
                state.duration = atof(optarg);
                break;
            case 'p':
                state.profile_duration = atof(optarg);
                break;
            case 'b':
                state.backend_count = atoi(optarg);
                break;
            case 'w':
                state.warehouse_count = atoi(optarg);
                break;
            case 'e':
                state.exp_backoff = true;
                break;
            case 'a':
                state.affinity = true;
                break;
            case 'h':
                Usage(stderr);
                exit(EXIT_FAILURE);
                break;
            default:
                LOG_ERROR("Unknown option: -%c-", c);
                Usage(stderr);
                exit(EXIT_FAILURE);
        }
    }

    // Static TPCC parameters
    state.item_count = 100000 * state.scale_factor;
    state.districts_per_warehouse = 10;
    state.customers_per_district = 3000 * state.scale_factor;
    state.new_orders_per_district = 900 * state.scale_factor;

    // Print configuration
    ValidateScaleFactor(state);
    ValidateDuration(state);
    ValidateProfileDuration(state);
    ValidateBackendCount(state);
    ValidateWarehouseCount(state);

    LOG_TRACE("%s : %d", "Run client affinity", state.affinity);
    LOG_TRACE("%s : %d", "Run exponential backoff", state.exp_backoff);
}



void WriteOutput() {
    std::ofstream out("outputfile.summary");

    int total_profile_memory = 0;
    for (auto &entry : state.profile_memory) {
        total_profile_memory += entry;
    }

    LOG_INFO("----------------------------------------------------------");
//    LOG_INFO("%lf %d %d :: %lf %lf %lf  %lf %lf %lf %d",
//             state.scale_factor,
//             state.backend_count,
//             state.warehouse_count,
//             state.warmup_throughput,
//             state.throughput,
//             state.abort_rate,
//             state.q2_throughput,
//             state.q2_abort_rate,
//             state.scan_latency,
//             total_profile_memory);
//
//    out << state.scale_factor << " ";
//    out << state.backend_count << " ";
//    out << state.warehouse_count << " ";
//    out << state.warmup_throughput << " ";
//    out << state.throughput << " ";
//    out << state.abort_rate << " ";
//    out << state.q2_throughput << " ";
//    out << state.q2_abort_rate << " ";
//    out << state.scan_latency << "\n";
    LOG_INFO("%s : %d", "warehouse_count", state.warehouse_count);
    LOG_INFO("%s : %d", "backend_count", state.backend_count);
    LOG_INFO("%s : %lf", "throughput(txns/s)", state.throughput);
    LOG_INFO("%s : %lf", "abort_rate", state.abort_rate);
    LOG_INFO("%s : %lf", "ch_q2* latency(ms)", state.scan_latency);


    for (size_t round_id = 0; round_id < state.profile_throughput.size();
         ++round_id) {
        out << "[" << std::setw(3) << std::left
            << state.profile_duration * round_id << " - " << std::setw(3)
            << std::left << state.profile_duration * (round_id + 1)
            << " s]: " << state.profile_throughput[round_id] << " "
            << state.profile_abort_rate[round_id];
    }
    out.flush();
    out.close();
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
