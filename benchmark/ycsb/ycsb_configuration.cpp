//
// Created by zhangqian on 2022/2/10.
//
#include <iomanip>
#include <algorithm>
#include <iostream>
#include <fstream>

#include "ycsb_configuration.h"

namespace mvstore {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  print help message \n"
          "   -k --scale_factor      :  # of K tuples \n"
          "   -d --duration          :  execution duration \n"
          "   -p --profile_duration  :  profile duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -o --operation_count   :  # of operations \n"
          "   -u --update_ratio      :  fraction of updates \n"
          "   -z --zipf_theta        :  theta to control skewness \n"
          "   -l --loader_count      :  # of loaders \n"
          //"   -J --wal_path          :  logging directory\n"
//          "   -U --warmup_duration   :  warmup duration(s)\n"
          "   -s --shuffle_keys      :  whether to shuffle keys at startup (Default: fasle)\n"
          "   -r --random_mode       :  whether key is random distribution access\n"
          "   -m --string_mode       :  whether key is string\n"
          "   -n --scan_mode         :  whether scan only workload\n"
  );
}

static struct option opts[] = {
    { "scale_factor", optional_argument, NULL, 'k' },
    { "duration", optional_argument, NULL, 'd' },
    { "profile_duration", optional_argument, NULL, 'p' },
    { "backend_count", optional_argument, NULL, 'b' },
    { "operation_count", optional_argument, NULL, 'o' },
    { "update_ratio", optional_argument, NULL, 'u' },
    { "zipf_theta", optional_argument, NULL, 'z' },
    { "loader_count", optional_argument, NULL, 'l' },
    //{ "wal_path", optional_argument, NULL, 'J' },
//    { "warmup_duration", optional_argument, NULL, 'U' },
    { "shuffle_keys", optional_argument, NULL, 's' },
    { "random_mode", optional_argument, NULL, 'r' },
    { "string_mode", optional_argument, NULL, 'm' },
    { "scan_mode", optional_argument, NULL, 'n' },
    { NULL, 0, NULL, 0 }
};


void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

}

void ValidateProfileDuration(const configuration &state) {
  if (state.profile_duration <= 0) {
    LOG_ERROR("Invalid profile_duration :: %lf", state.profile_duration);
    exit(EXIT_FAILURE);
  }

}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

}


void ValidateOperationCount(const configuration &state) {
  if (state.operation_count <= 0) {
    LOG_ERROR("Invalid operation_count :: %d", state.operation_count);
    exit(EXIT_FAILURE);
  }

}

void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    LOG_ERROR("Invalid update_ratio :: %lf", state.update_ratio);
    exit(EXIT_FAILURE);
  }

}

void ValidateZipfTheta(const configuration &state) {
  if (state.zipf_theta < 0 || state.zipf_theta > 1.0) {
    LOG_ERROR("Invalid zipf_theta :: %lf", state.zipf_theta);
    exit(EXIT_FAILURE);
  }

}


void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1000;
  state.duration = 10;
  state.warmup_duration = 0;
  state.profile_duration = 1;
  state.backend_count = 4;
  state.loader_count = 3;
  state.operation_count = 10;
  state.update_ratio = 0;
  state.zipf_theta = 0.0;
  state.exp_backoff = false;
  state.string_mode = false;
  state.scan_mode = false;
  state.random_mode = false;


  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv,
                        "hemnsALMItk:d:p:b:c:o:u:z:l:m:n:r:",
                        opts,
                        &idx);

    if (c == -1) break;

    switch (c) {
//      case 'J':
//        state.wal_path = optarg;
//        break;
//      case 'U':
//        state.warmup_duration = atof(optarg);
//        break;
      case 'l':
        state.loader_count = atoi(optarg);
        break;
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 'p':
        state.profile_duration = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'o':
        state.operation_count = atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;
      case 'z':
        state.zipf_theta = atof(optarg);
        break;
      case 'e':
        state.exp_backoff = true;
        break;
      case 'm':
        state.string_mode = true;
        break;
      case 'n':
        state.scan_mode = true;
        break;
	  case 'r':
        state.random_mode = true;
        break;
      case 's':
        state.shuffle_keys = true;
        break;
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configuration
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateProfileDuration(state);
  ValidateBackendCount(state);
  ValidateOperationCount(state);
  ValidateUpdateRatio(state);
  ValidateZipfTheta(state);
}


void WriteOutput() {
  std::ofstream out("outputfile.summary");

  int total_profile_memory = 0;
  for (auto &entry : state.profile_memory) {
    total_profile_memory += entry;
  }

  LOG_INFO("----------------------------------------------------------");
//  out << ("scale_factor,backend_count,operation_count,update_ratio,zipf_theta::throughput,abort_rate,scan_latency") << "\n";
//  LOG_INFO("%d %d %d %lf %lf :: %lf %lf %lf  %d",
//           state.scale_factor,
//           state.backend_count,
//           state.operation_count,
//           state.update_ratio,
//           state.zipf_theta,
////           state.warmup_throughput,
//           state.throughput,
//           state.abort_rate,
//           state.scan_latency);
////           total_profile_memory);
//
//  out << state.scale_factor << " ";
//  out << state.backend_count << " ";
//  out << state.operation_count << " ";
//  out << state.update_ratio << " ";
//  out << state.zipf_theta << " ";
////  out << state.warmup_throughput << " ";
//  out << state.throughput << " ";
//  out << state.abort_rate << " ";
//  out << state.scan_latency << "\n";
//  out << total_profile_memory << "\n";

    LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
    LOG_INFO("%s : %d", "backend_count", state.backend_count);
    LOG_INFO("%s : %d", "operation_count", state.operation_count);
    LOG_INFO("%s : %lf", "update_ratio", state.update_ratio);
    LOG_INFO("%s : %lf", "zipf_theta", state.zipf_theta);
    LOG_INFO("%s : %lf", "throughput(ops/s)", state.throughput);
    LOG_INFO("%s : %lf", "abort_rate", state.abort_rate);
    LOG_INFO("%s : %lf", "scan_latency(ms)", state.scan_latency);

  for (size_t round_id = 0; round_id < state.profile_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.profile_duration * round_id << " - " << std::setw(3)
        << std::left << state.profile_duration * (round_id + 1)
        << " s]: " << state.profile_throughput[round_id] << " "
        << state.profile_abort_rate[round_id] << " "
        << state.profile_memory[round_id] << "\n";
  }
  out.flush();
  out.close();
}

}
}
}
