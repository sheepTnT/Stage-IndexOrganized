//
// Created by zhangqian on 2022/2/10.
//
#pragma once

#include <string>
#include <cstring>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "../../include/common/logger.h"
#include "../../include/common/constants.h"
#include "../../include/common/env.h"

namespace mvstore {
namespace benchmark {
namespace ycsb {

#define COLUMN_COUNT 10

struct YCSBTupleEml {
    char key[16];
    char cols[COLUMN_COUNT][100];
    char *Key()  {
        return key;
    }
    char *GetData() const{
        char *data = new char[100*10];
        memcpy(data, &cols, 100*10);

        return data;
    }

};

struct YCSBTupleInt {
    uint32_t key;
    char cols[COLUMN_COUNT][100];
    uint32_t Key() const {
        return key;
    }
    char *GetData() const{
        char *data = new char[100*10];
        memcpy(data, &cols, 100*10);

        return data;
    }

};


class configuration {
public:
    // size of the table
    int scale_factor;

    // execution duration (in s)
    double duration;

    // profile duration (in s)
    double profile_duration;

    // number of backends
    int backend_count;

    // operation count in a transaction
    int operation_count;

    // update ratio
    double update_ratio;

    // contention level
    double zipf_theta;

    // exponential backoff
    bool exp_backoff;

    // vstore strings
    bool string_mode;
    bool scan_mode;
    bool random_mode;
    double scan_latency=0;
    bool vc_start=false;

    // number of loaders
    int loader_count;

    // throughput
    double warmup_throughput = 0;

    // throughput
    double throughput = 0;

    // abort rate
    double abort_rate = 0;

    std::vector<double> profile_throughput;

    std::vector<double> profile_abort_rate;

    std::vector<int> profile_memory;

    // warmup duration (in s)
    double warmup_duration = 0;

    bool shuffle_keys = false;

    std::string wal_path;

};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateProfileDuration(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateOperationCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateZipfTheta(const configuration &state);

void WriteOutput();

}
}
}
