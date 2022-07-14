//
// Created by zhangqian on 2022/2/10.
//
#pragma once

#include "../benchmark_common.h"
#include "ycsb_configuration.h"
#include "../../include/vstore/version_store.h"

namespace mvstore {

namespace benchmark {
namespace ycsb {

extern configuration state;
extern thread_local size_t num_rw_ops;
extern std::atomic<size_t> min_rowid;

void RunWorkload(VersionStore *version,  std::vector<uint32_t>&);

void RunWarmupWorkload(VersionStore *version,  std::vector<uint32_t>&);

bool RunInsert(VersionStore *version_store, const size_t thread_id);

bool RunMixed(VersionStore *version, const size_t thread_id, ZipfDistribution &zipf,
              FastRandom &rng,  std::vector<uint32_t>&);
bool RunMixedString(VersionStore *version, const size_t thread_id, ZipfDistribution &zipf,
              FastRandom &rng,  std::vector<uint32_t>&);

bool RunScan(VersionStore *version, const size_t thread_id, ZipfDistribution &zipf,
              FastRandom &rng,  std::vector<uint32_t>&);

void PinToCore(size_t core);

}
}
}
