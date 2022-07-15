# What is Stage ?
An index organized data layout scheme for in-memory database systems.

# Implementations
This repo contains our open-source implementations:
- An index_organized layout designed for in-memory DBMSs to store data.
- A staged approach to maintain the lifetimes of the record versions.
- An active version tree, which is a modified version of the [BzTree](https://github.com/sfu-dis/bztree) for volatile memory(DRAM).
- Optimistic concurrency control policy designed for hot record versions.
- YCSB/TPCC/HYBRID workload benchmarks refered to other researches, e.g.[spitfire](https://github.com/zxjcarrot/spitfire), [ermia](https://github.com/sfu-dis/ermia).

# Building

## Environment
The code has been tested on Ubuntu 20.04 with gcc version 9.4.0 and cmake3.17.

## Dependencies
```bash
sudo apt-get install libgoogle-perftools-dev libtbb-dev
......
```

## Build
```bash
sudo git clone https://github.com/gitzhqian/Stage.git
sudo cd stage
sudo mkdir build
sudo cd build
sudo cmake -DCMAKE_BUILD_TYPE=Release .. 
sudo make -j4 ycsb (sudo make -j4 tpcc)
```

# Running
Command line options available for the `ycsb` benchmark are as follows:
```bash
cd build
sudo ./ycsb -h
Command line options : ycsb <options> 
   -h --help              :  print help message 
   -k --scale_factor      :  # of K tuples 
   -d --duration          :  execution duration 
   -p --profile_duration  :  profile duration 
   -b --backend_count     :  # of backends 
   -o --operation_count   :  # of operations 
   -u --update_ratio      :  fraction of updates 
   -z --zipf_theta        :  theta to control skewness 
   -l --loader_count      :  # of loaders 
   -s --shuffle_keys      :  whether to shuffle keys at startup (Default: fasle)
   -r --random_mode       :  whether key is random distribution access
   -m --string_mode       :  whether key is string
   -n --scan_mode         :  whether scan only workload
```
Command line options available for the `tpcc` and `tpcc-hybrid` benchmark are as follows:
```bash
cd build
sudo ./tpcc -h
Command line options : ycsb <options> 
   -h --help              :  print help message 
   -d --duration          :  execution duration 
   -p --profile_duration  :  profile duration 
   -b --backend_count     :  # of backends 
   -e --exp_backoff       :  enable exponential backoff 
   -l --loader_count      :  # of loaders 
   -w --warehouse_count   :  # warehouse counts 
   -s --hybrid_rate       :  the ratio of query2 when mixing new-order and query2 
   -o --new_order_rate    :  the ratio of new-order when mixing new-order and stock-level 
   -r --stock_level_rate  :  the ratio of stock-level when mixing new-order and stock-level 
```
## Run it with YCSB 
```bash
sudo ./ycsb -k 10000 -d 20 -p 4 -b 4 -o 10 -u 0.2 -z 0.2 -l 4
```
The output looks like this:
```
 scale_factor : 10000
 backend_count : 4
 operation_count : 10
 update_ratio : 0.200000
 zipf_theta : 0.200000
 throughput(ops/s) : 1279115.500000
 abort_rate : 0.000000
 scan_latency(ms) : 0.000000

```
Our testing results:
<div align=left background=#c0c0c0><img src="https://user-images.githubusercontent.com/12605803/179177048-00bcebf9-d5d0-45e3-8a27-5c6688321948.png", width="800", height="300" /></div>


## Run it with TPCC/HYBRID
```bash
sudo ./tpcc -d 20 -b 6 -l 5 -w 30 -s 0 -o 1 -r 0
```
The output looks like this:
```
overall throughputs:
	total transactions: 324887
	total commit transactions: 319927
	total abort transactions: 4960
	q2 total commit transactions: 0
	q2 total abort transactions: 0
	new total commit transactions: 319927
	new total abort transactions: 4960
	stock total commit transactions: 0
	stock total abort transactions: 0

 ----------------------------------------------------------
 warehouse_count : 30
 backend_count : 6
 throughput(txns/s) : 15996.350000
 abort_rate : 0.015267
 ch_q2* latency(ms) : 0.000000

```
Our testing results with 20warehouse:
<div align=left><img src="https://user-images.githubusercontent.com/12605803/179178001-ed048d29-3fc3-4e7b-a247-6d510422b92b.png", width ="800", height="300" /></div>


# For Logging

## Hardware Setup
Stage implements redo logging by placing the log files on NVM. Stage places the log records on NVM-backed filesystem using `mmap`. Therefore, you need to configure the Optane DIMM in `app-direct` mode and mount an `fsdax` mode file system on top of the device. Check out this [tutorial](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html/storage_administration_guide/configuring-persistent-memory-for-file-system-direct-access-dax) on how to configure the device and the file system. Once the file system is configured and mounted, create one directory for storing NVM log files. Now, we set the default path by `/mnt/pmem5/heapfile`. Make sure you have the permission to read and write to the file.

## Intel PCM
Stage relies on [Processor Counter Monitor](https://github.com/opcm/pcm) to collect hardware metrics.
```bash
$ modprobe msr
```
## Setting
You shold modify the file `include\common\constants.h`
```bash
nvm_emulate = false
enable_pcm = true
```
The output looks like this:
```
PCM Metrics:
	L2 HitRatio: 0.864324
	L3 HitRatio: 0.928882
	L2 misses: 299818399
	L3 misses: 18261373
	IPC: 0.984794
	Cycles: 299084598522
	instructions: 294536833896
	DRAM Reads (bytes): 29125052800
	DRAM Writes (bytes): 25250544960
	NVM Reads (bytes): 0
	NVM Writes (bytes): 10667072

```
Our testing results with 40warehouse:
<div align=left><img src="https://user-images.githubusercontent.com/12605803/179178062-d5203ee9-69b4-4ab1-b22a-8bd49c931b5f.png" width="600", height="300" /></div>


# Caveats
- The main memory recovery(logging, checkpoint and restart) is not fully implemented.
- The memory reclaim design is not fully implemented.
