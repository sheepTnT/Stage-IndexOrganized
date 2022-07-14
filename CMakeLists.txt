cmake_minimum_required(VERSION 3.17)

include(ExternalProject)
include(FetchContent)
project(Mvstore)

##set(CMAKE_CXX_STANDARD 14)
##
##macro(use_cxx14)
##    if (CMAKE_VERSION VERSION_LESS "3.1")
##        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
##            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14")
##        endif ()
##    else ()
##        set (CMAKE_CXX_STANDARD 14)
##    endif ()
##endmacro(use_cxx14)
##
##use_cxx14()
##
###set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -L$PAPI_DIR/lib -I$PAPI_DIR/include")
###set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -DPCM_USE_PERF")
##
##if ("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "aarch64")
##    # gcc-aarch64 doesn't support -mcx16. We use gcc's libatomic instead.
##    # You must make sure you have libatomic.so.
##    if (NOT GCCLIBATOMIC_FOUND)
##        message(SEND_ERROR "ERROR: FOEDUS on AArch64 needs gcc's libatomic.so. We couldn't find it.")
##    endif ()
##else ()
##    # Enable GCC's built-in 128bit CAS via -mcx16. We don't need libatomic.so on x86.
##    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")
##endif ()
##include(CheckCXXCompilerFlag)
##CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
##if(COMPILER_SUPPORTS_MARCH_NATIVE)
##    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
##endif()
##
###find_library(NVM_MALLOC_LIBRARY nvmmalloc HINTS misc/nvm-malloc/)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()


find_package(OpenMP REQUIRED)

if(CMAKE_C_COMPILER_ID MATCHES "Clang")
    set(OpenMP_C "${CMAKE_C_COMPILER}")
    set(OpenMP_C_FLAGS "-fopenmp=libomp")
    set(OpenMP_C_LIB_NAMES "libomp" "libgomp" "libiomp5")
    set(OpenMP_libomp_LIBRARY ${OpenMP_C_LIB_NAMES})
    set(OpenMP_libgomp_LIBRARY ${OpenMP_C_LIB_NAMES})
    set(OpenMP_libiomp5_LIBRARY ${OpenMP_C_LIB_NAMES})
endif()
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(OpenMP_CXX "${CMAKE_CXX_COMPILER}")
    set(OpenMP_CXX_FLAGS "-fopenmp=libomp")
    set(OpenMP_CXX_LIB_NAMES "libomp" "libgomp" "libiomp5")
    set(OpenMP_libomp_LIBRARY ${OpenMP_CXX_LIB_NAMES})
    set(OpenMP_libgomp_LIBRARY ${OpenMP_CXX_LIB_NAMES})
    set(OpenMP_libiomp5_LIBRARY ${OpenMP_CXX_LIB_NAMES})
endif()


option(GOOGLE_FRAMEWORK "Use glog, gflags and gtest" ON)
if(${GOOGLE_FRAMEWORK})
    add_definitions(-DGOOGLE_FRAMEWORK)
    message(STATUS "GOOGLE_FRAMEWORK is defined, will use glog, gflags and gtest")
else()
    message(STATUS "GOOGLE_FRAMEWORK is not defined, will not use glog, gflags and gtest")
endif()

option(BUILD_TESTS "Build test cases using gflags, glog and gtest" ON)
if(${BUILD_TESTS})
    if(NOT ${GOOGLE_FRAMEWORK})
        message(FATAL_ERROR "BUILD_TESTS defined but GOOGLE_FRAMEWORK not defined")
    endif()
    add_definitions(-DBUILD_TESTS)
    message(STATUS "BUILD_TESTS: defined")
else()
    message(STATUS "BUILD_TESTS: not defined")
endif()


if(${GOOGLE_FRAMEWORK})
########################## glog #####################
# Google Log
    FetchContent_Declare(
            glog
            GIT_REPOSITORY https://github.com/google/glog.git
            GIT_TAG v0.4.0
    )
    FetchContent_GetProperties(glog)
    if (NOT glog_POPULATED)
        FetchContent_Populate(glog)
        set(WITH_GFLAGS OFF CACHE BOOL "we don't use gflags")
        add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
    endif ()


########################## gtest #####################
# Google Test
    FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG release-1.8.1
    )
    FetchContent_GetProperties(googletest)
    if (NOT googletest_POPULATED)
        FetchContent_Populate(googletest)
        add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
        include_directories(${googletest_SOURCE_DIR}/GTestExternal/googletest/include)
    endif ()
include(GoogleTest)

endif()
##
######################################################
if(${GOOGLE_FRAMEWORK})
    list(APPEND MVSTORE_LINK_LIBS
            glog::glog
            gtest_main
            )
endif()

######################## Intel PCM ########################
add_custom_command(OUTPUT libPCM.a
        COMMAND make lib
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/pcm)

add_custom_target(pcm DEPENDS libPCM.a)
###########################################################

# Find PAPI
#FIND_LIBRARY(PAPI_LIBRARIES NAMES libpapi.a papi)
#FIND_PATH(PAPI_INCLUDE_DIRS NAMES papi.h)
#INCLUDE(FindPackageHandleStandardArgs)
#FIND_PACKAGE_HANDLE_STANDARD_ARGS(PAPI DEFAULT_MSG PAPI_LIBRARIES PAPI_INCLUDE_DIRS)
#IF(PAPI_FOUND)
#    ADD_DEFINITIONS(-DUSE_PAPI)
#ENDIF(PAPI_FOUND)

#include_directories(include)
#include_directories(misc)
#include_directories(pcm)

include_directories("${PROJECT_SOURCE_DIR}/include"
        "${PROJECT_SOURCE_DIR}/pcm" "${PROJECT_SOURCE_DIR}/benchmark" "${PROJECT_SOURCE_DIR}/misc")
#add_subdirectory(src)

#file(GLOB_RECURSE MV_STORE_SRC_FILES src/common/*.cpp src/concurrency/*.cpp src/vstore/*.cpp misc/murmur/*.cpp pcm/cpucounters.cpp
#        pcm/msr.cpp pcm/pci.cpp pcm/pcm-memory.cpp pcm/client_bw.cpp)
#file(GLOB_RECURSE MV_STORE_SRC_FILES src/common/*.cpp src/concurrency/*.cpp src/vstore/*.cpp misc/murmur/*.cpp )
#file(GLOB_RECURSE YCSB_SRC_FILES  benchmark/ycsb/*.h benchmark/ycsb/*.cpp pcm/cpucounters.cpp pcm/msr.cpp pcm/pci.cpp pcm/client_bw.cpp pcm/mmio.cpp pcm/utils.cpp)
#file(GLOB_RECURSE TPCC_SRC_FILES  benchmark/tpcc/*.h benchmark/tpcc/*.cpp )

set(MV_STORE_SRC_FILES src/common/catalog.cpp src/common/coding.cpp src/common/concurrent_queue.cpp src/common/crc32c.cpp
        src/common/cuckoo_map.cpp src/common/env.cpp src/common/ephemeral_pool.cpp src/common/ephemeral_pool.cpp src/common/manager.cpp
        src/common/random.cpp src/common/raw_atomics.cpp src/common/status.cpp src/concurrency/transaction_context.cpp
        src/concurrency/transaction_manager.cpp src/vstore/b_tree.cpp src/vstore/log_io.cpp src/vstore/logging.cpp
        src/vstore/version_store.cpp misc/murmur/MurmurHash2.cpp)
set(YCSB_SRC_FILES benchmark/ycsb/ycsb.cpp benchmark/ycsb/ycsb_configuration.cpp benchmark/ycsb/ycsb_loader.cpp
        benchmark/ycsb/ycsb_mixed.cpp benchmark/ycsb/ycsb_workload.cpp)
set(TPCC_SRC_FILES benchmark/tpcc/tpcc.cpp benchmark/tpcc/tpcc_configuration.cpp benchmark/tpcc/tpcc_loader.cpp
        benchmark/tpcc/tpcc_new_order.cpp benchmark/tpcc/tpcc_stock_level.cpp benchmark/tpcc/tpcc_delivery.cpp
        benchmark/tpcc/tpcc_order_status.cpp benchmark/tpcc/tpcc_payment.cpp benchmark/tpcc/tpcc_workload.cpp )
set(TEST_BTREE_FILES test/testing_btree.cpp)
set(TEST_EXECUTE_FILES test/testing_transaction_util.h test/testing_transaction_util.cpp test/testing_execute.cpp)
set(TEST_VERSION_STORE_FILES test/testing_version_store.cpp)
#set(TEST_TXN_FILES test/testing_transaction_util.cpp test/testing_execute.cpp)
add_library(Mvstore ${MV_STORE_SRC_FILES})

add_dependencies(Mvstore pcm)
target_link_libraries(Mvstore PRIVATE
        ${PROJECT_SOURCE_DIR}/pcm/libPCM.a
        dl)
#target_link_libraries(Mvstore ${MVSTORE_LINK_LIBS})
#add_executable (btree_unit_test test/testing_btree.cpp ${MV_STORE_SRC_FILES})
#add_executable (execute_unit_test test/testing_transaction_util.h test/testing_transaction_util.cpp test/testing_execute.cpp ${MV_STORE_SRC_FILES})
#add_executable (version_store_unit_test test/testing_version_store.cpp ${MV_STORE_SRC_FILES})
#add_executable (txn_unit_test test/testing_transaction_util.cpp test/testing_execute.cpp ${MV_STORE_SRC_FILES})
#add_executable (multithreadbtree_unit_test test/testing_btree_multithread.cpp ${MV_STORE_SRC_FILES})
#add_executable (log_unit_test test/testing_log_produce_consume.cpp ${MV_STORE_SRC_FILES})
add_executable (ycsb ${YCSB_SRC_FILES})
add_executable (tpcc ${TPCC_SRC_FILES})
add_executable (btree_unit_test ${TEST_BTREE_FILES})
add_executable (execute_unit_test ${TEST_EXECUTE_FILES})
add_executable (version_store_unit_test ${TEST_VERSION_STORE_FILES})
#add_executable (txn_unit_test ${TEST_TXN_FILES})
#add_executable(Mvstore main.cpp)


#target_link_libraries(Mvstore ${MVSTORE_LINK_LIBS} /usr/share/papi)
#target_link_libraries(ycsb Mvstore tbb pthread tcmalloc papi ${MVSTORE_LINK_LIBS})
#target_link_libraries(tpcc Mvstore tbb pthread tcmalloc papi ${MVSTORE_LINK_LIBS})

target_link_libraries(ycsb Mvstore tbb pthread tcmalloc ${MVSTORE_LINK_LIBS})
target_link_libraries(tpcc Mvstore tbb pthread tcmalloc ${MVSTORE_LINK_LIBS})

target_link_libraries(btree_unit_test Mvstore tbb pthread tcmalloc  ${MVSTORE_LINK_LIBS})
target_link_libraries(version_store_unit_test Mvstore tbb pthread tcmalloc ${MVSTORE_LINK_LIBS})
#target_link_libraries(txn_unit_test Mvstore tbb pthread tcmalloc ${MVSTORE_LINK_LIBS})
target_link_libraries(execute_unit_test Mvstore tbb pthread tcmalloc ${MVSTORE_LINK_LIBS})

#target_link_libraries(btree_unit_test Mvstore tbb pthread   ${MVSTORE_LINK_LIBS})
#target_link_libraries(version_store_unit_test Mvstore tbb pthread   ${MVSTORE_LINK_LIBS})
#target_link_libraries(txn_unit_test Mvstore tbb pthread   ${MVSTORE_LINK_LIBS})
#target_link_libraries(execute_unit_test Mvstore tbb pthread   ${MVSTORE_LINK_LIBS})
#target_link_libraries(multithreadbtree_unit_test Mvstore tbb pthread   ${MVSTORE_LINK_LIBS})