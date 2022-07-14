//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// macros.h
//
// Identification: src/include/common/macros.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <assert.h>
#include <stdexcept>

namespace mvstore {
//===--------------------------------------------------------------------===//
// branch predictor hints
//===--------------------------------------------------------------------===//

#define likely_branch(x) __builtin_expect(!!(x), 1)
#define unlikely_branch(x) __builtin_expect(!!(x), 0)

//===--------------------------------------------------------------------===//
// attributes
//===--------------------------------------------------------------------===//

#define NEVER_INLINE __attribute__((noinline))

#ifdef NDEBUG
#define ALWAYS_INLINE __attribute__((always_inline))
#else
#define ALWAYS_INLINE
#endif

#ifdef __clang__
#define NO_CLONE
#else
#define NO_CLONE __attribute__((noclone))
#endif

#define UNUSED_ATTRIBUTE __attribute__((unused))
#define PACKED __attribute__((packed))

//===--------------------------------------------------------------------===//
// memfuncs
//===--------------------------------------------------------------------===//

#define USE_BUILTIN_MEMFUNCS

#ifdef USE_BUILTIN_MEMFUNCS
#define VSTORE_MEMCPY __builtin_memcpy
#define VSTORE_MEMSET __builtin_memset
#else
#define PELOTON_MEMCPY memcpy
#define PELOTON_MEMSET memset
#endif

//===--------------------------------------------------------------------===//
// packed
//===--------------------------------------------------------------------===//

#define __XCONCAT2(a, b) a##b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT                       \
  char __XCONCAT(__padout, __COUNTER__)[0] \
      __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

//===--------------------------------------------------------------------===//
// invariants
//===--------------------------------------------------------------------===//

#ifdef CHECK_INVARIANTS
#define INVARIANT(expr) PELOTON_ASSERT(expr)
#else
#define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

//===--------------------------------------------------------------------===//
// unimplemented
//===--------------------------------------------------------------------===//

// throw exception after the assert(), so that GCC knows
// we'll never return
#define PELOTON_UNIMPLEMENTED(what)        \
  do {                                \
    PELOTON_ASSERT(false);                 \
    throw ::std::runtime_error(what); \
  } while (0)
//
////===--------------------------------------------------------------------===//
//// ALWAYS_ASSERT
////===--------------------------------------------------------------------===//
//
//#ifdef NDEBUG
//#define PELOTON_ASSERT(expr) ((void)0)
//#else
//#define PELOTON_ASSERT(expr) assert((expr))
//#endif /* NDEBUG */
//
//#ifdef CHECK_INVARIANTS
//#define INVARIANT(expr) PELOTON_ASSERT(expr)
//#else
//#define INVARIANT(expr) ((void)0)
//#endif /* CHECK_INVARIANTS */

//===--------------------------------------------------------------------===//
// ALWAYS_ASSERT
//===--------------------------------------------------------------------===//

#ifdef NDEBUG
#define PELOTON_ASSERT(expr, message) ((void)0)
#else
/*
 * On assert failure, most existing implementations of C++ will print out the condition.
 * By ANDing the truthy not-null message and our initial expression together, we get
 * asserts-with-messages without needing to bring in iostream or logging.
 */
#define PELOTON_ASSERT(expr, message) assert((expr) && (message))
#endif /* NDEBUG */

//===--------------------------------------------------------------------===//
// Compiler version checks
//===--------------------------------------------------------------------===//

#if __GNUC__ > 6 || (__GNUC__ == 6 && __GNUC_MINOR__ >= 0)
#define GCC_AT_LEAST_6 1
#else
#define GCC_AT_LEAST_6 0
#endif

#if __GNUC__ > 5 || (__GNUC__ == 5 && __GNUC_MINOR__ >= 1)
#define GCC_AT_LEAST_51 1
#else
#define GCC_AT_LEAST_51 0
#endif

// g++-5.0 does not support overflow builtins
#if GCC_AT_LEAST_51
#define GCC_OVERFLOW_BUILTINS_DEFINED 1
#else
#define GCC_OVERFLOW_BUILTINS_DEFINED 0
#endif

//===--------------------------------------------------------------------===//
// Port to OSX
//===---------------------------
#ifdef __APPLE__
        #define off64_t off_t
#define MAP_ANONYMOUS MAP_ANON
#endif

//===--------------------------------------------------------------------===//
// utils
//===--------------------------------------------------------------------===//

#define ARRAY_NELEMS(a) (sizeof(a) / sizeof((a)[0]))

//===----------------------------------------------------------------------===//
// Handy macros to hide move/copy class constructors
//===----------------------------------------------------------------------===//

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)     \
  cname(const cname &) = delete; \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname) \
  cname(cname &&) = delete;  \
  cname &operator=(cname &&) = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

//===----------------------------------------------------------------------===//
// LLVM version checking macros
//===----------------------------------------------------------------------===//

#define LLVM_VERSION_GE(major, minor) \
  (LLVM_VERSION_MAJOR > (major) ||    \
   (LLVM_VERSION_MAJOR == (major) && LLVM_VERSION_MINOR >= (minor)))

#define LLVM_VERSION_EQ(major, minor) \
  (LLVM_VERSION_MAJOR == (major) && LLVM_VERSION_MINOR == (minor))

//===----------------------------------------------------------------------===//
// switch statements
//===----------------------------------------------------------------------===//

#if defined __clang__
#define PELOTON_FALLTHROUGH [[clang::fallthrough]]
#elif defined __GNUC__ && __GNUC__ >= 7
#define PELOTON_FALLTHROUGH [[fallthrough]]
#else
#define PELOTON_FALLTHROUGH
#endif


/**
 * Used to mark a class as only obtainable from reinterpreting a chunk of memory initialized as byte array or a buffer.
 *
 * Such classes typically have variable-length objects, that the c++ compiler cannot initialize or lay out correctly as
 * local variables. Thus we will have to keep track of the memory space and take care to only refer to these objects
 * with pointers.
 *
 * Typically classes marked with these will expose static factory methods that calculate the size of an object in memory
 * given some parameters and an Initialize method to construct a valid object from pointer to a chunk of memory
 */
#define MEM_REINTERPRETATION_ONLY(cname) \
  cname() = delete;                      \
  DISALLOW_COPY_AND_MOVE(cname)          \
  ~cname() = delete;

}  // namespace