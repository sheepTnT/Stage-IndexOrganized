/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef MVSTORE_ASSORTED_RAW_ATOMICS_H_
#define MVSTORE_ASSORTED_RAW_ATOMICS_H_

#include <stdint.h>

/**
 * @file foedus/assorted/raw_atomics.hpp
 * @ingroup ASSORTED
 * @brief Raw atomic operations that work for both C++11 and non-C++11 code.
 * @details
 * std::atomic provides atomic CAS etc, but it requires the data to be std::atomic<T>, rather than T
 * itself like what gcc's builtin function provides. This is problemetic when we have to store
 * T rather than std::atomic<T>. Also, we want to avoid C++11 in public headers.
 *
 * The methods in this header fill the gap.
 * This header defines, not just declares [1], the methods so that they are inlined.
 * We simply use gcc/clang's builtin. clang's gcc compatibility is so good that we don't need ifdef.
 *
 * [1] Except raw_atomic_compare_exchange_strong_uint128(). It's defined in cpp.
 *
 * @see foedus/assorted/atomic_fences.hpp
 */
namespace mvstore {
namespace assorted {

/**
 * @brief Atomic CAS.
 * @tparam T integer type
 * @ingroup ASSORTED
 */
template <typename T>
inline bool raw_atomic_compare_exchange_strong(T* target, T* expected, T desired) {
  // Use newer builtin instead of __sync_val_compare_and_swap
  return ::__atomic_compare_exchange_n(
    target,
    expected,
    desired,
    false,
    __ATOMIC_SEQ_CST,
    __ATOMIC_SEQ_CST);
  // T expected_val = *expected;
  // T old_val = ::__sync_val_compare_and_swap(target, expected_val, desired);
  // if (old_val == expected_val) {
  //   return true;
  // } else {
  //   *expected = old_val;
  //   return false;
  // }
}

/**
 * @brief Weak version of raw_atomic_compare_exchange_strong().
 * @ingroup ASSORTED
 * @copydetails raw_atomic_compare_exchange_strong()
 */
template <typename T>
inline bool raw_atomic_compare_exchange_weak(T* target, T* expected, T desired) {
  return ::__atomic_compare_exchange_n(
    target,
    expected,
    desired,
    true,  // weak
    __ATOMIC_SEQ_CST,
    __ATOMIC_SEQ_CST);
}
/**
 * @brief Atomic 128-bit CAS, which is not in the standard yet.
 * @param[in,out] ptr Points to 128-bit data. \b MUST \b BE \b 128-bit \b ALIGNED.
 * @param[in] old_value Points to 128-bit data. If ptr holds this value, we swap.
 * Unlike std::atomic_compare_exchange_strong, this arg is const.
 * @param[in] new_value Points to 128-bit data. We change the ptr to hold this value.
 * @return Whether the swap happened
 * @ingroup ASSORTED
 * @details
 * We shouldn't rely on it too much as double-word CAS is not provided in older CPU.
 * Once the C++ standard employs it, this method should go away. I will be graybeard by then, tho.
 * \attention You need to give "-mcx16" to GCC to use its builtin 128bit CAS.
 * Otherwise, __GCC_HAVE_SYNC_COMPARE_AND_SWAP_16 is not set and we have to resort to x86 assembly.
 * Check out "gcc -dM -E - < /dev/null".
 */
bool raw_atomic_compare_exchange_strong_uint128(
        uint64_t *ptr,
        const uint64_t *old_value,
        const uint64_t *new_value);

/**
* @brief Weak version of raw_atomic_compare_exchange_strong_uint128().
* @ingroup ASSORTED
*/
bool raw_atomic_compare_exchange_weak_uint128(
        uint64_t *ptr,
        const uint64_t *old_value,
        const uint64_t *new_value) ;
/**
 * @brief Atomic 128-bit CAS, which is not in the standard yet.
 * @param[in,out] ptr Points to 128-bit data. \b MUST \b BE \b 128-bit \b ALIGNED.
 * @param[in] old_value Points to 128-bit data. If ptr holds this value, we swap.
 * Unlike std::atomic_compare_exchange_strong, this arg is const.
 * @param[in] new_value Points to 128-bit data. We change the ptr to hold this value.
 * @return Whether the swap happened
 * @ingroup ASSORTED
 * @details
 * We shouldn't rely on it too much as double-word CAS is not provided in older CPU.
 * Once the C++ standard employs it, this method should go away. I will be graybeard by then, tho.
 * \attention You need to give "-mcx16" to GCC to use its builtin 128bit CAS.
 * Otherwise, __GCC_HAVE_SYNC_COMPARE_AND_SWAP_16 is not set and we have to resort to x86 assembly.
 * Check out "gcc -dM -E - < /dev/null".
 */
//bool raw_atomic_compare_exchange_strong_uint128(
//            unsigned long ptr,
//            const uint64_t *old_value,
//            const uint64_t *new_value) {
//    bool ret;
//#if defined(__GNUC__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
//    // gcc-x86 (-mcx16), then simply use __sync_bool_compare_and_swap.
//__uint128_t* ptr_casted = reinterpret_cast<__uint128_t*>(ptr);
//__uint128_t old_casted = *reinterpret_cast<const __uint128_t*>(old_value);
//__uint128_t new_casted = *reinterpret_cast<const __uint128_t*>(new_value);
//ret = ::__sync_bool_compare_and_swap(ptr_casted, old_casted, new_casted);
//#elif defined(__GNUC__) && defined(__aarch64__)
//    // gcc-AArch64 doesn't allow -mcx16. But, it supports __atomic_compare_exchange_16 with
//// libatomic.so. We need to link to it in that case.
//__uint128_t* ptr_casted = reinterpret_cast<__uint128_t*>(ptr);
//__uint128_t old_casted = *reinterpret_cast<const __uint128_t*>(old_value);
//__uint128_t new_casted = *reinterpret_cast<const __uint128_t*>(new_value);
//ret = ::__atomic_compare_exchange_16(
//ptr_casted,
//&old_casted,
//new_casted,
//false,              // strong CAS
//__ATOMIC_ACQ_REL,   // to make it atomic, of course acq_rel
//__ATOMIC_ACQUIRE);  // matters only when it fails. acquire is enough.
//#else  // everything else
//    // oh well, then resort to assembly, assuming x86. clang on ARMv8? oh please...
//    // see: linux/arch/x86/include/asm/cmpxchg_64.h
//    uint64_t junk;
//    asm volatile("lock; cmpxchg16b %2;setz %1"
////    : "=d"(junk), "=a"(ret), "+m" (*ptr)
//    : "=d"(junk), "=a"(ret), "+m" (ptr)
//    : "b"(new_value[0]), "c"(new_value[1]), "a"(old_value[0]), "d"(old_value[1]));
//    // Note on roll-our-own non-gcc ARMv8 cas16. It's doable, but...
//    // ARMv8 does have 128bit atomic instructions, called "pair" operations, such as ldaxp and stxp.
//    // There is actually a library that uses it:
//    // https://github.com/ivmai/libatomic_ops/blob/master/src/atomic_ops/sysdeps/gcc/aarch64.h
//    // (but this is GPL. Don't open the URL unless you are ready for it.)
//    // As of now (May 2014), GCC can't handle them, nor provide __uint128_t in ARMv8.
//    // I think it's coming, however. I'm waiting for it... if it's not coming, let's do ourselves.
//#endif
//    return ret;
//}

/**
 * @brief Weak version of raw_atomic_compare_exchange_strong_uint128().
 * @ingroup ASSORTED
 */
//inline bool raw_atomic_compare_exchange_weak_uint128(
//  uint64_t *ptr,
//  const uint64_t *old_value,
//  const uint64_t *new_value) {
//  if (ptr[0] != old_value[0] || ptr[1] != old_value[1]) {
//    return false;  // this comparison is fast but not atomic, thus 'weak'
//  } else {
//    return raw_atomic_compare_exchange_strong_uint128(reinterpret_cast<uint64_t>(ptr),
//                                                      old_value, new_value);
//  }
//}

/**
 * @brief Atomic Swap for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @param[in,out] target Points to the data to be swapped.
 * @param[in] desired This value will be installed.
 * @return returns the old value.
 * @ingroup ASSORTED
 * @details
 * This is a non-conditional swap, which always succeeds.
 */
template <typename T>
inline T raw_atomic_exchange(T* target, T desired) {
  return ::__atomic_exchange_n(target, desired, __ATOMIC_SEQ_CST);
  // Note: We must NOT use __sync_lock_test_and_set, which is only acquire-barrier for some
  // reason. We instead use GCC/Clang's __atomic_exchange() builtin.
  // return ::__sync_lock_test_and_set(target, desired);
  // see https://gcc.gnu.org/onlinedocs/gcc-4.4.3/gcc/Atomic-Builtins.html
  // and https://bugzilla.mozilla.org/show_bug.cgi?id=873799

  // BTW, __atomic_exchange_n/__ATOMIC_SEQ_CST demands a C++11-capable version of gcc/clang,
  // but FOEDUS anyway relies on C++11. It just allows the linked program to be
  // compiled without std=c++11. So, nothing lost.
}

/**
 * @brief Atomic fetch-add for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return the previous value.
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_fetch_add(T* target, T addendum) {
  return ::__atomic_fetch_add(target, addendum, __ATOMIC_SEQ_CST);
  // Just to align with above, use __atomic_fetch_add rather than __sync_fetch_and_add.
  // It's equivalent.
  // return ::__sync_fetch_and_add(target, addendum);
}

/**
 * @brief Atomic fetch-bitwise-and for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return the previous value.
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_fetch_and_bitwise_and(T* target, T operand) {
  return ::__atomic_fetch_and(target, operand, __ATOMIC_SEQ_CST);
}
/**
 * @brief Atomic fetch-bitwise-or for raw primitive types rather than std::atomic<T>.
 * @tparam T integer type
 * @return the previous value.
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_fetch_and_bitwise_or(T* target, T operand) {
  return ::__atomic_fetch_or(target, operand, __ATOMIC_SEQ_CST);
}
/**
 * @brief Atomic fetch-bitwise-xor for raw primitive types rather than std::atomic<T>
 * @tparam T integer type
 * @return the previous value.
 * @ingroup ASSORTED
 */
template <typename T>
inline T raw_atomic_fetch_and_bitwise_xor(T* target, T operand) {
  return ::__atomic_fetch_xor(target, operand, __ATOMIC_SEQ_CST);
}
}
}

#endif
