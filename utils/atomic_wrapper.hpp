#pragma once

#include <atomic>

template <typename T>
T load(T& ptr) {
    return __atomic_load_n(&ptr, __ATOMIC_RELAXED);
}

template <typename T>
T load_acquire(T& ptr) {
    return __atomic_load_n(&ptr, __ATOMIC_ACQUIRE);
}

template <typename T, typename T2>
void store(T& ptr, T2 val) {
    __atomic_store_n(&ptr, (T)val, __ATOMIC_RELAXED);
}

template <typename T, typename T2>
void store_release(T& ptr, T2 val) {
    __atomic_store_n(&ptr, (T)val, __ATOMIC_RELEASE);
}

template <typename T, typename T2>
bool compare_exchange(T& m, T& before, T2 after) {
    return __atomic_compare_exchange_n(
        &m, &before, (T)after, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

template <typename Int1, typename Int2>
Int1 fetch_add(Int1& m, Int2 v, int memorder = __ATOMIC_ACQ_REL) {
    return __atomic_fetch_add(&m, v, memorder);
}

template <typename T, typename T2>
T exchange(T& m, T2 after) {
    return __atomic_exchange_n(&m, after, __ATOMIC_ACQ_REL);
}