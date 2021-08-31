#pragma once

#include <cassert>
#include <unordered_map>

#include "utils/atomic_wrapper.hpp"
#include "utils/logger.hpp"

class RWLock {
public:
    RWLock()
        : cnt(0) {}

    void initialize() { cnt = 0; }

    void lock_shared() {
        int64_t expected;
        while (true) {
            expected = load_acquire(cnt);
            if (expected >= 0 && compare_exchange(cnt, expected, expected + 1)) {
                return;
            }
        }
    }

    void lock_upgrade() {
        // assume that slock is taken before exclusive lock
        int64_t expected;
        while (true) {
            expected = load_acquire(cnt);
            if (expected == 1 && compare_exchange(cnt, expected, -1)) {
                return;
            }
        }
    }

    void lock() {
        int64_t expected;
        while (true) {
            expected = load_acquire(cnt);
            if (expected == 0 && compare_exchange(cnt, expected, -1)) {
                return;
            }
        }
    }

    bool try_lock_shared() {
        int64_t expected = load_acquire(cnt);
        while (expected >= 0) {
            if (compare_exchange(cnt, expected, expected + 1)) {
                return true;
            }
        }
        return false;
    }

    bool try_lock_upgrade() {
        // assume that slock is taken before lock upgrade
        int64_t expected = load_acquire(cnt);
        if (expected == 1 && compare_exchange(cnt, expected, -1)) {
            return true;
        } else {
            return false;
        }
    }

    bool try_lock() {
        int64_t expected = load_acquire(cnt);
        if (expected == 0 && compare_exchange(cnt, expected, -1)) {
            return true;
        } else {
            return false;
        }
    }

    void unlock_shared() { fetch_add(cnt, -1); }

    void unlock() { fetch_add(cnt, 1); }

private:
    int64_t cnt = 0;
};
