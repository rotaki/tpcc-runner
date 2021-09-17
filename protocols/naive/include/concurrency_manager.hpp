#pragma once

#include <condition_variable>
#include <mutex>

#include "benchmarks/tpcc/include/config.hpp"
#include "utils/logger.hpp"

class LockTable {
public:
    void lock() {
        std::unique_lock<std::mutex> guard(latch);
        while (lock_var == 1) cond_var.wait(guard);
        lock_var = 1;
    }

    void unlock() {
        std::unique_lock<std::mutex> guard(latch);
        lock_var = 0;
        cond_var.notify_one();
    }

private:
    int lock_var = 0;
    std::mutex latch;
    std::condition_variable cond_var;
};

class ConcurrencyManager {
public:
    bool lock() {
        if (is_single_thread()) return true;
        if (has_lock) {
            return true;
        } else {
            lock_table.lock();
            has_lock = true;
            return true;
        }
    }

    void release() {
        if (is_single_thread()) return;
        if (has_lock) {
            lock_table.unlock();
        }
    }

private:
    bool is_single_thread() { return get_config().get_num_threads() == 1; }

    bool has_lock = false;
    static LockTable lock_table;
};

inline LockTable ConcurrencyManager::lock_table;
