#pragma once

#include <condition_variable>
#include <mutex>

class LockTable {
public:
    LockTable() = default;

    bool slock() {
        std::unique_lock<std::mutex> guard(latch);
        if (has_xlock()) {
            return false;
        }
        locks++;
        return true;
    }

    bool xlock() {
        std::unique_lock<std::mutex> guard(latch);
        if (has_other_slocks()) {
            return false;
        }
        locks = -1;
        return true;
    }

    void unlock() {
        std::unique_lock<std::mutex> guard(latch);
        if (locks > 1) {
            locks--;
        } else {
            locks = 0;
        }
    }

private:
    std::mutex latch;
    std::condition_variable cond_var;
    int locks = 0;

    bool has_xlock() { return locks < 0; }

    bool has_other_slocks() { return locks > 1; }
};

class ConcurrencyManager {
public:
    bool slock() {
        if (lock_type == 'S' || lock_type == 'X') {
            return true;
        } else if (lock_type == 0 && lock_table.slock()) {
            lock_type = 'S';
            return true;
        } else {
            return false;
        }
    }

    bool xlock() {
        if (lock_type == 'X') {
            return true;
        } else if (slock() && lock_table.xlock()) {
            lock_type = 'X';
            return true;
        } else {
            return false;
        }
    }
    void release() {
        lock_table.unlock();
        lock_type = 0;
    }

private:
    static LockTable lock_table;
    char lock_type = 0;
};

inline LockTable ConcurrencyManager::lock_table;