#pragma once

#include <condition_variable>
#include <mutex>

class LockTable {
public:
    LockTable();
    bool slock();
    bool xlock();
    void unlock();

private:
    std::mutex latch;
    std::condition_variable cond_var;
    int locks = 0;

    bool has_xlock();
    bool has_other_slocks();
};

class ConcurrencyManager {
public:
    bool slock();
    bool xlock();
    void release();

private:
    static LockTable lock_table;
    char lock_type = 0;
    bool has_xlock();
};