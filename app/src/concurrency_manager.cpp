#include "concurrency_manager.hpp"

LockTable::LockTable() {}

bool LockTable::slock() {
    std::unique_lock<std::mutex> guard(latch);
    if (has_xlock()) {
        return false;
    }
    locks++;
    return true;
}

bool LockTable::xlock() {
    std::unique_lock<std::mutex> guard(latch);
    if (has_other_slocks()) {
        return false;
    }
    locks = -1;
    return true;
}

void LockTable::unlock() {
    std::unique_lock<std::mutex> guard(latch);
    if (locks > 1) {
        locks--;
    } else {
        locks = 0;
    }
}

bool LockTable::has_xlock() {
    return locks < 0;
}

bool LockTable::has_other_slocks() {
    return locks > 1;
}

bool ConcurrencyManager::slock() {
    if (lock_type == 'S' || lock_type == 'X') {
        return true;
    } else if (lock_type == 0 && lock_table.slock()) {
        lock_type = 'S';
        return true;
    } else {
        return false;
    }
}

bool ConcurrencyManager::xlock() {
    if (lock_type == 'X') {
        return true;
    } else if (slock() && lock_table.xlock()) {
        lock_type = 'X';
        return true;
    } else {
        return false;
    }
}

void ConcurrencyManager::release() {
    lock_table.unlock();
    lock_type = 0;
}
