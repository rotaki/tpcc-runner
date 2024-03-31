#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "protocols/common/schema.hpp"

enum class LockType { READ = 0, WRITE = -1 };

template <typename Lock>
struct LockElement {
    uint64_t key;  // record を識別する．
    Lock* lock;
    LockType type;  // 0 read-mode, -1 write-mode

    LockElement(uint64_t key, Lock* lock, LockType locktype)
        : key(key)
        , lock(lock)
        , type(locktype) {}

    bool operator<(const LockElement& right) const { return this->key < right.key; }

    // Copy constructor
    LockElement(const LockElement& other) {
        key = other.key;
        lock = other.lock;
        type = other.type;
    }

    // move constructor
    LockElement(LockElement&& other) {
        key = other.key;
        lock = other.lock;
        type = other.type;
    }

    LockElement& operator=(LockElement&& other) noexcept {
        if (this != &other) {
            key = other.key;
            lock = other.lock;
            type = other.type;
        }
        return *this;
    }
};


template <typename Lock>
class LockList {
public:
    struct ValueID {
        uint64_t table_id;
        uint64_t key;

        ValueID(uint64_t k1, uint64_t k2)
            : table_id(k1)
            , key(k2) {}

        bool operator==(const ValueID& other) const {
            return table_id == other.table_id && key == other.key;
        }

        bool operator>=(const ValueID& other) const {
            if (table_id > other.table_id) {
                return true;
            }
            if (table_id == other.table_id) {
                return key >= other.key;
            }
            return false;
        }

        bool operator<=(const ValueID& other) const {
            if (table_id < other.table_id) {
                return true;
            }
            if (table_id == other.table_id) {
                return key <= other.key;
            }
            return false;
        }

        bool operator<(const ValueID& other) const {
            if (table_id < other.table_id) {
                return true;
            }
            if (table_id == other.table_id) {
                return key < other.key;
            }
            return false;
        }
    };

    struct ValueComparator {
        bool operator()(const ValueID& a, const ValueID& b) const {
            if (a.table_id != b.table_id) {
                return a.table_id < b.table_id;
            }
            return a.key < b.key;
        }
    };

    LockElement<Lock>* get_lock(ValueID vid) {
        auto it = lock_list.find(vid);
        if (it == lock_list.end()) {
            return nullptr;
        }
        return &(it->second);
    }

    size_t size() { return lock_list.size(); }

    typename std::map<ValueID, LockElement<Lock>, ValueComparator>::reverse_iterator back() {
        return lock_list.rbegin();
    }

    void clear() { lock_list.clear(); }

    typename std::map<ValueID, LockElement<Lock>, ValueComparator>::iterator begin() {
        return lock_list.begin();
    }

    typename std::map<ValueID, LockElement<Lock>, ValueComparator>::iterator end() {
        return lock_list.end();
    }

    void insert(ValueID vid, Lock* lock, LockType locktype) {
        lock_list.insert(std::make_pair(vid, LockElement<Lock>(vid.key, lock, locktype)));
    }

    void erase(
        typename std::map<ValueID, LockElement<Lock>, ValueComparator>::iterator first,
        typename std::map<ValueID, LockElement<Lock>, ValueComparator>::iterator last) {
        lock_list.erase(first, last);
    }

private:
    std::map<ValueID, LockElement<Lock>, ValueComparator> lock_list;
};