#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "protocols/common/schema.hpp"

enum class LockType { READ = 0, WRITE = 1 };

template <typename Lock>
struct LockElement {
    unsigned int key;  // record を識別する．
    Lock* lock;
    LockType type;  // 0 read-mode, 1 write-mode

    LockElement(unsigned int key, Lock* lock, LockType locktype)
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


template <typename Key, typename Lock>
class LockList {
public:
    using Table = std::vector<LockElement<Lock>>;
    Table& get_table(TableID table_id) { return tables[table_id]; }

    void clear() { tables.clear(); }

private:
    std::unordered_map<TableID, Table> tables;
};