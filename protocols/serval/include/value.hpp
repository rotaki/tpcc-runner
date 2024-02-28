#pragma once

#include <cstdint>
#include <mutex>

#include "protocols/common/readwritelock.hpp"
#include "utils/atomic_wrapper.hpp"

class Version {
public:
    alignas(64) uint64_t read_ts;  // latest read timestamp (mutable: read)
    uint64_t write_ts;             // latest write timestamp (immutable)
    Version* prev;                 // previous version (mutable: gc)
    void* rec;                     // nullptr if deleted = true (immutable)
    bool deleted;                  // (immutable)

    // update read timestamp if it is less than ts
    void update_readts(uint64_t ts) {
        uint64_t current_readts = load_acquire(read_ts);
        if (ts < current_readts) return;
        store_release(read_ts, ts);
    }

    uint64_t get_readts() { return load_acquire(read_ts); }
};

template <typename Version_>
struct Value {
    using Version = Version_;
    RWLock rwl;
    Version* version; // Global Version Chain
    
    // For contended versions
    void* ptr_to_version_array = nullptr; // Pointer to per-core version array
    uint64_t core_bitmap; 

    void initialize() {
        rwl.initialize();
        version = nullptr;
    }

    void lock() { rwl.lock(); }

    bool try_lock() { return rwl.try_lock(); }

    void unlock() { rwl.unlock(); }

    bool is_detached_from_tree() { return (version == nullptr); }

    bool is_empty() { return (version->deleted && version->prev == nullptr); }

    void trace_version_chain() {
        Version* temp = version;
        while (temp != nullptr) {
            LOG_TRACE(
                "rs: %lu, ws: %lu, deleted: %s", temp->read_ts, temp->write_ts,
                temp->deleted ? "true" : "false");
            temp = temp->prev;
        }
    }
};

//per-core version array の classまたはstructを定義する
/*
struct per-core_version array{
    bool is_contended;
    ptr from value;
    bitmap transaction_bitmap;
    Version pending_version;
}
*/
