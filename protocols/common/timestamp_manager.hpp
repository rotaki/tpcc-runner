#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <map>
#include <thread>
#include <vector>

#include "protocols/common/memory_allocator.hpp"
#include "protocols/common/transaction_id.hpp"
#include "utils/atomic_wrapper.hpp"
#include "utils/logger.hpp"

template <typename Protocol>
class Worker;

template <typename Protocol>
class TimeStampManager {
    friend Worker<Protocol>;

public:
    TimeStampManager(uint8_t num_workers, uint64_t interval)
        : num_workers(num_workers)
        , max_worker_id(num_workers - 1)
        , workers(num_workers, nullptr)
        , interval(interval) {}

    void start(int64_t duration_in_seconds) {
        auto t1 = std::chrono::high_resolution_clock::now();
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
            set_gc_ts();
            auto t2 = std::chrono::high_resolution_clock::now();
            auto d_s = std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count();
            if (d_s > duration_in_seconds) break;
        }
    }

    void set_worker(uint8_t worker_id, Worker<Protocol>* worker) { workers[worker_id] = worker; }

private:
    const uint8_t num_workers;
    const uint8_t max_worker_id;
    std::vector<Worker<Protocol>*> workers;
    uint64_t interval;

    std::pair<uint64_t, uint64_t> get_gc_txn_cnt() {
        uint64_t largest = 0;
        uint64_t smallest = UINT64_MAX;
        for (Worker<Protocol>* worker: workers) {
            if (worker == nullptr) continue;
            uint64_t txn_cnt = load_acquire(worker->txn_cnt);
            smallest = std::min(txn_cnt, smallest);
            largest = std::max(txn_cnt, largest);
        }
        return std::make_pair(smallest, largest);
    }

    // Rapid garbage collection
    void set_gc_ts() {
        auto p = get_gc_txn_cnt();
        uint64_t smallest = p.first;
        uint64_t largest = p.second;
        for (Worker<Protocol>* worker: workers) {
            if (worker != nullptr) {
                store_release(worker->smallest_txn_cnt, smallest - 1);
                store_release(worker->largest_txn_cnt, largest);
            }
        }
    }

    // ------------- Functions called by worker threads ---------------

    // One-sided synchronization
    void synchronize(uint8_t worker_id, uint64_t txn_cnt, uint8_t next_worker_offset) {
        if (max_worker_id == 0) return;  // no need to synchronize because single-threaded
        uint8_t offset = (next_worker_offset % max_worker_id) + 1;
        uint8_t next_worker_id = (worker_id + offset) % num_workers;
        if (workers[next_worker_id] == nullptr) return;
        uint64_t next_txn_cnt = load_acquire(workers[next_worker_id]->txn_cnt);
        if (txn_cnt < next_txn_cnt) store_release(workers[worker_id]->txn_cnt, next_txn_cnt);
    }
};


template <typename Protocol>
class Worker {
public:
    alignas(64) uint64_t txn_cnt{1};
    uint64_t smallest_txn_cnt{1};
    uint64_t largest_txn_cnt{1};

    Worker(TimeStampManager<Protocol>& tsm, uint32_t worker_id, uint64_t synchronize_cnt = 1000)
        : tsm(tsm)
        , worker_id(worker_id)
        , synchronize_cnt(synchronize_cnt)
        , next_worker_offset(0)
        , abort_cnt(0)
        , tx_counter(1) {}

    std::unique_ptr<Protocol> begin_tx() {
        TxID txid(worker_id, tx_counter);
        ++tx_counter;
        return std::make_unique<Protocol>(txid, get_new_ts(), get_smallest_ts(), get_largest_ts());
    }

    uint64_t get_new_ts() {
        abort_cnt = 0;
        uint64_t cnt = fetch_add(txn_cnt, 1);
        if (cnt % synchronize_cnt == 0) synchronize();
        return cnt << (sizeof(worker_id) * 8) | worker_id;
    }

    uint64_t get_abort_boosted_ts() {
        abort_cnt++;
        uint64_t cnt =
            fetch_add(txn_cnt, std::pow(2, std::min(abort_cnt, static_cast<uint64_t>(2))));
        if (cnt % synchronize_cnt == 0) synchronize();
        return cnt << (sizeof(worker_id) * 8) | worker_id;
    }

    uint64_t get_smallest_ts() { return get_ts(load_acquire(smallest_txn_cnt)); }

    uint64_t get_largest_ts() { return get_ts(load_acquire(largest_txn_cnt)); }

    uint64_t get_ts(uint64_t txn_cnt) { return txn_cnt << (sizeof(worker_id) * 8) | worker_id; }

    uint32_t get_id() { return static_cast<uint32_t>(worker_id); }

private:
    TimeStampManager<Protocol>& tsm;
    uint8_t worker_id;
    uint64_t synchronize_cnt;
    uint8_t next_worker_offset;
    uint64_t abort_cnt;
    uint32_t tx_counter;

    void synchronize() { tsm.synchronize(worker_id, load_acquire(txn_cnt), next_worker_offset++); }
};

class GarbageCollector {
public:
    static void collect(uint64_t current_largest_ts, void* ptr) {
        get_temp_gc_map().emplace(current_largest_ts, ptr);
    }

    static void remove(uint64_t smallest_ts, uint64_t new_largest_ts) {
        attach_new_ts(new_largest_ts);
        auto& gc_map = get_gc_map();
        auto end = gc_map.upper_bound(smallest_ts);
        for (auto iter = gc_map.begin(); iter != end; ++iter) {
            MemoryAllocator::deallocate(iter->second);
        }
        gc_map.erase(gc_map.begin(), end);
    }

private:
    // gc pointers that hold timestamp
    static std::multimap<uint64_t, void*>& get_gc_map() {
        thread_local std::multimap<uint64_t, void*> gc_map;
        return gc_map;
    }

    // temporary holds some gc pointers
    static std::multimap<uint64_t, void*>& get_temp_gc_map() {
        thread_local std::multimap<uint64_t, void*> gc_map;
        return gc_map;
    }

    static void attach_new_ts(uint64_t new_largest_ts) {
        auto& temp_gc_map = get_temp_gc_map();
        auto& gc_map = get_gc_map();
        auto end = temp_gc_map.upper_bound(new_largest_ts);
        auto iter = temp_gc_map.begin();
        while (iter != end) {
            auto element = temp_gc_map.extract(iter++);
            element.key() = new_largest_ts;
            gc_map.insert(std::move(element));
        }
    }
};