#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#include "protocols/common/memory_allocator.hpp"
#include "protocols/common/transaction_id.hpp"
#include "utils/atomic_wrapper.hpp"
#include "utils/logger.hpp"

template <typename Protocol>
class Worker;

template <typename Protocol>
class EpochManager {
public:
    EpochManager(uint32_t num_workers, uint64_t interval)
        : workers(num_workers, nullptr)
        , interval(interval) {}

    void start(int64_t duration_in_seconds) {
        auto t1 = std::chrono::high_resolution_clock::now();
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
            [[maybe_unused]] bool incremented = increment_epoch();
            LOG_DEBUG(incremented ? "Epoch incremented" : "Epoch NOT incremented");
            auto t2 = std::chrono::high_resolution_clock::now();
            auto d_s = std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count();
            if (d_s > duration_in_seconds) break;
        }
    }

    void set_worker(uint32_t worker_id, Worker<Protocol>* worker) {
        assert(workers[worker_id] == nullptr);
        workers[worker_id] = worker;
    }

    uint32_t get_smallest_worker_epoch() {
        uint32_t smallest = 0xFFFFFFFF;
        for (Worker<Protocol>* worker: workers) {
            if (worker == nullptr) continue;
            uint32_t worker_epoch = load_acquire(worker->get_worker_epoch());
            smallest = std::min(worker_epoch, smallest);
        }
        LOG_DEBUG("Smallest Worker Epoch: %u", smallest);
        return smallest;
    }

    bool increment_epoch() {
        uint32_t e = load_acquire(get_global_epoch());
        if (e == get_smallest_worker_epoch()) {
            fetch_add(get_global_epoch(), 1);
            return true;
        } else {
            return false;
        }
    }

    static uint32_t& get_global_epoch() {
        static uint32_t e{1};  // starts from 1
        return e;
    }

private:
    std::vector<Worker<Protocol>*> workers;
    uint64_t interval;
};

template <typename Protocol>
class Worker {
public:
    Worker(uint32_t worker_id)
        : worker_id(worker_id)
        , tx_counter(1) {}

    std::unique_ptr<Protocol> begin_tx() {
        store_release(get_worker_epoch(), load_acquire(EpochManager<Protocol>::get_global_epoch()));
        TxID txid(worker_id, tx_counter);
        ++tx_counter;
        return std::make_unique<Protocol>(txid, load_acquire(get_worker_epoch()));
    }

    uint32_t& get_worker_epoch() { return worker_epoch; }

    uint32_t get_id() { return worker_id; }

private:
    uint32_t worker_id;
    uint32_t tx_counter;
    alignas(64) uint32_t worker_epoch;
};

class GarbageCollector {
public:
    static void collect(uint32_t current_epoch, void* ptr) {
        auto& gc_container = get_gc_container();
        gc_container.emplace(current_epoch, ptr);
    }

    static void remove(uint32_t current_epoch) {
        auto& gc_container = get_gc_container();
        if (current_epoch >= 2) {
            auto end = gc_container.upper_bound(current_epoch - 2);
            LOG_DEBUG("Garbage removal epoch: %u", current_epoch - 2);

            for (auto iter = gc_container.begin(); iter != end; ++iter) {
                MemoryAllocator::deallocate(iter->second);
            }

            gc_container.erase(gc_container.begin(), end);
        }
    }

private:
    static std::multimap<uint32_t, void*>& get_gc_container() {
        thread_local std::multimap<uint32_t, void*> gc_container;
        return gc_container;
    }
};
