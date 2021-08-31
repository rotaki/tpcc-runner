#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

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
        static uint32_t e{0};
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
        : worker_id(worker_id) {}


    std::unique_ptr<Protocol> begin_tx() {
        store_release(get_worker_epoch(), load_acquire(EpochManager<Protocol>::get_global_epoch()));
        return std::make_unique<Protocol>(load_acquire(get_worker_epoch()));
    }

    uint32_t& get_worker_epoch() { return worker_epoch; }

    uint32_t get_id() { return worker_id; }

private:
    alignas(64) uint32_t worker_id{0};
    uint32_t worker_epoch{0};
};