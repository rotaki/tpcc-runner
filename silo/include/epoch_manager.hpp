#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <thread>

#include "silo/include/index.hpp"
#include "silo/include/schema.hpp"
#include "silo/include/worker.hpp"
#include "utils/logger.hpp"

class EpochManager {
public:
    EpochManager(uint32_t num_workers)
        : workers(num_workers, nullptr) {}

    void start(int64_t duration_in_seconds) {
        auto t1 = std::chrono::high_resolution_clock::now();
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(40));
            [[maybe_unused]] bool incremented = increment_epoch();
            LOG_DEBUG(incremented ? "Epoch incremented" : "Epoch NOT incremented");
            auto t2 = std::chrono::high_resolution_clock::now();
            auto d_s = std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count();
            if (d_s > duration_in_seconds) break;
        }
    }

    void set_worker(uint32_t worker_id, Worker* worker) {
        assert(workers[worker_id] == nullptr);
        workers[worker_id] = worker;
    }

    uint32_t get_smallest_worker_epoch() {
        uint32_t smallest = 0xFFFFFFFF;
        for (Worker* worker: workers) {
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
    std::vector<Worker*> workers;
};