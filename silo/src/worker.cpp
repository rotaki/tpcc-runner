#include "silo/include/worker.hpp"

#include "silo/include/epoch_manager.hpp"
#include "utils/atomic_wrapper.hpp"

Worker::Worker(uint32_t worker_id)
    : worker_id(worker_id) {}

std::unique_ptr<Silo> Worker::begin_tx() {
    store_release(get_worker_epoch(), load_acquire(EpochManager::get_global_epoch()));
    return std::make_unique<Silo>(load_acquire(get_worker_epoch()));
}

uint32_t& Worker::get_worker_epoch() {
    return worker_epoch;
}
