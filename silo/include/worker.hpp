#pragma once

#include <memory>

#include "silo/include/index.hpp"
#include "silo/include/schema.hpp"
#include "silo/include/silo.hpp"

class Worker {
public:
    Worker(uint32_t worker_id);

    std::unique_ptr<Silo> begin_tx();

    uint32_t& get_worker_epoch();

private:
    alignas(64) uint32_t worker_id{0};
    uint32_t worker_epoch{0};
};