#pragma once

#include <cstdint>
#include <mutex>

#include "protocols/common/readwritelock.hpp"
#include "protocols/mocc/include/epotemp.hpp"
#include "protocols/mocc/include/locklist.hpp"
#include "protocols/mocc/include/tidword.hpp"
#include "utils/atomic_wrapper.hpp"

struct Value {
    alignas(64) TidWord tidword;
    Epotemp epotemp;
    RWLock rwl;
    void* rec;

    void initialize() {
        rwl.initialize();
        tidword.epoch = 1;
        tidword.tid = 0;
        tidword.absent = false;
        epotemp.temp = 0;
        epotemp.epoch = 1;
    }

    void lock() { rwl.lock(); }

    void unlock() { rwl.unlock(); }
};
