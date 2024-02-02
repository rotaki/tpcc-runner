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
    Epotemp epotemp_;
    RWLock rwl;
    void* rec;

    void initialize() { 
        rwl.initialize(); 
        tidword.obj = 0;
        tidword.absent = true;
        epotemp_.temp = 0;
    }

    void lock() { rwl.lock(); }

    void unlock() { rwl.unlock(); }
};
