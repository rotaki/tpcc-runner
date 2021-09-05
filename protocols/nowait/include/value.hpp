#pragma once

#include <cstdint>

#include "protocols/common/readwritelock.hpp"

struct ValueSimple {
    alignas(64) RWLock rwl;
    void* rec;
};