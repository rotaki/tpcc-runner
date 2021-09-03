#pragma once

#include <cstdint>

#include "protocols/common/readwritelock.hpp"

struct Value {
    alignas(64) RWLock rwl;
    void* rec;
};