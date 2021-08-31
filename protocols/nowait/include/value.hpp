#pragma once

#include <cstdint>

#include "protocols/common/readwritelock.hpp"

using Rec = void;

struct Value {
    alignas(64) RWLock rwl;
    Rec* rec;
};