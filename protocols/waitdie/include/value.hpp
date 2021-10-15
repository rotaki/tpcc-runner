#pragma once

#include <cstdint>

#include "protocols/waitdie/include/waitdielock.hpp"

struct Value {
    Value()
        : wdl()
        , rec(nullptr) {}

    Value(void* rec)
        : wdl()
        , rec(rec) {}

    alignas(64) WaitDieLock wdl;
    void* rec;

    bool is_detached_from_tree() { return (rec == nullptr); }
};
