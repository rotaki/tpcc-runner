#pragma once

#include <cstdint>

struct Epotemp {
    union {
        alignas(64) uint64_t obj;
        struct {
            uint64_t temp : 32;
            uint64_t epoch : 32;
        };
    };

    Epotemp()
        : obj(0) {}

    Epotemp(uint64_t temp2, uint64_t epoch2)
        : temp(temp2)
        , epoch(epoch2) {}

    bool operator==(const Epotemp& right) const { return obj == right.obj; }

    bool operator!=(const Epotemp& right) const { return !operator==(right); }
};