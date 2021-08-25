#pragma once

#include <cstdint>

using Key = uint64_t;
using Rec = void;
using TableID = uint64_t;

struct TidWord {
    union {
        uint64_t obj = 0;
        struct {
            bool lock : 1;
            bool latest : 1;
            bool absent : 1;
            uint64_t tid : 29;
            uint64_t epoch : 32;
        };
    };
};

struct Value {
    alignas(64) TidWord tidword;
    Rec* rec;
};
