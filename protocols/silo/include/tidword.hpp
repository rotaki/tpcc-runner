#pragma once

#include <cstdint>

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

    TidWord()
        : obj(0){};

    TidWord(uint64_t obj)
        : obj(obj) {}

    TidWord(const TidWord& tidword)
        : obj(tidword.obj) {}

    TidWord& operator=(const TidWord& tidword) {
        obj = tidword.obj;
        return *this;
    }
};
