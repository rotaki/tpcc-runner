#pragma once

#include <cstdint>

struct TidWord {
    union {
        uint64_t obj;
        struct {
            bool absent : 1;
            uint64_t tid : 31;
            uint64_t epoch : 32;
        };
    };

    TidWord() { obj = 0; }

    TidWord(uint64_t obj)
        : obj(obj) {}

    TidWord(const TidWord& tidword)
        : obj(tidword.obj) {}

    TidWord& operator=(const TidWord& tidword) {
        obj = tidword.obj;
        return *this;
    }

    bool operator==(const TidWord& right) const { return obj == right.obj; }

    bool operator!=(const TidWord& right) const { return !operator==(right); }

    bool operator<(const TidWord& right) const { return this->obj < right.obj; }
};
