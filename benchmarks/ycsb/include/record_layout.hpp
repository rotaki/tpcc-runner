#pragma once

#include <cstdint>

#include "record_key.hpp"

template <uint64_t PayloadSize>
struct Payload {
    using Key = PayloadKey;
    Payload() {}
    char p[PayloadSize] = {};
};