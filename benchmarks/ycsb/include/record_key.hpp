#pragma once

#include <cstdint>

struct PayloadKey {
    PayloadKey();

    PayloadKey(uint64_t p_key);

    uint64_t p_key;
    uint64_t get_raw_key() { return p_key; }
};