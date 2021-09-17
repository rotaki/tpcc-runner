#include "benchmarks/ycsb/include/record_key.hpp"

PayloadKey::PayloadKey()
    : p_key(0) {}
PayloadKey::PayloadKey(uint64_t p_key)
    : p_key(p_key) {}