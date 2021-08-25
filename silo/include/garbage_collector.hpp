#pragma once

#include <cstdint>
#include <map>

#include "silo/include/keyvalue.hpp"
#include "silo/include/memory_allocator.hpp"

class GarbageCollector {
public:
    static void collect(uint32_t current_epoch, Rec* ptr);

    static void remove(uint32_t current_epoch);

private:
    static std::multimap<uint32_t, Rec*>& get_gc_container();
};