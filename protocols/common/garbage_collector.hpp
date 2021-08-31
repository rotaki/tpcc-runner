#pragma once

#include <cstdint>
#include <map>

#include "protocols/common/memory_allocator.hpp"
#include "utils/logger.hpp"

class GarbageCollector {
public:
    static void collect(uint32_t current_epoch, void* ptr) {
        auto& gc_container = get_gc_container();
        gc_container.emplace(current_epoch, ptr);
    }

    static void remove(uint32_t current_epoch) {
        auto& gc_container = get_gc_container();
        if (current_epoch >= 2) {
            auto end = gc_container.upper_bound(current_epoch - 2);
            LOG_DEBUG("Garbage removal epoch: %u", current_epoch - 2);

            for (auto iter = gc_container.begin(); iter != end; ++iter) {
                MemoryAllocator::deallocate(iter->second);
            }

            gc_container.erase(gc_container.begin(), end);
        }
    }

private:
    static std::multimap<uint32_t, void*>& get_gc_container() {
        thread_local std::multimap<uint32_t, void*> gc_container;
        return gc_container;
    }
};
