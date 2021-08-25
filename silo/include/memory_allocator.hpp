#pragma once

#include "mimalloc/include/mimalloc.h"

class MemoryAllocator {
public:
    static void* allocate(size_t size) { return mi_malloc(size); }

    static void* aligned_allocate(size_t size) { return mi_malloc(size); }

    static void deallocate(void* ptr) { return mi_free(ptr); }
};