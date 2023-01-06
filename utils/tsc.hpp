#pragma once

#include <cstdint>

inline uint64_t rdtscp() {
    uint64_t rax;
    uint64_t rdx;
    uint32_t aux;
    asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux)::);
    // NOTE: rdtscp measures "cycles" not "time"
    // seconds = cycles / frequency
    return (rdx << 32) | rax;
}
