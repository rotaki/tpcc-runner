#pragma once

#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <utility>

#include "random.hpp"
#include "zipf.hpp"

/**
 * Better strncpy().
 * out buffer will be null-terminated.
 * returned value is written size excluding the last null character.
 */
inline std::size_t copy_cstr(char* out, const char* in, std::size_t out_buf_size) {
    if (out_buf_size == 0) return 0;
    std::size_t i = 0;
    while (i < out_buf_size - 1) {
        if (in[i] == '\0') break;
        out[i] = in[i];
        i++;
    }
    out[i] = '\0';
    return i;
}

inline Xoshiro256PlusPlus& get_rand() {
    thread_local Xoshiro256PlusPlus r(std::random_device{}());
    return r;
}

inline uint64_t urand_int(uint64_t min, uint64_t max) {
    return (get_rand()() % (max - min + 1)) + min;
}

inline uint64_t zipf_int(double theta, uint64_t nr) {
    thread_local FastZipf fz(get_rand(), theta, nr);
    return fz();
}

inline double urand_double(uint64_t min, uint64_t max, size_t divider) {
    return urand_int(min, max) / static_cast<double>(divider);
}

inline size_t make_random_astring(char* out, size_t min_len, size_t max_len) {
    const char c[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    size_t len_c = strlen(c);
    size_t len_out = urand_int(min_len, max_len);
    for (size_t i = 0; i < len_out; i++) {
        out[i] = c[urand_int(static_cast<size_t>(0), len_c - 1)];
    }
    out[len_out] = '\0';
    return len_out;
}

// random n-string |x..y|
inline size_t make_random_nstring(char* out, size_t min_len, size_t max_len) {
    const char c[] = "0123456789";
    size_t len_c = strlen(c);
    size_t len_out = urand_int(min_len, max_len);
    for (size_t i = 0; i < len_out; i++) {
        out[i] = c[urand_int(static_cast<size_t>(0), len_c - 1)];
    }
    out[len_out] = '\0';
    return len_out;
}

struct Permutation {
public:
    Permutation(size_t min, size_t max)
        : perm(max - min + 1) {
        assert(min <= max);
        size_t i = min;
        for (std::size_t& val: perm) {
            val = i;
            i++;
        }
        assert(i - 1 == max);

        const size_t s = perm.size();
        for (size_t i = 0; i < s - 1; i++) {
            size_t j = urand_int(static_cast<size_t>(0), s - i - 1);
            assert(i + j < s);
            if (j != 0) std::swap(perm[i], perm[i + j]);
        }
    }

    size_t operator[](size_t i) const {
        assert(i < perm.size());
        return perm[i];
    }

private:
    std::vector<size_t> perm;
};

template <std::size_t N>
struct num {
    static const constexpr auto value = N;
};

template <class F, std::size_t... Is>
void constexpr_for(F func, std::index_sequence<Is...>) {
    (func(num<Is>{}), ...);
}

template <std::size_t N, typename F>
void constexpr_for(F func) {
    constexpr_for(func, std::make_index_sequence<N>());
}

template <typename T>
constexpr bool false_v = false;

template <typename... Args>
void unused(Args&&...) {}