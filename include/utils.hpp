#pragma once

#include <cassert>
#include <cstdint>
#include <stdexcept>

#include "random.hpp"
#include "record_layout.hpp"

class Config {
public:
    Config() = default;
    void set_num_warehouses(uint16_t n) { num_warehouses = n; }
    uint16_t get_num_warehouses() const { return num_warehouses; }

    void enable_random_abort() { does_random_abort = true; }

    bool get_random_abort_flag() const { return does_random_abort; }

private:
    uint16_t num_warehouses = 1;
    bool does_random_abort = false;
};

inline Config& get_mutable_config() {
    static Config c;
    return c;
}

inline const Config& get_config() {
    return get_mutable_config();
}

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

inline double urand_double(uint64_t min, uint64_t max, size_t divider) {
    return urand_int(min, max) / static_cast<double>(divider);
}

constexpr uint64_t get_constant_for_nurand(uint64_t A, bool is_load) {
    constexpr uint64_t C_FOR_C_LAST_IN_LOAD = 250;
    constexpr uint64_t C_FOR_C_LAST_IN_RUN = 150;
    constexpr uint64_t C_FOR_C_ID = 987;
    constexpr uint64_t C_FOR_OL_I_ID = 5987;

    static_assert(C_FOR_C_LAST_IN_LOAD <= 255);
    static_assert(C_FOR_C_LAST_IN_RUN <= 255);
    constexpr uint64_t delta = C_FOR_C_LAST_IN_LOAD - C_FOR_C_LAST_IN_RUN;
    static_assert(65 <= delta && delta <= 119 && delta != 96 && delta != 112);
    static_assert(C_FOR_C_ID <= 1023);
    static_assert(C_FOR_OL_I_ID <= 8191);

    switch (A) {
    case 255: return is_load ? C_FOR_C_LAST_IN_LOAD : C_FOR_C_LAST_IN_RUN;
    case 1023: return C_FOR_C_ID;
    case 8191: return C_FOR_OL_I_ID;
    default: return UINT64_MAX;  // bug
    }
}

// non-uniform random int
template <uint64_t A, bool IS_LOAD = false>
uint64_t nurand_int(uint64_t x, uint64_t y) {
    constexpr uint64_t C = get_constant_for_nurand(A, IS_LOAD);
    if (C == UINT64_MAX) {
        throw std::runtime_error("nurand_int bug");
    }
    return (((urand_int(0, A) | urand_int(x, y)) + C) % (y - x + 1)) + x;
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

inline void make_original(char* out) {
    size_t len = strlen(out);
    assert(len >= 8);
    memcpy(&out[urand_int(static_cast<size_t>(0), len - 8)], "ORIGINAL", 8);
}

// c_last
inline size_t make_clast(char* out, size_t num) {
    const char* candidates[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                "ESE", "ANTI",  "CALLY", "ATION", "EING"};
    assert(num < 1000);
    constexpr size_t buf_size = Customer::MAX_LAST + 1;
    int len = 0;
    for (size_t i: {num / 100, (num % 100) / 10, num % 10}) {
        len += copy_cstr(&out[len], candidates[i], buf_size - len);
    }
    assert(len < Customer::MAX_LAST);
    out[len] = '\0';
    return len;
}

// zip
inline void make_random_zip(char* out) {
    make_random_nstring(&out[0], 4, 4);
    out[4] = '1';
    out[5] = '1';
    out[6] = '1';
    out[7] = '1';
    out[8] = '1';
    out[9] = '\0';
}

// address
inline void make_random_address(Address& a) {
    make_random_astring(a.street_1, Address::MIN_STREET, Address::MAX_STREET);
    make_random_astring(a.street_2, Address::MIN_STREET, Address::MAX_STREET);
    make_random_astring(a.city, Address::MIN_CITY, Address::MAX_CITY);
    make_random_astring(a.state, Address::STATE, Address::STATE);
    make_random_zip(a.zip);
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