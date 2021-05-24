#pragma once

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "random.hpp"
#include "record_layout.hpp"

namespace RecordGeneratorUtils {
// uniform random int
uint64_t urand_int(uint64_t min, uint64_t max);
// uniform random double
double urand_double(uint64_t min, uint64_t max, size_t divider);

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

size_t make_random_astring(char* out, size_t min_len, size_t max_len);

// random n-string |x..y|
size_t make_random_nstring(char* out, size_t min_len, size_t max_len);

void make_original(char* out);

// c_last
size_t make_clast(char* out, size_t num);

// zip
void make_random_zip(char* out);

// address
void make_random_address(Address& a);

struct Permutation {
public:
    Permutation(size_t min, size_t max);
    size_t operator[](size_t i) const;

private:
    std::vector<size_t> perm;
};
}  // namespace RecordGeneratorUtils


namespace RecordGenerator {
using namespace RecordGeneratorUtils;

void create_item(Item& i, uint32_t i_id);

void create_warehouse(Warehouse& w, uint16_t w_id);

void create_stock(Stock& s, uint16_t s_w_id, uint32_t s_i_id);

void create_district(District& d, uint16_t d_w_id, uint8_t d_id);

void create_customer(Customer& c, uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, int64_t t);

void create_history(
    History& h, uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id,
    uint8_t h_d_id);

void create_order(Order& o, uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id);

void create_orderline(
    OrderLine& ol, uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id,
    uint32_t ol_i_id, uint8_t ol_number, int64_t o_entry_d);

void create_neworder(NewOrder& n, uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id);
}  // namespace RecordGenerator
