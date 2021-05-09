#pragma once

#include <cassert>
#include <cstdlib>
#include <iomanip>
#include <random>
#include <sstream>

#include "table_layout.hpp"

extern int NUM_WAREHOUSE;

namespace TableGenerator {
unsigned get_digit(int value);

std::string add_zero_padding(const int value, const unsigned precision);

// random int within |x..y|
uint64_t random_int(uint64_t min, uint64_t max);

float random_float(uint64_t min, uint64_t max, size_t divider);

enum NURandConstantType { C_LOAD = 250, C_RUN = 150, C_ID = 987, OL_I_ID = 5987 };

uint64_t get_a_from_constant_type(NURandConstantType t);

uint64_t nurand_int(NURandConstantType t, uint64_t x, uint64_t y);

// random a-string |x..y|
size_t make_random_astring(char* out, size_t min_len, size_t max_len);

// random n-string |x..y|
size_t make_random_nstring(char* out, size_t min_len, size_t max_len);

void make_original(char* out);

size_t make_clast(char* out, size_t num);

void make_random_zip(char* out);

void make_random_address(Address& a);

struct Permutation {
public:
    Permutation(size_t min, size_t max);
    size_t operator[](size_t i) const;

private:
    std::vector<size_t> perm;
};

// parameters are foregin key references + primary key (+ other if needed)
Warehouse& create_warehouse(Warehouse& w, uint16_t w_id);

District& create_district(District& d, uint16_t d_w_id, uint8_t d_id);

Customer& create_customer(Customer& c, uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, int64_t t);

History& create_history(
    History& h, uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t d_w_id,
    uint8_t h_d_id);

Order& create_order(Order& o, uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id);

NewOrder& create_neworder(NewOrder& n, uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id);

OrderLine& create_orderline(
    OrderLine& ol, uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id,
    uint32_t ol_i_id, uint8_t ol_number, int64_t o_entry_d);

Item& create_item(Item& i, uint32_t i_id);

Stock& create_stock(Stock& s, uint16_t s_w_id, uint32_t s_i_id);

void load_items_table();

void load_other_tables();
};  // namespace TableGenerator
