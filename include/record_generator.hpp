#pragma once

#include <cstdint>
#include <vector>

#include "random.hpp"
#include "table_layout.hpp"

namespace RecordGeneratorUtils {
// uniform random int
uint64_t urand_int(uint64_t min, uint64_t max);
// uniform random double
double urand_double(uint64_t min, uint64_t max, size_t divider);

enum NURandConstantType { C_LOAD = 250, C_RUN = 150, C_ID = 987, OL_I_ID = 5987 };

uint64_t get_a_from_constant_type(NURandConstantType t);

// non-uniform random int
uint64_t nurand_int(NURandConstantType t, uint64_t x, uint64_t y);

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

class NumWarehouse {
public:
    static void init(std::size_t n);
    static std::size_t get_num();

private:
    static std::size_t num_warehouse;
};

uint64_t create_item_key(uint32_t i_id);
void create_item(Item& i, uint32_t i_id);

uint64_t create_warehouse_key(uint16_t w_id);
void create_warehouse(Warehouse& w, uint16_t w_id);

uint64_t create_stock_key(uint16_t w_id, uint32_t i_id);
void create_stock(Stock& s, uint16_t s_w_id, uint32_t s_i_id);

uint64_t create_district_key(uint16_t w_id, uint8_t d_id);
void create_district(District& d, uint16_t d_w_id, uint8_t d_id);

uint64_t create_customer_key(uint16_t w_id, uint8_t d_id, uint32_t c_id);
void create_customer(Customer& c, uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, int64_t t);

uint64_t create_history_key(uint16_t w_id, uint8_t d_id, uint32_t c_id, uint8_t h_id);
void create_history(
    History& h, uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id,
    uint8_t h_d_id);

uint64_t create_order_key(uint16_t w_id, uint8_t d_id, uint32_t o_id);
void create_order(Order& o, uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id);

uint64_t create_orderline_key(uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_number);
void create_orderline(
    OrderLine& ol, uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id,
    uint32_t ol_i_id, uint8_t ol_number, int64_t o_entry_d);

uint64_t create_neworder_key(uint16_t w_id, uint8_t d_id, uint32_t o_id);
void create_neworder(NewOrder& n, uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id);
}  // namespace RecordGenerator
