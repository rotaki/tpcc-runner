#include "record_generator.hpp"

#include <cassert>

#include "random.hpp"

namespace RecordGeneratorUtils {
uint64_t urand_int(uint64_t min, uint64_t max) {
    return (Xoshiro256PlusPlus::rand() % (max - min + 1)) + min;
}

double urand_double(uint64_t min, uint64_t max, size_t divider) {
    return urand_int(min, max) / static_cast<double>(divider);
}

uint64_t get_a_from_constant_type(NURandConstantType t) {
    switch (t) {
    case C_LOAD: return 255;
    case C_RUN: return 255;
    case C_ID: return 1023;
    case OL_I_ID: return 8191;
    default: assert(false);
    }
}

uint64_t nurand_int(NURandConstantType t, uint64_t x, uint64_t y) {
    uint64_t A = get_a_from_constant_type(t);
    uint64_t C = static_cast<uint64_t>(t);
    return (((urand_int(static_cast<uint64_t>(0), A) | urand_int(x, y)) + C) % (y - x + 1)) + x;
}

size_t make_random_astring(char* out, size_t min_len, size_t max_len) {
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
size_t make_random_nstring(char* out, size_t min_len, size_t max_len) {
    const char c[] = "0123456789";
    size_t len_c = strlen(c);
    size_t len_out = urand_int(min_len, max_len);
    for (size_t i = 0; i < len_out; i++) {
        out[i] = c[urand_int(static_cast<size_t>(0), len_c - 1)];
    }
    out[len_out] = '\0';
    return len_out;
}

void make_original(char* out) {
    size_t len = strlen(out);
    assert(len >= 8);
    strcpy(&out[urand_int(static_cast<size_t>(0), len - 8)], "ORIGINAL");
}

// c_last
size_t make_clast(char* out, size_t num) {
    const char* candidates[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                "ESE", "ANTI",  "CALLY", "ATION", "EING"};
    assert(num < 1000);
    int len = 0;
    for (size_t i: {num / 100, (num % 100) / 10, num % 10}) {
        strcpy(&out[len], candidates[i]);
        len += strlen(candidates[i]);
    }
    assert(len < Customer::MAX_LAST);
    out[len] = '\0';
    return len;
}

// zip
void make_random_zip(char* out) {
    make_random_nstring(&out[0], 4, 4);
    out[4] = '1';
    out[5] = '1';
    out[6] = '1';
    out[7] = '1';
    out[8] = '1';
    out[9] = '\0';
}

// address
void make_random_address(Address& a) {
    make_random_astring(a.street_1, Address::MIN_STREET, Address::MAX_STREET);
    make_random_astring(a.street_2, Address::MIN_STREET, Address::MAX_STREET);
    make_random_astring(a.city, Address::MIN_CITY, Address::MAX_CITY);
    make_random_astring(a.state, Address::STATE, Address::STATE);
    make_random_zip(a.zip);
}

Permutation::Permutation(size_t min, size_t max)
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

size_t Permutation::operator[](size_t i) const {
    assert(i < perm.size());
    return perm[i];
}
}  // namespace RecordGeneratorUtils

namespace RecordGenerator {
void NumWarehouse::init(std::size_t n) {
    num_warehouse = n;
}

std::size_t NumWarehouse::get_num() {
    return num_warehouse;
}

uint64_t create_item_key(uint32_t i_id) {
    assert(i_id >= 1);
    assert(i_id <= Item::ITEMS);
    return i_id - 1;
}

void create_item(Item& i, uint32_t i_id) {
    i.i_id = i_id;                            // 200000 unique ids
    i.i_im_id = urand_int(1, 10000);          // 200000 unique ids
    i.i_price = urand_double(1, 10000, 100);  // numeric(5, 2)
    make_random_astring(i.i_name, Item::MIN_NAME, Item::MAX_NAME);
    make_random_astring(i.i_data, Item::MIN_DATA, Item::MAX_DATA);
    if (urand_int(0, 99) < 10) make_original(i.i_data);
}

uint64_t create_warehouse_key(uint16_t w_id) {
    assert(w_id >= 1);
    return w_id - 1;
}

void create_warehouse(Warehouse& w, uint16_t w_id) {
    w.w_id = w_id;                           // 2*W unique ids
    w.w_tax = urand_double(0, 2000, 10000);  // signed numeric(4, 4)
    w.w_ytd = 300000;                        // signed numeric(12, 2)
    make_random_astring(w.w_name, Warehouse::MIN_NAME, Warehouse::MAX_NAME);
    make_random_address(w.w_address);
}

uint64_t create_stock_key(uint16_t w_id, uint32_t i_id) {
    assert(i_id >= 1);
    assert(i_id <= Stock::STOCKS_PER_WARE);
    return create_warehouse_key(w_id) * Stock::STOCKS_PER_WARE + i_id - 1;
}

void create_stock(Stock& s, uint16_t s_w_id, uint32_t s_i_id) {
    s.s_i_id = s_i_id;  // 200000 unique ids
    s.s_w_id = s_w_id;
    s.s_quantity = urand_int(10, 100);  // signed numeric(4)
    s.s_ytd = 0;                        // numeric(8)
    s.s_order_cnt = 0;                  // numeric(4)
    s.s_remote_cnt = 0;                 // numeric(4)
    make_random_astring(s.s_dist_01, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_02, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_03, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_04, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_05, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_06, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_07, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_08, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_09, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_dist_10, Stock::DIST, Stock::DIST);
    make_random_astring(s.s_data, Stock::MIN_DATA, Stock::MAX_DATA);
    if (urand_int(0, 99) < 10) make_original(s.s_data);
}

uint64_t create_district_key(uint16_t w_id, uint8_t d_id) {
    assert(d_id >= 1);
    assert(d_id <= District::DISTS_PER_WARE);
    return create_warehouse_key(w_id) * District::DISTS_PER_WARE + d_id - 1;
}

void create_district(District& d, uint16_t d_w_id, uint8_t d_id) {
    d.d_w_id = d_w_id;
    d.d_id = d_id;
    d.d_next_o_id = 3001;                    // 10000000 unique ids
    d.d_tax = urand_double(0, 2000, 10000);  // signed numeric(4, 4)
    d.d_ytd = 30000.00;                      // signed numeric(12, 2)
    make_random_astring(d.d_name, District::MIN_NAME, District::MAX_NAME);
    make_random_address(d.d_address);
}

uint64_t create_customer_key(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
    assert(c_id >= 1);
    assert(c_id <= Customer::CUSTS_PER_DIST);
    return create_district_key(w_id, d_id) * Customer::CUSTS_PER_DIST + c_id - 1;
}

void create_customer(Customer& c, uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, int64_t t) {
    c.c_id = c_id;  // 96000 unique ids
    c.c_d_id = c_d_id;
    c.c_w_id = c_w_id;
    c.c_payment_cnt = 1;                          // numeric(4)
    c.c_delivery_cnt = 0;                         // numeric(4)
    c.c_since = t;                                // date and time
    c.c_credit_lim = 50000;                       // signed numeric(2, 2)
    c.c_discount = urand_double(0, 5000, 10000);  // signed numeric(4, 4)
    c.c_balance = -10.00;                         // signed numeric(12, 2)
    c.c_ytd_payment = 10.00;                      // signed numeric(12, 2)
    make_random_astring(c.c_first, Customer::MIN_FIRST, Customer::MAX_FIRST);
    strcpy(c.c_middle, "OL");
    (c_id <= 1000 ? make_clast(c.c_last, c_id - 1)
                  : make_clast(c.c_last, nurand_int(NURandConstantType::C_LOAD, 0, 999)));
    make_random_nstring(c.c_phone, Customer::PHONE, Customer::PHONE);
    (urand_int(0, 99) < 10 ? strcpy(c.c_credit, "BC") : strcpy(c.c_credit, "GC"));
    make_random_astring(c.c_data, Customer::MIN_DATA, Customer::MAX_DATA);
    make_random_address(c.c_address);
}

uint64_t create_history_key(uint16_t w_id, uint8_t d_id, uint32_t c_id, uint8_t h_id) {
    assert(h_id >= 1);
    assert(h_id <= History::HISTS_PER_CUST);
    return create_customer_key(w_id, d_id, c_id) * History::HISTS_PER_CUST + h_id - 1;
}

void create_history(
    History& h, uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id,
    uint8_t h_d_id) {
    h.h_c_id = h_c_id;
    h.h_c_d_id = h_c_d_id;
    h.h_c_w_id = h_c_w_id;
    h.h_d_id = h_d_id;
    h.h_w_id = h_w_id;
    h.h_date = static_cast<int64_t>(time(nullptr));  // date and time
    h.h_amount = 10.00;                              // signed numeric(6, 2)
    make_random_astring(h.h_data, History::MIN_DATA, History::MAX_DATA);
}

uint64_t create_order_key(uint16_t w_id, uint8_t d_id, uint32_t o_id) {
    assert(o_id >= 1);
    assert(o_id <= Order::ORDS_PER_DIST);
    return create_district_key(w_id, d_id) * Order::ORDS_PER_DIST + o_id - 1;
}

void create_order(Order& o, uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id) {
    o.o_id = o_id;  // 10000000 unique ids
    o.o_d_id = o_d_id;
    o.o_w_id = o_w_id;
    o.o_c_id = o_c_id;
    o.o_carrier_id = (o.o_id < 2101 ? urand_int(1, 10) : 0);  // 10 unique ids or null
    o.o_ol_cnt = urand_int(5, 15);                            // numeric(2)
    o.o_all_local = 1;                                        // numeric(1)
    o.o_entry_d = static_cast<int64_t>(time(nullptr));        // date and time
}

uint64_t create_orderline_key(uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_number) {
    assert(ol_number >= OrderLine::MIN_ORDLINES_PER_ORD);
    assert(ol_number <= OrderLine::MAX_ORDLINES_PER_ORD);
    return create_order_key(w_id, d_id, o_id)
        * (OrderLine::MAX_ORDLINES_PER_ORD - OrderLine::MIN_ORDLINES_PER_ORD + 1)
        + ol_number - OrderLine::MIN_ORDLINES_PER_ORD;
}

void create_orderline(
    OrderLine& ol, uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id,
    uint32_t ol_i_id, uint8_t ol_number, int64_t o_entry_d) {
    ol.ol_o_id = ol_o_id;
    ol.ol_d_id = ol_d_id;
    ol.ol_w_id = ol_w_id;
    ol.ol_number = ol_number;  // 15 unique ids
    ol.ol_i_id = ol_i_id;      // 200000 unique ids
    ol.ol_supply_w_id = ol_supply_w_id;
    ol.ol_delivery_d = (ol.ol_o_id < 2101 ? o_entry_d : 0);
    ol.ol_quantity = 5;  // numeric(2)
    ol.ol_amount =
        (ol.ol_o_id < 2101 ? 0.00 : urand_double(1, 999999, 100));  // signed numeric(6, 2)
    make_random_astring(ol.ol_dist_info, OrderLine::DIST_INFO, OrderLine::DIST_INFO);
}

uint64_t create_neworder_key(uint16_t w_id, uint8_t d_id, uint32_t o_id) {
    return create_order_key(w_id, d_id, o_id);
}

void create_neworder(NewOrder& no, uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    no.no_o_id = no_o_id;
    no.no_d_id = no_d_id;
    no.no_w_id = no_w_id;
}
}  // namespace RecordGenerator
