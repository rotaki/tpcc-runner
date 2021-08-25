#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <string_view>
#include <type_traits>

#include "utils/utils.hpp"

using Timestamp = int64_t;
inline Timestamp get_timestamp() {
    thread_local Timestamp i = 0;
    return i++;
}

// Keys are defined in record_key.hpp
struct ItemKey;
struct WarehouseKey;
struct StockKey;
struct DistrictKey;
struct CustomerKey;
struct OrderKey;
struct NewOrderKey;
struct OrderLineKey;

// Primary Key i_id
struct Item {
    using Key = ItemKey;
    static const int ITEMS = 100000;
    static const int MIN_NAME = 14;
    static const int MAX_NAME = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    static const int UNUSED_ID = 1;
    uint32_t i_id;     // 200000 unique ids
    uint32_t i_im_id;  // 200000 unique ids
    double i_price;    // numeric(5, 2)
    char i_name[MAX_NAME + 1];
    char i_data[MAX_DATA + 1];
    void deep_copy_from(const Item& src);
    void generate(uint32_t i_id_);
    void print();
};

struct Address {
    static const int MIN_STREET = 10;
    static const int MAX_STREET = 20;
    static const int MIN_CITY = 10;
    static const int MAX_CITY = 20;
    static const int STATE = 2;
    static const int ZIP = 9;
    char street_1[MAX_STREET + 1];
    char street_2[MAX_STREET + 1];
    char city[MAX_CITY + 1];
    char state[STATE + 1];
    char zip[ZIP + 1];
    void deep_copy_from(const Address& src);
};

// Primary Key w_id
struct Warehouse {
    using Key = WarehouseKey;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    uint16_t w_id;  // 2*W unique ids
    double w_tax;   // signed numeric(4, 4)
    double w_ytd;   // signed numeric(12, 2)
    char w_name[MAX_NAME + 1];
    Address w_address;
    void deep_copy_from(const Warehouse& src);
    void generate(uint16_t w_id_);
    void print();
};

// Primary Key (s_w_id, s_i_id)
// Foreign Key s_w_id references w_id
struct Stock {
    using Key = StockKey;
    static const int STOCKS_PER_WARE = 100000;
    static const int DIST = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    uint16_t s_w_id;
    uint32_t s_i_id;        // 200000 unique ids
    int16_t s_quantity;     // signed numeric(4)
    uint32_t s_ytd;         // numeric(8)
    uint16_t s_order_cnt;   // numeric(4)
    uint16_t s_remote_cnt;  // numeric(4)
    char s_dist[10][DIST + 1];
    char s_data[MAX_DATA + 1];
    void deep_copy_from(const Stock& src);
    void generate(uint16_t s_w_id_, uint32_t s_i_id_);
    void print();
};


// Primary Key (d_w_id, d_id)
// Foreign Key d_w_id references w_id
struct District {
    using Key = DistrictKey;
    static const int DISTS_PER_WARE = 10;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    uint16_t d_w_id;
    uint8_t d_id;          // 20 unique ids
    uint32_t d_next_o_id;  // 10000000 unique ids
    double d_tax;          // signed numeric(4, 4)
    double d_ytd;          // signed numeric(12, 2)
    char d_name[MAX_NAME + 1];
    Address d_address;
    void deep_copy_from(const District& src);
    void generate(uint16_t d_w_id_, uint8_t d_id_);
    void print();
};

struct CustomerSecondary;

// Primary Key (c_w_id, c_d_id, c_id)
// Foreign Key (c_w_id, c_d_id) references (d_w_id, d_id)
struct Customer {
    using Key = CustomerKey;
    static const int CUSTS_PER_DIST = 3000;
    static const int MIN_FIRST = 8;
    static const int MAX_FIRST = 16;
    static const int MAX_MIDDLE = 2;
    static const int MAX_LAST = 16;
    static const int PHONE = 16;
    static const int CREDIT = 2;
    static const int MIN_DATA = 300;
    static const int MAX_DATA = 500;
    static const int UNUSED_ID = 0;
    uint16_t c_w_id;
    uint8_t c_d_id;
    uint32_t c_id;            // 96000 unique ids
    uint16_t c_payment_cnt;   // numeric(4)
    uint16_t c_delivery_cnt;  // numeric(4)
    Timestamp c_since;        // date and time
    double c_credit_lim;      // signed numeric(2, 2)
    double c_discount;        // signed numeric(4, 4)
    double c_balance;         // signed numeric(12, 2)
    double c_ytd_payment;     // signed numeric(12, 2)
    char c_first[MAX_FIRST + 1];
    char c_middle[MAX_MIDDLE + 1];
    char c_last[MAX_LAST + 1];
    char c_phone[PHONE + 1];
    char c_credit[CREDIT + 1];  // "GC"=good, "BC"=bad
    char c_data[MAX_DATA + 1];  // miscellaneous information
    Address c_address;
    void deep_copy_from(const Customer& src);
    void generate(uint16_t c_w_id_, uint8_t c_d_id_, uint32_t c_id_, Timestamp t_);
    void print();
};

// Primary Key None
// Foreign Key (h_c_w_id, h_c_d_id, h_c_id) references (c_w_id, c_d_id, c_id)
// Foreign Key (h_w_id, h_d_id) references (d_w_id, d_id)
struct History {
    static const int HISTS_PER_CUST = 1;
    static const int MIN_DATA = 12;
    static const int MAX_DATA = 24;
    uint16_t h_c_w_id;
    uint8_t h_c_d_id;
    uint32_t h_c_id;
    uint16_t h_w_id;
    uint8_t h_d_id;
    Timestamp h_date;  // date and time
    double h_amount;   // signed numeric(6, 2)
    char h_data[MAX_DATA + 1];
    void deep_copy_from(const History& src);
    void generate(
        uint16_t h_c_w_id_, uint8_t h_c_d_id_, uint32_t h_c_id_, uint16_t h_w_id_, uint8_t h_d_id_);
    void print();
};

struct OrderSecondary;

// Primary Key (o_w_id, o_d_id, o_id)
// Foreign Key (o_w_id, o_d_id, o_c_id) references (c_w_id, c_d_id, c_id)
struct Order {
    using Key = OrderKey;
    static const int ORDS_PER_DIST = 3000;
    uint16_t o_w_id;
    uint8_t o_d_id;
    uint32_t o_id;  // 10000000 unique ids
    uint32_t o_c_id;
    uint8_t o_carrier_id;  // 10 unique ids or null
    uint8_t o_ol_cnt;      // numeric(2)
    uint8_t o_all_local;   // numeric(1)
    Timestamp o_entry_d;   // date and time
    void deep_copy_from(const Order& src);
    void generate(uint16_t o_w_id_, uint8_t o_d_id_, uint32_t o_id_, uint32_t o_c_id_);
    void print();
};

// Primary Key (no_w_id, no_d_id, no_o_id)
// Foreign Key (no_w_id, no_d_id, no_o_id) references (o_w_id, o_d_id, o_id)
struct NewOrder {
    using Key = NewOrderKey;
    uint16_t no_w_id;
    uint8_t no_d_id;
    uint32_t no_o_id;
    void deep_copy_from(const NewOrder& src);
    void generate(uint16_t no_w_id_, uint8_t no_d_id_, uint32_t no_o_id_);
    void print();
};

// Primary Key (ol_w_id, ol_d_id, ol_o_id, ol_number)
// Foregin Key (ol_w_id, ol_d_id, ol_o_id) references (o_w_id, o_d_id, o_id)
// Foreign Key (ol_supply_w_id, ol_i_id) references (s_w_id, s_i_id)
struct OrderLine {
    using Key = OrderLineKey;
    static const int MIN_ORDLINES_PER_ORD = 5;
    static const int MAX_ORDLINES_PER_ORD = 15;
    static const int DIST_INFO = 24;
    uint16_t ol_w_id;
    uint8_t ol_d_id;
    uint32_t ol_o_id;
    uint8_t ol_number;  // 15 unique ids
    uint32_t ol_i_id;   // 200000 unique ids
    uint16_t ol_supply_w_id;
    Timestamp ol_delivery_d;
    uint8_t ol_quantity;  // numeric(2)
    double ol_amount;     // signed numeric(6, 2)
    char ol_dist_info[DIST_INFO + 1];
    void deep_copy_from(const OrderLine& src);
    void generate(
        uint16_t ol_w_id_, uint8_t ol_d_id_, uint32_t ol_o_id_, uint8_t ol_number_,
        uint16_t ol_supply_w_id_, uint32_t ol_i_id_, int64_t o_entry_d_);
    void print();
};


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
