#pragma once

#include <cstdint>
#include <cstring>
#include <ctime>
#include <string_view>

using Timestamp = time_t;
inline Timestamp get_timestamp() {
    return ::time(nullptr);
}

enum Storage {
    WAREHOUSE = 0,
    DISTRICT,
    CUSTOMER,
    HISTORY,
    ORDER,
    NEWORDER,
    ORDERLINE,
    ITEM,
    STOCK
};


struct Address {
    friend struct Warehouse;
    friend struct District;
    friend struct Customer;
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

private:
    Address(){};
    inline void deep_copy(const Address& src) {
        if (this != &src) {
            strcpy(street_1, src.street_1);
            strcpy(street_2, src.street_2);
            strcpy(city, src.city);
            strcpy(state, src.state);
            strcpy(zip, src.zip);
        }
    }
};

struct Record {
    virtual ~Record(){};
};

// Primary Key w_id
struct Warehouse : Record {
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    uint16_t w_id;  // 2*W unique ids
    double w_tax;   // signed numeric(4, 4)
    double w_ytd;   // signed numeric(12, 2)
    char w_name[MAX_NAME + 1];
    Address w_address;
    inline void deep_copy(const Warehouse& src) {
        if (this != &src) {
            w_id = src.w_id;
            w_tax = src.w_tax;
            w_ytd = src.w_ytd;
            strcpy(w_name, src.w_name);
            w_address.deep_copy(src.w_address);
        }
    }
};

// Primary Key (d_w_id, d_id)
// Foreign Key d_w_id references w_id
struct District : Record {
    static const int DISTS_PER_WARE = 10;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    uint8_t d_id;  // 20 unique ids
    uint16_t d_w_id;
    uint32_t d_next_o_id;  // 10000000 unique ids
    double d_tax;          // signed numeric(4, 4)
    double d_ytd;          // signed numeric(12, 2)
    char d_name[MAX_NAME + 1];
    Address d_address;
    inline void deep_copy(const District& src) {
        if (this != &src) {
            d_id = src.d_id;
            d_w_id = src.d_w_id;
            d_next_o_id = src.d_next_o_id;
            d_tax = src.d_tax;
            d_ytd = src.d_ytd;
            strcpy(d_name, src.d_name);
            d_address.deep_copy(src.d_address);
        }
    }
};

// Primary Key (c_w_id, c_d_id, c_id)
// Foreign Key (c_w_id, c_d_id) references (d_w_id, d_id)
struct Customer : Record {
    static const int CUSTS_PER_DIST = 3000;
    static const int MIN_FIRST = 8;
    static const int MAX_FIRST = 16;
    static const int MAX_MIDDLE = 2;
    static const int MAX_LAST = 16;
    static const int PHONE = 16;
    static const int CREDIT = 2;
    static const int MIN_DATA = 300;
    static const int MAX_DATA = 500;
    uint32_t c_id;  // 96000 unique ids
    uint8_t c_d_id;
    uint16_t c_w_id;
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
    inline void deep_copy(const Customer& src) {
        if (this != &src) {
            c_id = src.c_id;
            c_d_id = src.c_d_id;
            c_w_id = src.c_w_id;
            c_payment_cnt = src.c_payment_cnt;
            c_delivery_cnt = src.c_delivery_cnt;
            c_since = src.c_since;
            c_credit_lim = src.c_credit_lim;
            c_discount = src.c_discount;
            c_balance = src.c_balance;
            c_ytd_payment = src.c_ytd_payment;
            strcpy(c_first, src.c_first);
            strcpy(c_middle, src.c_middle);
            strcpy(c_last, src.c_last);
            strcpy(c_phone, src.c_phone);
            strcpy(c_credit, src.c_credit);
            strcpy(c_data, src.c_data);
            c_address.deep_copy(src.c_address);
        }
    }
};

// Primary Key None
// Foreign Key (h_c_w_id, h_c_d_id, h_c_id) references (c_w_id, c_d_id, c_id)
// Foreign Key (h_w_id, h_d_id) references (d_w_id, d_id)
struct History : Record {
    static const int HISTS_PER_CUST = 1;
    static const int MIN_DATA = 12;
    static const int MAX_DATA = 24;
    uint32_t h_c_id;
    uint8_t h_c_d_id;
    uint16_t h_c_w_id;
    uint8_t h_d_id;
    uint16_t h_w_id;
    Timestamp h_date;  // date and time
    double h_amount;   // signed numeric(6, 2)
    char h_data[MAX_DATA + 1];
    inline void deep_copy(const History& src) {
        if (this != &src) {
            h_c_id = src.h_c_id;
            h_c_d_id = src.h_c_d_id;
            h_c_w_id = src.h_c_w_id;
            h_d_id = src.h_d_id;
            h_w_id = src.h_w_id;
            h_date = src.h_date;
            h_amount = src.h_amount;
            strcpy(h_data, src.h_data);
        }
    }
};

// Primary Key (o_w_id, o_d_id, o_id)
// Foreign Key (o_w_id, o_d_id, o_c_id) references (c_w_id, c_d_id, c_id)
struct Order : Record {
    static const int ORDS_PER_DIST = 3000;
    uint32_t o_id;  // 10000000 unique ids
    uint8_t o_d_id;
    uint16_t o_w_id;
    uint32_t o_c_id;
    uint8_t o_carrier_id;  // 10 unique ids or null
    uint8_t o_ol_cnt;      // numeric(2)
    uint8_t o_all_local;   // numeric(1)
    Timestamp o_entry_d;   // date and time
    inline void deep_copy(const Order& src) {
        if (this != &src) {
            o_id = src.o_id;
            o_d_id = src.o_d_id;
            o_w_id = src.o_w_id;
            o_c_id = src.o_c_id;
            o_carrier_id = src.o_carrier_id;
            o_ol_cnt = src.o_ol_cnt;
            o_all_local = src.o_all_local;
            o_entry_d = src.o_entry_d;
        }
    }
};

// Primary Key (no_w_id, no_d_id, no_o_id)
// Foreign Key (no_w_id, no_d_id, no_o_id) references (o_w_id, o_d_id, o_id)
struct NewOrder : Record {
    uint32_t no_o_id;
    uint8_t no_d_id;
    uint16_t no_w_id;
    inline void deep_copy(const NewOrder& src) {
        if (this != &src) {
            no_o_id = src.no_o_id;
            no_d_id = src.no_d_id;
            no_w_id = src.no_w_id;
        }
    }
};

// Primary Key (ol_w_id, ol_d_id, ol_o_id, ol_number)
// Foregin Key (ol_w_id, ol_d_id, ol_o_id) references (o_w_id, o_d_id, o_id)
// Foreign Key (ol_supply_w_id, ol_i_id) references (s_w_id, s_i_id)
struct OrderLine : Record {
    static const int MIN_ORDLINES_PER_ORD = 5;
    static const int MAX_ORDLINES_PER_ORD = 15;
    static const int DIST_INFO = 24;
    uint32_t ol_o_id;
    uint8_t ol_d_id;
    uint16_t ol_w_id;
    uint8_t ol_number;  // 15 unique ids
    uint32_t ol_i_id;   // 200000 unique ids
    uint16_t ol_supply_w_id;
    Timestamp ol_delivery_d;
    uint8_t ol_quantity;  // numeric(2)
    double ol_amount;     // signed numeric(6, 2)
    char ol_dist_info[DIST_INFO + 1];
    inline void deep_copy(const OrderLine& src) {
        if (this != &src) {
            ol_o_id = src.ol_o_id;
            ol_d_id = src.ol_d_id;
            ol_w_id = src.ol_w_id;
            ol_number = src.ol_number;
            ol_i_id = src.ol_i_id;
            ol_supply_w_id = src.ol_supply_w_id;
            ol_delivery_d = src.ol_delivery_d;
            ol_quantity = src.ol_quantity;
            ol_amount = src.ol_amount;
            strcpy(ol_dist_info, src.ol_dist_info);
        }
    }
};

// Primary Key i_id
struct Item : Record {
    static const int ITEMS = 100000;
    static const int MIN_NAME = 14;
    static const int MAX_NAME = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    uint32_t i_id;     // 200000 unique ids
    uint32_t i_im_id;  // 200000 unique ids
    double i_price;    // numeric(5, 2)
    char i_name[MAX_NAME + 1];
    char i_data[MAX_DATA + 1];
    inline void deep_copy(const Item& src) {
        if (this != &src) {
            i_id = src.i_id;
            i_im_id = src.i_im_id;
            i_price = src.i_price;
            strcpy(i_name, src.i_name);
            strcpy(i_data, src.i_data);
        }
    }
};

// Primary Key (s_w_id, s_i_id)
// Foreign Key s_w_id references w_id
struct Stock : Record {
    static const int STOCKS_PER_WARE = 100000;
    static const int DIST = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    uint32_t s_i_id;  // 200000 unique ids
    uint16_t s_w_id;
    int16_t s_quantity;     // signed numeric(4)
    uint32_t s_ytd;         // numeric(8)
    uint16_t s_order_cnt;   // numeric(4)
    uint16_t s_remote_cnt;  // numeric(4)
    char s_dist_01[DIST + 1];
    char s_dist_02[DIST + 1];
    char s_dist_03[DIST + 1];
    char s_dist_04[DIST + 1];
    char s_dist_05[DIST + 1];
    char s_dist_06[DIST + 1];
    char s_dist_07[DIST + 1];
    char s_dist_08[DIST + 1];
    char s_dist_09[DIST + 1];
    char s_dist_10[DIST + 1];
    char s_data[MAX_DATA + 1];

    inline void deep_copy(const Stock& src) {
        if (this != &src) {
            s_i_id = src.s_i_id;
            s_w_id = src.s_w_id;
            s_quantity = src.s_quantity;
            s_ytd = src.s_ytd;
            s_order_cnt = src.s_order_cnt;
            s_remote_cnt = src.s_remote_cnt;
            strcpy(s_dist_01, src.s_dist_01);
            strcpy(s_dist_02, src.s_dist_02);
            strcpy(s_dist_03, src.s_dist_03);
            strcpy(s_dist_04, src.s_dist_04);
            strcpy(s_dist_05, src.s_dist_05);
            strcpy(s_dist_06, src.s_dist_06);
            strcpy(s_dist_07, src.s_dist_07);
            strcpy(s_dist_08, src.s_dist_08);
            strcpy(s_dist_09, src.s_dist_09);
            strcpy(s_dist_10, src.s_dist_10);
            strcpy(s_data, src.s_data);
        }
    }
};
