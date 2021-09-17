#include "benchmarks/tpcc/include/record_layout.hpp"

#include <inttypes.h>

#include "utils/logger.hpp"
#include "utils/utils.hpp"


void Item::deep_copy_from(const Item& src) {
    if (this != &src) {
        i_id = src.i_id;
        i_im_id = src.i_im_id;
        i_price = src.i_price;
        copy_cstr(i_name, src.i_name, sizeof(i_name));
        copy_cstr(i_data, src.i_data, sizeof(i_data));
    }
}

void Item::generate(uint32_t i_id_) {
    i_id = i_id_;                             // 200000 unique ids
    i_im_id = urand_int(1, 10000);            // 200000 unique ids
    i_price = urand_double(100, 10000, 100);  // numeric(5, 2)
    make_random_astring(i_name, Item::MIN_NAME, Item::MAX_NAME);
    make_random_astring(i_data, Item::MIN_DATA, Item::MAX_DATA);
    if (urand_int(0, 99) < 10) make_original(i_data);
}

void Item::print() {
    LOG_TRACE(
        "[ITEM] i_id:%" PRIu32 " i_im_id:%" PRIu32 " i_price:%lf i_name:%s i_data:%s", i_id,
        i_im_id, i_price, i_name, i_data);
}

void Address::deep_copy_from(const Address& src) {
    if (this != &src) {
        copy_cstr(street_1, src.street_1, sizeof(street_1));
        copy_cstr(street_2, src.street_2, sizeof(street_2));
        copy_cstr(city, src.city, sizeof(city));
        copy_cstr(state, src.state, sizeof(state));
        copy_cstr(zip, src.zip, sizeof(zip));
    }
}

void Warehouse::deep_copy_from(const Warehouse& src) {
    if (this != &src) {
        w_id = src.w_id;
        w_tax = src.w_tax;
        w_ytd = src.w_ytd;
        copy_cstr(w_name, src.w_name, sizeof(w_name));
        w_address.deep_copy_from(src.w_address);
    }
}
void Warehouse::generate(uint16_t w_id_) {
    w_id = w_id_;                          // 2*W unique ids
    w_tax = urand_double(0, 2000, 10000);  // signed numeric(4, 4)
    w_ytd = 300000;                        // signed numeric(12, 2)
    make_random_astring(w_name, Warehouse::MIN_NAME, Warehouse::MAX_NAME);
    make_random_address(w_address);
}

void Warehouse::print() {
    LOG_TRACE(
        "[WARE] w_id:%" PRIu16
        " w_tax:%lf w_ytd:%lf w_name:%s street_1:%s street_2:%s city:%s state:%s zip:%s",
        w_id, w_tax, w_ytd, w_name, w_address.street_1, w_address.street_2, w_address.city,
        w_address.state, w_address.zip);
}

void Stock::deep_copy_from(const Stock& src) {
    if (this != &src) {
        s_i_id = src.s_i_id;
        s_w_id = src.s_w_id;
        s_quantity = src.s_quantity;
        s_ytd = src.s_ytd;
        s_order_cnt = src.s_order_cnt;
        s_remote_cnt = src.s_remote_cnt;
        memcpy(s_dist[0], src.s_dist[0], sizeof(10 * (Stock::DIST + 1)));
        copy_cstr(s_data, src.s_data, sizeof(s_data));
    }
}

void Stock::generate(uint16_t s_w_id_, uint32_t s_i_id_) {
    s_i_id = s_i_id_;  // 200000 unique ids
    s_w_id = s_w_id_;
    s_quantity = urand_int(10, 100);  // signed numeric(4)
    s_ytd = 0;                        // numeric(8)
    s_order_cnt = 0;                  // numeric(4)
    s_remote_cnt = 0;                 // numeric(4)
    for (size_t i = 0; i < 10; i++) {
        make_random_astring(s_dist[i], Stock::DIST, Stock::DIST);
    }
    make_random_astring(s_data, Stock::MIN_DATA, Stock::MAX_DATA);
    if (urand_int(0, 99) < 10) make_original(s_data);
}

void Stock::print() {
    LOG_TRACE(
        "[STO] s_w_id:%" PRIu16 " s_i_id:%" PRIu32 " s_quantity:%" PRIu32 " s_ytd:%" PRIu32
        " s_order_cnt:%" PRIu16 " s_remote:%" PRIu16
        " s_dist_01:%s s_dist_02:%s s_dist_03:%s s_dist_04:%s s_dist_05:%s s_dist_06:%s s_dist_07:%s s_dist_08:%s s_dist_09:%s s_dist_10:%s",
        s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist[0], s_dist[1],
        s_dist[2], s_dist[3], s_dist[4], s_dist[5], s_dist[6], s_dist[7], s_dist[8], s_dist[9]);
}

void District::deep_copy_from(const District& src) {
    if (this != &src) {
        d_id = src.d_id;
        d_w_id = src.d_w_id;
        d_next_o_id = src.d_next_o_id;
        d_tax = src.d_tax;
        d_ytd = src.d_ytd;
        copy_cstr(d_name, src.d_name, sizeof(d_name));
        d_address.deep_copy_from(src.d_address);
    }
}

void District::generate(uint16_t d_w_id_, uint8_t d_id_) {
    d_w_id = d_w_id_;
    d_id = d_id_;
    d_next_o_id = 3001;                    // 10000000 unique ids
    d_tax = urand_double(0, 2000, 10000);  // signed numeric(4, 4)
    d_ytd = 30000.00;                      // signed numeric(12, 2)
    make_random_astring(d_name, District::MIN_NAME, District::MAX_NAME);
    make_random_address(d_address);
}

void District::print() {
    LOG_TRACE(
        "[DIST] d_w_id:%" PRIu16 " d_id:%" PRIu8 " d_next_o_id:%" PRIu32
        " d_tax:%lf d_ytd:%lf d_name:%s street_1:%s street_2:%s city:%s state:%s zip:%s",
        d_w_id, d_id, d_next_o_id, d_tax, d_ytd, d_name, d_address.street_1, d_address.street_2,
        d_address.city, d_address.state, d_address.zip);
}

void Customer::deep_copy_from(const Customer& src) {
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
        copy_cstr(c_first, src.c_first, sizeof(c_first));
        copy_cstr(c_middle, src.c_middle, sizeof(c_middle));
        copy_cstr(c_last, src.c_last, sizeof(c_last));
        copy_cstr(c_phone, src.c_phone, sizeof(c_phone));
        copy_cstr(c_credit, src.c_credit, sizeof(c_credit));
        copy_cstr(c_data, src.c_data, sizeof(c_data));
        c_address.deep_copy_from(src.c_address);
    }
}
void Customer::generate(uint16_t c_w_id_, uint8_t c_d_id_, uint32_t c_id_, Timestamp t_) {
    c_id = c_id_;  // 96000 unique ids
    c_d_id = c_d_id_;
    c_w_id = c_w_id_;
    c_payment_cnt = 1;                          // numeric(4)
    c_delivery_cnt = 0;                         // numeric(4)
    c_since = t_;                               // date and time
    c_credit_lim = 50000;                       // signed numeric(2, 2)
    c_discount = urand_double(0, 5000, 10000);  // signed numeric(4, 4)
    c_balance = -10.00;                         // signed numeric(12, 2)
    c_ytd_payment = 10.00;                      // signed numeric(12, 2)
    make_random_astring(c_first, Customer::MIN_FIRST, Customer::MAX_FIRST);
    copy_cstr(c_middle, "OE", sizeof(c_middle));
    (c_id <= 1000 ? make_clast(c_last, c_id - 1)
                  : make_clast(c_last, nurand_int<255, true>(0, 999)));
    make_random_nstring(c_phone, Customer::PHONE, Customer::PHONE);
    (urand_int(0, 99) < 10 ? copy_cstr(c_credit, "BC", sizeof(c_credit))
                           : copy_cstr(c_credit, "GC", sizeof(c_credit)));
    ;
    make_random_astring(c_data, Customer::MIN_DATA, Customer::MAX_DATA);
    make_random_address(c_address);
}

void Customer::print() {
    LOG_TRACE(
        "[CUST] c_w_id:%" PRIu16 " c_d_id:%" PRIu8 " c_id:%" PRIu32 " c_payment_cnt:%" PRIu16
        " c_delivery_cnt:%" PRIu16 " c_since:%" PRId64
        " c_credit_lim:%lf c_discount:%lf c_balance:%lf c_ytd_payment:%lf c_first:%s c_middle:%s c_last:%s c_phone:%s c_credit:%s c_data:%s street_1:%s street_2:%s city:%s state:%s zip:%s",
        c_w_id, c_d_id, c_id, c_payment_cnt, c_delivery_cnt, c_since, c_credit_lim, c_discount,
        c_balance, c_ytd_payment, c_first, c_middle, c_last, c_phone, c_credit, c_data,
        c_address.street_1, c_address.street_2, c_address.city, c_address.state, c_address.zip);
}

void History::deep_copy_from(const History& src) {
    if (this != &src) {
        h_c_id = src.h_c_id;
        h_c_d_id = src.h_c_d_id;
        h_c_w_id = src.h_c_w_id;
        h_d_id = src.h_d_id;
        h_w_id = src.h_w_id;
        h_date = src.h_date;
        h_amount = src.h_amount;
        copy_cstr(h_data, src.h_data, sizeof(h_data));
    }
}

void History::generate(
    uint16_t h_c_w_id_, uint8_t h_c_d_id_, uint32_t h_c_id_, uint16_t h_w_id_, uint8_t h_d_id_) {
    h_c_id = h_c_id_;
    h_c_d_id = h_c_d_id_;
    h_c_w_id = h_c_w_id_;
    h_d_id = h_d_id_;
    h_w_id = h_w_id_;
    h_date = get_timestamp();
    h_amount = 10.00;  // signed numeric(6, 2)
    make_random_astring(h_data, History::MIN_DATA, History::MAX_DATA);
}

void History::print() {
    LOG_TRACE(
        "[HIST] h_c_w_id:%" PRIu16 " h_c_d_id:%" PRIu8 " h_c_id:%" PRIu32 " h_w_id:%" PRIu16
        " h_d_id:%" PRIu8 " h_date:%" PRId64,
        h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id, h_date);
}

void Order::deep_copy_from(const Order& src) {
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

void Order::generate(uint16_t o_w_id_, uint8_t o_d_id_, uint32_t o_id_, uint32_t o_c_id_) {
    o_id = o_id_;  // 10000000 unique ids
    o_d_id = o_d_id_;
    o_w_id = o_w_id_;
    o_c_id = o_c_id_;
    o_carrier_id = (o_id < 2101 ? urand_int(1, 10) : 0);  // 10 unique ids or null
    o_ol_cnt =
        urand_int(OrderLine::MIN_ORDLINES_PER_ORD, OrderLine::MAX_ORDLINES_PER_ORD);  // numeric(2)
    o_all_local = 1;                                                                  // numeric(1)
    o_entry_d = get_timestamp();
}

void Order::print() {
    LOG_TRACE(
        "[ORD] o_w_id:%" PRIu16 " o_d_id:%" PRIu8 " o_id:%" PRIu32 " o_c_id:%" PRIu32
        " o_carrier_id:%" PRIu8 " o_ol_cnt:%" PRIu8 " o_all_local:%" PRIu8 " o_entry_d:%" PRId64,
        o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d);
}

void NewOrder::deep_copy_from(const NewOrder& src) {
    if (this != &src) {
        no_o_id = src.no_o_id;
        no_d_id = src.no_d_id;
        no_w_id = src.no_w_id;
    }
}
void NewOrder::generate(uint16_t no_w_id_, uint8_t no_d_id_, uint32_t no_o_id_) {
    no_o_id = no_o_id_;
    no_d_id = no_d_id_;
    no_w_id = no_w_id_;
}

void NewOrder::print() {
    LOG_TRACE(
        "[NORD] no_w_id:%" PRIu16 " no_d_id:%" PRIu8 " no_o_id:%" PRIu32, no_w_id, no_d_id,
        no_o_id);
}

void OrderLine::deep_copy_from(const OrderLine& src) {
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
        copy_cstr(ol_dist_info, src.ol_dist_info, sizeof(ol_dist_info));
    }
}

void OrderLine::generate(
    uint16_t ol_w_id_, uint8_t ol_d_id_, uint32_t ol_o_id_, uint8_t ol_number_,
    uint16_t ol_supply_w_id_, uint32_t ol_i_id_, int64_t o_entry_d_) {
    ol_w_id = ol_w_id_;
    ol_d_id = ol_d_id_;
    ol_o_id = ol_o_id_;
    ol_number = ol_number_;  // 15 unique ids
    ol_i_id = ol_i_id_;      // 200000 unique ids
    ol_supply_w_id = ol_supply_w_id_;
    ol_delivery_d = (ol_o_id < 2101 ? o_entry_d_ : 0);
    ol_quantity = 5;                                                     // numeric(2)
    ol_amount = (ol_o_id < 2101 ? 0.00 : urand_double(1, 999999, 100));  // signed numeric(6, 2)
    make_random_astring(ol_dist_info, OrderLine::DIST_INFO, OrderLine::DIST_INFO);
}

void OrderLine::print() {
    LOG_TRACE(
        "[ORDL] ol_w_id:%" PRIu16 " ol_d_id:%" PRIu8 " ol_o_id:%" PRIu32 " ol_number:%" PRIu8
        " ol_i_id:%" PRIu32 " ol_supply_w_id:%" PRIu16 " ol_deliery_d:%" PRId64
        " ol_quantity:%" PRIu8 " ol_amount:%lf ol_dist_info:%s",
        ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity,
        ol_amount, ol_dist_info);
}
