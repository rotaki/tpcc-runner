#include "transaction_runner.hpp"

#include <inttypes.h>

namespace TransactionRunnerUtils {
namespace NewOrderUtils {
void create_neworder(NewOrder& no, uint16_t w_id, uint16_t d_id, uint32_t o_id) {
    no.no_w_id = w_id;
    no.no_d_id = d_id;
    no.no_o_id = o_id;
}

void create_order(
    Order& o, uint16_t w_id, uint8_t d_id, uint32_t c_id, uint32_t o_id, uint8_t ol_cnt,
    bool is_remote) {
    o.o_w_id = w_id;
    o.o_d_id = d_id;
    o.o_c_id = c_id;
    o.o_id = o_id;
    o.o_carrier_id = 0;
    o.o_ol_cnt = ol_cnt;
    o.o_all_local = !is_remote;
    o.o_entry_d = get_timestamp();
}

void create_orderline(
    OrderLine& ol, uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_num, uint32_t ol_i_id,
    uint16_t ol_supply_w_id, uint8_t ol_quantity, double ol_amount, const Stock& s) {
    ol.ol_w_id = w_id;
    ol.ol_d_id = d_id;
    ol.ol_o_id = o_id;
    ol.ol_number = ol_num;
    ol.ol_i_id = ol_i_id;
    ol.ol_supply_w_id = ol_supply_w_id;
    ol.ol_delivery_d = 0;
    ol.ol_quantity = ol_quantity;
    ol.ol_amount = ol_amount;
    auto pick_sdist = [&]() -> const char* {
        switch (d_id) {
        case 1: return s.s_dist_01;
        case 2: return s.s_dist_02;
        case 3: return s.s_dist_03;
        case 4: return s.s_dist_04;
        case 5: return s.s_dist_05;
        case 6: return s.s_dist_06;
        case 7: return s.s_dist_07;
        case 8: return s.s_dist_08;
        case 9: return s.s_dist_09;
        case 10: return s.s_dist_10;
        default: return nullptr;  // BUG
        }
    };
    copy_cstr(ol.ol_dist_info, pick_sdist(), sizeof(ol.ol_dist_info));
}

void modify_stock(Stock& s, uint8_t ol_quantity, bool is_remote) {
    if (s.s_quantity > ol_quantity + 10)
        s.s_quantity -= ol_quantity;
    else
        s.s_quantity = (s.s_quantity - ol_quantity) + 91;
    s.s_order_cnt += 1;
    if (is_remote) s.s_remote_cnt += 1;
}
}  // namespace NewOrderTx

namespace PaymentUtils {
void modify_customer(
    Customer& c, uint16_t w_id, uint8_t d_id, uint16_t c_w_id, uint8_t c_d_id, double h_amount) {
    c.c_balance -= h_amount;
    c.c_ytd_payment += h_amount;
    c.c_payment_cnt += 1;

    char new_data[Customer::MAX_DATA + 1];
    if (c.c_credit[0] == 'B' && c.c_credit[1] == 'C') {
        size_t len = snprintf(
            &new_data[0], Customer::MAX_DATA + 1,
            "| %4" PRIu32 " %2" PRIu8 " %4" PRIu16 " %2" PRIu16 " %4" PRIu16 " $%7.2f", c.c_id,
            c_d_id, c_w_id, d_id, w_id, h_amount);
        assert(len <= Customer::MAX_DATA);
        copy_cstr(&new_data[len], &c.c_data[0], sizeof(new_data) - len);
        copy_cstr(c.c_data, new_data, sizeof(c.c_data));
    }
}

void create_history(
    History& h, uint16_t w_id, uint8_t d_id, uint32_t c_id, uint16_t c_w_id, uint8_t c_d_id,
    double h_amount, const char* w_name, const char* d_name) {
    h.h_c_id = c_id;
    h.h_c_d_id = c_d_id;
    h.h_c_w_id = c_w_id;
    h.h_d_id = d_id;
    h.h_w_id = w_id;
    h.h_date = get_timestamp();
    h.h_amount = h_amount;
    snprintf(h.h_data, sizeof(h.h_data), "%-10.10s    %.10s", w_name, d_name);
}


}  // namespace PaymentTx
}  // namespace TransactionRunnerUtils