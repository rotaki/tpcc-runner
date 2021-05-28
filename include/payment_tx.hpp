#pragma once

#include <inttypes.h>

#include <cassert>
#include <cstdint>

#include "record_layout.hpp"
#include "tx_utils.hpp"

class PaymentTx {
public:
    PaymentTx(uint16_t w_id0) {
        input.generate(w_id0);
        output = {};
        output.w_id = input.w_id;
        output.d_id = input.d_id;
        output.c_id = input.c_id;
        output.c_w_id = input.c_w_id;
        output.c_d_id = input.c_d_id;
        output.h_amount = input.h_amount;
        output.h_date = get_timestamp();
    }

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        uint16_t c_w_id;
        uint8_t c_d_id;
        double h_amount;
        bool by_last_name;
        char c_last[Customer::MAX_LAST + 1];

        void generate(uint16_t w_id0) {
            const Config& c = get_config();
            uint16_t num_warehouses = c.get_num_warehouses();
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            h_amount = urand_double(100, 500000, 100);
            if (num_warehouses == 1 || urand_int(1, 100) <= 85) {
                c_w_id = w_id;
                c_d_id = d_id;
            } else {
                while ((c_w_id = urand_int(1, num_warehouses)) == w_id) {
                }
                assert(c_w_id != w_id);
                c_d_id = urand_int(1, District::DISTS_PER_WARE);
            }
            by_last_name = ((urand_int(1, 100) <= 60) ? 1 : 0);
            if (by_last_name) {
                c_id = Customer::UNUSED_ID;
                make_clast(c_last, nurand_int<255>(0, 999));
            } else {
                c_id = nurand_int<1023>(1, 3000);
            }
        }
    } input;

    struct Output {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        uint16_t c_w_id;
        uint8_t c_d_id;
        double h_amount;
        Timestamp h_date;

        double c_credit_lim;
        double c_discount;
        double c_balance;
        Timestamp c_since;

        Address w_address;
        Address d_address;
        Address c_address;

        char c_first[Customer::MAX_FIRST + 1];
        char c_middle[Customer::MAX_MIDDLE + 1];
        char c_last[Customer::MAX_LAST + 1];
        char c_phone[Customer::PHONE + 1];
        char c_credit[Customer::CREDIT + 1];
        char c_data[Customer::MAX_DATA + 1];
    } output;

    template <typename Transaction>
    Status run(Transaction& tx) {
        typename Transaction::Result res;

        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint32_t c_id = input.c_id;
        uint16_t c_w_id = input.c_w_id;
        uint8_t c_d_id = input.c_d_id;
        double h_amount = input.h_amount;
        const char* c_last = input.c_last;
        bool by_last_name = input.by_last_name;

        Warehouse w;
        Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
        res = tx.get_record(w, w_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        output.w_address.deep_copy_from(w.w_address);

        w.w_ytd += h_amount;
        res = tx.update_record(w_key, w);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        District d;
        District::Key d_key = District::Key::create_key(w_id, d_id);
        res = tx.get_record(d, d_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        output.d_address.deep_copy_from(d.d_address);

        d.d_ytd += h_amount;
        res = tx.update_record(d_key, d);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        Customer c;
        LOG_TRACE("by_last_name %s", by_last_name ? "true" : "false");
        if (by_last_name) {
            LOG_TRACE("c_last: %s", c_last);
            assert(c_id == Customer::UNUSED_ID);
            CustomerSecondary::Key c_last_key =
                CustomerSecondary::Key::create_key(c_w_id, c_d_id, c_last);
            res = tx.get_customer_by_last_name(c, c_last_key);
        } else {
            assert(c_id != Customer::UNUSED_ID);
            res = tx.get_record(c, Customer::Key::create_key(c_w_id, c_d_id, c_id));
        }
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        c_id = c.c_id;
        copy_cstr(output.c_first, c.c_first, sizeof(output.c_first));
        copy_cstr(output.c_middle, c.c_middle, sizeof(output.c_middle));
        copy_cstr(output.c_last, c.c_last, sizeof(output.c_last));
        output.c_address.deep_copy_from(c.c_address);
        copy_cstr(output.c_phone, c.c_phone, sizeof(output.c_phone));
        output.c_since = c.c_since;
        copy_cstr(output.c_credit, c.c_credit, sizeof(output.c_credit));
        output.c_credit_lim = c.c_credit_lim;
        output.c_discount = c.c_discount;
        output.c_balance = c.c_balance;

        if (c.c_credit[0] == 'B' && c.c_credit[1] == 'C') {
            copy_cstr(output.c_data, c.c_data, sizeof(output.c_data));
            modify_customer(c, w_id, d_id, c_w_id, c_d_id, h_amount);
            res = tx.update_record(Customer::Key::create_key(c_w_id, c_d_id, c_id), c);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
        }

        History h;
        create_history(h, w_id, d_id, c_id, c_w_id, c_d_id, h_amount, w.w_name, d.d_name);
        res = tx.insert_record(h);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        if (tx.commit()) {
            LOG_TRACE("commit success");
            return Status::SUCCESS;
        } else {
            LOG_TRACE("commit fail");
            return Status::SYSTEM_ABORT;
        }
    }

private:
    void modify_customer(
        Customer& c, uint16_t w_id, uint8_t d_id, uint16_t c_w_id, uint8_t c_d_id,
        double h_amount) {
        c.c_balance -= h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += 1;

        char new_data[Customer::MAX_DATA + 1];
        size_t len = snprintf(
            &new_data[0], Customer::MAX_DATA + 1,
            "| %4" PRIu32 " %2" PRIu8 " %4" PRIu16 " %2" PRIu16 " %4" PRIu16 " $%7.2f", c.c_id,
            c_d_id, c_w_id, d_id, w_id, h_amount);
        assert(len <= Customer::MAX_DATA);
        copy_cstr(&new_data[len], &c.c_data[0], sizeof(new_data) - len);
        copy_cstr(c.c_data, new_data, sizeof(c.c_data));
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
};
