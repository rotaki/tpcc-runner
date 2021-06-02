#pragma once

#include <inttypes.h>

#include <cassert>
#include <cstdint>

#include "record_key.hpp"
#include "record_layout.hpp"
#include "tx_utils.hpp"

class PaymentTx {
public:
    PaymentTx(uint16_t w_id0) {
        input.generate(w_id0);
        input.print();
    }

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        uint16_t c_w_id;
        uint8_t c_d_id;
        double h_amount;
        Timestamp h_date;
        bool by_last_name;
        char c_last[Customer::MAX_LAST + 1];

        void generate(uint16_t w_id0) {
            const Config& c = get_config();
            uint16_t num_warehouses = c.get_num_warehouses();
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            h_amount = urand_double(100, 500000, 100);
            h_date = get_timestamp();
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

        void print() {
            if (by_last_name) {
                LOG_TRACE(
                    "pay: w_id=%" PRIu16 " d_id=%" PRIu8 " c_w_id=%" PRIu16 " c_d_id=%" PRIu8
                    " h_amount=%.2f h_date=%ld by_last_name=t c_last=%s",
                    w_id, d_id, c_w_id, c_d_id, h_amount, h_date, c_last);
            } else {
                LOG_TRACE(
                    "pay: w_id=%" PRIu16 " d_id=%" PRIu8 " c_w_id=%" PRIu16 " c_d_id=%" PRIu8
                    " h_amount=%.2f h_date=%ld by_last_name=f c_id=%" PRIu32,
                    w_id, d_id, c_w_id, c_d_id, h_amount, h_date, c_id);
            }
        }

    } input;

    template <typename Transaction>
    Status run(Transaction& tx, Stat& stat, Output& out) {
        typename Transaction::Result res;

        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint32_t c_id = input.c_id;
        uint16_t c_w_id = input.c_w_id;
        uint8_t c_d_id = input.c_d_id;
        double h_amount = input.h_amount;
        Timestamp h_date = input.h_date;
        const char* c_last = input.c_last;
        bool by_last_name = input.by_last_name;

        out << w_id << d_id;

        Warehouse w;
        Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
        res = tx.get_record(w, w_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

        w.w_ytd += h_amount;
        res = tx.update_record(w_key, w);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

        District d;
        District::Key d_key = District::Key::create_key(w_id, d_id);
        res = tx.get_record(d, d_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

        d.d_ytd += h_amount;
        res = tx.update_record(d_key, d);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

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
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);
        c_id = c.c_id;

        out << c_id << c_d_id << c_w_id << h_amount << h_date;
        out << w.w_address << d.d_address;
        out << c.c_first << c.c_middle << c.c_last << c.c_address;
        out << c.c_phone << c.c_since << c.c_credit << c.c_credit_lim;
        out << c.c_discount << c.c_balance;

        c.c_balance -= h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += 1;

        if (c.c_credit[0] == 'B' && c.c_credit[1] == 'C') {
            out << c.c_data;
            modify_customer_data(c, w_id, d_id, c_w_id, c_d_id, h_amount);
        }

        res = tx.update_record(Customer::Key::create_key(c_w_id, c_d_id, c_id), c);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

        History h;
        create_history(h, w_id, d_id, c_id, c_w_id, c_d_id, h_amount, w.w_name, d.d_name);
        res = tx.insert_record(h);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

        if (tx.commit()) {
            LOG_TRACE("commit success");
            stat.num_commits[1]++;
            return Status::SUCCESS;
        } else {
            LOG_TRACE("commit fail");
            stat.num_sys_aborts[1]++;
            return Status::SYSTEM_ABORT;
        }
    }

private:
    void modify_customer_data(
        Customer& c, uint16_t w_id, uint8_t d_id, uint16_t c_w_id, uint8_t c_d_id,
        double h_amount) {
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

    template <typename Transaction>
    Status kill_tx(Transaction& tx, typename Transaction::Result res, Stat& stat) {
        return ::kill_tx(tx, res, stat, 1);
    }
};
