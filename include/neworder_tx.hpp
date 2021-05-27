#pragma once

#include <cstdint>

#include "record_layout.hpp"
#include "tx_utils.hpp"

class NewOrderTx {
public:
    NewOrderTx() { input.generate(1); }

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        uint8_t ol_cnt;
        bool rbk;
        bool is_remote;
        struct {
            uint32_t ol_i_id;
            uint16_t ol_supply_w_id;
            uint8_t ol_quantity;
        } items[OrderLine::MAX_ORDLINES_PER_ORD];

        void generate(uint16_t w_id0) {
            const Config& c = get_config();
            uint16_t num_warehouses = c.get_num_warehouses();
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            c_id = nurand_int<1023>(1, Customer::CUSTS_PER_DIST);
            rbk = (urand_int(1, 100) == 1 ? 1 : 0);
            is_remote = (urand_int(1, 100) == 1 ? 1 : 0);
            ol_cnt = urand_int(OrderLine::MIN_ORDLINES_PER_ORD, OrderLine::MAX_ORDLINES_PER_ORD);
            for (int i = 1; i <= ol_cnt; i++) {
                if (i == ol_cnt && rbk) {
                    items[i - 1].ol_i_id = Item::UNUSED_ID; /* set to an unused value */
                } else {
                    items[i - 1].ol_i_id = nurand_int<8191>(1, Item::ITEMS);
                }
                if (is_remote && num_warehouses > 1) {
                    uint16_t remote_w_id;
                    while ((remote_w_id = urand_int(1, num_warehouses)) == w_id) {
                    }
                    assert(remote_w_id != w_id);
                    items[i - 1].ol_supply_w_id = remote_w_id;
                } else {
                    items[i - 1].ol_supply_w_id = w_id;
                }
                items[i - 1].ol_quantity = urand_int(1, 10);
            }
        }
    };

    struct Output {};

    Input input;
    Output output;

    template <typename Transaction>
    Status run(Transaction& tx) {
        typename Transaction::Result res;

        bool is_remote = input.is_remote;
        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint32_t c_id = input.c_id;
        uint8_t ol_cnt = input.ol_cnt;

        Warehouse w;
        res = tx.get_record(w, Warehouse::Key::create_key(w_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        District d;
        District::Key d_key = District::Key::create_key(w_id, d_id);
        res = tx.get_record(d, d_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
        uint32_t o_id = (d.d_next_o_id)++;
        res = tx.update_record(d_key, d);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        Customer c;
        res = tx.get_record(c, Customer::Key::create_key(w_id, d_id, c_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        NewOrder no;
        create_neworder(no, w_id, d_id, o_id);
        res = tx.insert_record(no);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        Order o;
        create_order(o, w_id, d_id, c_id, o_id, ol_cnt, is_remote);
        res = tx.insert_record(o);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        for (uint8_t ol_num = 1; ol_num <= ol_cnt; ol_num++) {
            uint32_t ol_i_id = input.items[ol_num - 1].ol_i_id;
            uint16_t ol_supply_w_id = input.items[ol_num - 1].ol_supply_w_id;
            uint8_t ol_quantity = input.items[ol_num - 1].ol_quantity;

            Item i;
            if (ol_i_id == Item::UNUSED_ID) return Status::USER_ABORT;
            res = tx.get_record(i, Item::Key::create_key(ol_i_id));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);

            Stock s;
            Stock::Key s_key = Stock::Key::create_key(ol_supply_w_id, ol_i_id);
            res = tx.get_record(s, s_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
            modify_stock(s, ol_quantity, is_remote);

            res = tx.update_record(s_key, s);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);

            double ol_amount = ol_quantity * i.i_price;
            OrderLine ol;
            create_orderline(
                ol, w_id, d_id, o_id, ol_num, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, s);
            res = tx.insert_record(ol);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
        }

        if (tx.commit()) {
            LOG_TRACE("commit success");
            return Status::SUCCESS;
        } else {
            LOG_TRACE("commit fail");
            return Status::SYSTEM_ABORT;
        }
    }

private:
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
};