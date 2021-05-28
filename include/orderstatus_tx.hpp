#pragma once

#include <cstdint>

#include "record_layout.hpp"
#include "tx_utils.hpp"

class OrderStatusTx {
public:
    OrderStatusTx(uint16_t w_id0) { input.generate(w_id0); }

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        bool by_last_name;
        char c_last[Customer::MAX_DATA + 1];

        void generate(uint16_t w_id0) {
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            by_last_name = ((urand_int(1, 100) <= 60) ? 1 : 0);
            if (by_last_name) {
                c_id = Customer::UNUSED_ID;
                make_clast(c_last, nurand_int<255>(0, 999));
            } else {
                c_id = nurand_int<1023>(1, 3000);
            }
        }
    };

    struct Output {};

    Input input;
    Output output;

    template <typename Transaction>
    Status run(Transaction& tx) {
        typename Transaction::Result res;

        uint16_t c_w_id = input.w_id;
        uint8_t c_d_id = input.d_id;
        uint32_t c_id = input.c_id;
        const char* c_last = input.c_last;
        bool by_last_name = input.by_last_name;

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

        Order o;
        res = tx.get_order_by_customer_id(o, OrderSecondary::Key::create_key(c_w_id, c_d_id, c_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        std::set<uint32_t> ol_i_ids;
        std::set<uint16_t> ol_supply_w_ids;
        std::set<uint8_t> ol_quantities;
        std::set<double> ol_amounts;
        std::set<Timestamp> ol_delivery_ds;
        OrderLine::Key low = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_c_id, 0);
        OrderLine::Key up = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_c_id + 1, 0);
        res = tx.template range_query<OrderLine>(
            low, up,
            [&ol_i_ids, &ol_supply_w_ids, &ol_quantities, &ol_amounts,
             &ol_delivery_ds](OrderLine& ol) {
                ol_i_ids.insert(ol.ol_i_id);
                ol_supply_w_ids.insert(ol.ol_supply_w_id);
                ol_quantities.insert(ol.ol_quantity);
                ol_amounts.insert(ol.ol_amount);
                ol_delivery_ds.insert(ol.ol_delivery_d);
            });
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
};