#pragma once

#include <cstdint>
#include <deque>

#include "record_layout.hpp"
#include "tx_utils.hpp"

class OrderStatusTx {
public:
    OrderStatusTx(uint16_t w_id0) {
        input.generate(w_id0);
        output = {};
        output.w_id = input.w_id;
        output.d_id = input.d_id;
        output.c_id = input.c_id;
    }

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
    } input;

    struct Output {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;

        double c_balance;
        uint32_t o_id;
        Timestamp o_entry_d;
        uint8_t o_carrier_id;

        char c_first[Customer::MAX_FIRST + 1];
        char c_middle[Customer::MAX_MIDDLE + 1];
        char c_last[Customer::MAX_LAST + 1];

        struct OrderLineSubset {
            uint32_t ol_i_id;
            uint16_t ol_supply_w_id;
            uint8_t ol_quantity;
            double ol_amount;
            Timestamp ol_delivery_d;
            OrderLineSubset(
                uint32_t ol_i_id_, uint16_t ol_supply_w_id_, uint8_t ol_quantity_,
                double ol_amount_, Timestamp ol_delivery_d_)
                : ol_i_id(ol_i_id_)
                , ol_supply_w_id(ol_supply_w_id_)
                , ol_quantity(ol_quantity_)
                , ol_amount(ol_amount_)
                , ol_delivery_d(ol_delivery_d_) {}
        };

        std::deque<OrderLineSubset> lines;

    } output;


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
        output.c_balance = c.c_balance;
        copy_cstr(output.c_first, c.c_first, sizeof(output.c_first));
        copy_cstr(output.c_middle, c.c_middle, sizeof(output.c_middle));
        copy_cstr(output.c_last, c.c_last, sizeof(output.c_last));

        Order o;
        res = tx.get_order_by_customer_id(o, OrderSecondary::Key::create_key(c_w_id, c_d_id, c_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        output.o_id = o.o_id;
        output.o_entry_d = o.o_entry_d;
        output.o_carrier_id = o.o_carrier_id;

        OrderLine::Key low = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_c_id, 0);
        OrderLine::Key up = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_c_id + 1, 0);

        auto& lines = output.lines;

        res = tx.template range_query<OrderLine>(low, up, [&lines](OrderLine& ol) {
            lines.emplace_back(
                ol.ol_i_id, ol.ol_supply_w_id, ol.ol_quantity, ol.ol_amount, ol.ol_delivery_d);
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