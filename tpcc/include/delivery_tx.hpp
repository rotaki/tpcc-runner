#pragma once

#include <inttypes.h>

#include <cstdint>

#include "logger.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "tx_utils.hpp"


class DeliveryTx {
public:
    DeliveryTx(uint16_t w_id0) {
        input.generate(w_id0);
        input.print();
    }

    struct Input {
        uint16_t w_id;
        uint8_t o_carrier_id;
        Timestamp ol_delivery_d;

        void generate(uint16_t w_id0) {
            w_id = w_id0;
            o_carrier_id = urand_int(1, 10);
            ol_delivery_d = get_timestamp();
        }

        void print() {
            LOG_TRACE(
                "del: w_id=%" PRIu16 " o_carrier_id=%" PRIu8 " ol_delivery_d=%ld", w_id,
                o_carrier_id, ol_delivery_d);
        }
    } input;

    template <typename Transaction>
    Status run(Transaction& tx, Stat& stat, Output& out) {
        typename Transaction::Result res;
        TxHelper<Transaction> helper(tx, stat[TxType::Delivery]);

        uint16_t w_id = input.w_id;
        uint8_t o_carrier_id = input.o_carrier_id;

        out << w_id << o_carrier_id;

        for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            const NewOrder* no;
            NewOrder::Key no_low = NewOrder::Key::create_key(w_id, d_id, 0);
            res = tx.get_neworder_with_smallest_key_no_less_than(no, no_low);
            if (res == Transaction::Result::FAIL) continue;
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res);
            res = tx.template delete_record<NewOrder>(NewOrder::Key::create_key(*no));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res);

            Order* o;
            Order::Key o_key = Order::Key::create_key(w_id, d_id, no->no_o_id);
            res = tx.prepare_record_for_update(o, o_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res);
            o->o_carrier_id = o_carrier_id;

            double total_ol_amount = 0.0;
            OrderLine::Key o_low = OrderLine::Key::create_key(o->o_w_id, o->o_d_id, o->o_id, 0);
            OrderLine::Key o_up = OrderLine::Key::create_key(o->o_w_id, o->o_d_id, o->o_id + 1, 0);
            res =
                tx.template range_update<OrderLine>(o_low, o_up, [&total_ol_amount](OrderLine& ol) {
                    ol.ol_delivery_d = get_timestamp();
                    total_ol_amount += ol.ol_amount;
                });
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res);

            Customer* c;
            Customer::Key c_key = Customer::Key::create_key(w_id, d_id, o->o_c_id);
            res = tx.prepare_record_for_update(c, c_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res);
            c->c_balance += total_ol_amount;
            c->c_delivery_cnt += 1;

            out << d_id << o->o_id;
        }

        return helper.commit();
    };
};
