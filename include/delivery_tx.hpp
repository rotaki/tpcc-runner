#pragma once

#include <cstdint>

#include "record_layout.hpp"
#include "tx_utils.hpp"

class DeliveryTx {
public:
    DeliveryTx() { input.generate(0); }

    struct Input {
        uint16_t w_id;
        uint8_t o_carrier_id;
        Timestamp ol_delivery_d;

        void generate(uint16_t w_id0) {
            w_id = w_id0;
            o_carrier_id = urand_int(1, 10);
            ol_delivery_d = get_timestamp();
        }
    };

    struct Output {};

    Input input;
    Output output;

    template <typename Transaction>
    Status run(Transaction& tx) {
        typename Transaction::Result res;

        uint16_t w_id = input.w_id;
        uint8_t o_carrier_id = input.o_carrier_id;

        std::vector<uint8_t> delivery_skipped_dists;
        for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            NewOrder no;
            NewOrder::Key no_low = NewOrder::Key::create_key(w_id, d_id, 0);
            res = tx.get_neworder_with_smallest_key_no_less_than(no, no_low);
            if (res == Transaction::Result::FAIL) {
                delivery_skipped_dists.emplace_back(d_id);
                continue;
            }
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
            res = tx.template delete_record<NewOrder>(NewOrder::Key::create_key(no));

            Order o;
            Order::Key o_key = Order::Key::create_key(w_id, d_id, no.no_o_id);
            res = tx.get_record(o, o_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
            o.o_carrier_id = o_carrier_id;
            res = tx.update_record(o_key, o);
            if (not_succeeded(tx, res)) return kill_tx(tx, res);

            double total_ol_amount = 0.0;
            OrderLine::Key o_low = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_id, 0);
            OrderLine::Key o_up = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_id + 1, 0);
            res =
                tx.template range_update<OrderLine>(o_low, o_up, [&total_ol_amount](OrderLine& ol) {
                    ol.ol_amount = get_timestamp();
                    total_ol_amount += ol.ol_amount;
                });
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);

            Customer c;
            Customer::Key c_key = Customer::Key::create_key(w_id, d_id, o.o_c_id);
            res = tx.get_record(c, c_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
            c.c_balance += total_ol_amount;
            c.c_delivery_cnt += 1;
            res = tx.update_record(c_key, c);
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
    };
};
