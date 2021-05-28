#pragma once

#include <cstdint>

#include "record_layout.hpp"
#include "tx_utils.hpp"

class StockLevelTx {
public:
    StockLevelTx(uint16_t w_id0, uint8_t d_id0) {
        // initialize input
        input.generate(w_id0, d_id0);
    }

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint8_t threshold;

        void generate(uint16_t w_id0, uint8_t d_id0) {
            w_id = w_id0;
            d_id = d_id0;
            threshold = urand_int(10, 20);
        }
    };

    struct Output {};

    Input input;
    Output output;

    template <typename Transaction>
    Status run(Transaction& tx) {
        typename Transaction::Result res;

        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint8_t threshold = input.threshold;

        District d;
        res = tx.get_record(d, District::Key::create_key(w_id, d_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        std::set<uint32_t> s_i_ids;
        OrderLine::Key low = OrderLine::Key::create_key(w_id, d_id, d.d_next_o_id - 20, 0);
        OrderLine::Key up = OrderLine::Key::create_key(w_id, d_id, d.d_next_o_id + 1, 0);
        res = tx.template range_query<OrderLine>(low, up, [&s_i_ids](const OrderLine& ol) {
            if (ol.ol_i_id != Item::UNUSED_ID) s_i_ids.insert(ol.ol_i_id);
        });
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) kill_tx(tx, res);

        auto it = s_i_ids.begin();
        Stock s;
        while (it != s_i_ids.end()) {
            res = tx.get_record(s, Stock::Key::create_key(w_id, *it));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res);
            if (s.s_quantity >= threshold) {
                it = s_i_ids.erase(it);
            } else {
                it++;
            }
        }

        if (tx.commit()) {
            LOG_TRACE("commit success");
            return Status::SUCCESS;
        } else {
            LOG_TRACE("commit fail");
            return Status::SYSTEM_ABORT;
        }
    }
};