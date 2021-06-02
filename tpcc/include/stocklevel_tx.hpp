#pragma once

#include <inttypes.h>

#include <cstdint>

#include "record_key.hpp"
#include "record_layout.hpp"
#include "tx_utils.hpp"

class StockLevelTx {
public:
    StockLevelTx(uint16_t w_id0, uint8_t d_id0) {
        input.generate(w_id0, d_id0);
        input.print();
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

        void print() {
            LOG_TRACE(
                "stklvl: w_id=%" PRIu16 " d_id=%" PRIu8 " threshold=%" PRIu8, w_id, d_id,
                threshold);
        }

    } input;

    template <typename Transaction>
    Status run(Transaction& tx, Stat& stat, Output& out) {
        typename Transaction::Result res;

        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint8_t threshold = input.threshold;

        out << w_id << d_id << threshold;

        District d;
        res = tx.get_record(d, District::Key::create_key(w_id, d_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);

        std::set<uint32_t> s_i_ids;
        OrderLine::Key low = OrderLine::Key::create_key(w_id, d_id, d.d_next_o_id - 20, 0);
        OrderLine::Key up = OrderLine::Key::create_key(w_id, d_id, d.d_next_o_id + 1, 0);
        res = tx.template range_query<OrderLine>(low, up, [&s_i_ids](const OrderLine& ol) {
            if (ol.ol_i_id != Item::UNUSED_ID) s_i_ids.insert(ol.ol_i_id);
        });
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) kill_tx(tx, res, stat);

        auto it = s_i_ids.begin();
        Stock s;
        while (it != s_i_ids.end()) {
            res = tx.get_record(s, Stock::Key::create_key(w_id, *it));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return kill_tx(tx, res, stat);
            if (s.s_quantity >= threshold) {
                it = s_i_ids.erase(it);
            } else {
                it++;
            }
        }

        out << s_i_ids.size();

        if (tx.commit()) {
            LOG_TRACE("commit success");
            stat.num_commits[4]++;
            return Status::SUCCESS;
        } else {
            LOG_TRACE("commit fail");
            stat.num_sys_aborts[4]++;
            return Status::SYSTEM_ABORT;
        }
    }

private:
    template <typename Transaction>
    Status kill_tx(Transaction& tx, typename Transaction::Result res, Stat& stat) {
        return ::kill_tx(tx, res, stat, 4);
    }
};