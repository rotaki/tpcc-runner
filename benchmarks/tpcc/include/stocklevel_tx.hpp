#pragma once

#include <inttypes.h>

#include <cstdint>
#include <set>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "benchmarks/tpcc/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/tsc.hpp"

class StockLevelTx {
public:
    StockLevelTx(uint16_t w_id0) {
        input.generate(w_id0);
        input.print();
    }

    static constexpr char name[] = "StockLevel";
    static constexpr TxProfileID id = TxProfileID::STOCKLEVEL_TX;

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint8_t threshold;

        void generate(uint16_t w_id0) {
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            threshold = urand_int(10, 20);
        }

        void print() {
            LOG_TRACE(
                "stklvl: w_id=%" PRIu16 " d_id=%" PRIu8 " threshold=%" PRIu8, w_id, d_id,
                threshold);
        }

    } input;

    enum AbortID : uint8_t {
        GET_DISTRICT = 0,
        RANGE_GET_ORDERLINE = 1,
        GET_STOCK = 2,
        PRECOMMIT = 3,
        MAX = 4
    };

    template <AbortID a>
    static constexpr const char* abort_reason() {
        if constexpr (a == AbortID::GET_DISTRICT)
            return "GET_DISTRICT";
        else if constexpr (a == AbortID::RANGE_GET_ORDERLINE)
            return "RANGE_GET_ORDERLINE";
        else if constexpr (a == AbortID::GET_STOCK)
            return "GET_STOCK";
        else if constexpr (a == AbortID::PRECOMMIT)
            return "PRECOMMIT";
        else
            static_assert(false_v<a>, "undefined abort reason");
    }

    template <typename Transaction>
    Status run(Transaction& tx, Stat& stat, Output& out) {
        uint64_t start, end;
        start = rdtscp();
        typename Transaction::Result res;
        TxHelper<Transaction> helper(tx, stat[TxProfileID::STOCKLEVEL_TX]);

        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint8_t threshold = input.threshold;

        out << w_id << d_id << threshold;

        const District* d = nullptr;
        res = tx.get_record(d, District::Key::create_key(w_id, d_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, GET_DISTRICT);

        std::set<uint32_t> s_i_ids;
        OrderLine::Key low = OrderLine::Key::create_key(w_id, d_id, d->d_next_o_id - 20, 1);
        OrderLine::Key up = OrderLine::Key::create_key(w_id, d_id, d->d_next_o_id, 1);
        res = tx.template range_query<OrderLine>(low, up, [&s_i_ids](const OrderLine& ol) {
            if (ol.ol_i_id != Item::UNUSED_ID) s_i_ids.insert(ol.ol_i_id);
        });
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, RANGE_GET_ORDERLINE);

        auto it = s_i_ids.begin();
        const Stock* s = nullptr;
        while (it != s_i_ids.end()) {
            res = tx.get_record(s, Stock::Key::create_key(w_id, *it));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, GET_STOCK);
            if (s->s_quantity >= threshold) {
                it = s_i_ids.erase(it);
            } else {
                ++it;
            }
        }

        out << s_i_ids.size();
        end = rdtscp();
        return helper.commit(PRECOMMIT, end - start);
    }
};
