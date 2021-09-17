#pragma once

#include <cstdint>

#include "benchmarks/ycsb/include/record_layout.hpp"
#include "benchmarks/ycsb/include/tx_runner.hpp"
#include "benchmarks/ycsb/include/tx_utils.hpp"
#include "utils/logger.hpp"

template <typename Payload>
class ReadTx {
public:
    ReadTx() { input.generate(); }

    static constexpr char name[] = "ReadTx";
    static constexpr TxProfileID id = TxProfileID::READ_TX;

    struct Input {
        typename Payload::Key key[Config::get_max_reps_per_txn()] = {};

        void generate() {
            const Config& c = get_config();
            uint64_t reps = c.get_reps_per_txn();
            for (uint64_t i = 0; i < reps; ++i) {
                key[i] = zipf_int(c.get_contention(), c.get_num_records());
            }
        }
    } input;

    template <typename Transaction>
    Status run(Transaction& tx, Stat& stat) {
        typename Transaction::Result res;
        TxHelper<Transaction> helper(tx, stat[id]);
        const Config& c = get_config();
        uint64_t reps = c.get_reps_per_txn();

        const Payload* p = nullptr;
        for (uint64_t i = 0; i < reps; ++i) {
            res = tx.get_record(p, input.key[i]);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res);
        }

        return helper.commit();
    }
};