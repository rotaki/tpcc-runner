#pragma once

#include <cstdint>

#include "benchmarks/ycsb/include/record_layout.hpp"
#include "benchmarks/ycsb/include/tx_runner.hpp"
#include "benchmarks/ycsb/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

template <typename Payload>
class Tx {
public:
    Tx() { input.generate(); }

    static constexpr char name[] = "Tx";
    static constexpr TxProfileID id = TxProfileID::TX;

    struct Input {
        typename Payload::Key key[Config::get_max_reps_per_txn()];

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
        int r = c.get_read_propotion();
        int u = c.get_update_propotion();
        int rmw = c.get_readmodifywrite_propotion();

        for (uint64_t i = 0; i < reps; ++i) {
            int x = urand_int(1, 100);
            int y = 0;
            if (x <= (y += r)) {
                // READ
                const Payload* p = nullptr;
                res = tx.get_record(p, input.key[i]);
                LOG_TRACE("res: %d", static_cast<int>(res));
                if (not_succeeded(tx, res)) return helper.kill(res);
                continue;
            } else if (x <= (y += u)) {
                // UPDATE
                Payload* p = nullptr;
                res = tx.prepare_record_for_write(p, input.key[i]);
                LOG_TRACE("res: %d", static_cast<int>(res));
                if (not_succeeded(tx, res)) return helper.kill(res);

                p = new (p) Payload();  // initialize memory
                res = tx.finish_write(p);
                LOG_TRACE("res: %d", static_cast<int>(res));
                if (not_succeeded(tx, res)) return helper.kill(res);
                continue;
            } else if (x <= (y + rmw)) {
                // READ MODIFY WRITE
                Payload* p = nullptr;
                res = tx.prepare_record_for_update(p, input.key[i]);
                LOG_TRACE("res: %d", static_cast<int>(res));
                if (not_succeeded(tx, res)) return helper.kill(res);

                p = new (p) Payload();  // initialize memory
                res = tx.finish_update(p);
                LOG_TRACE("res: %d", static_cast<int>(res));
                if (not_succeeded(tx, res)) return helper.kill(res);
                continue;
            } else {
                throw std::runtime_error("operation not supported");
            }
        }

        return helper.commit();
    }
};