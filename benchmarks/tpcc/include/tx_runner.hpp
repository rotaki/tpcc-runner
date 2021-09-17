#pragma once

#include <stdexcept>

#include "benchmarks/tpcc/include/delivery_tx.hpp"
#include "benchmarks/tpcc/include/neworder_tx.hpp"
#include "benchmarks/tpcc/include/orderstatus_tx.hpp"
#include "benchmarks/tpcc/include/payment_tx.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "benchmarks/tpcc/include/stocklevel_tx.hpp"
#include "benchmarks/tpcc/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

template <typename TxProfile, typename Transaction>
inline Status run(Transaction& tx, Stat& stat, Output& out) {
    uint16_t w_id;
    const Config& c = get_config();
    if (c.get_fixed_warehouse_flag()) {
        w_id = tx.thread_id % c.get_num_warehouses() + 1;
    } else {
        w_id = urand_int(1, get_config().get_num_warehouses());
    }
    TxProfile p(w_id);
    return p.run(tx, stat, out);
}

template <typename TxProfile, typename Transaction>
inline bool run_with_retry(Transaction& tx, Stat& stat, Output& out) {
    for (;;) {
        Status res = run<TxProfile>(tx, stat, out);
        switch (res) {
        case SUCCESS: LOG_TRACE("success"); return true;
        case USER_ABORT:
            LOG_TRACE("user abort");
            tx.abort();
            return false;                                        // aborted by the user
        case SYSTEM_ABORT: LOG_TRACE("system abort"); continue;  // aborted by the tx engine
        case BUG: assert(false);  // throw std::runtime_error("Unexpected Transaction Bug");
        default: assert(false);
        }
    }
}
