#pragma once

#include <cstdint>
#include <stdexcept>

#include "benchmarks/ycsb/include/read_modify_write_tx.hpp"
#include "benchmarks/ycsb/include/read_tx.hpp"
#include "benchmarks/ycsb/include/tx_utils.hpp"
#include "benchmarks/ycsb/include/update_tx.hpp"
#include "utils/logger.hpp"

template <typename TxProfile, typename Transaction>
inline Status run(Transaction& tx, Stat& stat) {
    TxProfile p;
    return p.run(tx, stat);
}

template <typename TxProfile, typename Transaction>
inline bool run_with_retry(Transaction& tx, Stat& stat) {
    for (;;) {
        Status res = run<TxProfile>(tx, stat);
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
