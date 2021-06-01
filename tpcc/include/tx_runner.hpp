#pragma once

#include <iostream>
#include <set>
#include <stdexcept>

#include "delivery_tx.hpp"
#include "logger.hpp"
#include "neworder_tx.hpp"
#include "orderstatus_tx.hpp"
#include "payment_tx.hpp"
#include "record_layout.hpp"
#include "stocklevel_tx.hpp"
#include "tx_utils.hpp"
#include "utils.hpp"

template <typename T>
concept IsStockLevelTx = std::is_same<T, StockLevelTx>::value;

template <IsStockLevelTx TxProfile, typename Transaction>
inline Status run(Transaction& tx, Stat& stat, Output& out) {
    uint16_t w_id = urand_int(1, get_config().get_num_warehouses());
    uint8_t d_id = urand_int(1, District::DISTS_PER_WARE);
    TxProfile p(w_id, d_id);
    return p.run(tx, stat, out);
}

template <typename TxProfile, typename Transaction>
inline Status run(Transaction& tx, Stat& stat, Output& out) {
    uint16_t w_id = urand_int(1, get_config().get_num_warehouses());
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
