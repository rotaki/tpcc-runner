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

template <typename TxProfile, typename Transaction>
inline Status run(Transaction& tx) {
    TxProfile p;
    return p.run(tx);
}

template <typename TxProfile, typename Transaction>
inline bool run_with_retry(Transaction& tx) {
    for (;;) {
        Status res = run<TxProfile>(tx);
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
