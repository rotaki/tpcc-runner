#pragma once

#include "utils.hpp"

enum Status {
    SUCCESS = 0,   // if all stages of transaction return Result::SUCCESS
    USER_ABORT,    // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SYSTEM_ABORT,  // if any stage of a transaction returns Result::ABORT
    BUG            // if any stage of a transaciton returns unexpected Result::FAIL
};

template <typename Transaction>
inline bool not_succeeded(Transaction& tx, typename Transaction::Result& res) {
    const Config& c = get_config();
    bool flag = c.get_random_abort_flag();
    if (flag && res == Transaction::Result::SUCCESS && urand_int(1, 100) == 1) {
        tx.abort();
        res = Transaction::Result::ABORT;
    }
    return res != Transaction::Result::SUCCESS;
}

template <typename Transaction>
inline Status kill_tx(Transaction& tx, typename Transaction::Result res) {
    assert(not_succeeded(tx, res));
    if (res == Transaction::Result::FAIL) {
        return Status::BUG;
    } else {
        return Status::SYSTEM_ABORT;
    }
}