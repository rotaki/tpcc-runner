#pragma once

#include <cstdint>

struct TxID {
    union {
        uint64_t txid = 0;

        struct {
            uint64_t tx_counter : 32;
            uint64_t thread_id : 32;
        };
    };

    TxID()
        : txid(0) {}
    TxID(uint64_t txid)
        : txid(txid) {}
    TxID(uint32_t thread_id, uint32_t tx_counter)
        : tx_counter(tx_counter)
        , thread_id(thread_id) {}
};
