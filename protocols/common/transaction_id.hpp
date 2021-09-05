#pragma once

#include <cstdint>

struct TxID {
    union {
        uint64_t id = 0;
        struct {
            uint64_t tx_counter : 32;
            uint64_t thread_id : 32;
        };
    };

    TxID()
        : id(0) {}
    TxID(const TxID& txid)
        : id(txid.id) {}
    TxID(uint64_t txid)
        : id(txid) {}
    TxID(uint32_t thread_id, uint32_t tx_counter)
        : tx_counter(tx_counter)
        , thread_id(thread_id) {}

    bool operator==(const TxID& other) const {
        if (this == &other) return true;
        if (this->id == other.id)
            return true;
        else
            return false;
    }

    size_t hash() const noexcept { return id; }
};

struct TxIDHash {
    size_t operator()(const TxID& txid) const noexcept { return txid.hash(); }
};
