#pragma once

#include <cstdint>

using ItemKey = uint32_t;
using WarehouseKey = uint16_t;

struct StockKey {
    union {
        struct {
            uint64_t i_id : 32;
            uint64_t w_id : 16;
        };
        uint64_t s_key = 0;
    };
    inline bool operator<(const StockKey& rhs) const noexcept { return s_key < rhs.s_key; }
    inline static StockKey create_key(uint16_t w_id, uint32_t i_id) {
        StockKey k;
        k.w_id = w_id;
        k.i_id = i_id;
        return k;
    }
};

struct DistrictKey {
    union {
        struct {
            uint32_t d_id : 8;
            uint32_t w_id : 16;
        };
        uint32_t d_key = 0;
    };
    inline bool operator<(const DistrictKey& rhs) const noexcept { return d_key < rhs.d_key; }
    inline static DistrictKey create_key(uint16_t w_id, uint8_t d_id) {
        DistrictKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        return k;
    }
};

struct CustomerKey {
    union {
        struct {
            uint64_t c_id : 32;
            uint64_t d_id : 8;
            uint64_t w_id : 16;
        };
        uint64_t c_key = 0;
    };
    inline bool operator<(const CustomerKey& rhs) const noexcept { return c_key < rhs.c_key; }
    inline static CustomerKey create_key(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
        CustomerKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.c_id = c_id;
        return k;
    }
};

using HistoryKey = CustomerKey;

struct OrderKey {
    union {
        struct {
            uint64_t o_id : 32;
            uint64_t d_id : 8;
            uint64_t w_id : 16;
        };
        uint64_t o_key = 0;
    };
    inline bool operator<(const OrderKey& rhs) const noexcept { return o_key < rhs.o_key; }
    inline static OrderKey create_key(uint16_t w_id, uint8_t d_id, uint32_t o_id) {
        OrderKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.o_id = o_id;
        return k;
    }
};

struct OrderLineKey {
    union {
        struct {
            uint64_t ol_number : 8;
            uint64_t o_id : 32;
            uint64_t d_id : 8;
            uint64_t w_id : 16;
        };
        uint64_t ol_key;
    };
    inline bool operator<(const OrderLineKey& rhs) const noexcept { return ol_key < rhs.ol_key; }
    inline static OrderLineKey create_key(
        uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_number) {
        OrderLineKey k;
        k.ol_number = ol_number;
        k.w_id = w_id;
        k.d_id = d_id;
        k.o_id = o_id;
        return k;
    }
};

using NewOrderKey = OrderKey;
