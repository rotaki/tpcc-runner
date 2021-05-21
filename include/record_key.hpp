#pragma once

#include <cstdint>

#include "record_layout.hpp"

struct ItemKey {
    uint32_t i_key = 0;
    inline bool operator<(const ItemKey& rhs) const noexcept { return i_key < rhs.i_key; }
    inline bool operator==(const ItemKey& rhs) const noexcept { return i_key == rhs.i_key; }
    inline static ItemKey create_key(uint32_t i_id) {
        ItemKey k;
        k.i_key = i_id;
        return k;
    }
    inline static ItemKey create_key(const Item& i) {
        ItemKey k;
        k.i_key = i.i_id;
        return k;
    }
};

struct WarehouseKey {
    uint16_t w_key = 0;
    inline bool operator<(const WarehouseKey& rhs) const noexcept { return w_key < rhs.w_key; }
    inline bool operator==(const WarehouseKey& rhs) const noexcept { return w_key == rhs.w_key; }
    inline static WarehouseKey create_key(uint16_t w_id) {
        WarehouseKey k;
        k.w_key = w_id;
        return k;
    }
    inline static WarehouseKey create_key(const Warehouse& w) {
        WarehouseKey k;
        k.w_key = w.w_id;
        return k;
    }
};

struct StockKey {
    union {
        struct {
            uint64_t i_id : 32;
            uint64_t w_id : 16;
        };
        uint64_t s_key = 0;
    };
    inline bool operator<(const StockKey& rhs) const noexcept { return s_key < rhs.s_key; }
    inline bool operator==(const StockKey& rhs) const noexcept { return s_key == rhs.s_key; }
    inline static StockKey create_key(uint16_t w_id, uint32_t i_id) {
        StockKey k;
        k.w_id = w_id;
        k.i_id = i_id;
        return k;
    }
    inline static StockKey create_key(const Stock& s) {
        StockKey k;
        k.w_id = s.s_w_id;
        k.i_id = s.s_i_id;
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
    inline bool operator==(const DistrictKey& rhs) const noexcept { return d_key == rhs.d_key; }
    inline static DistrictKey create_key(uint16_t w_id, uint8_t d_id) {
        DistrictKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        return k;
    }
    inline static DistrictKey create_key(const District& d) {
        DistrictKey k;
        k.w_id = d.d_w_id;
        k.d_id = d.d_id;
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
    inline bool operator==(const CustomerKey& rhs) const noexcept { return c_key == rhs.c_key; }
    inline static CustomerKey create_key(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
        CustomerKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.c_id = c_id;
        return k;
    }
    inline static CustomerKey create_key(const Customer& c) {
        CustomerKey k;
        k.w_id = c.c_w_id;
        k.d_id = c.c_d_id;
        k.c_id = c.c_id;
        return k;
    }
};

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
    inline bool operator==(const OrderKey& rhs) const noexcept { return o_key == rhs.o_key; }
    inline static OrderKey create_key(uint16_t w_id, uint8_t d_id, uint32_t o_id) {
        OrderKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.o_id = o_id;
        return k;
    }
    inline static OrderKey create_key(const Order& o) {
        OrderKey k;
        k.w_id = o.o_w_id;
        k.d_id = o.o_d_id;
        k.o_id = o.o_id;
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
        uint64_t ol_key = 0;
    };
    inline bool operator<(const OrderLineKey& rhs) const noexcept { return ol_key < rhs.ol_key; }
    inline bool operator==(const OrderLineKey& rhs) const noexcept { return ol_key == rhs.ol_key; }
    inline static OrderLineKey create_key(
        uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_number) {
        OrderLineKey k;
        k.ol_number = ol_number;
        k.w_id = w_id;
        k.d_id = d_id;
        k.o_id = o_id;
        return k;
    }
    inline static OrderLineKey create_key(const OrderLine& ol) {
        OrderLineKey k;
        k.ol_number = ol.ol_number;
        k.w_id = ol.ol_w_id;
        k.d_id = ol.ol_d_id;
        k.o_id = ol.ol_o_id;
        return k;
    }
};

struct NewOrderKey {
    union {
        struct {
            uint64_t o_id : 32;
            uint64_t d_id : 8;
            uint64_t w_id : 16;
        };
        uint64_t o_key = 0;
    };
    inline bool operator<(const NewOrderKey& rhs) const noexcept { return o_key < rhs.o_key; }
    inline bool operator==(const NewOrderKey& rhs) const noexcept { return o_key == rhs.o_key; }
    inline static NewOrderKey create_key(uint16_t w_id, uint8_t d_id, uint32_t o_id) {
        NewOrderKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.o_id = o_id;
        return k;
    }
    inline static NewOrderKey create_key(const NewOrder& no) {
        NewOrderKey k;
        k.w_id = no.no_w_id;
        k.d_id = no.no_d_id;
        k.o_id = no.no_o_id;
        return k;
    }
};
