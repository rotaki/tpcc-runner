#pragma once

#include <deque>
#include <map>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"

struct CustomerSecondaryKey;
struct OrderSecondaryKey;

struct CustomerSecondary {
    using Key = CustomerSecondaryKey;
    Customer::Key key;
    CustomerSecondary() {}
    CustomerSecondary(Customer::Key key)
        : key(key.get_raw_key()) {}
    bool operator==(const CustomerSecondary& rhs) const noexcept { return key == rhs.key; };
};

struct CustomerSecondaryKey {
    union {
        struct {
            uint32_t d_id : 8;
            uint32_t w_id : 16;
            uint32_t not_used : 8;
        };
        uint32_t num;
    };
    char c_last[Customer::MAX_LAST + 1];
    CustomerSecondaryKey()
        : num(0){};
    CustomerSecondaryKey(uint32_t num)
        : num(num) {}
    CustomerSecondaryKey(const CustomerSecondaryKey& c) {
        d_id = c.d_id;
        w_id = c.w_id;
        not_used = 0;
        copy_cstr(c_last, c.c_last, sizeof(c_last));
    }

    int cmp_c_last(const CustomerSecondaryKey& rhs) const {
        return ::strncmp(c_last, rhs.c_last, Customer::MAX_LAST);
    }

    bool operator<(const CustomerSecondaryKey& rhs) const noexcept {
        if (num == rhs.num)
            return cmp_c_last(rhs) < 0;
        else
            return num < rhs.num;
    }
    bool operator==(const CustomerSecondaryKey& rhs) const noexcept {
        return num == rhs.num && cmp_c_last(rhs) == 0;
    }
    static CustomerSecondaryKey create_key(uint16_t w_id, uint8_t d_id, const char* c_last_in) {
        CustomerSecondaryKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        copy_cstr(k.c_last, c_last_in, sizeof(k.c_last));
        return k;
    }
    static CustomerSecondaryKey create_key(const Customer& c) {
        CustomerSecondaryKey k{0};
        k.w_id = c.c_w_id;
        k.d_id = c.c_d_id;
        copy_cstr(k.c_last, c.c_last, sizeof(k.c_last));
        return k;
    }
};

struct OrderSecondary {
    using Key = OrderSecondaryKey;
    Order::Key key;
    OrderSecondary() {}
    OrderSecondary(Order::Key key)
        : key(key.get_raw_key()) {}
    bool operator==(const OrderSecondary& rhs) const noexcept { return key == rhs.key; }
};

struct OrderSecondaryKey {
    union {
        struct {
            uint64_t o_id : 32;
            uint64_t c_id : 16;  // up to 65535 (actual: 3000)
            uint64_t d_id : 4;   // up to 15 (actual: 10)
            uint64_t w_id : 12;  // up to 4095
        };
        uint64_t o_sec_key;
    };
    OrderSecondaryKey()
        : o_sec_key(0){};
    OrderSecondaryKey(uint64_t o_sec_key)
        : o_sec_key(o_sec_key) {}
    uint64_t get_raw_key() const { return o_sec_key; }
    bool operator<(const OrderSecondaryKey& rhs) const noexcept {
        return o_sec_key < rhs.o_sec_key;
    }
    bool operator==(const OrderSecondaryKey& rhs) const noexcept {
        return o_sec_key == rhs.o_sec_key;
    }
    static OrderSecondaryKey create_key(uint16_t w_id, uint8_t d_id, uint32_t c_id, uint32_t o_id) {
        OrderSecondaryKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.c_id = c_id;
        k.o_id = o_id;
        return k;
    }
    static OrderSecondaryKey create_key(const Order& o) {
        OrderSecondaryKey k;
        k.w_id = o.o_w_id;
        k.d_id = o.o_d_id;
        k.c_id = o.o_c_id;
        k.o_id = o.o_id;
        return k;
    }
};

enum RecordID {
    UNUSED = 0,
    ITEM = 1,
    WAREHOUSE = 2,
    STOCK = 3,
    DISTRICT = 4,
    CUSTOMER = 5,
    CUSTOMER_SECONDARY = 6,
    HISTORY = 7,
    ORDER = 8,
    ORDER_SECONDARY = 9,
    NEWORDER = 10,
    ORDERLINE = 11
};

template <typename Record>
inline TableID get_id() {
    if constexpr (std::is_same<Record, Item>::value) {
        return ITEM;
    } else if constexpr (std::is_same<Record, Warehouse>::value) {
        return WAREHOUSE;
    } else if constexpr (std::is_same<Record, Stock>::value) {
        return STOCK;
    } else if constexpr (std::is_same<Record, District>::value) {
        return DISTRICT;
    } else if constexpr (std::is_same<Record, Customer>::value) {
        return CUSTOMER;
    } else if constexpr (std::is_same<Record, CustomerSecondary>::value) {
        assert(false);
        return CUSTOMER_SECONDARY;
    } else if constexpr (std::is_same<Record, History>::value) {
        return HISTORY;
    } else if constexpr (std::is_same<Record, Order>::value) {
        return ORDER;
    } else if constexpr (std::is_same<Record, OrderSecondary>::value) {
        return ORDER_SECONDARY;
    } else if constexpr (std::is_same<Record, NewOrder>::value) {
        return NEWORDER;
    } else if constexpr (std::is_same<Record, OrderLine>::value) {
        return ORDERLINE;
    } else {
        assert(false);
    }
}


template <typename T>
struct Traits;

template <>
struct Traits<Order> {
    using SecondaryIndexType = OrderSecondary;
};

template <typename Record>
using Secondary = typename Traits<Record>::SecondaryIndexType;

// Append only table without primary key
inline std::deque<History>& get_history_table() {
    thread_local std::deque<History> history_table;
    return history_table;
}

// Read only secondary table
inline std::multimap<CustomerSecondary::Key, CustomerSecondary>& get_customer_secondary_table() {
    static std::multimap<CustomerSecondary::Key, CustomerSecondary> customer_secondary_table;
    return customer_secondary_table;
}