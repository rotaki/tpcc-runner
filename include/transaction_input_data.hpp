#pragma once

#include <cassert>

#include "record_generator.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "utils.hpp"

namespace InputData {
using namespace RecordGeneratorUtils;

struct NewOrder {
    uint16_t w_id;
    uint8_t d_id;
    uint32_t c_id;
    bool rbk;
    bool is_remote;
    uint8_t ol_cnt;
    struct {
        uint32_t ol_i_id;
        uint16_t ol_supply_w_id;
        uint8_t ol_quantity;
    } items[OrderLine::MAX_ORDLINES_PER_ORD];

    inline void generate(uint16_t w_id0) {
        const Config& c = get_config();
        uint16_t num_warehouses = c.get_num_warehouses();
        w_id = w_id0;
        d_id = urand_int(1, District::DISTS_PER_WARE);
        c_id = nurand_int(NURandConstantType::C_ID, 1, Customer::CUSTS_PER_DIST);
        rbk = (urand_int(1, 100) == 1 ? 1 : 0);
        is_remote = (urand_int(1, 100) == 1 ? 1 : 0);
        ol_cnt = urand_int(OrderLine::MIN_ORDLINES_PER_ORD, OrderLine::MAX_ORDLINES_PER_ORD);
        for (int i = 1; i <= ol_cnt; i++) {
            if (i == ol_cnt && rbk) {
                items[i - 1].ol_i_id = Item::UNUSED_ID; /* set to an unused value */
            } else {
                items[i - 1].ol_i_id = nurand_int(NURandConstantType::OL_I_ID, 1, Item::ITEMS);
            }
            if (is_remote && num_warehouses > 1) {
                uint16_t remote_w_id;
                while ((remote_w_id = urand_int(1, num_warehouses)) == w_id) {
                }
                assert(remote_w_id != w_id);
                items[i - 1].ol_supply_w_id = remote_w_id;
            } else {
                items[i - 1].ol_supply_w_id = w_id;
            }
            items[i - 1].ol_quantity = urand_int(1, 10);
        }
    }
};

struct Payment {
    uint16_t w_id;
    uint8_t d_id;
    uint32_t c_id;
    uint16_t c_w_id;
    uint8_t c_d_id;
    double h_amount;
    bool by_last_name;
    char c_last[Customer::MAX_LAST + 1];

    inline void generate(uint16_t w_id0) {
        const Config& c = get_config();
        uint16_t num_warehouses = c.get_num_warehouses();
        w_id = w_id0;
        d_id = urand_int(1, District::DISTS_PER_WARE);
        h_amount = urand_double(100, 500000, 100);
        if (num_warehouses == 1 || urand_int(1, 100) <= 85) {
            c_w_id = w_id;
            c_d_id = d_id;
        } else {
            while ((c_w_id = urand_int(1, num_warehouses)) == w_id) {
            }
            assert(c_w_id != w_id);
            c_d_id = urand_int(1, District::DISTS_PER_WARE);
        }
        bool by_last_name = (urand_int(1, 100) <= 60);
        if (by_last_name) {
            c_id = Customer::UNUSED_ID;
            make_clast(c_last, nurand_int(NURandConstantType::C_RUN, 0, 999));
        } else {
            c_id = nurand_int(NURandConstantType::C_ID, 1, 3000);
        }
    }
};

struct OrderStatus {
    uint16_t w_id;
    uint8_t d_id;
    uint32_t c_id;
    bool by_last_name;
    char c_last[Customer::MAX_DATA + 1];

    void generate(uint16_t w_id0) {
        w_id = w_id0;
        d_id = urand_int(1, District::DISTS_PER_WARE);
        bool by_last_name = (urand_int(1, 100) <= 60);
        if (by_last_name) {
            c_id = Customer::UNUSED_ID;
            make_clast(c_last, nurand_int(NURandConstantType::C_RUN, 0, 999));
        } else {
            c_id = nurand_int(NURandConstantType::C_ID, 1, 3000);
        }
    }
};

struct Delivery {
    uint16_t w_id;
    uint8_t o_carrier_id;
    Timestamp ol_delivery_d;

    inline void generate(uint16_t w_id0) {
        w_id = w_id0;
        o_carrier_id = urand_int(1, 10);
        ol_delivery_d = get_timestamp();
    }
};

struct StockLevel {
    uint16_t w_id;
    uint8_t d_id;
    uint8_t threshold;

    inline void generate(uint16_t w_id0, uint8_t d_id0) {
        w_id = w_id0;
        d_id = d_id0;
        threshold = urand_int(10, 20);
    }
};
}  // namespace InputData
