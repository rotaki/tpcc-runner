#include <algorithm>

#include "benchmarks/tpcc/include/config.hpp"
#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "gtest/gtest.h"
#include "protocols/naive/include/database.hpp"
#include "protocols/naive/include/initializer.hpp"
#include "utils/utils.hpp"

class ConsistencyTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        Config& c = get_mutable_config();
        c.set_num_warehouses(num_warehouse);
        Initializer::load_all_tables();
    }

    static constexpr uint16_t num_warehouse = 1;
};

double get_sum_of_district_ytd_in_warehouse(uint16_t w_id) {
    Database& db = Database::get_db();
    double total_d_ytd = 0;
    RecordToTable<District>::type& t = db.get_table<District>();
    for (const auto& p: t) {
        const District& d = *p.second;
        if (d.d_w_id == w_id) {
            total_d_ytd += d.d_ytd;
        }
    }
    return total_d_ytd;
}

uint32_t get_max_order_id_from_order(uint16_t w_id, uint8_t d_id) {
    Database& db = Database::get_db();
    Order::Key o_key_low = Order::Key::create_key(w_id, d_id, 0);
    Order::Key o_key_high = Order::Key::create_key(w_id, d_id + 1, 0);
    auto iter_low = db.get_lower_bound_iter<Order>(o_key_low);
    auto iter_high = db.get_lower_bound_iter<Order>(o_key_high);
    uint32_t max_o_id = 0;
    for (auto it = iter_low; it != iter_high; ++it) {
        max_o_id = std::max(max_o_id, it->second->o_id);
    }
    return max_o_id;
}

uint32_t get_max_order_id_from_neworder(uint16_t w_id, uint8_t d_id) {
    Database& db = Database::get_db();
    NewOrder::Key no_key_low = NewOrder::Key::create_key(w_id, d_id, 0);
    NewOrder::Key no_key_high = NewOrder::Key::create_key(w_id, d_id + 1, 0);
    auto iter_low = db.get_lower_bound_iter<NewOrder>(no_key_low);
    auto iter_high = db.get_lower_bound_iter<NewOrder>(no_key_high);
    uint32_t max_o_id = 0;
    for (auto it = iter_low; it != iter_high; ++it) {
        max_o_id = std::max(max_o_id, it->second->no_o_id);
    }
    return max_o_id;
}

void check_min_max_order_id_in_neworder(uint16_t w_id, uint8_t d_id) {
    Database& db = Database::get_db();
    NewOrder::Key no_key_low = NewOrder::Key::create_key(w_id, d_id, 0);
    NewOrder::Key no_key_high = NewOrder::Key::create_key(w_id, d_id + 1, 0);
    auto iter_low = db.get_lower_bound_iter<NewOrder>(no_key_low);
    auto iter_high = db.get_lower_bound_iter<NewOrder>(no_key_high);
    uint32_t max_o_id = 0;
    uint32_t min_o_id = Order::ORDS_PER_DIST + 1;
    uint32_t cnt = 0;
    for (auto it = iter_low; it != iter_high; ++it) {
        cnt++;
        max_o_id = std::max(max_o_id, it->second->no_o_id);
        min_o_id = std::min(min_o_id, it->second->no_o_id);
    }
    ASSERT_GE(max_o_id, min_o_id);
    ASSERT_GE(max_o_id, 1);
    ASSERT_GE(min_o_id, 1);
    ASSERT_EQ(cnt, max_o_id - min_o_id + 1);
}


void check_order_orderline_relationship(uint16_t w_id, uint8_t d_id) {
    Database& db = Database::get_db();
    Order::Key o_key_low = Order::Key::create_key(w_id, d_id, 0);
    Order::Key o_key_high = Order::Key::create_key(w_id, d_id + 1, 0);
    auto o_iter_low = db.get_lower_bound_iter<Order>(o_key_low);
    auto o_iter_high = db.get_lower_bound_iter<Order>(o_key_high);

    int sum_ol_cnt = 0;
    for (auto it = o_iter_low; it != o_iter_high; ++it) {
        sum_ol_cnt += it->second->o_ol_cnt;
    }

    OrderLine::Key ol_key_low = OrderLine::Key::create_key(w_id, d_id, 0, 0);
    OrderLine::Key ol_key_high = OrderLine::Key::create_key(w_id, d_id + 1, 0, 0);
    auto ol_iter_low = db.get_lower_bound_iter<OrderLine>(ol_key_low);
    auto ol_iter_high = db.get_lower_bound_iter<OrderLine>(ol_key_high);

    int n = std::distance(ol_iter_low, ol_iter_high);
    ASSERT_EQ(sum_ol_cnt, n);
}

TEST_F(ConsistencyTest, Test1) {
    Database& db = Database::get_db();
    const Warehouse* w;
    for (uint16_t w_id = 1; w_id <= num_warehouse; w_id++) {
        db.get_record(w, Warehouse::Key::create_key(w_id));
        ASSERT_TRUE(w != nullptr);
        ASSERT_EQ(w->w_ytd, get_sum_of_district_ytd_in_warehouse(w_id));
    }
}

TEST_F(ConsistencyTest, Test2) {
    Database& db = Database::get_db();
    const District* d;
    for (uint16_t w_id = 1; w_id <= num_warehouse; w_id++) {
        for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            District::Key d_key = District::Key::create_key(w_id, d_id);
            db.get_record<District>(d, d_key);
            uint32_t d_next_o_id = d->d_next_o_id;
            ASSERT_EQ(d_next_o_id - 1, get_max_order_id_from_order(w_id, d_id));
            ASSERT_EQ(d_next_o_id - 1, get_max_order_id_from_neworder(w_id, d_id));
        }
    }
}

TEST_F(ConsistencyTest, Test3) {
    for (uint16_t w_id = 1; w_id <= num_warehouse; w_id++) {
        for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            check_min_max_order_id_in_neworder(w_id, d_id);
        }
    }
}

TEST_F(ConsistencyTest, Test4) {
    for (uint16_t w_id = 1; w_id <= num_warehouse; w_id++) {
        for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            check_order_orderline_relationship(w_id, w_id);
        }
    }
}
