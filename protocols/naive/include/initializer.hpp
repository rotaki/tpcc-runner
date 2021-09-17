#pragma once

#include <utility>

#include "benchmarks/tpcc/include/config.hpp"
#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "protocols/naive/include/database.hpp"
#include "utils/utils.hpp"

namespace Initializer {

inline void create_and_insert_item_record(uint32_t i_id) {
    Item::Key key = Item::Key::create_key(i_id);
    Item* i = Database::get_db().allocate_record<Item>(key);
    i->generate(i_id);
}

inline void create_and_insert_warehouse_record(uint16_t w_id) {
    Warehouse::Key key = Warehouse::Key::create_key(w_id);
    Warehouse* w = Database::get_db().allocate_record<Warehouse>(key);
    w->generate(w_id);
}

inline void create_and_insert_stock_record(uint16_t s_w_id, uint32_t s_i_id) {
    Stock::Key key = Stock::Key::create_key(s_w_id, s_i_id);
    Stock* s = Database::get_db().allocate_record<Stock>(key);
    s->generate(s_w_id, s_i_id);
}

inline void create_and_insert_district_record(uint16_t d_w_id, uint8_t d_id) {
    District::Key key = District::Key::create_key(d_w_id, d_id);
    District* d = Database::get_db().allocate_record<District>(key);
    d->generate(d_w_id, d_id);
}

inline void create_and_insert_customer_record(
    uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, Timestamp t) {
    Customer::Key key = Customer::Key::create_key(c_w_id, c_d_id, c_id);
    Customer* c = Database::get_db().allocate_record<Customer>(key);
    c->generate(c_w_id, c_d_id, c_id, t);
    CustomerSecondary cs;
    cs.ptr = c;
    CustomerSecondaryKey cs_key = CustomerSecondaryKey::create_key(*c);
    Database::get_db().insert_record(cs_key, cs);
}

inline void create_and_insert_history_record(
    uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id, uint8_t h_d_id) {
    auto h = std::make_unique<History>();
    h->generate(h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id);
    Database::get_db().insert_record<History>(std::move(h));
}

inline std::pair<Timestamp, uint8_t> create_and_insert_order_record(
    uint16_t o_w_id, uint8_t o_d_id, uint32_t o_id, uint32_t o_c_id) {
    Order::Key key = Order::Key::create_key(o_w_id, o_d_id, o_id);
    Order* o = Database::get_db().allocate_record<Order>(key);
    o->generate(o_w_id, o_d_id, o_id, o_c_id);
    OrderSecondary os;
    os.ptr = o;
    OrderSecondaryKey os_key = OrderSecondaryKey::create_key(*o);
    Database::get_db().insert_record(os_key, os);
    return std::make_pair(o->o_entry_d, o->o_ol_cnt);
}

inline void create_and_insert_neworder_record(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    NewOrder::Key key = NewOrder::Key::create_key(no_w_id, no_d_id, no_o_id);
    NewOrder* no = Database::get_db().allocate_record<NewOrder>(key);
    no->generate(no_w_id, no_d_id, no_o_id);
}

inline void create_and_insert_orderline_record(
    uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint8_t ol_number, uint16_t ol_supply_w_id,
    uint32_t ol_i_id, Timestamp o_entry_d) {
    OrderLine::Key key = OrderLine::Key::create_key(ol_w_id, ol_d_id, ol_o_id, ol_number);
    OrderLine* ol = Database::get_db().allocate_record<OrderLine>(key);
    ol->generate(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_supply_w_id, ol_i_id, o_entry_d);
};

inline void load_items_table() {
    for (int i_id = 1; i_id <= Item::ITEMS; i_id++) {
        create_and_insert_item_record(i_id);
    }
}

inline void load_histories_table(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
    create_and_insert_history_record(w_id, d_id, c_id, w_id, d_id);
}

inline void load_customers_table(uint16_t c_w_id, uint8_t c_d_id) {
    Timestamp t = get_timestamp();
    for (int c_id = 1; c_id <= Customer::CUSTS_PER_DIST; c_id++) {
        create_and_insert_customer_record(c_w_id, c_d_id, c_id, t);
        load_histories_table(c_w_id, c_d_id, c_id);
    }
}

inline void load_orderlines_table(
    uint8_t ol_cnt, uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, Timestamp o_entry_d) {
    for (uint8_t ol_number = 1; ol_number <= ol_cnt; ol_number++) {
        uint32_t ol_i_id = urand_int(1, 100000);
        create_and_insert_orderline_record(
            ol_w_id, ol_d_id, ol_o_id, ol_number, ol_w_id, ol_i_id, o_entry_d);
    }
}

inline void load_neworders_table(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    create_and_insert_neworder_record(no_w_id, no_d_id, no_o_id);
}

inline void load_orders_table(uint16_t o_w_id, uint8_t o_d_id) {
    Permutation p(1, Order::ORDS_PER_DIST);
    for (uint32_t o_id = 1; o_id <= Order::ORDS_PER_DIST; o_id++) {
        uint32_t o_c_id = p[o_id - 1];
        std::pair<Timestamp, uint8_t> out =
            create_and_insert_order_record(o_w_id, o_d_id, o_id, o_c_id);
        Timestamp o_entry_d = out.first;
        uint8_t ol_cnt = out.second;
        load_orderlines_table(ol_cnt, o_w_id, o_d_id, o_id, o_entry_d);
        if (o_id > 2100) {
            load_neworders_table(o_w_id, o_d_id, o_id);
        }
    }
}

inline void load_districts_table(uint16_t d_w_id) {
    for (int d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
        create_and_insert_district_record(d_w_id, d_id);
        load_customers_table(d_w_id, d_id);
        load_orders_table(d_w_id, d_id);
    }
}


inline void load_stocks_table(uint16_t s_w_id) {
    for (int s_id = 1; s_id <= Stock::STOCKS_PER_WARE; s_id++) {
        create_and_insert_stock_record(s_w_id, s_id);
    }
}

// Loading warehouses table eventually evokes loading of all the tables other than the items table.
inline void load_warehouses_table() {
    const size_t nr_w = get_config().get_num_warehouses();
    for (size_t w_id = 1; w_id <= nr_w; w_id++) {
        create_and_insert_warehouse_record(w_id);
        load_stocks_table(w_id);
        load_districts_table(w_id);
    }
}

inline void load_all_tables() {
    load_items_table();
    load_warehouses_table();
}
}  // namespace Initializer
