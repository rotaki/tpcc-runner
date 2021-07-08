#pragma once

#include <utility>

#include "config.hpp"
#include "database.hpp"
#include "masstree_index.hpp"
#include "memory_manager.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "record_with_header.hpp"
#include "utils.hpp"

namespace Initializer {

inline void create_and_insert_item_record(uint32_t i_id) {
    Item::Key key = Item::Key::create_key(i_id);
    RecordWithHeader<Item>* i = MemoryManager::allocate<Item>();
    i->rec.generate(i_id);
    MasstreeIndex<Item>::NodeInfo ni;
    Database::get_db().insert_record<Item>(key, i, ni);
}

inline void create_and_insert_warehouse_record(uint16_t w_id) {
    Warehouse::Key key = Warehouse::Key::create_key(w_id);
    RecordWithHeader<Warehouse>* w = MemoryManager::allocate<Warehouse>();
    w->rec.generate(w_id);
    MasstreeIndex<Warehouse>::NodeInfo ni;
    Database::get_db().insert_record<Warehouse>(key, w, ni);
}

inline void create_and_insert_stock_record(uint16_t s_w_id, uint32_t s_i_id) {
    Stock::Key key = Stock::Key::create_key(s_w_id, s_i_id);
    RecordWithHeader<Stock>* s = MemoryManager::allocate<Stock>();
    s->rec.generate(s_w_id, s_i_id);
    MasstreeIndex<Stock>::NodeInfo ni;
    Database::get_db().insert_record<Stock>(key, s, ni);
}

inline void create_and_insert_district_record(uint16_t d_w_id, uint8_t d_id) {
    District::Key key = District::Key::create_key(d_w_id, d_id);
    RecordWithHeader<District>* d = MemoryManager::allocate<District>();
    d->rec.generate(d_w_id, d_id);
    MasstreeIndex<District>::NodeInfo ni;
    Database::get_db().insert_record<District>(key, d, ni);
}

inline void create_and_insert_customer_record(
    uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, Timestamp t) {
    Customer::Key key = Customer::Key::create_key(c_w_id, c_d_id, c_id);
    RecordWithHeader<Customer>* c = MemoryManager::allocate<Customer>();
    c->rec.generate(c_w_id, c_d_id, c_id, t);
    MasstreeIndex<Customer>::NodeInfo ni;
    Database::get_db().insert_record<Customer>(key, c, ni);
    CustomerSecondaryKey cs_key = CustomerSecondaryKey::create_key(c->rec);
    Database::get_db().insert_record(cs_key, key);
}

inline void create_and_insert_history_record(
    uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id, uint8_t h_d_id) {
    RecordWithHeader<History>* h = MemoryManager::allocate<History>();
    h->rec.generate(h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id);
    Database::get_db().insert_record(h);
}

inline std::pair<Timestamp, uint8_t> create_and_insert_order_record(
    uint16_t o_w_id, uint8_t o_d_id, uint32_t o_id, uint32_t o_c_id) {
    Order::Key key = Order::Key::create_key(o_w_id, o_d_id, o_id);
    RecordWithHeader<Order>* o = MemoryManager::allocate<Order>();
    o->rec.generate(o_w_id, o_d_id, o_id, o_c_id);
    MasstreeIndex<Order>::NodeInfo ni;
    Database::get_db().insert_record<Order>(key, o, ni);
    OrderSecondaryKey os_key = OrderSecondaryKey::create_key(o->rec);
    Database::get_db().insert_record(os_key, key);
    return std::make_pair(o->rec.o_entry_d, o->rec.o_ol_cnt);
}

inline void create_and_insert_neworder_record(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    NewOrder::Key key = NewOrder::Key::create_key(no_w_id, no_d_id, no_o_id);
    RecordWithHeader<NewOrder>* no = MemoryManager::allocate<NewOrder>();
    no->rec.generate(no_w_id, no_d_id, no_o_id);
    MasstreeIndex<NewOrder>::NodeInfo ni;
    Database::get_db().insert_record<NewOrder>(key, no, ni);
}

inline void create_and_insert_orderline_record(
    uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint8_t ol_number, uint16_t ol_supply_w_id,
    uint32_t ol_i_id, Timestamp o_entry_d) {
    OrderLine::Key key = OrderLine::Key::create_key(ol_w_id, ol_d_id, ol_o_id, ol_number);
    RecordWithHeader<OrderLine>* ol = MemoryManager::allocate<OrderLine>();
    ol->rec.generate(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_supply_w_id, ol_i_id, o_entry_d);
    MasstreeIndex<OrderLine>::NodeInfo ni;
    Database::get_db().insert_record<OrderLine>(key, ol, ni);
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

// Loading warehouses table eventually evokes loading of all the tables except items table.
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
