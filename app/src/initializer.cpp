#include "initializer.hpp"

#include <iostream>

#include "db_wrapper.hpp"
#include "record_generator.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "utils.hpp"

namespace Initializer {
using namespace RecordGenerator;

void create_and_insert_item_record(uint32_t i_id) {
    ItemKey key = ItemKey::create_key(i_id);
    Item& i = DBWrapper::get_db().allocate_record<Item>(key);
    create_item(i, i_id);
}

void create_and_insert_warehouse_record(uint16_t w_id) {
    WarehouseKey key = WarehouseKey::create_key(w_id);
    Warehouse& w = DBWrapper::get_db().allocate_record<Warehouse>(key);
    create_warehouse(w, w_id);
}

void create_and_insert_stock_record(uint16_t s_w_id, uint32_t s_i_id) {
    StockKey key = StockKey::create_key(s_w_id, s_i_id);
    Stock& s = DBWrapper::get_db().allocate_record<Stock>(key);
    create_stock(s, s_w_id, s_i_id);
}

void create_and_insert_district_record(uint16_t d_w_id, uint8_t d_id) {
    DistrictKey key = DistrictKey::create_key(d_w_id, d_id);
    District& d = DBWrapper::get_db().allocate_record<District>(key);
    create_district(d, d_w_id, d_id);
}

void create_and_insert_customer_record(
    uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, Timestamp t) {
    CustomerKey key = CustomerKey::create_key(c_w_id, c_d_id, c_id);
    Customer& c = DBWrapper::get_db().allocate_record<Customer>(key);
    create_customer(c, c_w_id, c_d_id, c_id, t);
}

void create_and_insert_history_record(
    uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t d_w_id, uint8_t h_d_id) {
    History h;
    create_history(h, h_c_w_id, h_c_d_id, h_c_id, d_w_id, h_d_id);
    DBWrapper::get_db().get_table<History>().emplace_back(h);
}

Timestamp create_and_insert_order_record(
    uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id) {
    OrderKey key = OrderKey::create_key(o_w_id, o_d_id, o_id);
    Order& o = DBWrapper::get_db().allocate_record<Order>(key);
    create_order(o, o_w_id, o_d_id, o_c_id, o_id);
    return o.o_entry_d;
}

void create_and_insert_neworder_record(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    NewOrderKey key = NewOrderKey::create_key(no_w_id, no_d_id, no_o_id);
    NewOrder& no = DBWrapper::get_db().allocate_record<NewOrder>(key);
    create_neworder(no, no_w_id, no_d_id, no_o_id);
}

void create_and_insert_orderline_record(
    uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id, uint32_t ol_i_id,
    uint8_t ol_number, Timestamp o_entry_d) {
    OrderLineKey key = OrderLineKey::create_key(ol_w_id, ol_d_id, ol_o_id, ol_number);
    OrderLine& ol = DBWrapper::get_db().allocate_record<OrderLine>(key);
    create_orderline(ol, ol_w_id, ol_d_id, ol_o_id, ol_supply_w_id, ol_i_id, ol_number, o_entry_d);
};

void load_items_table() {
    for (int i_id = 1; i_id <= Item::ITEMS; i_id++) {
        create_and_insert_item_record(i_id);
    }
}

// Loading warehouses table eventually evokes loading of all the tables other than the items table.
void load_warehouses_table() {
    const Config& c = get_config();
    for (int w_id = 1; w_id <= c.get_num_warehouses(); w_id++) {
        create_and_insert_warehouse_record(w_id);
        load_stocks_table(w_id);
        load_districts_table(w_id);
    }
}

void load_stocks_table(uint16_t s_w_id) {
    for (int s_id = 1; s_id <= Stock::STOCKS_PER_WARE; s_id++) {
        create_and_insert_stock_record(s_w_id, s_id);
    }
}

void load_districts_table(uint16_t d_w_id) {
    for (int d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
        create_and_insert_district_record(d_w_id, d_id);
        load_customers_table(d_w_id, d_id);
        load_orders_table(d_w_id, d_id);
    }
}

void load_customers_table(uint16_t c_w_id, uint8_t c_d_id) {
    Timestamp t = get_timestamp();
    for (int c_id = 1; c_id <= Customer::CUSTS_PER_DIST; c_id++) {
        create_and_insert_customer_record(c_w_id, c_d_id, c_id, t);
        load_histories_table(c_w_id, c_d_id, c_id);
    }
}

void load_histories_table(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
    create_and_insert_history_record(w_id, d_id, c_id, w_id, d_id);
}

void load_orders_table(uint16_t o_w_id, uint8_t o_d_id) {
    Permutation p(1, Order::ORDS_PER_DIST);
    for (int o_id = 1; o_id <= Order::ORDS_PER_DIST; o_id++) {
        uint32_t o_c_id = p[o_id - 1];
        Timestamp o_entry_d = create_and_insert_order_record(o_w_id, o_d_id, o_c_id, o_id);

        load_orderlines_table(o_w_id, o_d_id, o_id, o_entry_d);
        if (o_id > 2100) {
            load_neworders_table(o_w_id, o_d_id, o_id);
        }
    }
}

void load_orderlines_table(
    uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, Timestamp o_entry_d) {
    uint8_t ol_cnt = urand_int(OrderLine::MIN_ORDLINES_PER_ORD, OrderLine::MAX_ORDLINES_PER_ORD);
    for (int ol_number = 1; ol_number <= ol_cnt; ol_number++) {
        uint32_t ol_i_id = urand_int(1, 100000);
        create_and_insert_orderline_record(
            ol_w_id, ol_d_id, ol_o_id, ol_w_id, ol_i_id, ol_number, o_entry_d);
    }
}

void load_neworders_table(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    create_and_insert_neworder_record(no_w_id, no_d_id, no_o_id);
}

}  // namespace Initializer
