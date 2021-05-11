#include "initializer.hpp"

#include "db_wrapper.hpp"
#include "record_generator.hpp"

namespace Initializer {
using namespace RecordGenerator;

void create_and_insert_item_record(uint32_t i_id) {
    uint64_t key = create_item_key(i_id);
    Item& i = DBWrapper::allocate_item_record(key);
    create_item(i, i_id);
}

void create_and_insert_warehouse_record(uint16_t w_id) {
    uint64_t key = create_warehouse_key(w_id);
    Warehouse& w = DBWrapper::allocate_warehouse_record(key);
    create_warehouse(w, w_id);
}

void create_and_insert_stock_record(uint16_t s_w_id, uint32_t s_i_id) {
    uint64_t key = create_stock_key(s_w_id, s_i_id);
    Stock& s = DBWrapper::allocate_stock_record(key);
    create_stock(s, s_w_id, s_i_id);
}


void create_and_insert_district_record(uint16_t d_w_id, uint8_t d_id) {
    uint64_t key = create_district_key(d_w_id, d_id);
    District& d = DBWrapper::allocate_district_record(key);
    create_district(d, d_w_id, d_id);
}

void create_and_insert_customer_record(uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, int64_t t) {
    uint64_t key = create_customer_key(c_w_id, c_d_id, c_id);
    Customer& c = DBWrapper::allocate_customer_record(key);
    create_customer(c, c_w_id, c_d_id, c_id, t);
}

void create_and_insert_history_record(
    uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t d_w_id, uint8_t h_d_id,
    uint8_t h_id) {
    uint64_t key = create_history_key(h_c_w_id, h_c_d_id, h_c_id, h_id);
    History& h = DBWrapper::allocate_history_record(key);
    create_history(h, h_c_w_id, h_c_d_id, h_c_id, d_w_id, h_d_id);
}

int64_t create_and_insert_order_record(
    uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id) {
    uint64_t key = create_order_key(o_w_id, o_d_id, o_id);
    Order& o = DBWrapper::allocate_order_record(key);
    create_order(o, o_w_id, o_d_id, o_c_id, o_id);
    return o.o_entry_d;
}

void create_and_insert_neworder_record(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    uint64_t key = create_neworder_key(no_w_id, no_d_id, no_o_id);
    NewOrder& no = DBWrapper::allocate_neworder_record(key);
    create_neworder(no, no_w_id, no_d_id, no_o_id);
}

void create_and_insert_orderline_record(
    uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id, uint32_t ol_i_id,
    uint8_t ol_number, int64_t o_entry_d) {
    uint64_t key = create_orderline_key(ol_w_id, ol_d_id, ol_o_id, ol_number);
    OrderLine& ol = DBWrapper::allocate_orderline_record(key);
    create_orderline(ol, ol_w_id, ol_d_id, ol_o_id, ol_supply_w_id, ol_i_id, ol_number, o_entry_d);
};

void load_items_table() {
    for (uint32_t i_id = 1; i_id <= Item::ITEMS; i_id++) {
        create_and_insert_item_record(i_id);
    }
}

// Loading warehouses table eventually evokes loading of all the tables other than the items table.
void load_warehouses_table() {
    for (uint16_t w_id = 1; w_id <= NumWarehouse::get_num(); w_id++) {
        create_and_insert_warehouse_record(w_id);
        load_stocks_table(w_id);
        load_districts_table(w_id);
    }
}

void load_stocks_table(uint16_t s_w_id) {
    for (uint32_t s_id = 1; s_id <= Stock::STOCKS_PER_WARE; s_id++) {
        create_and_insert_stock_record(s_w_id, s_id);
    }
}

void load_districts_table(uint16_t d_w_id) {
    for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
        create_and_insert_district_record(d_w_id, d_id);
        load_customers_table(d_w_id, d_id);
        load_orders_table(d_w_id, d_id);
    }
}

void load_customers_table(uint16_t c_w_id, uint8_t c_d_id) {
    int64_t t = static_cast<int64_t>(time(nullptr));
    for (uint32_t c_id = 1; c_id <= Customer::CUSTS_PER_DIST; c_id++) {
        create_and_insert_customer_record(c_w_id, c_d_id, c_id, t);
        load_histories_table(c_w_id, c_d_id, c_id);
    }
}

void load_histories_table(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
    create_and_insert_history_record(
        w_id, d_id, c_id, w_id, d_id, History::HISTS_PER_CUST);  // History::HISTS_PER_CUST = 1
}

void load_orders_table(uint16_t o_w_id, uint8_t o_d_id) {
    Permutation p(1, Order::ORDS_PER_DIST);
    for (uint32_t o_id = 1; o_id <= Order::ORDS_PER_DIST; o_id++) {
        uint32_t o_c_id = p[o_id - 1];
        int64_t o_entry_d = create_and_insert_order_record(o_w_id, o_d_id, o_c_id, o_id);

        load_orderlines_table(o_w_id, o_d_id, o_id, o_entry_d);
        if (o_id > 2100) {
            load_neworders_table(o_w_id, o_d_id, o_id);
        }
    }
}

void load_orderlines_table(uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, int64_t o_entry_d) {
    uint8_t ol_cnt = urand_int(OrderLine::MIN_ORDLINES_PER_ORD, OrderLine::MAX_ORDLINES_PER_ORD);
    for (uint8_t ol_number = 1; ol_number <= ol_cnt; ol_number++) {
        uint32_t ol_i_id = urand_int(1, 100000);
        create_and_insert_orderline_record(
            ol_w_id, ol_d_id, ol_o_id, ol_w_id, ol_i_id, ol_number, o_entry_d);
    }
}

void load_neworders_table(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
    create_and_insert_neworder_record(no_w_id, no_d_id, no_o_id);
}

}  // namespace Initializer
