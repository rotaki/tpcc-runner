#include "gtest/gtest.h"

// Glue code //////////////////////////////////////////////////////////////////
#include "db_wrapper.hpp"
#include "initializer.hpp"

// TPCC code //////////////////////////////////////////////////////////////////
#include "record_generator.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "utils.hpp"

using namespace RecordGenerator;

bool is_numeric(const char a) {
    const char c[] = "0123456789";
    for (size_t i = 0; i < strlen(c); i++) {
        if (c[i] == a) return true;
    }
    return false;
}

void check_item_record(const Item& i) {
    EXPECT_GE(i.i_id, 1);
    EXPECT_LE(i.i_id, 100000);
    EXPECT_GE(i.i_im_id, 1);
    EXPECT_LE(i.i_im_id, 10000);
    EXPECT_GE(strlen(i.i_name), 14);
    EXPECT_LE(strlen(i.i_name), 24);
    EXPECT_GE(i.i_price, 1.00);
    EXPECT_LE(i.i_price, 100.00);
    EXPECT_GE(strlen(i.i_data), 26);
    EXPECT_LE(strlen(i.i_data), 50);
}

void check_items_table() {
    Item i;
    uint32_t i_id = 1;
    ItemKey i_key = i_id;
    ASSERT_TRUE(DBWrapper::get_db().get_item_record(i, i_key));
    check_item_record(i);

    i_id = 100000;
    i_key = i_id;
    ASSERT_TRUE(DBWrapper::get_db().get_item_record(i, i_key));
    check_item_record(i);
}

void check_address(const Address& a) {
    EXPECT_GE(strlen(a.street_1), 10);
    EXPECT_LE(strlen(a.street_1), 20);
    EXPECT_GE(strlen(a.street_2), 10);
    EXPECT_LE(strlen(a.street_2), 20);
    EXPECT_GE(strlen(a.city), 10);
    EXPECT_LE(strlen(a.city), 20);
    EXPECT_EQ(strlen(a.state), 2);
    EXPECT_EQ(strlen(a.zip), 9);
    EXPECT_PRED1(is_numeric, a.zip[0]);
    EXPECT_PRED1(is_numeric, a.zip[3]);
    EXPECT_EQ(a.zip[4], '1');
    EXPECT_EQ(a.zip[8], '1');
}

void check_warehouse_record(uint16_t num_warehouse, const Warehouse& w) {
    EXPECT_GE(w.w_id, 1);
    EXPECT_LE(w.w_id, num_warehouse);
    EXPECT_GE(strlen(w.w_name), 6);
    EXPECT_LE(strlen(w.w_name), 10);
    check_address(w.w_address);
    EXPECT_GE(w.w_tax, 0.0000);
    EXPECT_LE(w.w_tax, 0.2000);
    EXPECT_EQ(w.w_ytd, 300000.00);
}

void check_warehouses_table(uint16_t num_warehouse) {
    Warehouse w;
    uint16_t w_id = 1;
    WarehouseKey w_key = w_id;
    ASSERT_TRUE(DBWrapper::get_db().get_warehouse_record(w, w_key));
    check_warehouse_record(num_warehouse, w);

    w_id = num_warehouse;
    w_key = w_id;
    ASSERT_TRUE(DBWrapper::get_db().get_warehouse_record(w, w_key));
    check_warehouse_record(num_warehouse, w);
}


void check_stock_record(uint16_t num_warehouse, const Stock& s) {
    EXPECT_GE(s.s_i_id, 1);
    EXPECT_LE(s.s_i_id, 100000);
    EXPECT_GE(s.s_w_id, 1);
    EXPECT_LE(s.s_w_id, num_warehouse);
    EXPECT_GE(s.s_quantity, 10);
    EXPECT_LE(s.s_quantity, 100);
    EXPECT_EQ(strlen(s.s_dist_01), 24);
    EXPECT_EQ(strlen(s.s_dist_02), 24);
    EXPECT_EQ(strlen(s.s_dist_03), 24);
    EXPECT_EQ(strlen(s.s_dist_04), 24);
    EXPECT_EQ(strlen(s.s_dist_05), 24);
    EXPECT_EQ(strlen(s.s_dist_06), 24);
    EXPECT_EQ(strlen(s.s_dist_07), 24);
    EXPECT_EQ(strlen(s.s_dist_08), 24);
    EXPECT_EQ(strlen(s.s_dist_09), 24);
    EXPECT_EQ(strlen(s.s_dist_10), 24);
    EXPECT_EQ(s.s_ytd, 0);
    EXPECT_EQ(s.s_order_cnt, 0);
    EXPECT_EQ(s.s_remote_cnt, 0);
    EXPECT_GE(strlen(s.s_data), 26);
    EXPECT_LE(strlen(s.s_data), 50);
}

void check_stocks_table(uint16_t num_warehouse) {
    Stock s;
    uint16_t w_id = 1;
    uint32_t i_id = 1;
    StockKey s_key = StockKey::create_key(w_id, i_id);
    ASSERT_TRUE(DBWrapper::get_db().get_stock_record(s, s_key));
    check_stock_record(num_warehouse, s);

    w_id = num_warehouse;
    i_id = 100000;
    s_key = StockKey::create_key(w_id, i_id);
    ASSERT_TRUE(DBWrapper::get_db().get_stock_record(s, s_key));
    check_stock_record(num_warehouse, s);
}

void check_district_record(uint16_t num_warehouse, const District& d) {
    EXPECT_GE(d.d_id, 1);
    EXPECT_LE(d.d_id, 10);
    EXPECT_GE(d.d_w_id, 1);
    EXPECT_LE(d.d_w_id, num_warehouse);
    EXPECT_GE(strlen(d.d_name), 6);
    EXPECT_LE(strlen(d.d_name), 10);
    check_address(d.d_address);
    EXPECT_GE(d.d_tax, 0.00000);
    EXPECT_LE(d.d_tax, 0.20000);
    EXPECT_EQ(d.d_ytd, 30000.00);
    EXPECT_EQ(d.d_next_o_id, 3001);
}

void check_districts_table(uint16_t num_warehouse) {
    District d;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    DistrictKey d_key = DistrictKey::create_key(w_id, d_id);
    ASSERT_TRUE(DBWrapper::get_db().get_district_record(d, d_key));
    check_district_record(num_warehouse, d);

    w_id = num_warehouse;
    d_id = 10;
    d_key = DistrictKey::create_key(w_id, d_id);
    ASSERT_TRUE(DBWrapper::get_db().get_district_record(d, d_key));
    check_district_record(num_warehouse, d);
}

void check_customer_record(uint16_t num_warehouse, const Customer& c) {
    EXPECT_GE(c.c_id, 1);
    EXPECT_LE(c.c_id, 3000);
    EXPECT_GE(c.c_d_id, 1);
    EXPECT_LE(c.c_d_id, 10);
    EXPECT_GE(c.c_w_id, 1);
    EXPECT_LE(c.c_w_id, num_warehouse);
    EXPECT_GE(strlen(c.c_last), 9);
    EXPECT_LE(strlen(c.c_last), 15);
    EXPECT_STREQ(c.c_middle, "OE");
    EXPECT_GE(strlen(c.c_first), 8);
    EXPECT_LE(strlen(c.c_first), 16);
    check_address(c.c_address);
    EXPECT_PRED1(is_numeric, c.c_phone[0]);
    EXPECT_PRED1(is_numeric, c.c_phone[15]);
    EXPECT_EQ(strlen(c.c_phone), 16);
    // C-Since
    if (c.c_credit[0] == 'G')
        EXPECT_STREQ(c.c_credit, "GC");
    else
        EXPECT_STREQ(c.c_credit, "BC");
    EXPECT_EQ(c.c_credit_lim, 50000.00);
    EXPECT_GE(c.c_discount, 0.0000);
    EXPECT_LE(c.c_discount, 0.5000);
    EXPECT_EQ(c.c_balance, -10.00);
    EXPECT_EQ(c.c_ytd_payment, 10.00);
    EXPECT_EQ(c.c_payment_cnt, 1);
    EXPECT_EQ(c.c_delivery_cnt, 0);
    EXPECT_GE(strlen(c.c_data), 300);
    EXPECT_LE(strlen(c.c_data), 500);
}

void check_customers_table(uint16_t num_warehouse) {
    Customer c;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t c_id = 1;
    CustomerKey c_key = CustomerKey::create_key(w_id, d_id, c_id);
    ASSERT_TRUE(DBWrapper::get_db().get_customer_record(c, c_key));
    check_customer_record(num_warehouse, c);

    w_id = num_warehouse;
    d_id = 10;
    c_id = 3000;
    c_key = CustomerKey::create_key(w_id, d_id, c_id);
    ASSERT_TRUE(DBWrapper::get_db().get_customer_record(c, c_key));
    check_customer_record(num_warehouse, c);
}

void check_history_record(uint16_t num_warehouse, const History& h) {
    EXPECT_GE(h.h_c_id, 1);
    EXPECT_LE(h.h_c_id, 3000);
    EXPECT_GE(h.h_d_id, 1);
    EXPECT_LE(h.h_d_id, 10);
    EXPECT_EQ(h.h_d_id, h.h_c_d_id);
    EXPECT_GE(h.h_w_id, 1);
    EXPECT_LE(h.h_w_id, num_warehouse);
    EXPECT_EQ(h.h_w_id, h.h_c_w_id);
    // H_Date
    EXPECT_EQ(h.h_amount, 10.00);
    EXPECT_GE(strlen(h.h_data), 12);
    EXPECT_LE(strlen(h.h_data), 24);
}

void check_histories_table(uint16_t num_warehouse) {
    History h;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t c_id = 1;
    HistoryKey h_key = HistoryKey::create_key(w_id, d_id, c_id);
    ASSERT_TRUE(DBWrapper::get_db().get_history_record(h, h_key));
    check_history_record(num_warehouse, h);

    w_id = num_warehouse;
    d_id = 10;
    c_id = 3000;
    h_key = HistoryKey::create_key(w_id, d_id, c_id);
    ASSERT_TRUE(DBWrapper::get_db().get_history_record(h, h_key));
    check_history_record(num_warehouse, h);
}

void check_order_record(uint16_t num_warehouse, const Order& o) {
    EXPECT_GE(o.o_id, 1);
    EXPECT_LE(o.o_id, 3000);
    EXPECT_GE(o.o_c_id, 1);
    EXPECT_LE(o.o_c_id, 3000);
    EXPECT_GE(o.o_d_id, 1);
    EXPECT_LE(o.o_d_id, 10);
    EXPECT_GE(o.o_w_id, 1);
    EXPECT_LE(o.o_w_id, num_warehouse);
    // O_ENTRY_D
    if (o.o_id < 2101) {
        EXPECT_GE(o.o_carrier_id, 1);
        EXPECT_LE(o.o_carrier_id, 10);
    } else {
        EXPECT_EQ(o.o_carrier_id, 0);
    }
    EXPECT_GE(o.o_ol_cnt, 5);
    EXPECT_LE(o.o_ol_cnt, 15);
    EXPECT_EQ(o.o_all_local, 1);
}

void check_orders_table(uint16_t num_warehouse) {
    Order o;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t o_id = 1;
    OrderKey o_key = OrderKey::create_key(w_id, d_id, o_id);
    ASSERT_TRUE(DBWrapper::get_db().get_order_record(o, o_key));
    check_order_record(num_warehouse, o);

    w_id = num_warehouse;
    d_id = 10;
    o_id = 3000;
    o_key = OrderKey::create_key(w_id, d_id, o_id);
    ASSERT_TRUE(DBWrapper::get_db().get_order_record(o, o_key));
    check_order_record(num_warehouse, o);
}

void check_orderline_record(uint16_t num_warehouse, const OrderLine& ol) {
    EXPECT_GE(ol.ol_o_id, 1);
    EXPECT_LE(ol.ol_o_id, 3000);
    EXPECT_GE(ol.ol_d_id, 1);
    EXPECT_LE(ol.ol_d_id, 10);
    EXPECT_GE(ol.ol_w_id, 1);
    EXPECT_LE(ol.ol_w_id, num_warehouse);
    EXPECT_GE(ol.ol_number, 1);
    EXPECT_LE(ol.ol_number, 15);
    EXPECT_GE(ol.ol_i_id, 1);
    EXPECT_LE(ol.ol_i_id, 100000);
    EXPECT_EQ(ol.ol_supply_w_id, ol.ol_w_id);
    if (ol.ol_o_id < 2101) {
        // OL_DELIVERY_D
    } else {
        EXPECT_EQ(ol.ol_delivery_d, 0);
    }
    EXPECT_EQ(ol.ol_quantity, 5);
    if (ol.ol_o_id < 2101) {
        EXPECT_EQ(ol.ol_amount, 0.00);
    } else {
        EXPECT_GE(ol.ol_amount, 0.01);
        EXPECT_LE(ol.ol_amount, 9999.99);
    }
    EXPECT_EQ(strlen(ol.ol_dist_info), 24);
}

void check_orderlines_table(uint16_t num_warehouse) {
    OrderLine ol;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t o_id = 1;
    uint8_t ol_number = 1;
    OrderLineKey ol_key = OrderLineKey::create_key(w_id, d_id, o_id, ol_number);
    ASSERT_TRUE(DBWrapper::get_db().get_orderline_record(ol, ol_key));
    check_orderline_record(num_warehouse, ol);

    w_id = num_warehouse;
    d_id = 10;
    o_id = 3000;
    ol_number = 5;
    ol_key = OrderLineKey::create_key(w_id, d_id, o_id, ol_number);
    ASSERT_TRUE(DBWrapper::get_db().get_orderline_record(ol, ol_key));
    check_orderline_record(num_warehouse, ol);
}

void check_neworder_record(uint16_t num_warehouse, const NewOrder& no) {
    EXPECT_GE(no.no_o_id, 2101);
    EXPECT_LE(no.no_o_id, 3000);
    EXPECT_GE(no.no_d_id, 1);
    EXPECT_LE(no.no_d_id, 10);
    EXPECT_GE(no.no_w_id, 1);
    EXPECT_LE(no.no_w_id, num_warehouse);
}

void check_neworders_table(uint16_t num_warehouse) {
    NewOrder no;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t o_id = 2101;
    NewOrderKey no_key = NewOrderKey::create_key(w_id, d_id, o_id);
    ASSERT_TRUE(DBWrapper::get_db().get_neworder_record(no, no_key));
    check_neworder_record(num_warehouse, no);

    w_id = num_warehouse;
    d_id = 10;
    o_id = 3000;
    no_key = NewOrderKey::create_key(w_id, d_id, o_id);
    ASSERT_TRUE(DBWrapper::get_db().get_neworder_record(no, no_key));
    check_neworder_record(num_warehouse, no);
}

TEST(LoadTablesSuit, CheckItemValidity) {
    Initializer::load_items_table();
    check_items_table();
}

TEST(LoadTableSuit, CheckOtherValidity) {
    int num_warehouse = 5;
    Config& c = get_mutable_config();
    c.set_num_warehouses(num_warehouse);
    Initializer::load_warehouses_table();

    check_warehouses_table(num_warehouse);
    check_stocks_table(num_warehouse);
    check_districts_table(num_warehouse);
    check_customers_table(num_warehouse);
    check_histories_table(num_warehouse);
    check_orders_table(num_warehouse);
    check_orderlines_table(num_warehouse);
    check_neworders_table(num_warehouse);
}
