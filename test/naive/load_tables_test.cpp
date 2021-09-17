#include "benchmarks/tpcc/include/config.hpp"
#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "gtest/gtest.h"
#include "protocols/naive/include/database.hpp"
#include "protocols/naive/include/initializer.hpp"
#include "utils/utils.hpp"

class InitialPopulationTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        Config& c = get_mutable_config();
        c.set_num_warehouses(num_warehouse);
        Initializer::load_all_tables();
    }

    static constexpr uint16_t num_warehouse = 1;
};

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
    const Item* i;
    uint32_t i_id = 1;
    Item::Key i_key = Item::Key::create_key(i_id);
    Database::get_db().get_record<Item>(i, i_key);
    ASSERT_TRUE(i != nullptr);
    check_item_record(*i);

    i_id = 100000;
    i_key = Item::Key::create_key(i_id);
    Database::get_db().get_record<Item>(i, i_key);
    ASSERT_TRUE(i != nullptr);
    check_item_record(*i);
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

void check_warehouse_record(const Warehouse& w) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    EXPECT_GE(w.w_id, 1);
    EXPECT_LE(w.w_id, num_warehouse);
    EXPECT_GE(strlen(w.w_name), 6);
    EXPECT_LE(strlen(w.w_name), 10);
    check_address(w.w_address);
    EXPECT_GE(w.w_tax, 0.0000);
    EXPECT_LE(w.w_tax, 0.2000);
    EXPECT_EQ(w.w_ytd, 300000.00);
}

void check_warehouses_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const Warehouse* w;
    uint16_t w_id = 1;
    Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
    Database::get_db().get_record<Warehouse>(w, w_key);
    ASSERT_TRUE(w != nullptr);
    check_warehouse_record(*w);

    w_id = num_warehouse;
    w_key = Warehouse::Key::create_key(w_id);
    Database::get_db().get_record<Warehouse>(w, w_key);
    ASSERT_TRUE(w != nullptr);
    check_warehouse_record(*w);
}

void check_stock_record(const Stock& s) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    EXPECT_GE(s.s_i_id, 1);
    EXPECT_LE(s.s_i_id, 100000);
    EXPECT_GE(s.s_w_id, 1);
    EXPECT_LE(s.s_w_id, num_warehouse);
    EXPECT_GE(s.s_quantity, 10);
    EXPECT_LE(s.s_quantity, 100);
    EXPECT_EQ(strlen(s.s_dist[0]), 24);
    EXPECT_EQ(strlen(s.s_dist[1]), 24);
    EXPECT_EQ(strlen(s.s_dist[2]), 24);
    EXPECT_EQ(strlen(s.s_dist[3]), 24);
    EXPECT_EQ(strlen(s.s_dist[4]), 24);
    EXPECT_EQ(strlen(s.s_dist[5]), 24);
    EXPECT_EQ(strlen(s.s_dist[6]), 24);
    EXPECT_EQ(strlen(s.s_dist[7]), 24);
    EXPECT_EQ(strlen(s.s_dist[8]), 24);
    EXPECT_EQ(strlen(s.s_dist[9]), 24);
    EXPECT_EQ(s.s_ytd, 0);
    EXPECT_EQ(s.s_order_cnt, 0);
    EXPECT_EQ(s.s_remote_cnt, 0);
    EXPECT_GE(strlen(s.s_data), 26);
    EXPECT_LE(strlen(s.s_data), 50);
}

void check_stocks_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const Stock* s;
    uint16_t w_id = 1;
    uint32_t i_id = 1;
    Stock::Key s_key = Stock::Key::create_key(w_id, i_id);
    Database::get_db().get_record<Stock>(s, s_key);
    ASSERT_TRUE(s != nullptr);
    check_stock_record(*s);

    w_id = num_warehouse;
    i_id = 100000;
    s_key = Stock::Key::create_key(w_id, i_id);
    Database::get_db().get_record<Stock>(s, s_key);
    ASSERT_TRUE(s != nullptr);
    check_stock_record(*s);
}


void check_district_record(const District& d) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
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

void check_districts_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const District* d;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    District::Key d_key = District::Key::create_key(w_id, d_id);
    Database::get_db().get_record<District>(d, d_key);
    ASSERT_TRUE(d != nullptr);
    check_district_record(*d);

    w_id = num_warehouse;
    d_id = 10;
    d_key = District::Key::create_key(w_id, d_id);
    Database::get_db().get_record<District>(d, d_key);
    ASSERT_TRUE(d != nullptr);
    check_district_record(*d);
}

void check_customer_record(const Customer& c) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
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

void check_customers_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const Customer* c;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t c_id = 1;
    Customer::Key c_key = Customer::Key::create_key(w_id, d_id, c_id);
    Database::get_db().get_record<Customer>(c, c_key);
    ASSERT_TRUE(c != nullptr);
    check_customer_record(*c);

    w_id = num_warehouse;
    d_id = 10;
    c_id = 3000;
    c_key = Customer::Key::create_key(w_id, d_id, c_id);
    Database::get_db().get_record<Customer>(c, c_key);
    ASSERT_TRUE(c != nullptr);
    check_customer_record(*c);
}


void check_history_record(const History& h) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
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

void check_histories_table() {
    // uint16_t num_warehouse = get_config().get_num_warehouses();
    // History h;
    // uint16_t w_id = 1;
    // uint8_t d_id = 1;
    // uint32_t c_id = 1;
    // HistoryKey h_key = HistoryKey::create_key(w_id, d_id, c_id);
    // ASSERT_TRUE(Database::get_db().get_history_record(h, h_key));
    // check_history_record(num_warehouse, h);

    // w_id = num_warehouse;
    // d_id = 10;
    // c_id = 3000;
    // h_key = HistoryKey::create_key(w_id, d_id, c_id);
    // ASSERT_TRUE(Database::get_db().get_history_record(h, h_key));
    // check_history_record(num_warehouse, h);
}


void check_order_record(const Order& o) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
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

void check_orders_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const Order* o;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t o_id = 1;
    Order::Key o_key = Order::Key::create_key(w_id, d_id, o_id);
    Database::get_db().get_record<Order>(o, o_key);
    ASSERT_TRUE(o != nullptr);
    check_order_record(*o);

    w_id = num_warehouse;
    d_id = 10;
    o_id = 3000;
    o_key = Order::Key::create_key(w_id, d_id, o_id);
    Database::get_db().get_record<Order>(o, o_key);
    ASSERT_TRUE(o != nullptr);
    check_order_record(*o);
}

void check_orderline_record(const OrderLine& ol) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
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

void check_orderlines_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const OrderLine* ol;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t o_id = 1;
    uint8_t ol_number = 1;
    OrderLine::Key ol_key = OrderLine::Key::create_key(w_id, d_id, o_id, ol_number);
    Database::get_db().get_record<OrderLine>(ol, ol_key);
    ASSERT_TRUE(ol != nullptr);
    check_orderline_record(*ol);

    w_id = num_warehouse;
    d_id = 10;
    o_id = 3000;
    ol_number = 5;
    ol_key = OrderLine::Key::create_key(w_id, d_id, o_id, ol_number);
    Database::get_db().get_record<OrderLine>(ol, ol_key);
    ASSERT_TRUE(ol != nullptr);
    check_orderline_record(*ol);
}

void check_neworder_record(const NewOrder& no) {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    EXPECT_GE(no.no_o_id, 2101);
    EXPECT_LE(no.no_o_id, 3000);
    EXPECT_GE(no.no_d_id, 1);
    EXPECT_LE(no.no_d_id, 10);
    EXPECT_GE(no.no_w_id, 1);
    EXPECT_LE(no.no_w_id, num_warehouse);
}

void check_neworders_table() {
    uint16_t num_warehouse = get_config().get_num_warehouses();
    const NewOrder* no;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    uint32_t o_id = 2101;
    NewOrder::Key no_key = NewOrder::Key::create_key(w_id, d_id, o_id);
    Database::get_db().get_record<NewOrder>(no, no_key);
    ASSERT_TRUE(no != nullptr);
    check_neworder_record(*no);

    w_id = num_warehouse;
    d_id = 10;
    o_id = 3000;
    no_key = NewOrder::Key::create_key(w_id, d_id, o_id);
    Database::get_db().get_record<NewOrder>(no, no_key);
    ASSERT_TRUE(no != nullptr);
    check_neworder_record(*no);
}

TEST_F(InitialPopulationTest, RecordValidityTest) {
    check_items_table();
    check_warehouses_table();
    check_stocks_table();
    check_districts_table();
    check_customers_table();
    check_histories_table();
    check_orders_table();
    check_orderlines_table();
    check_neworders_table();
}
