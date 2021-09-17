#pragma once

#include "benchmarks/tpcc/include/config.hpp"
#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "protocols/common/memory_allocator.hpp"
#include "protocols/common/schema.hpp"
#include "protocols/tpcc_common/record_misc.hpp"
#include "utils/utils.hpp"

template <typename Index>
class Initializer {
private:
    using Key = typename Index::Key;
    using Value = typename Index::Value;
    using Version = typename Value::Version;

    static void insert_into_index(TableID table_id, Key key, void* rec) {
        Value* val = reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
        Version* version =
            reinterpret_cast<Version*>(MemoryAllocator::aligned_allocate(sizeof(Version)));
        val->initialize();
        val->version = version;
        version->read_ts = 0;
        version->write_ts = 0;
        version->prev = nullptr;
        version->rec = rec;
        version->deleted = false;
        Index::get_index().insert(table_id, key, val);
    }

    static void create_and_insert_item_record(uint32_t i_id) {
        Item::Key key = Item::Key::create_key(i_id);
        Item* i = reinterpret_cast<Item*>(MemoryAllocator::aligned_allocate(sizeof(Item)));
        i->generate(i_id);
        insert_into_index(get_id<Item>(), key.get_raw_key(), reinterpret_cast<void*>(i));
    }

    static void create_and_insert_warehouse_record(uint16_t w_id) {
        Warehouse::Key key = Warehouse::Key::create_key(w_id);
        Warehouse* w =
            reinterpret_cast<Warehouse*>(MemoryAllocator::aligned_allocate(sizeof(Warehouse)));
        w->generate(w_id);
        insert_into_index(get_id<Warehouse>(), key.get_raw_key(), reinterpret_cast<void*>(w));
    }

    static void create_and_insert_stock_record(uint16_t s_w_id, uint32_t s_i_id) {
        Stock::Key key = Stock::Key::create_key(s_w_id, s_i_id);
        Stock* s = reinterpret_cast<Stock*>(MemoryAllocator::aligned_allocate(sizeof(Stock)));
        s->generate(s_w_id, s_i_id);
        insert_into_index(get_id<Stock>(), key.get_raw_key(), reinterpret_cast<void*>(s));
    }

    static void create_and_insert_district_record(uint16_t d_w_id, uint8_t d_id) {
        District::Key key = District::Key::create_key(d_w_id, d_id);
        District* d =
            reinterpret_cast<District*>(MemoryAllocator::aligned_allocate(sizeof(District)));
        d->generate(d_w_id, d_id);
        insert_into_index(get_id<District>(), key.get_raw_key(), reinterpret_cast<void*>(d));
    }

    static void create_and_insert_customer_record(
        uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, Timestamp t) {
        Customer::Key key = Customer::Key::create_key(c_w_id, c_d_id, c_id);
        Customer* c =
            reinterpret_cast<Customer*>(MemoryAllocator::aligned_allocate(sizeof(Customer)));
        c->generate(c_w_id, c_d_id, c_id, t);
        insert_into_index(get_id<Customer>(), key.get_raw_key(), reinterpret_cast<void*>(c));
        CustomerSecondary cs;
        cs.key.c_key = key.c_key;
        CustomerSecondaryKey cs_key = CustomerSecondaryKey::create_key(*c);
        get_customer_secondary_table().emplace(cs_key, cs);
    }

    static void create_and_insert_history_record(
        uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id, uint8_t h_d_id) {
        auto& t = get_history_table();
        t.emplace_back();
        auto& h = t.back();
        h.generate(h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id);
    }

    static std::pair<Timestamp, uint8_t> create_and_insert_order_record(
        uint16_t o_w_id, uint8_t o_d_id, uint32_t o_id, uint32_t o_c_id) {
        Order::Key key = Order::Key::create_key(o_w_id, o_d_id, o_id);
        Order* o = reinterpret_cast<Order*>(MemoryAllocator::aligned_allocate(sizeof(Order)));
        o->generate(o_w_id, o_d_id, o_id, o_c_id);
        insert_into_index(get_id<Order>(), key.get_raw_key(), reinterpret_cast<void*>(o));
        OrderSecondary* os = reinterpret_cast<OrderSecondary*>(
            MemoryAllocator::aligned_allocate(sizeof(OrderSecondary)));
        os->key.o_key = key.o_key;
        OrderSecondaryKey os_key = OrderSecondaryKey::create_key(*o);
        insert_into_index(
            get_id<OrderSecondary>(), os_key.get_raw_key(), reinterpret_cast<void*>(os));
        return std::make_pair(o->o_entry_d, o->o_ol_cnt);
    }

    static void create_and_insert_neworder_record(
        uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
        NewOrder::Key key = NewOrder::Key::create_key(no_w_id, no_d_id, no_o_id);
        NewOrder* no =
            reinterpret_cast<NewOrder*>(MemoryAllocator::aligned_allocate(sizeof(NewOrder)));
        no->generate(no_w_id, no_d_id, no_o_id);
        insert_into_index(get_id<NewOrder>(), key.get_raw_key(), reinterpret_cast<void*>(no));
    }

    static void create_and_insert_orderline_record(
        uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint8_t ol_number,
        uint16_t ol_supply_w_id, uint32_t ol_i_id, Timestamp o_entry_d) {
        OrderLine::Key key = OrderLine::Key::create_key(ol_w_id, ol_d_id, ol_o_id, ol_number);
        OrderLine* ol =
            reinterpret_cast<OrderLine*>(MemoryAllocator::aligned_allocate(sizeof(OrderLine)));
        ol->generate(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_supply_w_id, ol_i_id, o_entry_d);
        insert_into_index(get_id<OrderLine>(), key.get_raw_key(), reinterpret_cast<void*>(ol));
    };

    static void load_items_table() {
        for (int i_id = 1; i_id <= Item::ITEMS; i_id++) {
            create_and_insert_item_record(i_id);
        }
    }

    static void load_histories_table(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
        create_and_insert_history_record(w_id, d_id, c_id, w_id, d_id);
    }

    static void load_customers_table(uint16_t c_w_id, uint8_t c_d_id) {
        Timestamp t = get_timestamp();
        for (int c_id = 1; c_id <= Customer::CUSTS_PER_DIST; c_id++) {
            create_and_insert_customer_record(c_w_id, c_d_id, c_id, t);
            load_histories_table(c_w_id, c_d_id, c_id);
        }
    }

    static void load_orderlines_table(
        uint8_t ol_cnt, uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, Timestamp o_entry_d) {
        for (uint8_t ol_number = 1; ol_number <= ol_cnt; ol_number++) {
            uint32_t ol_i_id = urand_int(1, 100000);
            create_and_insert_orderline_record(
                ol_w_id, ol_d_id, ol_o_id, ol_number, ol_w_id, ol_i_id, o_entry_d);
        }
    }

    static void load_neworders_table(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id) {
        create_and_insert_neworder_record(no_w_id, no_d_id, no_o_id);
    }

    static void load_orders_table(uint16_t o_w_id, uint8_t o_d_id) {
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

    static void load_districts_table(uint16_t d_w_id) {
        for (int d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            create_and_insert_district_record(d_w_id, d_id);
            load_customers_table(d_w_id, d_id);
            load_orders_table(d_w_id, d_id);
        }
    }


    static void load_stocks_table(uint16_t s_w_id) {
        for (int s_id = 1; s_id <= Stock::STOCKS_PER_WARE; s_id++) {
            create_and_insert_stock_record(s_w_id, s_id);
        }
    }

    // Loading warehouses table eventually evokes loading of all the tables other than the items
    // table.
    static void load_warehouses_table() {
        const size_t nr_w = get_config().get_num_warehouses();
        for (size_t w_id = 1; w_id <= nr_w; w_id++) {
            create_and_insert_warehouse_record(w_id);
            load_stocks_table(w_id);
            load_districts_table(w_id);
        }
    }


public:
    static void load_all_tables() {
        Schema& sch = Schema::get_mutable_schema();
        sch.set_record_size(get_id<Item>(), sizeof(Item));
        sch.set_record_size(get_id<Warehouse>(), sizeof(Warehouse));
        sch.set_record_size(get_id<Stock>(), sizeof(Stock));
        sch.set_record_size(get_id<District>(), sizeof(District));
        sch.set_record_size(get_id<Customer>(), sizeof(Customer));
        sch.set_record_size(get_id<Order>(), sizeof(Order));
        sch.set_record_size(get_id<OrderSecondary>(), sizeof(OrderSecondary));
        sch.set_record_size(get_id<OrderLine>(), sizeof(OrderLine));
        sch.set_record_size(get_id<NewOrder>(), sizeof(NewOrder));

        load_items_table();
        load_warehouses_table();
    }
};