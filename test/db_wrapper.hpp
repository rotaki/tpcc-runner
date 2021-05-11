#pragma once

#include <cstdint>
#include <map>

#include "table_layout.hpp"

class DBWrapper {
private:
    DBWrapper();
    ~DBWrapper();
    static std::map<uint64_t, Item> items;
    static std::map<uint64_t, Warehouse> warehouses;
    static std::map<uint64_t, Stock> stocks;
    static std::map<uint64_t, District> districts;
    static std::map<uint64_t, Customer> customers;
    static std::map<uint64_t, History> histories;
    static std::map<uint64_t, Order> orders;
    static std::map<uint64_t, NewOrder> neworders;
    static std::map<uint64_t, OrderLine> orderlines;

public:
    static Item& allocate_item_record(uint64_t key);
    static Warehouse& allocate_warehouse_record(uint64_t key);
    static Stock& allocate_stock_record(uint64_t key);
    static District& allocate_district_record(uint64_t key);
    static Customer& allocate_customer_record(uint64_t key);
    static History& allocate_history_record(uint64_t key);
    static Order& allocate_order_record(uint64_t key);
    static NewOrder& allocate_neworder_record(uint64_t key);
    static OrderLine& allocate_orderline_record(uint64_t key);
};
