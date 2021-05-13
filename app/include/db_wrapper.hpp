#pragma once

#include <cstdint>
#include <map>

#include "record_key.hpp"
#include "record_layout.hpp"

class DBWrapper {
private:
    DBWrapper();
    ~DBWrapper();
    std::map<ItemKey, Item> items;
    std::map<WarehouseKey, Warehouse> warehouses;
    std::map<StockKey, Stock> stocks;
    std::map<DistrictKey, District> districts;
    std::map<CustomerKey, Customer> customers;
    std::map<HistoryKey, History> histories;
    std::map<OrderKey, Order> orders;
    std::map<NewOrderKey, NewOrder> neworders;
    std::map<OrderLineKey, OrderLine> orderlines;

public:
    DBWrapper(DBWrapper const&) = delete;
    DBWrapper& operator=(DBWrapper const&) = delete;

    static DBWrapper& get_db();

    Item& allocate_item_record(ItemKey key);
    bool get_item_record(Item& w, ItemKey key);

    Warehouse& allocate_warehouse_record(WarehouseKey key);
    bool get_warehouse_record(Warehouse& w, WarehouseKey key);

    Stock& allocate_stock_record(StockKey key);
    bool get_stock_record(Stock& w, StockKey key);

    District& allocate_district_record(DistrictKey key);
    bool get_district_record(District& w, DistrictKey key);

    Customer& allocate_customer_record(CustomerKey key);
    bool get_customer_record(Customer& w, CustomerKey key);

    History& allocate_history_record(HistoryKey key);
    bool get_history_record(History& w, HistoryKey key);

    Order& allocate_order_record(OrderKey key);
    bool get_order_record(Order& w, OrderKey key);

    NewOrder& allocate_neworder_record(NewOrderKey key);
    bool get_neworder_record(NewOrder& w, NewOrderKey key);

    OrderLine& allocate_orderline_record(OrderLineKey key);
    bool get_orderline_record(OrderLine& ol, OrderLineKey key);
};
