#pragma once

#include <cstdint>
#include <map>

#include "table_layout.hpp"

class DBWrapper {
private:
  DBWrapper();
  ~DBWrapper();
  std::map<uint64_t, Item> items;
  std::map<uint64_t, Warehouse> warehouses;
  std::map<uint64_t, Stock> stocks;
  std::map<uint64_t, District> districts;
  std::map<uint64_t, Customer> customers;
  std::map<uint64_t, History> histories;
  std::map<uint64_t, Order> orders;
  std::map<uint64_t, NewOrder> neworders;
  std::map<uint64_t, OrderLine> orderlines;

public:
  DBWrapper(DBWrapper const&) = delete;
  DBWrapper& operator=(DBWrapper const&) = delete;

  static DBWrapper& get_db();
  
  Item& allocate_item_record(uint64_t key);
  Warehouse& allocate_warehouse_record(uint64_t key);
  Stock& allocate_stock_record(uint64_t key);
  District& allocate_district_record(uint64_t key);
  Customer& allocate_customer_record(uint64_t key);
  History& allocate_history_record(uint64_t key);
  Order& allocate_order_record(uint64_t key);
  NewOrder& allocate_neworder_record(uint64_t key);
  OrderLine& allocate_orderline_record(uint64_t key);
};
