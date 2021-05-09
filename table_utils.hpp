#pragma once

#include "db_wrapper.hpp"
#include "table_layout.hpp"

namespace TableUtils {
std::string serialize_warehouse(const Warehouse* w);
void deserialize_warehouse(Warehouse* w, const std::string& value);

std::string serialize_district(const District* d);
void deserialize_district(District* d, const std::string& value);

std::string serialize_customer(const Customer* c);
void deserialize_customer(Customer* c, const std::string& value);

std::string serialize_history(const History* h);
void deserialize_history(History* h, const std::string& value);

std::string serialize_order(const Order* o);
void deserialize_order(Order* o, const std::string& value);

std::string serialize_neworder(const NewOrder* n);
void deserialize_neworder(NewOrder* n, const std::string& value);

std::string serialize_orderline(const OrderLine* o);
void deserialize_orderline(OrderLine* o, const std::string& value);

std::string serialize_item(const Item* i);
void deserialize_item(Item* i, const std::string& value);

std::string serialize_stock(const Stock* s);
void deserialize_stock(Stock* s, const std::string& stock);
};  // namespace TableUtils
