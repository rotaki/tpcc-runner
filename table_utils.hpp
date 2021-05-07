#pragma once

#include "db_wrapper.hpp"
#include "table_layout.hpp"

class TableUtils {
public:
    static std::string serialize_warehouse(const Warehouse* w);
    static void deserialize_warehouse(Warehouse* w, const std::string& value);

    static std::string serialize_district(const District* d);
    static void deserialize_district(District* d, const std::string& value);

    static std::string serialize_customer(const Customer* c);
    static void deserialize_customer(Customer* c, const std::string& value);

    static std::string serialize_history(const History* h);
    static void deserialize_history(History* h, const std::string& value);

    static std::string serialize_order(const Order* o);
    static void deserialize_order(Order* o, const std::string& value);

    static std::string serialize_neworder(const NewOrder* n);
    static void deserialize_neworder(NewOrder* n, const std::string& value);

    static std::string serialize_orderline(const OrderLine* o);
    static void deserialize_orderline(OrderLine* o, const std::string& value);

    static std::string serialize_item(const Item* i);
    static void deserialize_item(Item* i, const std::string& value);

    static std::string serialize_stock(const Stock* s);
    static void deserialize_stock(Stock* s, const std::string& stock);
};
