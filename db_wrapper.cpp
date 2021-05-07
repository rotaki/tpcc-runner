#include "db_wrapper.hpp"

#include <cassert>

#include "table_utils.hpp"

std::map<std::string, std::string> warehouses;
std::map<std::string, std::string> districts;
std::map<std::string, std::string> customers;
std::map<std::string, std::string> histories;
std::map<std::string, std::string> orders;
std::map<std::string, std::string> neworders;
std::map<std::string, std::string> orderlines;
std::map<std::string, std::string> items;
std::map<std::string, std::string> stocks;

DBWrapper::DBWrapper() {}

bool DBWrapper::insert_record(Storage st, const std::string& key, const Record* record) {
    switch (st) {
    case WAREHOUSE:
        if (warehouses.find(key) != warehouses.end()) {
            return false;
        } else {
            warehouses[key] =
                TableUtils::serialize_warehouse(static_cast<const Warehouse*>(record));
            return true;
        }
    case DISTRICT:
        if (districts.find(key) != districts.end()) {
            return false;
        } else {
            districts[key] = TableUtils::serialize_district(static_cast<const District*>(record));
            return true;
        }
    case CUSTOMER:
        if (customers.find(key) != customers.end()) {
            return false;
        } else {
            customers[key] = TableUtils::serialize_customer(static_cast<const Customer*>(record));
            return true;
        }
    case HISTORY:
        if (histories.find(key) != histories.end()) {
            return false;
        } else {
            histories[key] = TableUtils::serialize_history(static_cast<const History*>(record));
            return true;
        }
    case ORDER:
        if (orders.find(key) != histories.end()) {
            return false;
        } else {
            orders[key] = TableUtils::serialize_order(static_cast<const Order*>(record));
            return true;
        }
    case NEWORDER:
        if (neworders.find(key) != neworders.end()) {
            return false;
        } else {
            neworders[key] = TableUtils::serialize_neworder(static_cast<const NewOrder*>(record));
            return true;
        }
    case ORDERLINE:
        if (orderlines.find(key) != orderlines.end()) {
            return false;
        } else {
            orderlines[key] =
                TableUtils::serialize_orderline(static_cast<const OrderLine*>(record));
            return true;
        }
    case ITEM:
        if (items.find(key) != items.end()) {
            return false;
        } else {
            items[key] = TableUtils::serialize_item(static_cast<const Item*>(record));
            return true;
        }
    case STOCK:
        if (stocks.find(key) != stocks.end()) {
            return false;
        } else {
            stocks[key] = TableUtils::serialize_stock(static_cast<const Stock*>(record));
            return true;
        }
    default: assert(false);
    }
}


bool DBWrapper::find_record(Storage st, const std::string& key, Record* record) {
    switch (st) {
    case WAREHOUSE:
        if (warehouses.find(key) == warehouses.end()) {
            return false;
        } else {
            TableUtils::deserialize_warehouse(static_cast<Warehouse*>(record), warehouses[key]);
            return true;
        }
    case DISTRICT:
        if (districts.find(key) == districts.end()) {
            return false;
        } else {
            TableUtils::deserialize_district(static_cast<District*>(record), districts[key]);
            return true;
        }
    case CUSTOMER:
        if (customers.find(key) == customers.end()) {
            return false;
        } else {
            TableUtils::deserialize_customer(static_cast<Customer*>(record), customers[key]);
            return true;
        }
    case HISTORY:
        if (histories.find(key) == histories.end()) {
            return false;
        } else {
            TableUtils::deserialize_history(static_cast<History*>(record), histories[key]);
            return true;
        }
    case ORDER:
        if (orders.find(key) == histories.end()) {
            return false;
        } else {
            TableUtils::deserialize_order(static_cast<Order*>(record), orders[key]);
            return true;
        }
    case NEWORDER:
        if (neworders.find(key) == neworders.end()) {
            return false;
        } else {
            TableUtils::deserialize_neworder(static_cast<NewOrder*>(record), neworders[key]);
            return true;
        }
    case ORDERLINE:
        if (orderlines.find(key) == orderlines.end()) {
            return false;
        } else {
            TableUtils::deserialize_orderline(static_cast<OrderLine*>(record), orderlines[key]);
            return true;
        }
    case ITEM:
        if (items.find(key) == items.end()) {
            return false;
        } else {
            TableUtils::deserialize_item(static_cast<Item*>(record), items[key]);
            return true;
        }
    case STOCK:
        if (stocks.find(key) == stocks.end()) {
            return false;
        } else {
            TableUtils::deserialize_stock(static_cast<Stock*>(record), stocks[key]);
            return true;
        }
    default: assert(false);
    }
}
