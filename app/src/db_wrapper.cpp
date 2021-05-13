#include "db_wrapper.hpp"

DBWrapper::DBWrapper() {}

DBWrapper::~DBWrapper() {}

DBWrapper& DBWrapper::get_db() {
    static DBWrapper db;
    return db;
}

Item& DBWrapper::allocate_item_record(uint64_t key) {
    return items[key];
}

bool DBWrapper::get_item_record(Item& i, uint64_t key) {
    if (items.find(key) == items.end()) {
        return false;
    } else {
        i = items[key];
        return true;
    }
}

Warehouse& DBWrapper::allocate_warehouse_record(uint64_t key) {
    return warehouses[key];
}

bool DBWrapper::get_warehouse_record(Warehouse& w, uint64_t key) {
    if (warehouses.find(key) == warehouses.end()) {
        return false;
    } else {
        w = warehouses[key];
        return true;
    }
}

Stock& DBWrapper::allocate_stock_record(uint64_t key) {
    return stocks[key];
}

bool DBWrapper::get_stock_record(Stock& s, uint64_t key) {
    if (stocks.find(key) == stocks.end()) {
        return false;
    } else {
        s = stocks[key];
        return true;
    }
}

District& DBWrapper::allocate_district_record(uint64_t key) {
    return districts[key];
}

bool DBWrapper::get_district_record(District& d, uint64_t key) {
    if (districts.find(key) == districts.end()) {
        return false;
    } else {
        d = districts[key];
        return true;
    }
}

Customer& DBWrapper::allocate_customer_record(uint64_t key) {
    return customers[key];
}

bool DBWrapper::get_customer_record(Customer& c, uint64_t key) {
    if (customers.find(key) == customers.end()) {
        return false;
    } else {
        c = customers[key];
        return true;
    }
}

History& DBWrapper::allocate_history_record(uint64_t key) {
    return histories[key];
}

bool DBWrapper::get_history_record(History& h, uint64_t key) {
    if (histories.find(key) == histories.end()) {
        return false;
    } else {
        h = histories[key];
        return true;
    }
}

Order& DBWrapper::allocate_order_record(uint64_t key) {
    return orders[key];
}

bool DBWrapper::get_order_record(Order& o, uint64_t key) {
    if (orders.find(key) == orders.end()) {
        return false;
    } else {
        o = orders[key];
        return true;
    }
}

NewOrder& DBWrapper::allocate_neworder_record(uint64_t key) {
    return neworders[key];
}

bool DBWrapper::get_neworder_record(NewOrder& no, uint64_t key) {
    if (neworders.find(key) == neworders.end()) {
        return false;
    } else {
        no = neworders[key];
        return true;
    }
}

OrderLine& DBWrapper::allocate_orderline_record(uint64_t key) {
    return orderlines[key];
}

bool DBWrapper::get_orderline_record(OrderLine& ol, uint64_t key) {
    if (orderlines.find(key) == orderlines.end()) {
        return false;
    } else {
        ol = orderlines[key];
        return true;
    }
}
