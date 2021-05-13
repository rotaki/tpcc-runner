#include "db_wrapper.hpp"

DBWrapper::DBWrapper() {}

DBWrapper::~DBWrapper() {}

DBWrapper& DBWrapper::get_db() {
    static DBWrapper db;
    return db;
}

Item& DBWrapper::allocate_item_record(ItemKey key) {
    return items[key];
}

bool DBWrapper::get_item_record(Item& i, ItemKey key) {
    if (items.find(key) == items.end()) {
        return false;
    } else {
        i.deep_copy(items[key]);
        return true;
    }
}

Warehouse& DBWrapper::allocate_warehouse_record(WarehouseKey key) {
    return warehouses[key];
}

bool DBWrapper::get_warehouse_record(Warehouse& w, WarehouseKey key) {
    if (warehouses.find(key) == warehouses.end()) {
        return false;
    } else {
        w.deep_copy(warehouses[key]);
        return true;
    }
}

Stock& DBWrapper::allocate_stock_record(StockKey key) {
    return stocks[key];
}

bool DBWrapper::get_stock_record(Stock& s, StockKey key) {
    if (stocks.find(key) == stocks.end()) {
        return false;
    } else {
        s.deep_copy(stocks[key]);
        return true;
    }
}

District& DBWrapper::allocate_district_record(DistrictKey key) {
    return districts[key];
}

bool DBWrapper::get_district_record(District& d, DistrictKey key) {
    if (districts.find(key) == districts.end()) {
        return false;
    } else {
        d.deep_copy(districts[key]);
        return true;
    }
}

Customer& DBWrapper::allocate_customer_record(CustomerKey key) {
    return customers[key];
}

bool DBWrapper::get_customer_record(Customer& c, CustomerKey key) {
    if (customers.find(key) == customers.end()) {
        return false;
    } else {
        c.deep_copy(customers[key]);
        return true;
    }
}

History& DBWrapper::allocate_history_record(HistoryKey key) {
    return histories[key];
}

bool DBWrapper::get_history_record(History& h, HistoryKey key) {
    if (histories.find(key) == histories.end()) {
        return false;
    } else {
        h.deep_copy(histories[key]);
        return true;
    }
}

Order& DBWrapper::allocate_order_record(OrderKey key) {
    return orders[key];
}

bool DBWrapper::get_order_record(Order& o, OrderKey key) {
    if (orders.find(key) == orders.end()) {
        return false;
    } else {
        o.deep_copy(orders[key]);
        return true;
    }
}

NewOrder& DBWrapper::allocate_neworder_record(NewOrderKey key) {
    return neworders[key];
}

bool DBWrapper::get_neworder_record(NewOrder& no, NewOrderKey key) {
    if (neworders.find(key) == neworders.end()) {
        return false;
    } else {
        no.deep_copy(neworders[key]);
        return true;
    }
}

OrderLine& DBWrapper::allocate_orderline_record(OrderLineKey key) {
    return orderlines[key];
}

bool DBWrapper::get_orderline_record(OrderLine& ol, OrderLineKey key) {
    if (orderlines.find(key) == orderlines.end()) {
        return false;
    } else {
        ol.deep_copy(orderlines[key]);
        return true;
    }
}
