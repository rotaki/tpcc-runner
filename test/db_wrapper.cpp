#include "db_wrapper.hpp"

Warehouse& DBWrapper::allocate_warehouse_record(uint64_t key) {
    return warehouses[key];
}

District& DBWrapper::allocate_district_record(uint64_t key) {
    return districts[key];
}

Customer& DBWrapper::allocate_customer_record(uint64_t key) {
    return customers[key];
}

History& DBWrapper::allocate_history_record(uint64_t key) {
    return histories[key];
}

Order& DBWrapper::allocate_order_record(uint64_t key) {
    return orders[key];
}

NewOrder& DBWrapper::allocate_neworder_record(uint64_t key) {
    return neworders[key];
}

OrderLine& DBWrapper::allocate_orderline_record(uint64_t key) {
    return orderlines[key];
}

Item& DBWrapper::allocate_item_record(uint64_t key) {
    return items[key];
}

Stock& DBWrapper::allocate_stock_record(uint64_t key) {
    return stocks[key];
}
