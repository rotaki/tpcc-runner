#pragma once

#include <cassert>
#include <cstdint>
#include <map>
#include <vector>

#include "record_key.hpp"
#include "record_layout.hpp"

template <typename Record>
struct RecordToTable {
    using Table = std::map<typename Record::Key, Record>;
};

template <>
struct RecordToTable<History> {
    using Table = std::vector<History>;
};

class DBWrapper {
private:
    DBWrapper();
    ~DBWrapper();
    RecordToTable<Item>::Table items;
    RecordToTable<Warehouse>::Table warehouses;
    RecordToTable<Stock>::Table stocks;
    RecordToTable<District>::Table districts;
    RecordToTable<Customer>::Table customers;
    RecordToTable<History>::Table histories;
    RecordToTable<Order>::Table orders;
    RecordToTable<NewOrder>::Table neworders;
    RecordToTable<OrderLine>::Table orderlines;

public:
    DBWrapper(DBWrapper const&) = delete;
    DBWrapper& operator=(DBWrapper const&) = delete;

    static DBWrapper& get_db();

    template <typename Record>
    typename RecordToTable<Record>::Table& get_table() {
        if constexpr (std::is_same<Record, Item>::value) {
            return items;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            return warehouses;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            return stocks;
        } else if constexpr (std::is_same<Record, District>::value) {
            return districts;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            return customers;
        } else if constexpr (std::is_same<Record, History>::value) {
            return histories;
        } else if constexpr (std::is_same<Record, Order>::value) {
            return orders;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            return neworders;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            return orderlines;
        } else {
            assert(false);
        }
    }

    // Cannot be used for History records since it does not have a primary key
    template <typename Record>
    Record& allocate_record(typename Record::Key key) {
        return get_table<Record>()[key];
    }

    // Cannot be used for History records since it does not have a primary key
    template <typename Record>
    bool get_record(Record& rec, typename Record::Key& key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            rec.deep_copy(t[key]);
            return true;
        }
    }

    // Cannot be used for History records since it does not have a primary key
    template <typename Record>
    bool insert_record(const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        typename Record::Key key = Record::Key::create_key(rec);
        if (t.find(key) == t.end()) {
            t[key].deep_copy(rec);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool update_record(typename Record::Key key, const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            assert(key == Record::Key::create_key(rec));
            t[key].deep_copy(rec);
            return true;
        }
    }

    template <typename Record>
    bool delete_record(typename Record::Key key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            t.erase(key);
            return true;
        }
    }
};
