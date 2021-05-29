#pragma once

#include <cassert>
#include <cstdint>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "logger.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"

template <typename Record>
struct RecordToTable {
    using Table = std::map<typename Record::Key, Record>;
};

template <>
struct RecordToTable<CustomerSecondary> {
    using Table = std::multimap<CustomerSecondary::Key, CustomerSecondary>;
};

template <>
struct RecordToTable<OrderSecondary> {
    using Table = std::multimap<OrderSecondary::Key, OrderSecondary>;
};

struct HistoryTable {
    std::deque<History>& get_local_deque() {
        thread_local std::deque<History> history_table;
        return history_table;
    }
};

template <>
struct RecordToTable<History> {
    using Table = HistoryTable;
};

class Database {
private:
    Database() = default;
    ~Database() = default;
    RecordToTable<Item>::Table items;
    RecordToTable<Warehouse>::Table warehouses;
    RecordToTable<Stock>::Table stocks;
    RecordToTable<District>::Table districts;
    RecordToTable<Customer>::Table customers;
    RecordToTable<CustomerSecondary>::Table customers_secondary;
    RecordToTable<History>::Table histories;
    RecordToTable<Order>::Table orders;
    RecordToTable<OrderSecondary>::Table orders_secondary;
    RecordToTable<NewOrder>::Table neworders;
    RecordToTable<OrderLine>::Table orderlines;

public:
    Database(Database const&) = delete;
    Database& operator=(Database const&) = delete;

    static Database& get_db() {
        static Database db;
        return db;
    }

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
        } else if constexpr (std::is_same<Record, CustomerSecondary>::value) {
            return customers_secondary;
        } else if constexpr (std::is_same<Record, History>::value) {
            return histories;
        } else if constexpr (std::is_same<Record, Order>::value) {
            return orders;
        } else if constexpr (std::is_same<Record, OrderSecondary>::value) {
            return orders_secondary;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            return neworders;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            return orderlines;
        } else {
            assert(false);
        }
    }

    template <typename Record>
    typename RecordToTable<Record>::Table::iterator get_lower_bound_iter(typename Record::Key key) {
        return get_table<Record>().lower_bound(key);
    }

    template <typename Record>
    typename RecordToTable<Record>::Table::iterator get_upper_bound_iter(typename Record::Key key) {
        return get_table<Record>().upper_bound(key);
    }

    // Cannot be used for History records since it does not have a primary key
    template <typename Record>
    Record& allocate_record(typename Record::Key key) {
        return get_table<Record>()[key];
    }

    template <typename Record>
    bool lookup_record(typename Record::Key key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        return (t.find(key) != t.end());
    }

    // Cannot be used for History records since it does not have a primary key
    template <typename Record>
    bool get_record(Record& rec, typename Record::Key key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            rec.deep_copy_from(t[key]);
            return true;
        }
    }

    template <IsHistory Record>
    bool insert_record(const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        auto& deq = t.get_local_deque();
        deq.emplace_back();
        deq.back().deep_copy_from(rec);
        return true;
    }

    template <HasSecondary Record>
    bool insert_record(const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        typename Record::Key key = Record::Key::create_key(rec);
        if (t.find(key) == t.end()) {
            t[key].deep_copy_from(rec);
            typename Record::Secondary sec_rec(&(t[key]));
            return insert_record<typename Record::Secondary>(sec_rec);
        } else {
            return false;
        }
    }

    template <IsSecondary Record>
    bool insert_record(const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (rec.ptr != nullptr) {
            typename Record::Key key = Record::Key::create_key(*(rec.ptr));
            t.insert(std::pair<typename Record::Key, Record>(key, rec));
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool insert_record(const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        typename Record::Key key = Record::Key::create_key(rec);
        if (t.find(key) == t.end()) {
            t[key].deep_copy_from(rec);
            return true;
        } else {
            return false;
        }
    }

    // updating records of secondary and history table is not needed under the specification
    template <typename Record>
    bool update_record(typename Record::Key key, const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            assert(key == Record::Key::create_key(rec));
            t[key].deep_copy_from(rec);
            return true;
        }
    }

    // deleting records with secondary and history table should not occur under the specification
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