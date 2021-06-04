#pragma once

#include <cassert>
#include <cstdint>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "cache.hpp"
#include "logger.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"

template <typename Record>
struct RecordToTable {
    using Table = std::map<typename Record::Key, std::unique_ptr<Record>>;
};

struct HistoryTable {
    std::deque<std::unique_ptr<History>>& get_local_deque() {
        thread_local std::deque<std::unique_ptr<History>> history_table;
        return history_table;
    }
};

template <>
struct RecordToTable<History> {
    using Table = HistoryTable;
};

template <>
struct RecordToTable<CustomerSecondary> {
    using Table = std::multimap<typename CustomerSecondary::Key, CustomerSecondary>;
};

template <>
struct RecordToTable<OrderSecondary> {
    using Table = std::multimap<typename OrderSecondary::Key, OrderSecondary>;
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

    // Cannot be used for History, CustomerSecondary, OrderSecondary
    template <typename Record>
    Record* allocate_record(typename Record::Key key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            t[key] = std::move(Cache::allocate<Record>());
            return t[key].get();
        } else {
            assert(t[key]);
            return t[key].get();
        }
    }

    // Cannot be used for History, CustomerSecondary, OrderSecondary
    template <typename Record>
    bool get_record(const Record*& rec_ptr, typename Record::Key key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            rec_ptr = nullptr;
            return false;
        } else {
            rec_ptr = t[key].get();
            return true;
        }
    }

    template <IsHistory Record>
    bool insert_record(std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        auto& deq = t.get_local_deque();
        deq.push_back(std::move(rec_ptr));
        return true;
    }

    template <HasSecondary Record>
    bool insert_record(typename Record::Key key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            t.emplace(key, std::move(rec_ptr));
            // create secondary record and insert
            typename Record::Secondary sec_rec(t[key].get());
            typename Record::Secondary::Key sec_key = Record::Secondary::Key::create_key(*(t[key]));
            return insert_record(sec_key, sec_rec);
        } else {
            return false;
        }
    }

    template <IsSecondary Record>
    bool insert_record(typename Record::Key key, const Record& rec) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            t.emplace(key, rec);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool insert_record(typename Record::Key key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            t.emplace(key, std::move(rec_ptr));
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool update_record(typename Record::Key key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            Cache::deallocate<Record>(std::move(t[key]));
            t[key] = std::move(rec_ptr);
            return true;
        }
    }

    template <typename Record>
    bool delete_record(typename Record::Key key) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        if (t.find(key) == t.end()) {
            return false;
        } else {
            Cache::deallocate<Record>(std::move(t[key]));
            t.erase(key);
            return true;
        }
    }
};
