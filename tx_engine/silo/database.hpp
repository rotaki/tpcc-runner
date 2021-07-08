#pragma once

#include <cassert>
#include <concepts>
#include <cstdint>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "logger.hpp"
#include "masstree_index.hpp"
#include "memory_manager.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "record_with_header.hpp"

template <typename Record>
struct RecordToTable {
    using Table = MasstreeIndex<Record>;
};

struct HistoryTable {
    std::deque<RecordWithHeader<History>*>& get_local_deque() {
        thread_local std::deque<RecordWithHeader<History>*> history_table;
        return history_table;
    }
};

template <>
struct RecordToTable<History> {
    using Table = HistoryTable;
};

template <>
struct RecordToTable<OrderSecondary> {
    using Table = std::multimap<OrderSecondary::Key, Order::Key>;
};

template <>
struct RecordToTable<CustomerSecondary> {
    using Table = std::multimap<CustomerSecondary::Key, Customer::Key>;
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

    // Cannot be used for History, CustomerSecondary, OrderSecondary
    template <typename Record>
    bool get_record(
        const RecordWithHeader<Record>*& rec_ptr, typename Record::Key key,
        typename MasstreeIndex<Record>::NodeInfo& ni) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        rec_ptr = t.get(key.get_raw_key(), ni);
        return (rec_ptr == nullptr ? false : true);
    }

    bool insert_record(RecordWithHeader<History>* rec_ptr) {
        typename RecordToTable<History>::Table& t = get_table<History>();
        auto& deq = t.get_local_deque();
        deq.push_back(rec_ptr);
        return true;
    }

    bool insert_record(CustomerSecondary::Key sec_key, Customer::Key pri_key) {
        auto& t = get_table<CustomerSecondary>();
        t.emplace(sec_key, pri_key);
        return true;
    }

    bool insert_record(OrderSecondary::Key sec_key, Order::Key pri_key) {
        auto& t = get_table<OrderSecondary>();
        t.emplace(sec_key, pri_key);
        return true;
    }

    template <typename Record>
    bool insert_record(
        typename Record::Key key, RecordWithHeader<Record>* rec_ptr,
        typename MasstreeIndex<Record>::NodeInfo& ni) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        return t.insert(key.get_raw_key(), rec_ptr, ni);
    }

    template <typename Record>
    bool update_record(
        typename Record::Key key, RecordWithHeader<Record>* rec_ptr,
        typename MasstreeIndex<Record>::NodeInfo& ni) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        RecordWithHeader<Record>* old_rec_ptr = nullptr;
        if (t.update(old_rec_ptr, key.get_raw_key(), rec_ptr, ni)) {
            if (old_rec_ptr != nullptr) MemoryManager::deallocate<Record>(old_rec_ptr);
            return true;
        } else {
            assert(old_rec_ptr == nullptr);
            return false;
        }
    }

    template <typename Record>
    bool delete_record(typename Record::Key key, typename MasstreeIndex<Record>::NodeInfo& ni) {
        typename RecordToTable<Record>::Table& t = get_table<Record>();
        RecordWithHeader<Record>* old_rec_ptr = nullptr;
        if (t.remove(old_rec_ptr, key.get_raw_key(), ni)) {
            if (old_rec_ptr != nullptr) MemoryManager::deallocate<Record>(old_rec_ptr);
            return true;
        } else {
            assert(old_rec_ptr == nullptr);
            return false;
        }
    }
};

template <>
inline bool Database::insert_record<Customer>(
    Customer::Key key, RecordWithHeader<Customer>* rec_ptr, MasstreeIndex<Customer>::NodeInfo& ni) {
    auto& t = get_table<Customer>();
    if (t.insert(key.get_raw_key(), rec_ptr, ni)) {
        // create secondary record and insert
        CustomerSecondary::Key sec_key = CustomerSecondary::Key::create_key(rec_ptr->rec);
        return insert_record(sec_key, key);
    } else {
        return false;
    }
}

template <>
inline bool Database::insert_record<Order>(
    Order::Key key, RecordWithHeader<Order>* rec_ptr, typename MasstreeIndex<Order>::NodeInfo& ni) {
    auto& t = get_table<Order>();
    if (t.insert(key.get_raw_key(), rec_ptr, ni)) {
        // create secondary record and insert
        OrderSecondary::Key sec_key = OrderSecondary::Key::create_key(rec_ptr->rec);
        return insert_record(sec_key, key);
    } else {
        return false;
    }
}