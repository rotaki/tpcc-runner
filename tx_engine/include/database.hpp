#pragma once

#include <cassert>
#include <concepts>
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


template <typename T>
concept UseOrderedMap = is_any<T, Order, OrderLine, Customer, NewOrder>::value;

template <typename T>
concept UseUnorderedMap = is_any<T, Item, Warehouse, Stock, District>::value;


template <typename Key>
requires requires(Key k) {
    { k.hash() }
    noexcept->std::same_as<size_t>;
}
struct Hash {
    size_t operator()(const Key& key) const noexcept { return key.hash(); }
};


template <typename Record>
struct RecordToTable {
    using type = void;
};

template <UseOrderedMap Record>
struct RecordToTable<Record> {
    using type = std::map<typename Record::Key, std::unique_ptr<Record>>;
};

template <UseUnorderedMap Record>
struct RecordToTable<Record> {
    using type = std::unordered_map<
        typename Record::Key, std::unique_ptr<Record>, Hash<typename Record::Key>>;
};

struct HistoryTable {
    std::deque<std::unique_ptr<History>>& get_local_deque() {
        thread_local std::deque<std::unique_ptr<History>> history_table;
        return history_table;
    }
};

template <IsHistory Record>
struct RecordToTable<Record> {
    using type = HistoryTable;
};

template <IsSecondary Record>
struct RecordToTable<Record> {
    using type = std::multimap<typename Record::Key, Record>;
};


template <typename Record>
struct RecordToIterator {
    using type = typename RecordToTable<Record>::type::iterator;
};

template <>
struct RecordToIterator<History> {
    using type = size_t;  // dummy
};


class Database {
private:
    Database() = default;
    ~Database() = default;

    RecordToTable<Item>::type items;
    RecordToTable<Warehouse>::type warehouses;
    RecordToTable<Stock>::type stocks;
    RecordToTable<District>::type districts;
    RecordToTable<Customer>::type customers;
    RecordToTable<CustomerSecondary>::type customers_secondary;
    RecordToTable<History>::type histories;
    RecordToTable<Order>::type orders;
    RecordToTable<OrderSecondary>::type orders_secondary;
    RecordToTable<NewOrder>::type neworders;
    RecordToTable<OrderLine>::type orderlines;

public:
    Database(Database const&) = delete;
    Database& operator=(Database const&) = delete;

    static Database& get_db() {
        static Database db;
        return db;
    }

    template <typename Record>
    typename RecordToTable<Record>::type& get_table() {
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
    requires UseOrderedMap<Record> || IsSecondary<Record>
    typename RecordToIterator<Record>::type get_lower_bound_iter(typename Record::Key key) {
        return get_table<Record>().lower_bound(key);
    }

    template <typename Record>
    requires UseOrderedMap<Record> || IsSecondary<Record>
    typename RecordToIterator<Record>::type get_upper_bound_iter(typename Record::Key key) {
        return get_table<Record>().upper_bound(key);
    }

    // Cannot be used for History, CustomerSecondary, OrderSecondary
    template <typename Record>
    Record* allocate_record(typename Record::Key key) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto it = t.find(key);
        if (it == t.end()) {
            bool ret;
            std::tie(it, ret) = t.emplace(key, Cache::allocate<Record>());
            assert(ret);
        }
        return it->second.get();
    }

    // Cannot be used for History, CustomerSecondary, OrderSecondary
    template <typename Record>
    typename RecordToIterator<Record>::type get_record(
        const Record*& rec_ptr, typename Record::Key key) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto it = t.find(key);
        rec_ptr = (it == t.end()) ? nullptr : it->second.get();
        return it;
    }

    template <IsHistory Record>
    bool insert_record(std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto& deq = t.get_local_deque();
        deq.push_back(std::move(rec_ptr));
        return true;
    }

    template <HasSecondary Record>
    bool insert_record(typename Record::Key key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto [it, ret] = t.try_emplace(key, std::move(rec_ptr));
        if (!ret) return false;

        // create secondary record and insert
        Record& rec = *it->second;
        typename Record::Secondary sec_rec(&rec);
        typename Record::Secondary::Key sec_key = Record::Secondary::Key::create_key(rec);
        return insert_record(sec_key, sec_rec);
    }

#ifndef NDEBUG
    template <IsSecondary Record>
    bool multimap_find(
        typename RecordToTable<Record>::type& t, typename Record::Key key,
        const Record& rec) const {
        bool found = false;
        auto it0 = t.lower_bound(key);
        auto it1 = t.upper_bound(key);
        while (it0 != it1) {
            if (it0->second == rec) {
                found = true;
                break;
            }
            ++it0;
        }
        return found;
    }
#endif

    template <IsSecondary Record>
    bool insert_record(typename Record::Key key, const Record& rec) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
#ifndef NDEBUG
        if (multimap_find(t, key, rec)) return false;
#endif
        t.emplace(key, rec);
        return true;
    }

    template <typename Record>
    requires(UseUnorderedMap<Record> || UseOrderedMap<Record>)
        && (!HasSecondary<Record>)bool insert_record(
            typename Record::Key key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto [it, ret] = t.try_emplace(key, std::move(rec_ptr));
        return ret;
    }

    template <typename Record>
    requires UseUnorderedMap<Record> || UseOrderedMap<Record>
    bool update_record(
        typename RecordToIterator<Record>::type iter, std::unique_ptr<Record> rec_ptr) {
        std::unique_ptr<Record>& dst = iter->second;
        Cache::deallocate<Record>(std::move(dst));
        dst = std::move(rec_ptr);
        return true;
    }

    template <typename Record>
    requires UseUnorderedMap<Record> || UseOrderedMap<Record>
    bool delete_record(typename RecordToIterator<Record>::type iter) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        Cache::deallocate<Record>(std::move(iter->second));
        t.erase(iter);
        return true;
    }
};
