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
#include "type_tuple.hpp"


template <typename T>
concept UseOrderedMap = is_any<T, Order, OrderLine, Customer, NewOrder>::value;

template <typename T>
concept UseUnorderedMap = is_any<T, Item, Warehouse, Stock, District>::value;

template <typename T>
concept HasHashFunction = requires(const T& key) {
    { key.hash() }
    noexcept->std::same_as<size_t>;
};

template <HasHashFunction Key>
struct Hash {
    size_t operator()(const Key& key) const noexcept { return key.hash(); }
};

template <typename Record>
struct RecordToTable {
    using type = void;
};

template <UseOrderedMap Record>
struct RecordToTable<Record> {
    using type = std::map<typename Record::Key, Record*>;
};

template <UseUnorderedMap Record>
struct RecordToTable<Record> {
    using type = std::unordered_map<
        typename Record::Key, Record*, Hash<typename Record::Key>>;
};

struct HistoryTable {
    std::deque<History*>& get_local_deque() {
        thread_local std::deque<History*> history_table;
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

    using TT = TypeTuple<
        RecordToTable<Item>::type, RecordToTable<Warehouse>::type, RecordToTable<Stock>::type,
        RecordToTable<District>::type, RecordToTable<Customer>::type,
        RecordToTable<CustomerSecondary>::type, RecordToTable<History>::type,
        RecordToTable<Order>::type, RecordToTable<OrderSecondary>::type,
        RecordToTable<NewOrder>::type, RecordToTable<OrderLine>::type>;
    TT table_tuple;

public:
    Database(Database const&) = delete;
    Database& operator=(Database const&) = delete;

    static Database& get_db() {
        static Database db;
        return db;
    }

    template <typename Record>
    typename RecordToTable<Record>::type& get_table() {
        return get<typename RecordToTable<Record>::type>(table_tuple);
    }

    template <typename Record>
        requires UseOrderedMap<Record> || IsSecondary<Record> typename RecordToIterator<Record>::type get_lower_bound_iter(typename Record::Key key) {
        return get_table<Record>().lower_bound(key);
    }

    template <typename Record>
        requires UseOrderedMap<Record> || IsSecondary<Record> typename RecordToIterator<Record>::type get_upper_bound_iter(typename Record::Key key) {
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
        return it->second;
    }

    // Cannot be used for History, CustomerSecondary, OrderSecondary
    template <typename Record>
    typename RecordToIterator<Record>::type get_record(
        const Record*& rec_ptr, typename Record::Key key) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto it = t.find(key);
        rec_ptr = (it == t.end()) ? nullptr : it->second;
        return it;
    }

    template <IsHistory Record>
    bool insert_record(Record* rec_ptr) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto& deq = t.get_local_deque();
        deq.push_back(rec_ptr);
        return true;
    }

    template <HasSecondary Record>
    bool insert_record(typename Record::Key key, Record* rec_ptr) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto [it, ret] = t.try_emplace(key, rec_ptr);
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
        requires(UseUnorderedMap<Record> || UseOrderedMap<Record>) && (!HasSecondary<Record>)
        bool insert_record(typename Record::Key key, Record* rec_ptr) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        auto [it, ret] = t.try_emplace(key, rec_ptr);
        return ret;
    }

    template <typename Record>
        requires UseUnorderedMap<Record> || UseOrderedMap<Record> 
        bool update_record(typename RecordToIterator<Record>::type iter, Record* rec_ptr) {
        Record*& dst = iter->second;
        Cache::deallocate<Record>(dst);
        dst = rec_ptr;
        return true;
    }

    template <typename Record>
        requires UseUnorderedMap<Record> || UseOrderedMap<Record> 
        bool delete_record(typename RecordToIterator<Record>::type iter) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        Cache::deallocate<Record>(iter->second);
        t.erase(iter);
        return true;
    }
};
