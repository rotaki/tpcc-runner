#pragma once

#include <cassert>
#include <concepts>
#include <cstdint>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "protocols/naive/include/cache.hpp"
#include "protocols/naive/include/type_tuple.hpp"
#include "utils/logger.hpp"

struct CustomerSecondaryKey;
struct OrderSecondaryKey;

struct CustomerSecondary {
    using Key = CustomerSecondaryKey;
    Customer* ptr = nullptr;
    CustomerSecondary() {}
    CustomerSecondary(Customer* ptr)
        : ptr(ptr) {}
    bool operator==(const CustomerSecondary& rhs) const noexcept { return ptr == rhs.ptr; };
};

struct CustomerSecondaryKey {
    union {
        struct {
            uint32_t d_id : 8;
            uint32_t w_id : 16;
        };
        uint32_t num = 0;
    };
    char c_last[Customer::MAX_LAST + 1];
    CustomerSecondaryKey() = default;
    CustomerSecondaryKey(const CustomerSecondaryKey& c) {
        d_id = c.d_id;
        w_id = c.w_id;
        copy_cstr(c_last, c.c_last, sizeof(c_last));
    }

    int cmp_c_last(const CustomerSecondaryKey& rhs) const {
        return ::strncmp(c_last, rhs.c_last, Customer::MAX_LAST);
    }

    bool operator<(const CustomerSecondaryKey& rhs) const noexcept {
        if (num == rhs.num)
            return cmp_c_last(rhs) < 0;
        else
            return num < rhs.num;
    }
    bool operator==(const CustomerSecondaryKey& rhs) const noexcept {
        return num == rhs.num && cmp_c_last(rhs) == 0;
    }
    static CustomerSecondaryKey create_key(uint16_t w_id, uint8_t d_id, const char* c_last_in) {
        CustomerSecondaryKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        copy_cstr(k.c_last, c_last_in, sizeof(k.c_last));
        return k;
    }
    static CustomerSecondaryKey create_key(const Customer& c) {
        CustomerSecondaryKey k;
        k.w_id = c.c_w_id;
        k.d_id = c.c_d_id;
        copy_cstr(k.c_last, c.c_last, sizeof(k.c_last));
        return k;
    }
};

struct OrderSecondary {
    using Key = OrderSecondaryKey;
    Order* ptr = nullptr;
    OrderSecondary() {}
    OrderSecondary(Order* ptr)
        : ptr(ptr) {}
    bool operator==(const OrderSecondary& rhs) const noexcept { return ptr == rhs.ptr; }
};

struct OrderSecondaryKey {
    union {
        struct {
            uint64_t c_id : 32;
            uint64_t d_id : 8;
            uint64_t w_id : 16;
        };
        uint64_t o_sec_key = 0;
    };
    bool operator<(const OrderSecondaryKey& rhs) const noexcept {
        return o_sec_key < rhs.o_sec_key;
    }
    bool operator==(const OrderSecondaryKey& rhs) const noexcept {
        return o_sec_key == rhs.o_sec_key;
    }
    static OrderSecondaryKey create_key(uint16_t w_id, uint8_t d_id, uint32_t c_id) {
        OrderSecondaryKey k;
        k.w_id = w_id;
        k.d_id = d_id;
        k.c_id = c_id;
        return k;
    }
    static OrderSecondaryKey create_key(const Order& o) {
        OrderSecondaryKey k;
        k.w_id = o.o_w_id;
        k.d_id = o.o_d_id;
        k.c_id = o.o_c_id;
        return k;
    }
};

template <typename T, typename... Ts>
struct is_any : std::disjunction<std::is_same<T, Ts>...> {};

template <typename T>
concept HasSecondary = is_any<T, Customer, Order>::value;

template <typename T>
concept IsSecondary = is_any<T, CustomerSecondary, OrderSecondary>::value;

template <typename T>
concept IsHistory = std::is_same<T, History>::value;

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

template <typename T>
struct Traits;

template <>
struct Traits<Order> {
    using SecondaryIndexType = OrderSecondary;
};

template <>
struct Traits<Customer> {
    using SecondaryIndexType = CustomerSecondary;
};

template <typename Record>
using Secondary = typename Traits<Record>::SecondaryIndexType;

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
        Secondary<Record> sec_rec(&rec);
        typename Secondary<Record>::Key sec_key = Secondary<Record>::Key::create_key(rec);
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
        requires UseUnorderedMap<Record> || UseOrderedMap<Record> bool update_record(typename RecordToIterator<Record>::type iter, std::unique_ptr<Record> rec_ptr) {
        std::unique_ptr<Record>& dst = iter->second;
        Cache::deallocate<Record>(std::move(dst));
        dst = std::move(rec_ptr);
        return true;
    }

    template <typename Record>
        requires UseUnorderedMap<Record> || UseOrderedMap<Record> bool delete_record(typename RecordToIterator<Record>::type iter) {
        typename RecordToTable<Record>::type& t = get_table<Record>();
        Cache::deallocate<Record>(std::move(iter->second));
        t.erase(iter);
        return true;
    }
};
