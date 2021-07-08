#pragma once

#include <algorithm>
#include <cassert>

#include "concurrency_manager.hpp"
#include "database.hpp"
#include "logger.hpp"
#include "masstree_index.hpp"
#include "writeset.hpp"

class Transaction {
public:
    Transaction(Database& db)
        : db(db)
        , ws(db) {
        cm.lock();
    }

    ~Transaction() { cm.release(); }

    void abort() { ws.clear_all(); }

    bool commit() {
        if (ws.apply_to_database()) {
            return true;
        } else {
            abort();
            return false;
        }
    }

    enum Result {
        SUCCESS,
        FAIL,  // e.g. not found, already exists
        ABORT  // e.g. could not acquire lock/latch and no-wait-> system abort
    };

    template <typename Record>
    Result get_record(const Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in db
        const RecordWithHeader<Record>* rec_with_header_ptr = nullptr;
        typename MasstreeIndex<Record>::NodeInfo ni;
        if (db.get_record<Record>(rec_with_header_ptr, rec_key, ni)) {
            assert(rec_with_header_ptr != nullptr);
            rec_ptr = &(rec_with_header_ptr->rec);
            assert(rec_ptr != nullptr);
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    template <typename Record>
    Result prepare_record_for_insert(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset
        RecordWithHeader<Record>* rec_with_header_ptr =
            ws.apply_insert_to_writeset<Record>(rec_key);
        if (rec_with_header_ptr) {
            rec_ptr = &(rec_with_header_ptr->rec);
            assert(rec_ptr != nullptr);
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    Result prepare_record_for_insert(History*& rec_ptr) {
        // rec_ptr points to data in writeset
        RecordWithHeader<History>* rec_with_header_ptr = ws.apply_insert_to_writeset();
        if (rec_with_header_ptr) {
            rec_ptr = &(rec_with_header_ptr->rec);
            assert(rec_ptr != nullptr);
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    template <typename Record>
    Result prepare_record_for_update(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset copied from db
        RecordWithHeader<Record>* rec_with_header_ptr =
            ws.apply_update_to_writeset<Record>(rec_key);
        rec_ptr = (rec_with_header_ptr == nullptr ? nullptr : &(rec_with_header_ptr->rec));
        return (rec_ptr == nullptr ? Result::FAIL : Result::SUCCESS);
    }

    template <typename Record>
    Result delete_record(typename Record::Key rec_key) {
        if (ws.apply_delete_to_writeset<Record>(rec_key)) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    Result get_customer_by_last_name(const Customer*& c, CustomerSecondary::Key c_sec_key) {
        auto& t = db.get_table<CustomerSecondary>();
        auto it = t.lower_bound(c_sec_key);
        std::deque<Customer::Key> temp;

        while (it != t.end() && it->first == c_sec_key) {
            temp.push_back(it->second);
            ++it;
        }

        if (temp.empty()) {
            c = nullptr;
            return Result::FAIL;
        }

        // TODO: implement properly
        return get_record(c, temp[0]);
    }

    Result get_customer_by_last_name_and_prepare_for_update(
        Customer*& c, CustomerSecondary::Key c_sec_key) {
        const Customer* c_temp = nullptr;
        if (get_customer_by_last_name(c_temp, c_sec_key) == Result::FAIL) return Result::FAIL;

        // create update record in writeset
        Customer::Key c_key = Customer::Key::create_key(*c_temp);
        RecordWithHeader<Customer>* c_with_header_ptr =
            ws.apply_update_to_writeset<Customer>(c_key);
        c = (c_with_header_ptr == nullptr ? nullptr : &(c_with_header_ptr->rec));
        return c == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    Result get_order_by_customer_id(const Order*& o, OrderSecondary::Key o_sec_key) {
        auto& t = db.get_table<OrderSecondary>();
        auto it = t.lower_bound(o_sec_key);
        uint32_t max_o_id = 0;
        const Order* o_ptr = nullptr;
        while (it != t.end() && it->first == o_sec_key) {
            Order::Key key = it->second;
            LOG_TRACE("orderkey: %lu, ", key.get_raw_key());
            const Order* p = nullptr;
            if (get_record(p, key) == Result::SUCCESS && p->o_id > max_o_id) {
                max_o_id = p->o_id;
                o_ptr = p;
            }
            ++it;
        }
        o = o_ptr;
        return o == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    Result get_neworder_with_smallest_key_no_less_than(const NewOrder*& no, NewOrder::Key low) {
        auto& t = db.get_table<NewOrder>();
        uint64_t low_raw = low.get_raw_key();
        uint64_t high_raw = -1;
        using MT = MasstreeIndex<NewOrder>;
        bool found = false;
        t.read_range_forward(
            low_raw, high_raw, 1,
            {[&](const MT::LeafType* leaf, uint64_t version, bool& continue_flag) {
                 (void)leaf;
                 (void)version;
                 (void)continue_flag;
             },
             [&](const typename MT::Str& key, const MT::ValueType* value, bool& continue_flag) {
                 (void)continue_flag;
                 NewOrder::Key actual_key{
                     __builtin_bswap64(*reinterpret_cast<const uint64_t*>(key.s))};
                 if (actual_key.w_id == low.w_id && actual_key.d_id == low.d_id) {
                     found = true;
                     no = &(value->rec);
                     // need not set `continue_flag = false` because scan count is 1
                 }
             }});
        return found ? Result::SUCCESS : Result::FAIL;
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_query(typename Record::Key low, typename Record::Key up, Func&& func) {
        auto& t = db.get_table<Record>();
        using MT = MasstreeIndex<Record>;
        uint64_t low_raw = low.get_raw_key();
        uint64_t high_raw = up.get_raw_key();
        t.read_range_forward(
            low_raw, high_raw, -1,
            {[&](const typename MT::LeafType* leaf, uint64_t version, bool& continue_flag) {
                 (void)leaf;
                 (void)version;
                 (void)continue_flag;
             },
             [&](const typename MT::Str& key, const typename MT::ValueType* value,
                 bool& continue_flag) {
                 (void)key;
                 (void)continue_flag;
                 func(value->rec);
             }});
        return Result::SUCCESS;
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_update(typename Record::Key low, typename Record::Key up, Func&& func) {
        auto& t = db.get_table<Record>();
        using MT = MasstreeIndex<Record>;
        uint64_t low_raw = low.get_raw_key();
        uint64_t high_raw = up.get_raw_key();
        t.read_range_forward(
            low_raw, high_raw, -1,
            {[&](const typename MT::LeafType* leaf, uint64_t version, bool& continue_flag) {
                 (void)leaf;
                 (void)version;
                 (void)continue_flag;
             },
             [&](const typename MT::Str& key, const typename MT::ValueType* value,
                 bool& continue_flag) {
                 (void)value;
                 (void)continue_flag;
                 uint64_t actual_key{__builtin_bswap64(*reinterpret_cast<const uint64_t*>(key.s))};
                 typename Record::Key rec_key(actual_key);
                 // TODO: use `value` to avoid additional tree traverse
                 RecordWithHeader<Record>* rec_ptr = ws.apply_update_to_writeset<Record>(rec_key);
                 func(rec_ptr->rec);
             }});
        return Result::SUCCESS;
    }

private:
    Database& db;
    WriteSet ws;
    ConcurrencyManager cm;
};