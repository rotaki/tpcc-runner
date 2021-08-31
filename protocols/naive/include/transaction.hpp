#pragma once

#include <algorithm>
#include <cassert>
#include <deque>

#include "protocols/naive/include/concurrency_manager.hpp"
#include "protocols/naive/include/database.hpp"
#include "protocols/naive/include/writeset.hpp"
#include "utils/logger.hpp"


class Transaction {
public:
    uint32_t thread_id = 0;

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

    // Do not use this function for read-modify-write.
    // Use prepare_record_for_update() intead.
    template <typename Record>
    Result get_record(const Record*& rec_ptr, typename Record::Key rec_key) {
        // We assume the write set does not hold the corresponding record.
        db.get_record<Record>(rec_ptr, rec_key);
        return rec_ptr == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    template <IsHistory Record>
    Result prepare_record_for_insert(Record*& rec_ptr) {
        rec_ptr = ws.apply_insert_to_writeset<Record>();
        return rec_ptr == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    template <typename Record>
    Result finish_insert([[maybe_unused]] Record* rec_ptr) {
        return Result::SUCCESS;
    }

    template <typename Record>
    Result prepare_record_for_insert(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset
        rec_ptr = ws.apply_insert_to_writeset<Record>(rec_key);
        return rec_ptr == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    // Get record and prepare for update.
    template <typename Record>
    Result prepare_record_for_update(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset copied from db
        rec_ptr = ws.apply_update_to_writeset<Record>(rec_key);
        return rec_ptr == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    template <typename Record>
    Result finish_update([[maybe_unused]] Record* rec_ptr) {
        return Result::SUCCESS;
    }

    template <typename Record>
    Result prepare_record_for_delete(
        [[maybe_unused]] const Record*& rec_ptr, typename Record::Key rec_key) {
        if (ws.apply_delete_to_writeset<Record>(rec_key)) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    template <typename Record>
    Result finish_delete([[maybe_unused]] Record* rec_ptr) {
        return Result::SUCCESS;
    }

    Result get_customer_by_last_name(
        const Customer*& c, uint16_t w_id, uint8_t d_id, const char* c_last) {
        CustomerSecondary::Key c_sec_key = CustomerSecondary::Key::create_key(w_id, d_id, c_last);
        auto& t = db.get_table<CustomerSecondary>();
        auto it = t.lower_bound(c_sec_key);
        std::deque<const CustomerSecondary*> temp;

        while (it != t.end() && it->first == c_sec_key) {
            temp.push_back(&it->second);
            ++it;
        }
        if (temp.empty()) {
            c = nullptr;
            return Result::FAIL;
        }
        std::sort(
            temp.begin(), temp.end(),
            [](const CustomerSecondary* lhs, const CustomerSecondary* rhs) {
                return ::strncmp(lhs->ptr->c_first, rhs->ptr->c_first, Customer::MAX_FIRST) < 0;
            });
        c = temp[(temp.size() + 1) / 2 - 1]->ptr;
        assert(c != nullptr);
        return Result::SUCCESS;
    }

    Result get_customer_by_last_name_and_prepare_for_update(
        Customer*& c, uint16_t w_id, uint8_t d_id, const char* c_last) {
        const Customer* c_temp = nullptr;
        if (get_customer_by_last_name(c_temp, w_id, d_id, c_last) == Result::FAIL)
            return Result::FAIL;

        // create update record in writeset
        Customer::Key c_key = Customer::Key::create_key(*c_temp);
        c = ws.apply_update_to_writeset<Customer>(c_key);
        assert(c != nullptr);
        return Result::SUCCESS;
    }

    Result get_order_by_customer_id(const Order*& o, uint16_t w_id, uint8_t d_id, uint32_t c_id) {
        OrderSecondary::Key o_sec_key = OrderSecondary::Key::create_key(w_id, d_id, c_id);
        auto& t = db.get_table<OrderSecondary>();
        auto it = t.lower_bound(o_sec_key);
        uint32_t max_o_id = 0;
        const Order* o_ptr = nullptr;
        while (it != t.end() && it->first == o_sec_key) {
            const Order* p = it->second.ptr;
            if (p->o_id > max_o_id) {
                max_o_id = p->o_id;
                o_ptr = p;
            }
            ++it;
        }
        o = o_ptr;
        return o == nullptr ? Result::FAIL : Result::SUCCESS;
    }

    Result get_neworder_with_smallest_key_no_less_than(const NewOrder*& no, NewOrder::Key low) {
        auto low_iter = db.get_lower_bound_iter<NewOrder>(low);
        if (low_iter->first.w_id == low.w_id && low_iter->first.d_id == low.d_id) {
            no = low_iter->second.get();
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_query(typename Record::Key low, typename Record::Key up, Func&& func) {
        auto& t = db.get_table<Record>();
        auto it = t.lower_bound(low);
        while (it != t.end() && it->first < up) {
            func(*it->second);
            ++it;
        }
        return Result::SUCCESS;
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_update(typename Record::Key low, typename Record::Key up, Func&& func) {
        auto& t = db.get_table<Record>();
        auto it = t.lower_bound(low);
        while (it != t.end() && it->first < up) {
            Record* rec_ptr = ws.apply_update_to_writeset<Record>(it->first);
            assert(rec_ptr != nullptr);
            func(*rec_ptr);
            ++it;
        }
        return Result::SUCCESS;
    }

private:
    Database& db;
    WriteSet ws;
    ConcurrencyManager cm;
};
