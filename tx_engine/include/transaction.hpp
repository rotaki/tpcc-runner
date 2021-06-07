#pragma once

#include <algorithm>
#include <cassert>

#include "concurrency_manager.hpp"
#include "database.hpp"
#include "logger.hpp"
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
        if (db.get_record<Record>(rec_ptr, rec_key)) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    template <IsHistory Record>
    Result prepare_record_for_insert(Record*& rec_ptr) {
        rec_ptr = ws.apply_insert_to_writeset<Record>();
        if (rec_ptr) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    template <typename Record>
    Result prepare_record_for_insert(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset
        rec_ptr = ws.apply_insert_to_writeset<Record>(rec_key);
        if (rec_ptr) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    template <typename Record>
    Result prepare_record_for_update(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset copied from db
        rec_ptr = ws.apply_update_to_writeset<Record>(rec_key);
        if (rec_ptr) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
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
        auto low_iter = db.get_lower_bound_iter<CustomerSecondary>(c_sec_key);
        auto up_iter = db.get_upper_bound_iter<CustomerSecondary>(c_sec_key);

        if (low_iter == up_iter) return Result::FAIL;
        int n = std::distance(low_iter, up_iter);

        std::vector<CustomerSecondary> temp;
        temp.reserve(n);
        for (auto it = low_iter; it != up_iter; ++it) {
            assert(it->second.ptr != nullptr);
            temp.push_back(it->second);
        }

        std::sort(
            temp.begin(), temp.end(),
            [](const CustomerSecondary& lhs, const CustomerSecondary& rhs) {
                return strncmp(lhs.ptr->c_first, rhs.ptr->c_first, Customer::MAX_FIRST) < 0;
            });

        c = temp[(n + 1) / 2 - 1].ptr;
        if (c) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    Result get_customer_by_last_name_and_prepare_for_update(
        Customer*& c, CustomerSecondary::Key c_sec_key) {
        const Customer* c_temp = nullptr;
        get_customer_by_last_name(c_temp, c_sec_key);

        if (!c_temp) {
            return Result::FAIL;
        }

        // create update record in writeset
        Customer::Key c_key = Customer::Key::create_key(*c_temp);
        c = ws.apply_update_to_writeset<Customer>(c_key);

        if (c) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
    }

    Result get_order_by_customer_id(const Order*& o, OrderSecondary::Key o_sec_key) {
        auto low_iter = db.get_lower_bound_iter<OrderSecondary>(o_sec_key);
        auto up_iter = db.get_upper_bound_iter<OrderSecondary>(o_sec_key);
        if (low_iter == up_iter) return Result::FAIL;

        uint32_t max_o_id = 0;
        Order* o_ptr = nullptr;
        for (auto it = low_iter; it != up_iter; ++it) {
            assert(it->second.ptr != nullptr);
            if (it->second.ptr->o_id > max_o_id) {
                max_o_id = it->second.ptr->o_id;
                o_ptr = it->second.ptr;
            }
        }
        o = o_ptr;
        if (o) {
            return Result::SUCCESS;
        } else {
            return Result::FAIL;
        }
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
        auto low_iter = db.get_lower_bound_iter<Record>(low);
        auto up_iter = db.get_lower_bound_iter<Record>(up);
        for (auto it = low_iter; it != up_iter; ++it) {
            func(*(it->second));
        }
        return Result::SUCCESS;
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_update(typename Record::Key low, typename Record::Key up, Func&& func) {
        auto low_iter = db.get_lower_bound_iter<Record>(low);
        auto up_iter = db.get_lower_bound_iter<Record>(up);

        for (auto it = low_iter; it != up_iter; ++it) {
            Record* rec_ptr = ws.apply_update_to_writeset<Record>(it->first);
            assert(rec_ptr != nullptr);
            func(*(rec_ptr));
        }
        return Result::SUCCESS;
    }

private:
    Database& db;
    WriteSet ws;
    ConcurrencyManager cm;
};
