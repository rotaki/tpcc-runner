#pragma once

#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <deque>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "protocols/common/epoch_manager.hpp"
#include "protocols/tpcc_common/record_misc.hpp"

template <typename Protocol>
class Transaction {
public:
    uint32_t thread_id = 0;

    Transaction(Worker<Protocol>& worker)
        : thread_id(worker.get_id())
        , protocol(std::move(worker.begin_tx())) {}

    ~Transaction() {}

    void abort() { protocol->abort(); }

    bool commit() {
        if (protocol->precommit()) {
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
        rec_ptr = reinterpret_cast<const Record*>(
            protocol->read(get_id<Record>(), rec_key.get_raw_key()));
        return rec_ptr == nullptr ? Result::ABORT : Result::SUCCESS;
    }

    Result prepare_record_for_insert(History*& rec_ptr) {
        auto& t = get_history_table();
        t.emplace_back();
        rec_ptr = &(t.back());
        return Result::SUCCESS;
    }

    template <typename Record>
    Result prepare_record_for_insert(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset
        rec_ptr =
            reinterpret_cast<Record*>(protocol->insert(get_id<Record>(), rec_key.get_raw_key()));
        return rec_ptr == nullptr ? Result::ABORT : Result::SUCCESS;
    }

    template <typename Record>
    Result finish_insert([[maybe_unused]] Record* rec_ptr) {
        // secondary index insert
        if constexpr (std::is_same<Record, Order>::value) {
            Secondary<Record>* sec = nullptr;
            auto sec_key = Secondary<Record>::Key::create_key(*rec_ptr);
            Result res = prepare_record_for_insert(sec, sec_key);
            if (res != Result::SUCCESS) return res;
            typename Record::Key pri_key = Record::Key::create_key(*rec_ptr);
            sec->key = pri_key;
        }
        return Result::SUCCESS;
    }

    // Get record and prepare for update.
    template <typename Record>
    Result prepare_record_for_update(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to data in writeset copied from db
        rec_ptr =
            reinterpret_cast<Record*>(protocol->update(get_id<Record>(), rec_key.get_raw_key()));
        return rec_ptr == nullptr ? Result::ABORT : Result::SUCCESS;
    }

    template <typename Record>
    Result finish_update([[maybe_unused]] Record* rec_ptr) {
        // Secondary index update is not needed in TPC-C
        return Result::SUCCESS;
    }

    template <typename Record>
    Result prepare_record_for_delete(const Record*& rec_ptr, typename Record::Key rec_key) {
        rec_ptr =
            reinterpret_cast<Record*>(protocol->remove(get_id<Record>(), rec_key.get_raw_key()));
        return rec_ptr == nullptr ? Result::ABORT : Result::SUCCESS;
    }

    template <typename Record>
    Result finish_delete([[maybe_unused]] Record* rec_ptr) {
        // Secondary index delete is not needed in TPC-C
        return Result::SUCCESS;
    }

    Result get_customer_by_last_name(
        const Customer*& c, uint16_t w_id, uint8_t d_id, const char* c_last) {
        CustomerSecondary::Key c_sec_key = CustomerSecondary::Key::create_key(w_id, d_id, c_last);
        auto& t = get_customer_secondary_table();
        auto it = t.lower_bound(c_sec_key);
        std::deque<Customer::Key> keys;
        std::deque<const Customer*> recs;

        while (it != t.end() && it->first == c_sec_key) {
            keys.push_back(it->second.key);
            ++it;
        }

        if (keys.empty()) {
            c = nullptr;
            return Result::FAIL;
        }

        for (auto iter = keys.begin(); iter != keys.end(); ++iter) {
            recs.emplace_back();
            Result res = get_record(recs.back(), *iter);
            if (res != Result::SUCCESS) return res;
        }

        if (recs.empty()) {
            c = nullptr;
            return Result::FAIL;
        }

        std::sort(recs.begin(), recs.end(), [](const Customer* lhs, const Customer* rhs) {
            return ::strncmp(lhs->c_first, rhs->c_first, Customer::MAX_FIRST) < 0;
        });

        c = recs[(recs.size() + 1) / 2 - 1];
        assert(c != nullptr);
        return Result::SUCCESS;
    }

    Result get_customer_by_last_name_and_prepare_for_update(
        Customer*& c, uint16_t w_id, uint8_t d_id, const char* c_last) {
        const Customer* c_temp = nullptr;
        Result res = get_customer_by_last_name(c_temp, w_id, d_id, c_last);
        if (res != Result::SUCCESS) return res;

        // create update record in writeset
        Customer::Key c_key = Customer::Key::create_key(*c_temp);
        return prepare_record_for_update(c, c_key);
    }

    Result get_order_by_customer_id(const Order*& o, uint16_t w_id, uint8_t d_id, uint32_t c_id) {
        OrderSecondary::Key o_sec_low_key = OrderSecondary::Key::create_key(w_id, d_id, c_id, 0);
        OrderSecondary::Key o_sec_high_key =
            OrderSecondary::Key::create_key(w_id, d_id, c_id + 1, 0);
        std::map<uint64_t, void*> kr_map;
        bool scanned = protocol->read_scan(
            get_id<OrderSecondary>(), o_sec_low_key.get_raw_key(), o_sec_high_key.get_raw_key(), 1,
            true, kr_map);

        if (scanned) {
            for (auto& [k, r]: kr_map) {
                assert(r);
                auto o_sec = reinterpret_cast<OrderSecondary*>(r);
                Order::Key o_key = o_sec->key;
                return get_record(o, o_key);
            }
            assert(false);
        }
        return Result::ABORT;
    }

    Result get_neworder_with_smallest_key_no_less_than(const NewOrder*& no, NewOrder::Key low) {
        std::map<uint64_t, void*> kr_map;
        bool scanned = protocol->read_scan(
            get_id<NewOrder>(), low.get_raw_key(), UINT64_MAX, 1, false, kr_map);
        if (scanned) {
            for (auto& [k, r]: kr_map) {
                assert(r);
                NewOrder::Key nk(k);
                if (nk.w_id == low.w_id && nk.d_id == low.d_id) {
                    no = reinterpret_cast<NewOrder*>(r);
                    return Result::SUCCESS;
                } else {
                    return Result::FAIL;
                }
                assert(false);
            }
            return Result::FAIL;
        }
        return Result::ABORT;
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_query(typename Record::Key low, typename Record::Key up, Func&& func) {
        std::map<uint64_t, void*> kr_map;
        bool scanned = protocol->read_scan(
            get_id<Record>(), low.get_raw_key(), up.get_raw_key(), -1, false, kr_map);
        if (scanned) {
            for (auto& [k, r]: kr_map) {
                assert(r);
                Record* rec = reinterpret_cast<Record*>(r);
                func(*rec);
            }
            return Result::SUCCESS;
        } else {
            return Result::ABORT;
        }
    }

    // [low ,up)
    template <typename Record, typename Func>
    Result range_update(typename Record::Key low, typename Record::Key up, Func&& func) {
        std::map<uint64_t, void*> kr_map;
        bool scanned = protocol->update_scan(
            get_id<Record>(), low.get_raw_key(), up.get_raw_key(), -1, false, kr_map);
        Result res;
        if (scanned) {
            for (auto& [k, r]: kr_map) {
                assert(r);
                Record* rec = reinterpret_cast<Record*>(r);
                func(*rec);
                res = finish_update(rec);
                if (res != Result::SUCCESS) return res;
            }
            return Result::SUCCESS;
        } else {
            return Result::ABORT;
        }
    }

private:
    std::unique_ptr<Protocol> protocol = nullptr;
};
