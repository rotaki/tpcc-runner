#pragma once

#pragma once

#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <deque>

#include "protocols/common/timestamp_manager.hpp"
#include "protocols/ycsb_common/record_misc.hpp"

template <typename Protocol>
class Transaction {
public:
    uint32_t thread_id = 0;
    Worker<Protocol>& worker;

    Transaction(Worker<Protocol>& worker)
        : thread_id(worker.get_id())
        , worker(worker)
        , protocol(std::move(worker.begin_tx())) {}

    ~Transaction() {}

    void abort() {
        protocol->abort();
        protocol->set_new_ts(
            worker.get_abort_boosted_ts(), worker.get_smallest_ts(), worker.get_largest_ts());
    }

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
        // Secondary index update is not needed in YCSB
        return Result::SUCCESS;
    }

    // Unconditional Write (This will not place key in readset)
    template <typename Record>
    Result prepare_record_for_write(Record*& rec_ptr, typename Record::Key rec_key) {
        // rec_ptr points to allocated record
        rec_ptr =
            reinterpret_cast<Record*>(protocol->write(get_id<Record>(), rec_key.get_raw_key()));
        return rec_ptr == nullptr ? Result::ABORT : Result::SUCCESS;
    }

    template <typename Record>
    Result finish_write([[maybe_unused]] Record* rec_ptr) {
        // Secondary index update is not needed in YCSB
        return Result::SUCCESS;
    }

private:
    std::unique_ptr<Protocol> protocol = nullptr;
};