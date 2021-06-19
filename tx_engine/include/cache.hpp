#pragma once

#include <deque>
#include <memory>
#include <stdexcept>

#include "mimalloc-new-delete.h"
#include "record_layout.hpp"
#include "type_tuple.hpp"

template <typename Record>
struct RecordMemoryCache {
public:
    Record* allocate() {
        if (cache.empty()) {
            return new Record;
        } else {
            Record* ptr = cache.back();
            cache.pop_back();
            return ptr;
        }
    }

    void deallocate(Record*& rec_ptr) {
        cache.push_back(rec_ptr);
        if (cache.size() > n) {
            delete cache.front();
            cache.pop_front();
        }
        rec_ptr = nullptr;
    }

private:
    std::deque<Record*> cache;
    size_t n = 30;  // TODO: move the constant to configuration.
};

class Cache {
public:
    template <typename Record>
    RecordMemoryCache<Record>& get_rmc() {
        return get<RecordMemoryCache<Record>>(rmc_tuple);
    }

    template <typename Record>
    static Record* allocate() {
        Cache& cache = get_cache();
        return cache.get_rmc<Record>().allocate();
    }

    template <typename Record>
    static void deallocate(Record* rec_ptr) {
        Cache& cache = get_cache();
        return cache.get_rmc<Record>().deallocate(rec_ptr);
    }

private:
    static Cache& get_cache() {
        thread_local Cache c;
        return c;
    }

    using TT = TypeTuple<
        RecordMemoryCache<Item>, RecordMemoryCache<Warehouse>, RecordMemoryCache<Stock>,
        RecordMemoryCache<District>, RecordMemoryCache<Customer>, RecordMemoryCache<History>,
        RecordMemoryCache<Order>, RecordMemoryCache<OrderLine>, RecordMemoryCache<NewOrder>>;

    TT rmc_tuple;
};
