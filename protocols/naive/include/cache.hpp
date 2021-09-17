#pragma once

#include <deque>
#include <memory>
#include <stdexcept>

#include "benchmarks/tpcc/include/record_layout.hpp"
#include "mimalloc/include/mimalloc-new-delete.h"
#include "protocols/naive/include/type_tuple.hpp"

template <typename Record>
struct RecordMemoryCache {
public:
    std::unique_ptr<Record> allocate() {
        if (cache.empty()) {
            return std::unique_ptr<Record>(new Record);
        } else {
            std::unique_ptr<Record> ptr = std::move(cache.back());
            cache.pop_back();
            return ptr;
        }
    }

    void deallocate(std::unique_ptr<Record> rec_ptr) {
        cache.push_back(std::move(rec_ptr));
        if (cache.size() > n) cache.pop_front();
    }

private:
    std::deque<std::unique_ptr<Record>> cache;
    size_t n = 30;  // TODO: move the constant to configuration.
};

class Cache {
public:
    template <typename Record>
    RecordMemoryCache<Record>& get_rmc() {
        return get<RecordMemoryCache<Record>>(rmc_tuple);
    }

    template <typename Record>
    static std::unique_ptr<Record> allocate() {
        Cache& cache = get_cache();
        return cache.get_rmc<Record>().allocate();
    }

    template <typename Record>
    static void deallocate(typename std::unique_ptr<Record> rec_ptr) {
        Cache& cache = get_cache();
        return cache.get_rmc<Record>().deallocate(std::move(rec_ptr));
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
