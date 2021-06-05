#pragma once

#include <deque>
#include <memory>
#include <stdexcept>

#include "mimalloc-new-delete.h"
#include "record_layout.hpp"

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
        if (cache.size() > n) {
            delete rec_ptr.release();
        } else {
            cache.push_back(std::move(rec_ptr));
        }
    }

private:
    std::deque<std::unique_ptr<Record>> cache;
    size_t n = 30;
};

class Cache {
public:
    template <typename Record>
    RecordMemoryCache<Record>& get_rmc() {
        if constexpr (std::is_same<Record, Item>::value) {
            return rmc_i;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            return rmc_w;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            return rmc_s;
        } else if constexpr (std::is_same<Record, District>::value) {
            return rmc_d;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            return rmc_c;
        } else if constexpr (std::is_same<Record, History>::value) {
            return rmc_h;
        } else if constexpr (std::is_same<Record, Order>::value) {
            return rmc_o;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            return rmc_no;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            return rmc_ol;
        } else {
            throw std::runtime_error("Undefined Record");
        }
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

    RecordMemoryCache<Item> rmc_i;
    RecordMemoryCache<Warehouse> rmc_w;
    RecordMemoryCache<Stock> rmc_s;
    RecordMemoryCache<District> rmc_d;
    RecordMemoryCache<Customer> rmc_c;
    RecordMemoryCache<History> rmc_h;
    RecordMemoryCache<Order> rmc_o;
    RecordMemoryCache<OrderLine> rmc_ol;
    RecordMemoryCache<NewOrder> rmc_no;
};
