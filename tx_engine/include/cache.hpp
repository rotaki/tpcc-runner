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
    size_t n = 20;
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
        cache.add_one<Record>();
        return cache.get_rmc<Record>().allocate();
    }

    template <typename Record>
    static void deallocate(typename std::unique_ptr<Record> rec_ptr) {
        Cache& cache = get_cache();
        cache.minus_one<Record>();
        return cache.get_rmc<Record>().deallocate(std::move(rec_ptr));
    }

    static void print_memory_usage() {
        Cache& cache = get_cache();
        cache.print_memory_usage_details();
    }

private:
    static Cache& get_cache() {
        thread_local Cache c;
        return c;
    }

    template<typename Record>
    void add_one() {
        if constexpr (std::is_same<Record, Item>::value) {
            m_i = ++num_i;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            m_w = ++num_w;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            m_s = ++num_s;
        } else if constexpr (std::is_same<Record, District>::value) {
            m_d = ++num_d;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            m_c = ++num_c;
        } else if constexpr (std::is_same<Record, History>::value) {
            m_h = ++num_h;
        } else if constexpr (std::is_same<Record, Order>::value) {
            m_o = ++num_o;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            m_no = ++num_no;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            m_ol = ++num_ol;
        } else {
            throw std::runtime_error("Undefined Record");
        }
    }

    template<typename Record>
    void minus_one() {
        if constexpr (std::is_same<Record, Item>::value) {
            num_i--;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            num_w--;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            num_s--;
        } else if constexpr (std::is_same<Record, District>::value) {
            num_d--;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            num_c--;
        } else if constexpr (std::is_same<Record, History>::value) {
            num_h--;
        } else if constexpr (std::is_same<Record, Order>::value) {
            num_o--;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            num_no--;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            num_ol--;
        } else {
            throw std::runtime_error("Undefined Record");
        }
    }

    void print_memory_usage_details() {
        printf("i:%d w:%d s:%d c:%d h:%d o:%d ol:%d no:%d\n", m_i, m_w, m_s, m_c, m_h, m_o, m_ol, m_no);
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

    int num_i = 0;
    int num_w = 0;
    int num_s = 0;
    int num_d = 0;
    int num_c = 0;
    int num_h = 0;
    int num_o = 0;
    int num_ol = 0;
    int num_no = 0;
    int m_i = 0;
    int m_w = 0;
    int m_s = 0;
    int m_d = 0;
    int m_c = 0;
    int m_h = 0;
    int m_o = 0;
    int m_ol = 0;
    int m_no = 0;
};
