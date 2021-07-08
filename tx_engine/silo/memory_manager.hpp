#pragma once

#include <deque>
#include <memory>
#include <stdexcept>

#include "mimalloc-new-delete.h"
#include "record_layout.hpp"
#include "record_with_header.hpp"


template <typename Record>
struct RecordMemoryManager {
public:
    ~RecordMemoryManager() {
        for (size_t i = 0; i < record_memory.size(); ++i) {
            delete record_memory[i];
        }
    }

    RecordWithHeader<Record>* allocate() {
        return reinterpret_cast<RecordWithHeader<Record>*>(new RecordWithHeader<Record>);
    }

    void deallocate(RecordWithHeader<Record>* rec_ptr) { record_memory.push_back(rec_ptr); }

private:
    std::deque<RecordWithHeader<Record>*> record_memory;
};

class MemoryManager {
public:
    template <typename Record>
    RecordMemoryManager<Record>& get_rmm() {
        if constexpr (std::is_same<Record, Item>::value) {
            return rmm_i;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            return rmm_w;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            return rmm_s;
        } else if constexpr (std::is_same<Record, District>::value) {
            return rmm_d;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            return rmm_c;
        } else if constexpr (std::is_same<Record, History>::value) {
            return rmm_h;
        } else if constexpr (std::is_same<Record, Order>::value) {
            return rmm_o;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            return rmm_no;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            return rmm_ol;
        } else {
            throw std::runtime_error("Undefined Record");
        }
    }

    template <typename Record>
    static RecordWithHeader<Record>* allocate() {
        MemoryManager& mm = get_memory_manager();
        return mm.get_rmm<Record>().allocate();
    }

    template <typename Record>
    static void deallocate(RecordWithHeader<Record>* rec_ptr) {
        MemoryManager& mm = get_memory_manager();
        return mm.get_rmm<Record>().deallocate(rec_ptr);
    }

private:
    static MemoryManager& get_memory_manager() {
        thread_local MemoryManager mm;
        return mm;
    }

    RecordMemoryManager<Item> rmm_i;
    RecordMemoryManager<Warehouse> rmm_w;
    RecordMemoryManager<Stock> rmm_s;
    RecordMemoryManager<District> rmm_d;
    RecordMemoryManager<Customer> rmm_c;
    RecordMemoryManager<History> rmm_h;
    RecordMemoryManager<Order> rmm_o;
    RecordMemoryManager<OrderLine> rmm_ol;
    RecordMemoryManager<NewOrder> rmm_no;
};
