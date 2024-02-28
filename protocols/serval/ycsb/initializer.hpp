#pragma once

#include "benchmarks/ycsb/include/config.hpp"
#include "benchmarks/ycsb/include/record_key.hpp"
#include "benchmarks/ycsb/include/record_layout.hpp"
#include "protocols/common/memory_allocator.hpp"
#include "protocols/common/schema.hpp"
#include "protocols/silo/include/tidword.hpp"
#include "protocols/ycsb_common/record_misc.hpp"
#include "utils/utils.hpp"

template <typename Index>
class Initializer {
private:
    using Key = typename Index::Key;
    using Value = typename Index::Value;
    using Version = typename Value::Version;

    static void insert_into_index(TableID table_id, Key key, void* rec) {
        Value* val = reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
        Version* version =
            reinterpret_cast<Version*>(MemoryAllocator::aligned_allocate(sizeof(Version)));
        val->initialize();
        val->version = version;
        version->read_ts = 0;
        version->write_ts = 0;
        version->prev = nullptr;
        version->rec = rec;
        version->deleted = false;
        Index::get_index().insert(table_id, key, val);
    }

public:
    template <typename Record>
    static void load_all_tables() {
        Schema& sch = Schema::get_mutable_schema();
        sch.set_record_size(get_id<Record>(), sizeof(Record));

        const Config& c = get_config();

        for (uint64_t key = 0; key < c.get_num_records(); key++) {
            void* rec = new (MemoryAllocator::allocate(sizeof(Record))) Record();
            insert_into_index(get_id<Record>(), key, rec);
        }
    }
};