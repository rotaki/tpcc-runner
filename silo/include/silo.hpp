#pragma once

#include <cassert>
#include <map>
#include <set>
#include <unordered_map>
#include <utility>

#include "silo/include/garbage_collector.hpp"
#include "silo/include/index.hpp"
#include "silo/include/keyvalue.hpp"
#include "silo/include/memory_allocator.hpp"
#include "silo/include/readwriteset.hpp"
#include "silo/include/schema.hpp"

class NodeSet {
public:
    NodeMap& get_nodemap(TableID table_id);

private:
    std::unordered_map<TableID, NodeMap> ns;
};

class Silo {
public:
    Silo(uint32_t epoch);

    ~Silo();

    const Rec* read(TableID table_id, Key key);

    Rec* insert(TableID table_id, Key key);

    Rec* update(TableID table_id, Key key);

    bool read_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, bool reverse,
        std::map<Key, Rec*>& kv_map);

    bool update_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, bool reverse,
        std::map<Key, Rec*>& kv_map);

    Rec* remove(TableID table_id, Key key);

    bool precommit();

    void abort();

private:
    uint32_t starting_epoch;
    std::set<TableID> tables;
    ReadSet rs;
    WriteSet ws;
    ValidationSet vs;
    NodeSet ns;

    void read_record_and_tidword(Value& val, Rec* rec, TidWord& tw, size_t rec_size);

    void read_tidword(Value& val, TidWord& tw);

    void lock(Value& val);

    void unlock(Value& val);

    void unlock_writeset();

    void unlock_writeset(TableID end_table_id, Key end_key);

    bool is_readable(const TidWord& tw);

    bool is_valid(const TidWord& current, const TidWord& expected);
};