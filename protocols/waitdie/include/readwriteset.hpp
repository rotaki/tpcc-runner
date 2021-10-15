#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "protocols/common/schema.hpp"

using Rec = void;

enum ReadWriteType { READ = 0, UPDATE, INSERT, DELETE };

template <typename Value>
struct ReadWriteElement {
    ReadWriteElement(Rec* rec, ReadWriteType rwt, bool is_new, Value* val)
        : rec(rec)
        , rwt(rwt)
        , is_new(is_new)
        , val(val) {}

    Rec* rec = nullptr;  // nullptr when rwt is READ or DELETE
                         // points to local record when rwt is UPDATE or INSERT
    ReadWriteType rwt = READ;
    bool is_new;  // if newly inserted
    Value* val;   // pointer to index
};

template <typename Key, typename Value>
class ReadWriteSet {
public:
    using Table = std::unordered_map<Key, ReadWriteElement<Value>>;
    Table& get_table(TableID table_id) { return rws[table_id]; }

private:
    std::unordered_map<TableID, Table> rws;
};