#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "protocols/common/schema.hpp"

using Rec = void;

enum ReadWriteType { READ = 0, UPDATE, INSERT, DELETE };

template <typename Value>
struct ReadWriteElement {
    using Version = typename Value::Version;
    ReadWriteElement(Rec* read_rec, Rec* write_rec, ReadWriteType rwt, bool is_new, Value* val)
        : read_rec(read_rec)
        , write_rec(write_rec)
        , rwt(rwt)
        , is_new(is_new)
        , val(val){};
    Rec* read_rec = nullptr;   // nullptr when rwt is INSERT
                               // points to shared record when rwt is READ or UPDATE or DELETE
    Rec* write_rec = nullptr;  // nullptr when rwt is READ, DELETE
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

template <typename Key, typename Value>
class WriteSet {
public:
    using P = std::pair<Key, typename std::unordered_map<Key, ReadWriteElement<Value>>::iterator>;
    std::vector<P>& get_table(TableID table_id) { return ws[table_id]; }

private:
    std::unordered_map<TableID, std::vector<P>> ws;
};