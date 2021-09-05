#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <utility>

#include "protocols/common/schema.hpp"
#include "protocols/silo/include/tidword.hpp"
#include "protocols/silo/include/value.hpp"

using Rec = void;

enum ReadWriteType { READ = 0, UPDATE, INSERT, DELETE };

template <typename Value>
struct ReadWriteElement {
    ReadWriteElement(Rec* rec, const TidWord& tidword, ReadWriteType rwt, bool is_new, Value* val)
        : rec(rec)
        , tw(tidword)
        , rwt(rwt)
        , is_new(is_new)
        , val(val){};
    Rec* rec = nullptr;  // nullptr when rwt is READ or DELETE
                         // points to local record when rwt is UPDATE or INSERT
    TidWord tw;          // tidword when the data is read first
    ReadWriteType rwt = ReadWriteType::READ;
    bool is_new;  // if newly inserted
    Value* val;   // pointer to index
};

template <>
struct ReadWriteElement<ValueTest> {
    using Value = ValueTest;
    ReadWriteElement(
        Rec* rec, const TidWord& tidword, ReadWriteType rwt, bool is_new, Value* val, TxID txid)
        : rec(rec)
        , tw(tidword)
        , rwt(rwt)
        , is_new(is_new)
        , val(val)
        , txid(txid){};
    Rec* rec = nullptr;  // nullptr when rwt is READ or DELETE
                         // points to local record when rwt is UPDATE or INSERT
    TidWord tw;          // tidword when the data is read first
    ReadWriteType rwt = ReadWriteType::READ;
    bool is_new;  // if newly inserted
    Value* val;   // pointer to index
    TxID txid;    // transaction id read during execution
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