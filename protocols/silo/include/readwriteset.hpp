#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "indexes/masstree.hpp"
#include "protocols/common/schema.hpp"
#include "protocols/silo/include/value.hpp"

enum WriteType { UPDATE = 0, INSERT, DELETE };

struct WriteElement {
    WriteElement(Rec* rec, WriteType wt, bool is_new)
        : rec(rec)
        , wt(wt)
        , is_new(is_new) {}
    Rec* rec = nullptr;
    WriteType wt = UPDATE;
    bool is_new = false;  // true if key is new(inserted) to shared memory. (This is
                          // needed because WriteType::INSERT would change if
                          // followed by Read or Delete operation.)
};

class WriteSet {
public:
    std::map<Key, WriteElement>& get_table(TableID table_id) { return ws[table_id]; }

private:
    std::unordered_map<TableID, std::map<Key, WriteElement>> ws;
};

struct ReadElement {
    ReadElement(Rec* rec, const TidWord& tidword)
        : rec(rec) {
        tw.obj = tidword.obj;
    }
    Rec* rec = nullptr;
    TidWord tw;
};

class ReadSet {
public:
    std::map<Key, ReadElement>& get_table(TableID table_id) { return rs[table_id]; }

private:
    std::unordered_map<TableID, std::map<Key, ReadElement>> rs;
};

class ValidationSet {
public:
    std::unordered_map<Key, Value*>& get_table(TableID table_id) { return vs[table_id]; }

private:
    std::unordered_map<TableID, std::unordered_map<Key, Value*>> vs;
};