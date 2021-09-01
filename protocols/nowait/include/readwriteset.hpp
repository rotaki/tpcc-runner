#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "protocols/common/schema.hpp"
#include "protocols/nowait/include/value.hpp"

enum ReadWriteType { READ = 0, UPDATE, INSERT, DELETE };

struct ReadWriteElement {
    ReadWriteElement(Rec* rec, ReadWriteType rwt, bool is_new, Value* val)
        : rec(rec)
        , rwt(rwt)
        , is_new(is_new)
        , val(val){};
    Rec* rec = nullptr;  // nullptr when rwt is READ or DELETE
                         // points to local record when rwt is UPDATE or INSERT
    ReadWriteType rwt = READ;
    bool is_new;  // if newly inserted
    Value* val;   // pointer to index
};


template <typename Key>
class ReadWriteSet {
public:
    std::unordered_map<Key, ReadWriteElement>& get_table(TableID table_id) { return rws[table_id]; }

private:
    std::unordered_map<TableID, std::unordered_map<Key, ReadWriteElement>> rws;
};