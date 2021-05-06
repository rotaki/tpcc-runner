#pragma once

#include "db_wrapper.hpp"

DBWrapper::DBWrapper() {
    indexes.resize(DB_SIZE);
}

bool DBWrapper::insert_record(Storage st, std::string_view key, const Record* record) {
    if (indexes[static_cast<int>(st)].find(key) != indexes[static_cast<int>(st)].end()) {
        return false;
    } else {
        indexes[static_cast<int>(st)][key] = record->serialize();
        return false;
    }
}

bool DBWrapper::find_record(Storage st, std::string_view key, Record* record) {
    if (indexes[static_cast<int>(st)].find(key) == indexes[static_cast<int>(st)].end()) {
        return false;
    } else {
        record->deserialize(indexes[static_cast<int>(st)].at(key));
        return true;
    }
}
