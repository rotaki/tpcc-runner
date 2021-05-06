#pragma once

#include <map>
#include <string_view>
#include <vector>

#include "table_layout.hpp"

enum Storage {
    WAREHOUSE = 0,
    DISTRICT,
    CUSTOMER,
    HISTORY,
    ORDER,
    NEWORDER,
    ORDERLINE,
    ITEM,
    STOCK
};

class DBWrapper {
public:
    static const int DB_SIZE = 9;
    // setup emtpy tables
    DBWrapper();
    // get serialized version of record and store it into db
    bool insert_record(Storage st, std::string_view key, const Record* record);

    // store it into record if it exists
    bool find_record(Storage st, std::string_view key, Record* record);

private:
    std::vector<std::map<std::string_view, std::string_view>> indexes;
};
