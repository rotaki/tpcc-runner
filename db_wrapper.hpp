#pragma once

#include <map>
#include <string>
#include <vector>

#include "table_layout.hpp"

class DBWrapper {
public:
    static const int DB_SIZE = 9;
    // setup emtpy tables
    DBWrapper();
    // store record into db
    bool insert_record(Storage st, const std::string& key, const Record* record);
    // store into record if exists
    bool find_record(Storage st, const std::string& key, Record* record);

private:
    std::map<std::string, std::string> warehouses;
    std::map<std::string, std::string> districts;
    std::map<std::string, std::string> customers;
    std::map<std::string, std::string> histories;
    std::map<std::string, std::string> orders;
    std::map<std::string, std::string> neworders;
    std::map<std::string, std::string> orderlines;
    std::map<std::string, std::string> items;
    std::map<std::string, std::string> stocks;
};
