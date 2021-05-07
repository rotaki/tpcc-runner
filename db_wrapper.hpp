#pragma once

#include <map>
#include <string>
#include <vector>

#include "table_layout.hpp"

extern std::map<std::string, std::string> warehouses;
extern std::map<std::string, std::string> districts;
extern std::map<std::string, std::string> customers;
extern std::map<std::string, std::string> histories;
extern std::map<std::string, std::string> orders;
extern std::map<std::string, std::string> neworders;
extern std::map<std::string, std::string> orderlines;
extern std::map<std::string, std::string> items;
extern std::map<std::string, std::string> stocks;

class DBWrapper {
public:
  // setup emtpy tables
  DBWrapper();
  // store record into db
  bool insert_record(Storage st, const std::string& key, const Record* record);
  // store into record if exists
  bool find_record(Storage st, const std::string& key, Record* record);
};
