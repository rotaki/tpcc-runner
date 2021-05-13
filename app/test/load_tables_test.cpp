#include "initializer.hpp"
#include "utils.hpp"

#include "gtest/gtest.h"

TEST(GlueSuit, LoadTables) {
  Config& c = get_mutable_config();
  c.set_num_warehouses(3);
  Initializer::load_items_table();
  Initializer::load_warehouses_table();
}
