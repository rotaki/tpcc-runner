#include "initializer.hpp"
#include "utils.hpp"

int main() {
  Config& c = get_mutable_config();
  c.set_num_warehouses(3);

  Initializer::load_items_table();
  // Initializer::load_warehouses_table();
  
  // TableGenerator::load_items_table();
  // std::cout << NUM_WAREHOUSE << std::endl;
  // DBWrapper db;
  // Warehouse w;
  // w.w_id = 10;
  // w.w_tax = 10.0;
  // w.w_ytd = 5.0;
  // strcpy(w.w_name, "bar");
  // strcpy(w.w_address.street_1, "foo");
  // strcpy(w.w_address.street_2, "hoge");
  // strcpy(w.w_address.city, "tokyo");
  // strcpy(w.w_address.state, "LA");
  // strcpy(w.w_address.zip, "123456");
  // db.insert_record(Storage::WAREHOUSE, "bar", &w);

  // Warehouse new_w;
  // db.find_record(Storage::WAREHOUSE, "bar", &new_w);
  // std::cout << new_w.w_address.zip << std::endl;
  // auto x = TableUtils::serialize_warehouse(&new_w);
  // std::cout << x << std::endl;
}
