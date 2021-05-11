#include "utils.hpp"

Config::Config() {}

void Config::set_num_warehouses(uint16_t n) {
  num_warehouses = n;
}

uint16_t Config::get_num_warehouses() const{
  return num_warehouses;
}

Config& get_mutable_config() {
  static Config c;
  return c;
}

const Config& get_config() {
  return get_mutable_config();
}


