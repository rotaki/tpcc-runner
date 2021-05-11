#pragma once

#include <cstdint>

class Config {
public:
  Config();
  void set_num_warehouses(uint16_t n);
  uint16_t get_num_warehouses() const;
private:
  uint16_t num_warehouses;
};

Config& get_mutable_config();

const Config& get_config();
