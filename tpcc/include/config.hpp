#pragma once

#include <cstdint>

class Config {
public:
    Config() = default;
    void set_num_warehouses(uint16_t n) { num_warehouses = n; }
    uint16_t get_num_warehouses() const { return num_warehouses; }

    void enable_random_abort() { does_random_abort = true; }

    bool get_random_abort_flag() const { return does_random_abort; }

private:
    uint16_t num_warehouses = 1;
    bool does_random_abort = false;
};

inline Config& get_mutable_config() {
    static Config c;
    return c;
}

inline const Config& get_config() {
    return get_mutable_config();
}