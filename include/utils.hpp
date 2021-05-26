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

/**
 * Better strncpy().
 * out buffer will be null-terminated.
 * returned value is written size excluding the last null character.
 */
inline std::size_t copy_cstr(char* out, const char* in, std::size_t out_buf_size) {
    if (out_buf_size == 0) return 0;
    std::size_t i = 0;
    while (i < out_buf_size - 1) {
        if (in[i] == '\0') break;
        out[i] = in[i];
        i++;
    }
    out[i] = '\0';
    return i;
}
