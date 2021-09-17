#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <stdexcept>

class Workload {
    friend class Config;

public:
    void set_workload(int r, int u, int rmw) {
        read_propotion = r;
        update_propotion = u;
        readmodifywrite_propotion = rmw;
        if (r + u + rmw != 100) throw std::runtime_error("invalid workload");
    }

private:
    int read_propotion = -1;
    int update_propotion = -1;
    int readmodifywrite_propotion = -1;
};

class Config {
public:
    Config() = default;
    void set_contention(double c) { contention = c; }
    double get_contention() const { return contention; }

    void set_workload_type(const std::string& workload_type) {
        if (workload_type == "A") {
            // Update heavy
            w.set_workload(50, 50, 0);
        } else if (workload_type == "B") {
            // Read heavy
            w.set_workload(95, 5, 0);
        } else if (workload_type == "C") {
            // Read only
            w.set_workload(100, 0, 0);
        } else if (workload_type == "F") {
            // Read-modify-write
            w.set_workload(50, 0, 50);
        } else {
            printf("Invalid workload_type, must be either of A,B,C,F\n");
            printf(
                "See https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads for details\n");
            throw std::runtime_error("unknown workload");
        }
    }

    void set_num_records(uint64_t nr) { num_records = nr; }
    uint64_t get_num_records() const { return num_records; }

    void set_num_threads(size_t n) { num_threads = n; }
    size_t get_num_threads() const { return num_threads; }

    void enable_random_abort() { does_random_abort = true; }
    bool get_random_abort_flag() const { return does_random_abort; }

    int get_read_propotion() const {
        if (w.read_propotion < 0) throw std::runtime_error("workload unset");
        return w.read_propotion;
    }

    int get_update_propotion() const {
        if (w.update_propotion < 0) throw std::runtime_error("workload unset");
        return w.update_propotion;
    }

    int get_readmodifywrite_propotion() const {
        if (w.readmodifywrite_propotion < 0) throw std::runtime_error("workload unset");
        return w.readmodifywrite_propotion;
    }

    void set_reps_per_txn(uint64_t reps) {
        uint64_t max = get_max_reps_per_txn();
        if (reps > max) {
            printf("reps must be less than or equal to %lu\n", max);
            throw std::runtime_error("invalid reps per txn");
        }
        reps_per_txn = reps;
    }

    uint64_t get_reps_per_txn() const { return reps_per_txn; }

    static constexpr uint64_t get_max_reps_per_txn() {
        constexpr uint64_t max_reps = 32;
        return max_reps;
    }

private:
    Workload w;
    double contention = 0;
    uint64_t num_records = 0;
    size_t num_threads = 1;
    uint64_t reps_per_txn;
    bool does_random_abort = false;
};

inline Config& get_mutable_config() {
    static Config c;
    return c;
}

inline const Config& get_config() {
    return get_mutable_config();
}
