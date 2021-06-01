#pragma once

#include "config.hpp"
#include "utils.hpp"

enum Status {
    SUCCESS = 0,   // if all stages of transaction return Result::SUCCESS
    USER_ABORT,    // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SYSTEM_ABORT,  // if any stage of a transaction returns Result::ABORT
    BUG            // if any stage of a transaciton returns unexpected Result::FAIL
};

class Output {
public:
    template <typename T>
    Output& operator<<(const T& t) {
        merge(&t, sizeof(T));
        return *this;
    };

    void merge(const void* data, size_t size) {
        uint64_t temp;
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
        while (size >= sizeof(uint64_t)) {
            memcpy(&temp, ptr, sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            size -= sizeof(uint64_t);
            out += temp;
        }
        if (size > 0) {
            temp = 0;
            memcpy(&temp, ptr, size);
            out += temp;
        }
    }

    void invalidate() { out = 0; }

private:
    uint64_t out = 0;
};

struct Stat {
    size_t num_commits = 0;
    size_t num_usr_aborts = 0;
    size_t num_sys_aborts = 0;
};

struct ThreadLocalData {
    alignas(64) Stat stat;
    Output out;
};

template <typename Transaction>
inline bool not_succeeded(Transaction& tx, typename Transaction::Result& res) {
    const Config& c = get_config();
    bool flag = c.get_random_abort_flag();
    if (flag && res == Transaction::Result::SUCCESS && urand_int(1, 100) == 1) {
        tx.abort();
        res = Transaction::Result::ABORT;
    }
    return res != Transaction::Result::SUCCESS;
}

template <typename Transaction>
inline Status kill_tx(Transaction& tx, typename Transaction::Result res, Stat& stat) {
    assert(not_succeeded(tx, res));
    if (res == Transaction::Result::FAIL) {
        return Status::BUG;
    } else {
        stat.num_sys_aborts++;
        return Status::SYSTEM_ABORT;
    }
}
