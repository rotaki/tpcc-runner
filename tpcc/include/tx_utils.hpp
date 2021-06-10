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


struct TxType {
    enum Value : uint8_t {
        NewOrder = 0,
        Payment = 1,
        OrderStatus = 2,
        Delivery = 3,
        StockLevel = 4,
        Max = 5,
    };

    Value value;

    TxType() = default;
    TxType(Value v)
        : value(v){};
    TxType(uint8_t i)
        : value(Value(i)) {
        assert(i < Max);
    }

    operator size_t() const { return size_t(value); }

    static const char* name(TxType tx_type) {
        assert(tx_type < Max);
        constexpr const char* n[] = {
            "NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"};
        return n[tx_type];
    }
};


struct Stat {
    struct PerTxType {
        size_t num_commits = 0;
        size_t num_usr_aborts = 0;
        size_t num_sys_aborts = 0;

        void add(const PerTxType& rhs) {
            num_commits += rhs.num_commits;
            num_usr_aborts += rhs.num_usr_aborts;
            num_sys_aborts += rhs.num_sys_aborts;
        }
    };

    PerTxType& operator[](TxType tx_type) { return per_type_[tx_type]; }
    const PerTxType& operator[](TxType tx_type) const { return per_type_[tx_type]; }

    void add(const Stat& rhs) {
        for (size_t i = 0; i < TxType::Max; i++) {
            per_type_[i].add(rhs.per_type_[i]);
        }
    }
    PerTxType aggregate_perf() const {
        PerTxType out;
        for (size_t i = 0; i < TxType::Max; i++) {
            out.add(per_type_[i]);
        }
        return out;
    }

private:
    PerTxType per_type_[TxType::Max];
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
struct TxHelper {
    Transaction& tx_;
    Stat::PerTxType& per_type_;

    explicit TxHelper(Transaction& tx, Stat::PerTxType& per_type_)
        : tx_(tx)
        , per_type_(per_type_) {}

    Status kill(typename Transaction::Result res) {
        switch (res) {
        case Transaction::Result::FAIL: return Status::BUG;
        case Transaction::Result::ABORT: per_type_.num_sys_aborts++; return Status::SYSTEM_ABORT;
        default: throw std::runtime_error("wrong Transaction::Result");
        }
    }

    Status commit() {
        if (tx_.commit()) {
            per_type_.num_commits++;
            return Status::SUCCESS;
        } else {
            per_type_.num_sys_aborts++;
            return Status::SYSTEM_ABORT;
        }
    }

    Status usr_abort() {
        per_type_.num_usr_aborts++;
        return Status::USER_ABORT;
    }
};
