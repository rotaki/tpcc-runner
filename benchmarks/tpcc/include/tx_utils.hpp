#pragma once

#include <algorithm>

#include "benchmarks/tpcc/include/config.hpp"
#include "utils/utils.hpp"

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

enum TxProfileID : uint8_t {
    NEWORDER_TX = 0,
    PAYMENT_TX = 1,
    ORDERSTATUS_TX = 2,
    DELIVERY_TX = 3,
    STOCKLEVEL_TX = 4,
    MAX = 5,
};

class NewOrderTx;
class PaymentTx;
class OrderStatusTx;
class DeliveryTx;
class StockLevelTx;

template <TxProfileID i>
struct TxType;
template <>
struct TxType<TxProfileID::NEWORDER_TX> {
    using Profile = NewOrderTx;
};

template <>
struct TxType<TxProfileID::PAYMENT_TX> {
    using Profile = PaymentTx;
};

template <>
struct TxType<TxProfileID::ORDERSTATUS_TX> {
    using Profile = OrderStatusTx;
};

template <>
struct TxType<TxProfileID::DELIVERY_TX> {
    using Profile = DeliveryTx;
};

template <>
struct TxType<TxProfileID::STOCKLEVEL_TX> {
    using Profile = StockLevelTx;
};

template <TxProfileID i>
using TxProfile = typename TxType<i>::Profile;

struct Stat {
    static const size_t ABORT_DETAILS_SIZE = 20;
    struct PerTxType {
        size_t num_commits = 0;
        size_t num_usr_aborts = 0;
        size_t num_sys_aborts = 0;
        size_t abort_details[ABORT_DETAILS_SIZE] = {};
        uint64_t total_latency = 0;
        uint64_t min_latency = UINT64_MAX;
        uint64_t max_latency = 0;

        void add(const PerTxType& rhs, bool with_abort_details) {
            num_commits += rhs.num_commits;
            num_usr_aborts += rhs.num_usr_aborts;
            num_sys_aborts += rhs.num_sys_aborts;

            if (with_abort_details) {
                for (size_t i = 0; i < ABORT_DETAILS_SIZE; ++i) {
                    abort_details[i] += rhs.abort_details[i];
                }
            }

            total_latency += rhs.total_latency;
            min_latency = std::min(min_latency, rhs.min_latency);
            max_latency = std::max(max_latency, rhs.max_latency);
        }
    };

    PerTxType& operator[](TxProfileID tx_type) { return per_type_[tx_type]; }
    const PerTxType& operator[](TxProfileID tx_type) const { return per_type_[tx_type]; }

    void add(const Stat& rhs) {
        for (size_t i = 0; i < TxProfileID::MAX; i++) {
            per_type_[i].add(rhs.per_type_[i], true);
        }
    }
    PerTxType aggregate_perf() const {
        PerTxType out;
        for (size_t i = 0; i < TxProfileID::MAX; i++) {
            out.add(per_type_[i], false);
        }
        return out;
    }

private:
    PerTxType per_type_[TxProfileID::MAX];
};

struct ThreadLocalData {
    alignas(64) Stat stat;
    Output out;
};

template <typename Transaction>
inline bool not_succeeded(Transaction& tx, typename Transaction::Result& res) {
    const Config& c = get_config();
    bool flag = c.get_random_abort_flag();
    // Randomized abort
    if (flag && res == Transaction::Result::SUCCESS && urand_int(1, 100) == 1) {
        res = Transaction::Result::ABORT;
    }
    if (res == Transaction::Result::ABORT) {
        tx.abort();
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

    Status kill(typename Transaction::Result res, uint8_t abort_id) {
        switch (res) {
        case Transaction::Result::FAIL: return Status::BUG;
        case Transaction::Result::ABORT:
            per_type_.num_sys_aborts++;
            per_type_.abort_details[abort_id]++;
            return Status::SYSTEM_ABORT;
        default: throw std::runtime_error("wrong Transaction::Result");
        }
    }

    Status commit(uint8_t abort_id, uint64_t time) {
        if (tx_.commit()) {
            per_type_.total_latency += time;
            per_type_.min_latency = std::min(per_type_.min_latency, time);
            per_type_.max_latency = std::max(per_type_.max_latency, time);
            per_type_.num_commits++;
            return Status::SUCCESS;
        } else {
            per_type_.num_sys_aborts++;
            per_type_.abort_details[abort_id]++;
            return Status::SYSTEM_ABORT;
        }
    }

    Status usr_abort() {
        per_type_.num_usr_aborts++;
        return Status::USER_ABORT;
    }
};
