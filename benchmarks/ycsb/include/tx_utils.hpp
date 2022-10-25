#pragma once

#include "benchmarks/ycsb/include/config.hpp"
#include "utils/utils.hpp"

enum Status {
    SUCCESS = 0,   // if all stages of transaction return Result::SUCCESS
    USER_ABORT,    // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SYSTEM_ABORT,  // if any stage of a transaction returns Result::ABORT
    BUG            // if any stage of a transaciton returns unexpected Result::FAIL
};

enum TxProfileID : uint8_t { READ_TX = 0, UPDATE_TX = 1, READMODIFYWRITE_TX = 2, MAX = 3 };

template <typename Record>
class ReadTx;

template <typename Record>
class ReadModifyWriteTx;

template <typename Record>
class UpdateTx;

template <TxProfileID i>
struct TxType;

template <>
struct TxType<TxProfileID::READ_TX> {
    template <typename Record>
    using Profile = ReadTx<Record>;
};

template <>
struct TxType<TxProfileID::READMODIFYWRITE_TX> {
    template <typename Record>
    using Profile = ReadModifyWriteTx<Record>;
};

template <>
struct TxType<TxProfileID::UPDATE_TX> {
    template <typename Record>
    using Profile = UpdateTx<Record>;
};

template <TxProfileID i, typename Record>
using TxProfile = typename TxType<i>::template Profile<Record>;

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
    PerTxType& operator[](TxProfileID tx_type) { return per_type_[tx_type]; }
    const PerTxType& operator[](TxProfileID tx_type) const { return per_type_[tx_type]; }

    void add(const Stat& rhs) {
        for (size_t i = 0; i < TxProfileID::MAX; i++) {
            per_type_[i].add(rhs.per_type_[i]);
        }
    }
    PerTxType aggregate_perf() const {
        PerTxType out;
        for (size_t i = 0; i < TxProfileID::MAX; i++) {
            out.add(per_type_[i]);
        }
        return out;
    }

private:
    PerTxType per_type_[TxProfileID::MAX];
};

struct ThreadLocalData {
    alignas(64) Stat stat;
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
