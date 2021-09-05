#pragma once

#include <deque>
#include <unordered_map>

#include "protocols/common/transaction_id.hpp"

enum OperationType { R, W };

template <typename Key>
struct TrackedRWInfo {
    TrackedRWInfo(OperationType ot, TableID table_id, Key key, TxID txid)
        : ot(ot)
        , table_id(table_id)
        , key(key)
        , txid(txid) {}

    OperationType ot;
    TableID table_id;
    Key key;
    TxID txid;
};

template <typename Key>
struct ThreadLocalRWTracker {
    using PerTxRWHistory = typename std::deque<TrackedRWInfo<Key>>;
    using RWHistory = std::unordered_map<TxID, PerTxRWHistory, TxIDHash>;

    static PerTxRWHistory& get_table(TxID txid) {
        auto& rwh = get_rwhistory();
        return rwh[txid];
    }

    static RWHistory& get_rwhistory() {
        thread_local RWHistory rwh;
        return rwh;
    }
};

template <typename Index>
struct TrackedNodeInfo {
    TrackedNodeInfo(
        TableID table_id, uint32_t epoch, uintptr_t nodeid, uint64_t old_version,
        std::vector<uint64_t>&& created_versions)
        : table_id(table_id)
        , epoch(epoch)
        , nodeid(nodeid)
        , old_version(old_version)
        , created_versions(created_versions) {}
    TableID table_id;
    uint32_t epoch;
    uintptr_t nodeid;
    uint64_t old_version;
    std::vector<uint64_t> created_versions;
};

template <typename Index>
struct ThreadLocalNodeTracker {
    using PerTxNodeHistory = typename std::deque<TrackedNodeInfo<Index>>;
    using NodeHistory = std::unordered_map<TxID, PerTxNodeHistory, TxIDHash>;

    static PerTxNodeHistory& get_table(TxID txid) {
        auto& nh = get_nodehistory();
        return nh[txid];
    }

    static NodeHistory& get_nodehistory() {
        thread_local NodeHistory nh;
        return nh;
    }
};