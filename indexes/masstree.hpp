#pragma once

#include <map>

#include "indexes/masstree_wrapper.hpp"
#include "protocols/common/schema.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

template <typename Value>
class MasstreeIndexes {
public:
    using Key = uint64_t;
    using MT = MasstreeWrapper<Value>;
    using KVMap = std::map<Key, Value*>;
    using NodeInfo = typename MT::node_info_t;
    using NodeMap =
        std::unordered_map<const typename MT::leaf_type*, uint64_t>;  // key: node pointer, value:
                                                                      // version

    enum Result {
        OK = 0,
        NOT_FOUND,
        BAD_INSERT,
        NOT_INSERTED,
        NOT_DELETED,
        BAD_SCAN,
    };

    Result find(TableID table_id, Key key, Value*& val) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        NodeInfo ni;
        val = mt.get_value(reinterpret_cast<char*>(&key_buf), sizeof(Key));
        if (val == nullptr)
            return NOT_FOUND;
        else
            return OK;
    }

    Result find(TableID table_id, Key key, Value*& val, NodeMap& nm) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        NodeInfo ni;
        val = mt.get_value_and_get_nodeinfo_on_failure(
            reinterpret_cast<char*>(&key_buf), sizeof(Key), ni);
        if (val == nullptr) {
            // node that would contain the missing key is added
            nm.emplace(reinterpret_cast<const typename MT::leaf_type*>(ni.node), ni.new_version);
            return NOT_FOUND;
        } else {
            return OK;
        }
    }

    Result insert(TableID table_id, Key key, Value* val) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        NodeInfo ni;
        bool inserted = mt.insert_value(reinterpret_cast<char*>(&key_buf), sizeof(Key), val);
        return inserted ? OK : NOT_INSERTED;
    }

    Result insert(TableID table_id, Key key, Value* val, NodeMap& nm) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        NodeInfo ni;
        bool inserted = mt.insert_value_and_get_nodeinfo_on_success(
            reinterpret_cast<char*>(&key_buf), sizeof(Key), val, ni);
        if (inserted) {
            auto itr = nm.find(reinterpret_cast<const typename MT::leaf_type*>(ni.node));
            if (itr == nm.end()) {
                return OK;
            }
            if (itr->second != ni.old_version) {
                return BAD_INSERT;
            } else {
                // update old node version if it exists
                itr->second = ni.new_version;
                return OK;
            }
        } else {
            return NOT_INSERTED;
        }
    }

    Result get_next_kv(TableID table_id, Key lkey, Key& next_key, Value*& next_value) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = true;
        Key rkey_buf = byte_swap(UINT64_MAX);
        bool rexclusive = false;

        mt.scan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[](const typename MT::leaf_type* leaf, uint64_t version, bool& continue_flag) {
                 unused(leaf, version, continue_flag);
             },
             [&next_key, &next_value](
                 const typename MT::Str& key, Value* val, bool& continue_flag) {
                 unused(continue_flag);
                 Key actual_key{__builtin_bswap64(*(reinterpret_cast<const uint64_t*>(key.s)))};
                 next_key = actual_key;
                 next_value = val;
                 return;
             }},
            1);

        return OK;
    }

    // [lkey --> rkey)
    Result get_kv_in_range(TableID table_id, Key lkey, Key rkey, int64_t count, KVMap& kv_map) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = false;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = true;

        mt.scan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[](const typename MT::leaf_type* leaf, uint64_t version, bool& continue_flag) {
                 unused(leaf, version, continue_flag);
             },
             [&kv_map](const typename MT::Str& key, Value* val, bool& continue_flag) {
                 unused(continue_flag);
                 Key actual_key{__builtin_bswap64(*(reinterpret_cast<const uint64_t*>(key.s)))};
                 kv_map.emplace(actual_key, val);
                 return;
             }},
            count);

        return OK;
    }

    // [lkey --> rkey) with NodeInfo
    Result get_kv_in_range(
        TableID table_id, Key lkey, Key rkey, int64_t count, KVMap& kv_map, NodeMap& nm) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = false;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = true;

        bool exception_caught = false;

        mt.scan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[&nm, &exception_caught](
                 const typename MT::leaf_type* leaf, uint64_t version, bool& continue_flag) {
                 auto it = nm.find(leaf);
                 if (it == nm.end())
                     nm.emplace_hint(it, leaf, version);
                 else if (it->second != version) {
                     exception_caught = true;
                     continue_flag = false;
                 }
             },
             [&kv_map](const typename MT::Str& key, Value* val, bool& continue_flag) {
                 (void)continue_flag;
                 Key actual_key{__builtin_bswap64(*(reinterpret_cast<const uint64_t*>(key.s)))};
                 kv_map.emplace(actual_key, val);
                 return;
             }},
            count);

        return exception_caught ? BAD_SCAN : OK;
    }

    // (lkey <-- rkey]
    Result get_kv_in_rev_range(TableID table_id, Key lkey, Key rkey, int64_t count, KVMap& kv_map) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = true;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = false;

        mt.rscan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[](const typename MT::leaf_type* leaf, uint64_t version, bool& continue_flag) {
                 unused(leaf, version, continue_flag);
             },
             [&kv_map](const typename MT::Str& key, Value* val, bool& continue_flag) {
                 unused(continue_flag);
                 Key actual_key{__builtin_bswap64(*(reinterpret_cast<const uint64_t*>(key.s)))};
                 kv_map.emplace(actual_key, val);
                 return;
             }},
            count);

        return OK;
    }

    // (lkey <-- rkey] with NodeInfo
    Result get_kv_in_rev_range(
        TableID table_id, Key lkey, Key rkey, int64_t count, KVMap& kv_map, NodeMap& nm) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = true;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = false;

        bool exception_caught = false;

        mt.rscan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[&nm, &exception_caught](
                 const typename MT::leaf_type* leaf, uint64_t version, bool& continue_flag) {
                 auto it = nm.find(leaf);
                 if (it == nm.end())
                     nm.emplace_hint(it, leaf, version);
                 else if (it->second != version) {
                     exception_caught = true;
                     continue_flag = false;
                 }
             },
             [&kv_map](const typename MT::Str& key, Value* val, bool& continue_flag) {
                 (void)continue_flag;
                 Key actual_key{__builtin_bswap64(*(reinterpret_cast<const uint64_t*>(key.s)))};
                 kv_map.emplace(actual_key, val);
                 return;
             }},
            count);

        return exception_caught ? BAD_SCAN : OK;
    }

    Result remove(TableID table_id, Key key) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        if (mt.remove_value(reinterpret_cast<char*>(&key_buf), sizeof(Key))) {
            return OK;
        } else {
            return NOT_DELETED;
        }
    }

    uint64_t get_version_value(TableID table_id, const typename MT::leaf_type* node) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        return mt.get_version_value(node);
    }

    // Other
    static MasstreeIndexes<Value>& get_index() {
        static MasstreeIndexes<Value> idx;
        return idx;
    }

private:
    std::unordered_map<TableID, MT> indexes;

    Key byte_swap(Key key) {
        Key key_buf{__builtin_bswap64(key)};
        return key_buf;
    }
};
