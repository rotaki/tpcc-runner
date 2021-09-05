#pragma once

#include <map>

#include "indexes/masstree_wrapper.hpp"
#include "protocols/common/schema.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

template <typename Value_>
class MasstreeIndexes {
public:
    using Key = uint64_t;
    using Value = Value_;
    using MT = MasstreeWrapper<Value>;
    using NodeInfo = typename MT::node_info_t;
    using LeafNodeID = const typename MT::leaf_type*;
    using NodeMap = std::unordered_map<LeafNodeID, uint64_t>;  // key: node pointer, value:
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
            nm.emplace(reinterpret_cast<LeafNodeID>(ni.node), ni.new_version);
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
            auto itr = nm.find(reinterpret_cast<LeafNodeID>(ni.node));
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
            {[](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
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
    Result get_kv_in_range(
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = false;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = true;

        mt.scan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
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
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map,
        NodeMap& nm) {
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
            {[&nm, &exception_caught](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
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
    Result get_kv_in_rev_range(
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = true;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = false;

        mt.rscan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
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
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map,
        NodeMap& nm) {
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
            {[&nm, &exception_caught](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
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


    uint64_t get_version_value(TableID table_id, LeafNodeID node) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        return mt.get_version_value(node);
    }

    // Other
    static MasstreeIndexes<Value>& get_index() {
        static MasstreeIndexes<Value> idx;
        return idx;
    }

    // ---------------------  For Serialization Test -------------------------

    struct RWNodeVersion {
        RWNodeVersion(uint64_t v_read)
            : v_read(v_read)
            , v_writes{} {}
        RWNodeVersion(uint64_t v_read, uint64_t v_write)
            : v_read(v_read) {
            v_writes.push_back(v_write);
        }
        void add_write_version(uint64_t v) { v_writes.push_back(v); }
        uint64_t v_read;
        std::vector<uint64_t> v_writes;
    };

    using RWNodeTable = std::unordered_map<LeafNodeID, RWNodeVersion>;

    Result insert(TableID table_id, Key key, Value* val, NodeMap& nm, RWNodeTable& rwnt) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        NodeInfo ni;
        bool inserted = mt.insert_value_and_get_nodeinfo_on_success(
            reinterpret_cast<char*>(&key_buf), sizeof(Key), val, ni);
        auto lnid = reinterpret_cast<LeafNodeID>(ni.node);
        auto rwn_iter = rwnt.find(lnid);
        if (rwn_iter == rwnt.end()) {
            rwnt.emplace_hint(
                rwn_iter, std::piecewise_construct, std::forward_as_tuple(lnid),
                std::forward_as_tuple(ni.old_version, ni.new_version));
        } else {
            rwn_iter->second.add_write_version(ni.new_version);
        }
        if (inserted) {
            auto itr = nm.find(lnid);
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

    Result remove(TableID table_id, Key key, RWNodeTable& rwnt) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        NodeInfo ni;
        if (mt.remove_value_and_get_nodeinfo_on_success(
                reinterpret_cast<char*>(&key_buf), sizeof(Key), ni)) {
            auto lnid = reinterpret_cast<LeafNodeID>(ni.node);
            auto rwn_iter = rwnt.find(lnid);
            if (rwn_iter == rwnt.end()) {
                rwnt.emplace_hint(
                    rwn_iter, std::piecewise_construct, std::forward_as_tuple(lnid),
                    std::forward_as_tuple(ni.old_version, ni.new_version));
            } else {
                rwn_iter->second.add_write_version(ni.new_version);
            }
            return OK;
        } else {
            return NOT_DELETED;
        }
    }


    // [lkey --> rkey)
    Result get_kv_in_range(
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map,
        RWNodeTable& rwnt) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = false;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = true;

        mt.scan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[&rwnt](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
                 unused(continue_flag);
                 auto rwn_iter = rwnt.find(leaf);
                 if (rwn_iter == rwnt.end()) {
                     rwnt.emplace_hint(rwn_iter, leaf, version);
                 }
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
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map,
        NodeMap& nm, RWNodeTable& rwnt) {
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
            {[&nm, &exception_caught, &rwnt](
                 LeafNodeID leaf, uint64_t version, bool& continue_flag) {
                 auto rwn_iter = rwnt.find(leaf);
                 if (rwn_iter == rwnt.end()) {
                     rwnt.emplace_hint(rwn_iter, leaf, version);
                 }
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
    Result get_kv_in_rev_range(
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map,
        RWNodeTable& rwnt) {
        auto& mt = indexes[table_id];
        mt.thread_init(0);
        Key lkey_buf = byte_swap(lkey);
        bool lexclusive = true;
        Key rkey_buf = byte_swap(rkey);
        bool rexclusive = false;

        mt.rscan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(Key), lexclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(Key), rexclusive,
            {[&rwnt](LeafNodeID leaf, uint64_t version, bool& continue_flag) {
                 unused(continue_flag);
                 auto rwn_iter = rwnt.find(leaf);
                 if (rwn_iter == rwnt.end()) {
                     rwnt.emplace_hint(rwn_iter, leaf, version);
                 }
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
        TableID table_id, Key lkey, Key rkey, int64_t count, std::map<Key, Value*>& kv_map,
        NodeMap& nm, RWNodeTable& rwnt) {
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
            {[&nm, &exception_caught, &rwnt](
                 LeafNodeID leaf, uint64_t version, bool& continue_flag) {
                 auto rwn_iter = rwnt.find(leaf);
                 if (rwn_iter == rwnt.end()) {
                     rwnt.emplace_hint(rwn_iter, leaf, version);
                 }
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

private:
    std::unordered_map<TableID, MT> indexes;

    Key byte_swap(Key key) {
        Key key_buf{__builtin_bswap64(key)};
        return key_buf;
    }
};