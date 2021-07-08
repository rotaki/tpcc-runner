#pragma once

#include "masstree_wrapper.hpp"
#include "record_with_header.hpp"

// masstree customized for silo
volatile mrcu_epoch_type active_epoch = 1;
volatile std::uint64_t globalepoch = 1;
volatile bool recovering = false;

template <typename Record>
class MasstreeIndex {
public:
    using Key = uint64_t;
    using ValueType = RecordWithHeader<Record>;
    using NodeInfo = typename MasstreeWrapper<ValueType>::node_info_t;
    using Callback = typename MasstreeWrapper<ValueType>::Callback;
    using LeafType = typename MasstreeWrapper<ValueType>::leaf_type;
    using Str = typename MasstreeWrapper<ValueType>::Str;

    ValueType* get(Key key, NodeInfo& ni) {
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        return mt.get_value_and_get_nodeinfo_on_failure(
            reinterpret_cast<char*>(&key_buf), sizeof(key_buf), ni);
    }

    bool insert(Key key, ValueType* value, NodeInfo& ni) {
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        return mt.insert_value_and_get_nodeinfo_on_success(
            reinterpret_cast<char*>(&key_buf), sizeof(key_buf), value, ni);
    }

    bool remove(ValueType*& old_value, Key key, NodeInfo& ni) {
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        // need some gc scheme on the old_value
        return mt.remove_value_and_get_nodeinfo_on_failure(
            old_value, reinterpret_cast<char*>(&key_buf), sizeof(key_buf), ni);
    }

    bool update(ValueType*& old_value, Key key, ValueType* new_value, NodeInfo& ni) {
        mt.thread_init(0);
        Key key_buf = byte_swap(key);
        // need some gc scheme on the old?value
        return mt.update_value_and_get_nodeinfo_on_failure(
            old_value, reinterpret_cast<char*>(&key_buf), sizeof(key_buf), new_value, ni);
    }

    // actual_scan_num = min(size([l_key, rkey)), scan_num)
    // scan_num = -1 to scan all elements in range
    // scan order [l_key --> rkey)
    void read_range_forward(Key l_key, Key r_key, size_t scan_num, Callback&& callback) {
        assert(l_key <= r_key);
        mt.thread_init(0);
        Key lkey_buf = byte_swap(l_key);
        Key rkey_buf = byte_swap(r_key);
        bool l_exclusive = false;
        bool r_exclusive = true;

        mt.scan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(lkey_buf), l_exclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(rkey_buf), r_exclusive, std::move(callback),
            scan_num);
    }

    // actual_scan_num = min(size((l_key, rkey]), scan_num)
    // scan_num = -1 to scan all elements in range
    // scan order (l_key <-- rkey]
    void read_range_backward(Key l_key, Key r_key, size_t scan_num, Callback&& callback) {
        assert(l_key <= r_key);
        mt.thread_init(0);
        Key lkey_buf = byte_swap(l_key);
        Key rkey_buf = byte_swap(r_key);
        bool l_exclusive = true;
        bool r_exclusive = false;

        mt.rscan(
            reinterpret_cast<char*>(&lkey_buf), sizeof(lkey_buf), l_exclusive,
            reinterpret_cast<char*>(&rkey_buf), sizeof(rkey_buf), r_exclusive, std::move(callback),
            scan_num);
    }

private:
    MasstreeWrapper<ValueType> mt;

    Key byte_swap(Key key) {
        Key key_buf{__builtin_bswap64(key)};
        return key_buf;
    }
};