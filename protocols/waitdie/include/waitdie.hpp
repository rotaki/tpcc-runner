#pragma once

#include <cassert>
#include <cstring>
#include <set>
#include <stdexcept>

#include "protocols/common/timestamp_manager.hpp"
#include "protocols/common/transaction_id.hpp"
#include "protocols/waitdie/include/readwriteset.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

template <typename Index>
class WaitDie {
public:
    using Key = typename Index::Key;
    using Value = typename Index::Value;
    using LeafNode = typename Index::LeafNode;

    WaitDie(TxID txid, uint64_t ts, uint64_t smallest_ts, uint64_t largest_ts)
        : txid(txid)
        , start_ts(ts)
        , smallest_ts(smallest_ts)
        , largest_ts(largest_ts) {
        LOG_INFO("START Tx, ts: %lu, s_ts: %lu, l_ts: %lu", start_ts, smallest_ts, largest_ts);
    }

    ~WaitDie() { GarbageCollector::remove(smallest_ts, largest_ts); }

    void set_new_ts(uint64_t start_ts_, uint64_t smallest_ts_, uint64_t largest_ts_) {
        start_ts = start_ts_;
        smallest_ts = smallest_ts_;
        largest_ts = largest_ts_;
    }

    const Rec* read(TableID table_id, Key key) {
        LOG_INFO(
            "READ (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        Index& idx = Index::get_index();
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            // Abort if key is not found in table
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) {
                return nullptr;  // abort
            }
            // Get read lock
            if (!val->wdl.try_lock_shared(start_ts)) {
                return nullptr;
            }

            if (val->is_detached_from_tree()) {
                val->wdl.unlock_shared(start_ts);
                return nullptr;
            }

            // Place it into readwriteset
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, ReadWriteType::READ, false, val));
            return val->rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            return rw_iter->second.val->rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            return rw_iter->second.rec;
        } else if (rwt == ReadWriteType::DELETE) {
            return nullptr;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    Rec* insert(TableID table_id, Key key) {
        LOG_INFO(
            "INSERT (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;

            // Abort if key exist in table

            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::OK) return nullptr;

            // Get next key write lock
            Key next_key = 0;
            Value* next_value = nullptr;
            res = idx.get_next_kv(table_id, key, next_key, next_value);
            assert(next_key != 0);
            assert(next_value != nullptr);
            if (res != Index::Result::OK) return nullptr;

            auto next_iter = rw_table.find(next_key);
            if (next_iter == rw_table.end()) {
                if (!next_value->wdl.try_lock(start_ts)) return nullptr;
            } else if (next_iter->second.rwt == ReadWriteType::READ) {
                if (!next_value->wdl.try_lock_upgrade(start_ts)) return nullptr;
            }

            Value* new_val = static_cast<Value*>(
                new (MemoryAllocator::aligned_allocate(sizeof(Value))) Value());  // construct
            new_val->wdl.try_lock(start_ts);
            res = idx.insert(table_id, key, new_val);
            if (res == Index::Result::NOT_INSERTED) {
                MemoryAllocator::deallocate(new_val);
                return nullptr;
            }

            // Unlock next key
            next_value->wdl.unlock(start_ts);

            // Place record to modify into localset
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, ReadWriteType::INSERT, true, new_val));
            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ || rwt == ReadWriteType::UPDATE
            || rwt == ReadWriteType::INSERT) {
            return nullptr;
        } else if (rwt == ReadWriteType::DELETE) {
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rec;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    Rec* update(TableID table_id, Key key) {
        LOG_INFO(
            "UPDATE (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;

            // Get write lock
            if (!val->wdl.try_lock(start_ts)) return nullptr;

            // Check deleted
            if (val->is_detached_from_tree()) {
                val->wdl.unlock(start_ts);
                return nullptr;
            }

            // Allocate memory for write
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, val->rec, record_size);
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));

            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            if (!rw_iter->second.val->wdl.try_lock_upgrade(start_ts)) return nullptr;
            // Localset will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, rw_iter->second.val->rec, record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            return rw_iter->second.rec;
        } else if (rwt == ReadWriteType::DELETE) {
            return nullptr;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    Rec* write(TableID table_id, Key key) {
        LOG_INFO(
            "WRITE (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        return upsert(table_id, key);
    }

    Rec* upsert(TableID table_id, Key key) {
        LOG_INFO(
            "UPSERT (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);

            // Insert if not found in index
            if (res == Index::Result::NOT_FOUND) {
                // Get next key write lock
                Key next_key;
                Value* next_value;
                res = idx.get_next_kv(table_id, key, next_key, next_value);
                if (res != Index::Result::OK) return nullptr;
                auto next_iter = rw_table.find(next_key);
                if (next_iter == rw_table.end()) {
                    if (!next_value->wdl.try_lock(start_ts)) return nullptr;
                } else if (next_iter->second.rwt == ReadWriteType::READ) {
                    if (!next_value->wdl.try_lock_upgrade(start_ts)) return nullptr;
                }

                // Insert new record
                Value* new_val = static_cast<Value*>(
                    new (MemoryAllocator::aligned_allocate(sizeof(Value))) Value());  // construct
                new_val->wdl.try_lock(start_ts);
                res = idx.insert(table_id, key, new_val);
                if (res == Index::Result::NOT_INSERTED) {
                    MemoryAllocator::deallocate(new_val);
                    return nullptr;
                }

                // Unlock next key
                next_value->wdl.unlock(start_ts);

                // Place record to modify into localset
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::INSERT, true, new_val));
                return rec;
            } else if (res == Index::Result::OK) {
                // Update if found in index
                // Get write lock
                if (!val->wdl.try_lock(start_ts)) return nullptr;

                // Check deleted
                if (val->is_detached_from_tree()) {
                    val->wdl.unlock(start_ts);
                    return nullptr;
                }

                // Allocate memory for write
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                memcpy(rec, val->rec, record_size);
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));
                return rec;
            } else {
                throw std::runtime_error("invalid state");
            }
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            if (!rw_iter->second.val->wdl.try_lock_upgrade(start_ts)) return nullptr;
            // Localset will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, rw_iter->second.val->rec, record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            return rw_iter->second.rec;
        } else if (rwt == ReadWriteType::DELETE) {
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rec;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    bool read_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, [[maybe_unused]] bool rev,
        std::map<Key, Rec*>& kr_map) {
        // no reverse scan in waitdie
        if (rev == true) throw std::runtime_error("reverse scan not supported in waitdie");
        LOG_INFO(
            "READ_SCAN (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, lk: %lu, rk: %lu, c: %ld)", start_ts,
            smallest_ts, largest_ts, table_id, lkey, rkey, count);

        Index& idx = Index::get_index();
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);

        bool abort_flag = false;

        auto per_node_func = [&](LeafNode* leaf, uint64_t version, bool& continue_flag) {
            unused(leaf, version, continue_flag);
        };

        auto per_kv_func = [&](Key key, Value* val, bool& continue_flag) {
            auto rw_iter = rw_table.find(key);
            if (rw_iter == rw_table.end()) {
                if (!val->wdl.try_lock_shared(start_ts)) {
                    // failed to acquire lock
                    continue_flag = false;
                    abort_flag = true;
                    return;  // abort
                }
                if (val->is_detached_from_tree()) {
                    // key is deleted -> ignore
                    val->wdl.unlock_shared(start_ts);
                    return;
                }
                // Place it into readwriteset
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(nullptr, ReadWriteType::READ, false, val));

                kr_map.emplace(key, val->rec);
            } else {
                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::READ) {
                    kr_map.emplace(key, rw_iter->second.val->rec);
                } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                    kr_map.emplace(key, rw_iter->second.rec);
                } else if (rwt == ReadWriteType::DELETE) {
                    throw std::runtime_error("deleted value");
                } else {
                    throw std::runtime_error("invalid state");
                }
            }

            if (count != -1 && static_cast<int64_t>(kr_map.size()) >= count) continue_flag = false;
        };

        [[maybe_unused]] typename Index::Result res =
            idx.get_kv_in_range(table_id, lkey, rkey, per_node_func, per_kv_func);
        if (abort_flag) {
            return false;  // abort
        } else {
            assert(res == Index::Result::OK);
            return true;
        }
    }


    bool update_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, bool rev,
        std::map<Key, Rec*>& kr_map) {
        // no reverse scan in waitdie
        if (rev == true) throw std::runtime_error("reverse scan not supported in waitdie");
        LOG_INFO(
            "UPDATE_SCAN (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, lk: %lu, rk: %lu, c: %ld)",
            start_ts, smallest_ts, largest_ts, table_id, lkey, rkey, count);

        const Schema& sch = Schema::get_schema();
        size_t record_size = sch.get_record_size(table_id);
        Index& idx = Index::get_index();
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);

        bool abort_flag = false;

        auto per_node_func = [&](LeafNode* leaf, uint64_t version, bool& continue_flag) {
            unused(leaf, version, continue_flag);
        };

        auto per_kv_func = [&](Key key, Value* val, bool& continue_flag) {
            auto rw_iter = rw_table.find(key);
            if (rw_iter == rw_table.end()) {
                if (!val->wdl.try_lock(start_ts)) {
                    // failed to acquire lock
                    continue_flag = false;
                    abort_flag = true;
                    return;
                }
                if (val->is_detached_from_tree()) {
                    // key is deleted -> ignore
                    val->wdl.unlock(start_ts);
                    return;
                }

                // Allocate memory for write
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                memcpy(rec, val->rec, record_size);
                // Place it into readwriteset
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));

                kr_map.emplace(key, rec);
            } else {
                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::READ) {
                    // Upgrade lock
                    if (!rw_iter->second.val->wdl.try_lock_upgrade(start_ts)) {
                        // failed to upgrade lock
                        continue_flag = false;
                        abort_flag = true;
                        return;
                    }
                    // Localset will point to allocated record
                    Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                    memcpy(rec, rw_iter->second.val->rec, record_size);
                    rw_iter->second.rec = rec;
                    rw_iter->second.rwt = ReadWriteType::UPDATE;
                    kr_map.emplace(key, rec);
                } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                    kr_map.emplace(key, rw_iter->second.rec);
                } else if (rwt == ReadWriteType::DELETE) {
                    throw std::runtime_error("deleted value");
                } else {
                    throw std::runtime_error("invalid state");
                }
            }

            if (count != -1 && static_cast<int64_t>(kr_map.size()) >= count) continue_flag = false;
        };

        [[maybe_unused]] typename Index::Result res =
            idx.get_kv_in_range(table_id, lkey, rkey, per_node_func, per_kv_func);
        if (abort_flag) {
            return false;  // abort
        } else {
            assert(res == Index::Result::OK);
            return true;
        }
    }

    const Rec* remove(TableID table_id, Key key) {
        LOG_INFO(
            "REMOVE (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        Index& idx = Index::get_index();

        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            // Abort if not found in index
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;

            // Get write lock
            if (!val->wdl.try_lock(start_ts)) return nullptr;

            // Check deleted
            if (val->is_detached_from_tree()) {
                val->wdl.unlock(start_ts);
                return nullptr;
            }

            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, ReadWriteType::DELETE, false, val));
            return val->rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            if (!rw_iter->second.val->wdl.try_lock_upgrade(start_ts)) return nullptr;
            rw_iter->second.rwt = ReadWriteType::DELETE;
            return rw_iter->second.val->rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            MemoryAllocator::deallocate(rw_iter->second.rec);
            rw_iter->second.rec = nullptr;
            rw_iter->second.rwt = ReadWriteType::DELETE;
            return rw_iter->second.val->rec;
        } else if (rwt == ReadWriteType::DELETE) {
            return nullptr;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    bool precommit() {
        LOG_INFO("PRECOMMIT, ts: %lu, s_ts: %lu, l_ts: %lu", start_ts, smallest_ts, largest_ts);
        Index& idx = Index::get_index();

        // unlock read lock
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                if (rw_iter->second.rwt == ReadWriteType::READ) {
                    rw_iter->second.val->wdl.unlock_shared(start_ts);
                }
            }
        }

        // write
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::READ) {
                    // do nothing
                } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                    Rec* old = exchange(rw_iter->second.val->rec, rw_iter->second.rec);
                    rw_iter->second.val->wdl.unlock(start_ts);
                    MemoryAllocator::deallocate(old);
                } else if (rwt == ReadWriteType::DELETE) {
                    idx.remove(table_id, rw_iter->first);
                    Rec* old = exchange(rw_iter->second.val->rec, nullptr);
                    rw_iter->second.val->wdl.unlock(start_ts);
                    MemoryAllocator::deallocate(old);
                    GarbageCollector::collect(largest_ts, rw_iter->second.val);
                } else {
                    throw std::runtime_error("invalid state");
                }
            }
        }

        return true;
    }

    void abort() {
        Index& idx = Index::get_index();

        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                // For failed inserts
                if (rw_iter->second.is_new) {
                    idx.remove(table_id, rw_iter->first);
                    GarbageCollector::collect(largest_ts, rw_iter->second.val);
                }

                // Deallocate local write memory and unlock if it is not a new record
                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::READ) {
                    rw_iter->second.val->wdl.unlock_shared(start_ts);
                } else if ((rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT)) {
                    MemoryAllocator::deallocate(rw_iter->second.rec);
                    rw_iter->second.val->wdl.unlock(start_ts);
                } else if (rwt == ReadWriteType::DELETE) {
                    rw_iter->second.val->wdl.unlock(start_ts);
                }
            }
            rw_table.clear();
        }
        tables.clear();
    }

private:
    TxID txid;
    uint64_t start_ts;     // starting timestamp of transaction
    uint64_t smallest_ts;  // workers smallest timestamp observed
    uint64_t largest_ts;   // workers largets timestamp observed
    std::set<TableID> tables;
    ReadWriteSet<Key, Value> rws;
};