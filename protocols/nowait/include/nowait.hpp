#pragma once

#include <cstring>
#include <map>
#include <set>
#include <stdexcept>
#include <unordered_map>

#include "protocols/common/epoch_manager.hpp"
#include "protocols/nowait/include/readwriteset.hpp"

template <typename Index>
class NoWait {
public:
    using Key = typename Index::Key;
    using Value = typename Index::Value;

    NoWait(TxID txid, uint32_t epoch)
        : txid(txid)
        , starting_epoch(epoch) {
        LOG_INFO("START Tx, e: %u", starting_epoch);
    }

    ~NoWait() {
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            rw_table.clear();
        }
        GarbageCollector::remove(starting_epoch);
    }

    const Rec* read(TableID table_id, Key key) {
        LOG_INFO("READ (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);
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
            if (!val->rwl.try_lock_shared()) {
                return nullptr;  // abort
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
        LOG_INFO("INSERT (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

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
                if (!next_value->rwl.try_lock()) return nullptr;
            } else if (next_iter->second.rwt == ReadWriteType::READ) {
                if (!next_value->rwl.try_lock_upgrade()) return nullptr;
            }

            // Insert new record
            Value* new_val =
                reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
            new_val->rwl.initialize();
            new_val->rwl.lock();
            new_val->rec = nullptr;
            res = idx.insert(table_id, key, new_val);
            if (res == Index::Result::NOT_INSERTED) {
                MemoryAllocator::deallocate(new_val);
                return nullptr;  // abort
            }

            // Unlock next key
            next_value->rwl.unlock();

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
        LOG_INFO("UPDATE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            // Abort if not found in index
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;  // abort

            // Get write lock
            if (!val->rwl.try_lock()) return nullptr;

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
            if (!rw_iter->second.val->rwl.try_lock_upgrade()) return nullptr;
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
        LOG_INFO("WRITE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);
        return upsert(table_id, key);
    }

    Rec* upsert(TableID table_id, Key key) {
        LOG_INFO("UPSERT (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

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
                    if (!next_value->rwl.try_lock()) return nullptr;
                } else if (next_iter->second.rwt == ReadWriteType::READ) {
                    if (!next_value->rwl.try_lock_upgrade()) return nullptr;
                }

                // Insert new record
                Value* new_val =
                    reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
                new_val->rwl.initialize();
                new_val->rwl.lock();
                new_val->rec = nullptr;
                res = idx.insert(table_id, key, new_val);
                if (res == Index::Result::NOT_INSERTED) {
                    MemoryAllocator::deallocate(new_val);
                    return nullptr;  // abort
                }
                // Unlock next key
                next_value->rwl.unlock();
                // Place record to modify into localset
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::INSERT, true, new_val));
                return rec;
            } else if (res == Index::Result::OK) {
                // Update if found in index
                // Get write lock
                if (!val->rwl.try_lock()) return nullptr;

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
            if (!rw_iter->second.val->rwl.try_lock_upgrade()) return nullptr;
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
        // no reverse scan in nowait
        if (rev == true) throw std::runtime_error("reverse scan not supported in nowait");
        LOG_INFO(
            "READ_SCAN (e: %u, t: %lu, lk: %lu, rk: %lu, c: %ld)", starting_epoch, table_id, lkey,
            rkey, count);
        Index& idx = Index::get_index();

        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        std::map<Key, Value*> kv_map;

        [[maybe_unused]] typename Index::Result res;
        res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map);
        assert(res == Index::Result::OK);

        for (auto& [key, val]: kv_map) {
            auto rw_iter = rw_table.find(key);

            if (rw_iter == rw_table.end()) {
                // Get read lock
                if (!val->rwl.try_lock_shared()) return false;  // abort
                // Place it into readwriteset
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(nullptr, ReadWriteType::READ, false, val));
                kr_map.emplace(key, val->rec);
                continue;
            }

            auto rwt = rw_iter->second.rwt;
            if (rwt == ReadWriteType::READ) {
                kr_map.emplace(key, rw_iter->second.val->rec);
            } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                kr_map.emplace(key, rw_iter->second.rec);
            } else if (rwt == ReadWriteType::DELETE) {
                return false;
            } else {
                throw std::runtime_error("invalid state");
            }
        }
        return true;
    }

    bool update_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, [[maybe_unused]] bool rev,
        std::map<Key, Rec*>& kr_map) {
        // no reverse scan in nowait
        if (rev == true) throw std::runtime_error("reverse scan not supported in nowait");
        LOG_INFO(
            "UPDATE_SCAN (e: %u, t: %lu, lk: %lu, rk: %lu, c: %ld)", starting_epoch, table_id, lkey,
            rkey, count);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        std::map<Key, Value*> kv_map;

        [[maybe_unused]] typename Index::Result res;
        res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map);
        assert(res == Index::Result::OK);

        for (auto& [key, val]: kv_map) {
            auto rw_iter = rw_table.find(key);

            if (rw_iter == rw_table.end()) {
                // Get write lock
                if (!val->rwl.try_lock()) return false;
                // Allocate memory for write
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                memcpy(rec, val->rec, record_size);
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));
                kr_map.emplace(key, rec);
                continue;
            }

            auto rwt = rw_iter->second.rwt;
            if (rwt == ReadWriteType::READ) {
                // Upgrade lock
                if (!rw_iter->second.val->rwl.try_lock_upgrade()) return false;
                // Localset will point to allocated record
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                memcpy(rec, rw_iter->second.val->rec, record_size);
                rw_iter->second.rec = rec;
                rw_iter->second.rwt = ReadWriteType::UPDATE;
                kr_map.emplace(key, rec);
            } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                kr_map.emplace(key, rw_iter->second.rec);
            } else if (rwt == ReadWriteType::DELETE) {
                return false;
            } else {
                throw std::runtime_error("invalid state");
            }
        }
        return true;
    }

    const Rec* remove(TableID table_id, Key key) {
        LOG_INFO("REMOVE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        Index& idx = Index::get_index();

        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            // Abort if not found in index
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;  // abort

            // Get write lock
            if (!val->rwl.try_lock()) return nullptr;

            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, ReadWriteType::DELETE, false, val));
            return val->rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            if (!rw_iter->second.val->rwl.try_lock_upgrade()) return nullptr;
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
        LOG_INFO("PRECOMMIT, e: %u", starting_epoch);

        Index& idx = Index::get_index();

        // unlock read lock
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                if (rw_iter->second.rwt == ReadWriteType::READ)
                    rw_iter->second.val->rwl.unlock_shared();
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
                    rw_iter->second.val->rwl.unlock();
                    MemoryAllocator::deallocate(old);
                } else if (rwt == ReadWriteType::DELETE) {
                    idx.remove(table_id, rw_iter->first);
                    MemoryAllocator::deallocate(rw_iter->second.val->rec);
                    GarbageCollector::collect(starting_epoch, rw_iter->second.val);
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
                    GarbageCollector::collect(starting_epoch, rw_iter->second.val);
                }

                // Deallocate local write memory and unlock if it is not a new record
                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::READ) {
                    rw_iter->second.val->rwl.unlock_shared();
                } else if ((rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT)) {
                    MemoryAllocator::deallocate(rw_iter->second.rec);
                    if (!rw_iter->second.is_new) rw_iter->second.val->rwl.unlock();
                } else if (rwt == ReadWriteType::DELETE) {
                    if (!rw_iter->second.is_new) rw_iter->second.val->rwl.unlock();
                }
            }

            rw_table.clear();
        }
        tables.clear();
    }

private:
    TxID txid;
    uint32_t starting_epoch;
    std::set<TableID> tables;
    ReadWriteSet<Key, Value> rws;
};