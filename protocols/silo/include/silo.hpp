#pragma once

#include <algorithm>
#include <cassert>
#include <cstring>
#include <set>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "protocols/common/epoch_manager.hpp"
#include "protocols/common/schema.hpp"
#include "protocols/silo/include/readwriteset.hpp"
#include "protocols/silo/include/tidword.hpp"

template <typename Index>
class Silo {
public:
    using Key = typename Index::Key;
    using Value = typename Index::Value;
    class NodeSet {
    public:
        typename Index::NodeMap& get_nodemap(TableID table_id) { return ns[table_id]; }

    private:
        std::unordered_map<TableID, typename Index::NodeMap> ns;
    };

    Silo(TxID txid, uint32_t epoch)
        : txid(txid)
        , starting_epoch(epoch) {
        LOG_INFO("START Tx, e: %u", starting_epoch);
    }

    ~Silo() { GarbageCollector::remove(starting_epoch); }

    const Rec* read(TableID table_id, Key key) {
        LOG_INFO("READ (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        Index& idx = Index::get_index();
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);
        auto& nm = ns.get_nodemap(table_id);

        if (rw_iter == rw_table.end()) {
            // Abort if key is not found in index
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val, nm);
            if (res == Index::Result::NOT_FOUND) return nullptr;
            // Read record pointer and tidword from index
            Rec* rec = nullptr;
            TidWord tw;
            get_record_pointer(*val, rec, tw);
            // Null check
            if (!is_readable(tw)) return nullptr;
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, tw, ReadWriteType::READ, false, val));
            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Read record poitner and tidword from index
            Rec* rec = nullptr;
            TidWord tw;
            get_record_pointer(*(rw_iter->second.val), rec, tw);
            if (!is_same(tw, rw_iter->second.tw)) return nullptr;
            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
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

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);
        auto& nm = ns.get_nodemap(table_id);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::OK) return nullptr;  // abort

            Value* new_val =
                reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
            new_val->rec = nullptr;
            new_val->tidword.obj = 0;
            new_val->tidword.latest = 1;  // exist in index
            new_val->tidword.absent = 1;  // cannot be seen by others
            res = idx.insert(table_id, key, new_val, nm);

            if (res == Index::Result::NOT_INSERTED) {
                MemoryAllocator::deallocate(new_val);
                return nullptr;  // abort
            }

            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            auto new_iter = rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, new_val->tidword, ReadWriteType::INSERT, true, new_val));

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, new_iter);

            if (res == Index::Result::BAD_INSERT) return nullptr;

            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ || rwt == ReadWriteType::UPDATE
            || rwt == ReadWriteType::INSERT) {
            return nullptr;
        } else if (rwt == ReadWriteType::DELETE) {
            assert(rw_iter->second.rec == nullptr);
            // Check if tidword stored locally is the latest
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
            // Allocate memory for write
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

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            // Abort if key not found in index
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;
            // Copy record and tidword from index
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            TidWord tw;
            copy_record(*val, rec, tw, record_size);
            // Null check
            if (!is_readable(tw)) {
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }
            // Place it in readwrite set
            auto new_iter = rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, tw, ReadWriteType::UPDATE, false, val));
            // Place it in write set
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, new_iter);
            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            assert(rw_iter->second.rec == nullptr);
            // Local set will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            TidWord tw;
            copy_record(*rw_iter->second.val, rec, tw, record_size);
            // Check if tidword stored locally is the latest
            if (!is_same(tw, rw_iter->second.tw)) {
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            // Place it in writeset
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);
            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            // Check if tidword stored locally is the latest
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
            return rw_iter->second.rec;
        } else if (rwt == ReadWriteType::DELETE) {
            return nullptr;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    Rec* write(TableID table_id, Key key) {
        LOG_INFO("WRITE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);
        auto& nm = ns.get_nodemap(table_id);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);

            if (res == Index::Result::NOT_FOUND) {
                // Insert if not found in index
                Value* new_val =
                    reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
                new_val->rec = nullptr;
                new_val->tidword.obj = 0;
                new_val->tidword.latest = 1;  // exist in index
                new_val->tidword.absent = 1;  // cannot be seen by others

                res = idx.insert(table_id, key, new_val, nm);
                if (res == Index::Result::NOT_INSERTED) {
                    MemoryAllocator::deallocate(new_val);
                    return nullptr;  // abort
                }

                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(
                        rec, new_val->tidword, ReadWriteType::INSERT, true, new_val));

                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);

                if (res == Index::Result::BAD_INSERT) return nullptr;

                return rec;
            } else if (res == Index::Result::OK) {
                // Update if found in index
                // Copy record and tidword from index
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                copy_record(*val, rec, tw, record_size);
                // Null check
                if (!is_readable(tw)) {
                    MemoryAllocator::deallocate(rec);
                    return nullptr;
                }
                // Place it in readwrite set
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw, ReadWriteType::INSERT, false, val));
                // Place it in write set
                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);

                return rec;
            } else {
                throw std::runtime_error("invalid state");
            }
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            assert(rw_iter->second.rec == nullptr);
            // Local set will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            TidWord tw;
            copy_record(*rw_iter->second.val, rec, tw, record_size);

            // Check if tidword stored locally is the latest
            if (!is_same(tw, rw_iter->second.tw)) {
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;

            // Place it in writeset
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);

            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            // Check if tidword stored locally is the latest
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
            return rw_iter->second.rec;
        } else if (rwt == ReadWriteType::DELETE) {
            assert(rw_iter->second.rec == nullptr);
            // Check if tidword stored locally is the latest
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
            // Allocate memory for write
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rec;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    Rec* upsert(TableID table_id, Key key) {
        LOG_INFO("UPSERT (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);
        auto& nm = ns.get_nodemap(table_id);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);

            if (res == Index::Result::NOT_FOUND) {
                // Insert if not found in index
                Value* new_val =
                    reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
                new_val->rec = nullptr;
                new_val->tidword.obj = 0;
                new_val->tidword.latest = 1;  // exist in index
                new_val->tidword.absent = 1;  // cannot be seen by others

                res = idx.insert(table_id, key, new_val, nm);
                if (res == Index::Result::NOT_INSERTED) {
                    MemoryAllocator::deallocate(new_val);
                    return nullptr;  // abort
                }

                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(
                        rec, new_val->tidword, ReadWriteType::INSERT, true, new_val));

                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);

                if (res == Index::Result::BAD_INSERT) return nullptr;

                return rec;
            } else if (res == Index::Result::OK) {
                // Update if found in index
                // Copy record and tidword from index
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                copy_record(*val, rec, tw, record_size);
                // Null check
                if (!is_readable(tw)) {
                    MemoryAllocator::deallocate(rec);
                    return nullptr;
                }
                // Place it in readwrite set
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw, ReadWriteType::UPDATE, false, val));
                // Place it in write set
                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);

                return rec;
            } else {
                throw std::runtime_error("invalid state");
            }
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            assert(rw_iter->second.rec == nullptr);
            // Local set will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            TidWord tw;
            copy_record(*rw_iter->second.val, rec, tw, record_size);

            // Check if tidword stored locally is the latest
            if (!is_same(tw, rw_iter->second.tw)) {
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;

            // Place it in writeset
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);

            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            // Check if tidword stored locally is the latest
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
            return rw_iter->second.rec;
        } else if (rwt == ReadWriteType::DELETE) {
            assert(rw_iter->second.rec == nullptr);
            // Check if tidword stored locally is the latest
            if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return nullptr;
            // Allocate memory for write
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rec;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    bool read_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, bool reverse,
        std::map<Key, Rec*>& kr_map) {
        LOG_INFO(
            "READ_SCAN (e: %u, t: %lu, lk: %lu, rk: %lu, c: %ld)", starting_epoch, table_id, lkey,
            rkey, count);
        Index& idx = Index::get_index();

        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        std::map<Key, Value*> kv_map;
        typename Index::Result res;
        if (reverse) {
            res = idx.get_kv_in_rev_range(table_id, lkey, rkey, count, kv_map, nm);
        } else {
            res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map, nm);
        }
        if (res == Index::Result::BAD_SCAN) return false;

        for (auto& [key, val]: kv_map) {
            auto rw_iter = rw_table.find(key);

            if (rw_iter == rw_table.end()) {
                // Read record pointer and tidword from index
                Rec* rec = nullptr;
                TidWord tw;
                get_record_pointer(*val, rec, tw);
                // Null check
                if (!is_readable(tw)) return false;
                // Place it into readwrite set
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(nullptr, tw, ReadWriteType::READ, false, val));
                kr_map.emplace(key, rec);
                continue;
            }

            auto rwt = rw_iter->second.rwt;
            if (rwt == ReadWriteType::READ) {
                // Read record poitner and tidword from index
                Rec* rec = nullptr;
                TidWord tw;
                get_record_pointer(*(rw_iter->second.val), rec, tw);
                if (!is_same(tw, rw_iter->second.tw)) return false;
                kr_map.emplace(key, rec);
            } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return false;
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
        TableID table_id, Key lkey, Key rkey, int64_t count, bool reverse,
        std::map<Key, Rec*>& kr_map) {
        LOG_INFO(
            "UPDATE_SCAN (e: %u, t: %lu, lk: %lu, rk: %lu, c: %ld)", starting_epoch, table_id, lkey,
            rkey, count);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        std::map<Key, Value*> kv_map;
        typename Index::Result res;
        if (reverse) {
            res = idx.get_kv_in_rev_range(table_id, lkey, rkey, count, kv_map, nm);
        } else {
            res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map, nm);
        }
        if (res == Index::Result::BAD_SCAN) return false;

        for (auto& [key, val]: kv_map) {
            auto rw_iter = rw_table.find(key);

            if (rw_iter == rw_table.end()) {
                // Copy record and tidword from index
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                copy_record(*val, rec, tw, record_size);
                // Null check
                if (!is_readable(tw)) {
                    MemoryAllocator::deallocate(rec);
                    return false;
                }
                // Place it in readwrite set
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw, ReadWriteType::UPDATE, false, val));
                // Place it in write set
                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);
                kr_map.emplace(key, rec);
                continue;
            }

            auto rwt = rw_iter->second.rwt;
            if (rwt == ReadWriteType::READ) {
                assert(rw_iter->second.rec == nullptr);
                // Local set will point to allocated record
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                copy_record(*rw_iter->second.val, rec, tw, record_size);
                // Check if tidword stored locally is the latest
                if (!is_same(tw, rw_iter->second.tw)) {
                    MemoryAllocator::deallocate(rec);
                    return false;
                }
                rw_iter->second.rec = rec;
                rw_iter->second.rwt = ReadWriteType::UPDATE;
                // Place it in writeset
                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, rw_iter);
                kr_map.emplace(key, rec);
            } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                if (!is_tidword_latest(*(rw_iter->second.val), rw_iter->second.tw)) return false;
                kr_map.emplace(key, rw_iter->second.rec);
            } else if (rwt == ReadWriteType::DELETE) {
                assert(rw_iter->second.rec == nullptr);
                return false;
            } else {
                throw std::runtime_error("invalid state");
            }
        }
        return true;
    }

    Rec* remove(TableID table_id, Key key) {
        LOG_INFO("REMOVE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        Index& idx = Index::get_index();

        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            // Abort if not found in index
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;

            // Read record pointer and tidword from index
            Rec* rec = nullptr;
            TidWord tw;
            get_record_pointer(*val, rec, tw);

            // Null check
            if (!is_readable(tw)) return nullptr;
            auto new_iter = rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, tw, ReadWriteType::DELETE, false, val));

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, new_iter);

            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            assert(rw_iter->second.rec == nullptr);
            // Read record pointer and tidword from index
            Rec* rec = nullptr;
            TidWord tw;
            get_record_pointer(*(rw_iter->second.val), rec, tw);
            // Check if tidword stored locally is the latest
            if (!is_same(tw, rw_iter->second.tw)) return nullptr;
            // Place it in writeset
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);
            rw_iter->second.rwt = ReadWriteType::DELETE;
            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            // Read record pointer and tidword from index
            Rec* rec = nullptr;
            TidWord tw;
            get_record_pointer(*(rw_iter->second.val), rec, tw);
            // Check if tidword stored locally is the latest
            if (!is_same(tw, rw_iter->second.tw)) return nullptr;
            // Deallocate locally allocated record
            MemoryAllocator::deallocate(rw_iter->second.rec);
            rw_iter->second.rec = nullptr;
            rw_iter->second.rwt = ReadWriteType::DELETE;
            return rec;
        } else if (rwt == ReadWriteType::DELETE) {
            return nullptr;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    bool precommit() {
        LOG_INFO("PRECOMMIT, e: %u", starting_epoch);

        Index& idx = Index::get_index();

        TidWord commit_tw;
        commit_tw.obj = 0;

        LOG_INFO("  P1 (Lock WriteSet)");

        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            std::sort(w_table.begin(), w_table.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.first <= rhs.first;
            });
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                LOG_DEBUG("     LOCK (t: %lu, k: %lu)", table_id, w_iter->first);
                auto rw_iter = w_iter->second;
                lock(*(rw_iter->second.val));
                TidWord current;
                current.obj = load_acquire(rw_iter->second.val->tidword.obj);
                if (!rw_iter->second.is_new && !is_readable(current)) {
                    LOG_DEBUG("     UNREADABLE (t: %lu, k: %lu)", table_id, w_iter->first);
                    unlock_writeset(table_id, w_iter->first);
                    return false;
                }
                if (commit_tw.tid < current.tid) commit_tw.tid = current.tid;
            }
        }

        commit_tw.epoch = load_acquire(EpochManager<Silo<Index>>::get_global_epoch());
        LOG_INFO("  SERIAL POINT (se: %u, ce: %lu)", starting_epoch, commit_tw.epoch);

        // Phase 2.1 (Validate ReadSet)
        LOG_INFO("  P2.1 (Validate ReadSet)");
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::INSERT) continue;
                // rwt is either READ or UPDATE or DELETE
                TidWord current, expected;
                current.obj = load_acquire(rw_iter->second.val->tidword.obj);
                expected.obj = rw_iter->second.tw.obj;
                if (!is_valid(current, expected) || (current.lock && rwt == ReadWriteType::READ)) {
                    unlock_writeset();
                    return false;
                }
                if (commit_tw.tid < current.tid) commit_tw.tid = current.tid;
            }
        }

        // Generate tid that is bigger than that of read/writeset
        commit_tw.tid++;
        LOG_INFO("  COMMIT TID: %lu", commit_tw.tid);

        // Phase 2.2 (Validate NodeSet)
        LOG_INFO("  P2.2 (Validate NodeSet) ");
        for (TableID table_id: tables) {
            auto& nm = ns.get_nodemap(table_id);
            for (auto iter = nm.begin(); iter != nm.end(); ++iter) {
                uint64_t current_version = idx.get_version_value(table_id, iter->first);
                if (iter->second != current_version) {
                    LOG_DEBUG(
                        "NODE VERIFY FAILED (t: %lu, old_v: %lu, new_v: %lu)", table_id,
                        iter->second, current_version);
                    unlock_writeset();
                    return false;
                }
            }
        }

        // Phase 3 (Write to Shared Memory)
        LOG_INFO("  P3 (Write to Shared Memory)");
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                auto rw_iter = w_iter->second;
                auto rwt = rw_iter->second.rwt;
                Rec* old = exchange(rw_iter->second.val->rec, rw_iter->second.rec);
                TidWord new_tw;
                new_tw.epoch = commit_tw.epoch;
                new_tw.tid = commit_tw.tid;
                new_tw.latest = !(rwt == ReadWriteType::DELETE);
                new_tw.absent = (rwt == ReadWriteType::DELETE);
                new_tw.lock = 0;  // unlock
                store_release(rw_iter->second.val->tidword.obj, new_tw.obj);
                GarbageCollector::collect(commit_tw.epoch, old);
                if (rwt == ReadWriteType::DELETE) {
                    idx.remove(table_id, w_iter->first);
                    GarbageCollector::collect(commit_tw.epoch, rw_iter->second.val);
                }
            }
        }

        LOG_INFO("PRECOMMIT SUCCESS");
        return true;
    }

    void abort() {
        Index& idx = Index::get_index();

        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                auto rw_iter = w_iter->second;
                // For failed inserts
                if (rw_iter->second.is_new) {
                    lock(*(rw_iter->second.val));
                    assert(load_acquire(rw_iter->second.val->rec) == nullptr);
                    TidWord tw;
                    tw.obj = load_acquire(rw_iter->second.val->tidword.obj);
                    tw.absent = 1;
                    tw.latest = 0;
                    tw.lock = 0;
                    store_release(rw_iter->second.val->tidword.obj, tw.obj);
                    idx.remove(table_id, w_iter->first);
                    GarbageCollector::collect(starting_epoch, rw_iter->second.val);
                }

                auto rwt = rw_iter->second.rwt;
                if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
                    MemoryAllocator::deallocate(rw_iter->second.rec);
                }
            }
            w_table.clear();
            auto& rw_table = rws.get_table(table_id);
            rw_table.clear();
            auto& nm = ns.get_nodemap(table_id);
            nm.clear();
        }
        tables.clear();
    }

private:
    TxID txid;
    uint32_t starting_epoch;
    std::set<TableID> tables;
    ReadWriteSet<Key, Value> rws;
    WriteSet<Key, Value> ws;
    NodeSet ns;

    void get_record_pointer(Value& val, Rec*& rec, TidWord& tw) {
        TidWord expected;
        expected.obj = load_acquire(val.tidword.obj);
        while (true) {
            // loop while locked
            while (expected.lock) {
                expected.obj = load_acquire(val.tidword.obj);
            }
            // read record and tidword
            rec = load_acquire(val.rec);  // val.rec could be nullptr
            tw.obj = load_acquire(val.tidword.obj);

            // check if not changed
            if (is_same(tw, expected)) return;

            expected.obj = tw.obj;
        }
    }

    void copy_record(Value& val, Rec* rec, TidWord& tw, uint64_t rec_size) {
        if (rec == nullptr) throw std::runtime_error("memory not allocated");
        TidWord expected;
        expected.obj = load_acquire(val.tidword.obj);
        while (true) {
            // loop while locked
            while (expected.lock) {
                expected.obj = load_acquire(val.tidword.obj);
            }
            // copy record and tidword
            Rec* temp = load_acquire(val.rec);
            if (temp) memcpy(rec, temp, rec_size);
            tw.obj = load_acquire(val.tidword.obj);

            // check if not changed
            if (is_same(tw, expected)) return;

            expected.obj = tw.obj;
        }
    }

    void lock(Value& val) {
        TidWord expected, desired;
        expected.obj = load_acquire(val.tidword.obj);
        while (true) {
            if (expected.lock) {
                expected.obj = load_acquire(val.tidword.obj);
            } else {
                desired.obj = expected.obj;
                desired.lock = 1;
                if (compare_exchange(val.tidword.obj, expected.obj, desired.obj)) break;
            }
        }
    }

    void unlock_writeset() { unlock_writeset(UINT64_MAX, UINT64_MAX); }

    void unlock_writeset(TableID end_table_id, Key end_key) {
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                LOG_DEBUG("UNLOCK (t: %lu, k: %lu)", table_id, w_iter->first);
                auto rw_iter = w_iter->second;
                unlock(*(rw_iter->second.val));
                if (table_id == end_table_id && w_iter->first == end_key) return;
            }
        }
    }

    void unlock(Value& val) {
        TidWord desired;
        desired.obj = load_acquire(val.tidword.obj);
        assert(desired.lock);
        desired.lock = 0;
        store_release(val.tidword.obj, desired.obj);
    }

    bool is_tidword_latest(Value& val, const TidWord& current) {
        Rec* rec = nullptr;
        TidWord tw;
        get_record_pointer(val, rec, tw);
        return is_same(tw, current);
    }

    bool is_same(const TidWord& lhs, const TidWord& rhs) { return lhs.obj == rhs.obj; }

    bool is_readable(const TidWord& tw) { return !tw.absent && tw.latest; }

    bool is_valid(const TidWord& current, const TidWord& expected) {
        return current.epoch == expected.epoch && current.tid == expected.tid
            && is_readable(current);
    }
};