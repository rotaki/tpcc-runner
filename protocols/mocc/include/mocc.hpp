#pragma once

#include <cassert>
#include <cstring>
#include <set>
#include <stdexcept>
#include <algorithm>
#include <cmath>

#include "protocols/common/epoch_manager.hpp"
#include "protocols/common/readwritelock.hpp"
#include "protocols/common/transaction_id.hpp"
#include "protocols/mocc/include/epotemp.hpp"
#include "protocols/mocc/include/locklist.hpp"
#include "protocols/mocc/include/readwriteset.hpp"
#include "protocols/mocc/include/tidword.hpp"
#include "utils/atomic_wrapper.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

Epotemp* EpotempAry;

enum class TransactionStatus : uint8_t {
    inFlight,
    committed,
    aborted,
};

template <typename Index>
class MOCC {
public:
    using Key = typename Index::Key;
    using Value = typename Index::Value;
    class NodeSet {
    public:
        typename Index::NodeMap& get_nodemap(TableID table_id) { return ns[table_id]; }

    private:
        std::unordered_map<TableID, typename Index::NodeMap> ns;
    };

    MOCC(TxID txid, uint32_t epoch)
        : txid(txid)
        , starting_epoch(epoch) {
        LOG_INFO("START Tx, e: %u", starting_epoch);
    }

    ~MOCC() { GarbageCollector::remove(starting_epoch); }

    const Rec* read(TableID table_id, Key key) {
        LOG_INFO("READ (t: %lu, k: %lu)", table_id, key);
        Index& idx = Index::get_index();
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;

            // Read version chain and get the correct version
            Epotemp loadepot;
            TidWord expected, desired;

            size_t epotemp_index;
            epotemp_index = key * sizeof(Value) / per_xx_temp;
            loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);
            bool need_verification;
            need_verification = true;
            auto& rtr_locks = retrospective_locklists.get_table(table_id);
            auto rtr_lock = search_locklists(rtr_locks, key);
            if (rtr_lock != nullptr) {
                lock(table_id, key, val, rtr_lock->type);
                if (status == TransactionStatus::aborted) {
                    return nullptr;
                } else {
                    need_verification = false;
                }
            } else if (loadepot.temp >= temp_threshold) {
                lock(table_id, key, val, LockType::READ);
                if (status == TransactionStatus::aborted) {
                    return nullptr;
                } else {
                    need_verification = false;
                }
            }

            if (need_verification) {
                expected.obj = load_acquire(val->tidword.obj);
                auto& curr_locks = current_locklists.get_table(table_id);
                for (;;) {
                    // rwl is -1 when there is a write lock
                    const int64_t write_locked = -1;
                    while (val->rwl.get_lock_cnt() == write_locked) {
                        if (curr_locks.size() > 0 && key < curr_locks.back().key) {
                            status = TransactionStatus::aborted;
                            rw_table.emplace_hint(
                                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                                std::forward_as_tuple(nullptr, ReadWriteType::READ, false, val));
                            return nullptr;
                        } else {
                            expected.obj = load_acquire(val->tidword.obj);
                        }
                    }
                    desired.obj = load_acquire(val->tidword.obj);
                    if (expected == desired)
                        break;
                    else
                        expected.obj = desired.obj;
                }
            } else {
                expected.obj = load_acquire(val->tidword.obj);
            }


            // Place it into readwriteset
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, expected, ReadWriteType::READ, false, val));
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
        LOG_INFO("IN (t: %lu, k: %lu)", table_id, key);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res != Index::Result::OK) return nullptr;

            // TODO: confirm if next key locking is necessary
            // In Nemoto implementation, next key locking is not implemented

            // Get next key write lock
            Key next_key;
            Value* next_value;
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
            new_val->initialize();
            new_val->lock();
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
            auto new_iter = rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, ReadWriteType::INSERT, true, new_val));

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, new_iter);

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
        LOG_INFO("UPDATE (t: %lu, k: %lu)", table_id, key);
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

            // Update if found in index
            Epotemp loadepot;

            size_t epotemp_index;
            epotemp_index = key * sizeof(Value) / per_xx_temp;
            loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);

            // Get write lock
            if (loadepot.temp >= temp_threshold) lock(table_id, key, val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            LockElement<RWLock> *in_rtr_locklist;
            in_rtr_locklist = search_locklists(retrospective_locklists.get_table(table_id), key);

            if (in_rtr_locklist != nullptr) lock(table_id, key, val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            // Allocate memory for write
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, val->rec, record_size);
            auto new_iter = rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, new_iter);

            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            Epotemp loadepot;

            size_t epotemp_index;
            epotemp_index = key * sizeof(Value) / per_xx_temp;
            loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);

            // Get write lock
            if (loadepot.temp >= temp_threshold) lock(table_id, key, rw_iter->second.val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            LockElement<RWLock> *in_rtr_locklist;
            in_rtr_locklist = search_locklists(retrospective_locklists.get_table(table_id), key);

            if (in_rtr_locklist != nullptr) lock(table_id, key, rw_iter->second.val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            // Localset will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, rw_iter->second.val->rec, record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);
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
        LOG_INFO("WRITE (t: %lu, k: %lu)", table_id, key);
        return upsert(table_id, key);
    }

    Rec* upsert(TableID table_id, Key key) {
        LOG_INFO("UPSERT (t: %lu, k: %lu)", table_id, key);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) {
                // TODO: confirm if next key locking is necessary
                // In Nemoto implementation, next key locking is not implemented
                
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
                new_val->initialize();
                new_val->lock();
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
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::INSERT, true, new_val));

                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);

                return rec;


            } else if (res == Index::Result::OK) {
                // Update if found in index
                Epotemp loadepot;

                size_t epotemp_index;
                epotemp_index = key * sizeof(Value) / per_xx_temp;
                loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);

                // Get write lock
                if (loadepot.temp >= temp_threshold) lock(table_id, key, val, LockType::WRITE);
                if (status == TransactionStatus::aborted) return nullptr;

                LockElement<RWLock> *in_rtr_locklist;
                in_rtr_locklist = search_locklists(retrospective_locklists.get_table(table_id), key);

                if (in_rtr_locklist != nullptr) lock(table_id, key, val, LockType::WRITE);
                if (status == TransactionStatus::aborted) return nullptr;

                // Allocate memory for write
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                memcpy(rec, val->rec, record_size);
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));
                
                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);

                return rec;
            } else {
                throw std::runtime_error("invalid state");
            }
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            Epotemp loadepot;

            size_t epotemp_index;
            epotemp_index = key * sizeof(Value) / per_xx_temp;
            loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);

            // Get write lock
            if (loadepot.temp >= temp_threshold) lock(table_id, key, rw_iter->second.val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            LockElement<RWLock> *in_rtr_locklist;
            in_rtr_locklist = search_locklists(retrospective_locklists.get_table(table_id), key);

            if (in_rtr_locklist != nullptr) lock(table_id, key, rw_iter->second.val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            // Localset will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, rw_iter->second.val->rec, record_size);
            rw_iter->second.rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);
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
        TableID table_id, Key lkey, Key rkey, int64_t count, bool rev,
        std::map<Key, Rec*>& kr_map) {
        // no reverse scan in nowait
        if (rev == true) throw std::runtime_error("reverse scan not supported in mocc");
        LOG_INFO("READ_SCAN (t: %lu, lk: %lu, rk: %lu, c: %ld)", table_id, lkey, rkey, count);

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
                if (read(table_id, key) == nullptr) return false;
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
        TableID table_id, Key lkey, Key rkey, int64_t count, bool rev,
        std::map<Key, Rec*>& kr_map) {
        if (rev == true) throw std::runtime_error("reverse scan not supported in mocc");
        LOG_INFO("UPDATE_SCAN (t: %lu, lk: %lu, rk: %lu, c: %ld)", table_id, lkey, rkey, count);

        Index& idx = Index::get_index();

        tables.insert(table_id);
        std::map<Key, Value*> kv_map;

        [[maybe_unused]] typename Index::Result res;
        res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map);
        assert(res == Index::Result::OK);

        for (auto& [key, val]: kv_map) {
            auto rec = update(table_id, key);
            if (rec == nullptr) return false;
            kr_map.emplace(key, rec);
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

            // Update if found in index
            Epotemp loadepot;

            size_t epotemp_index;
            epotemp_index = key * sizeof(Value) / per_xx_temp;
            loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);

            // Get write lock
            if (loadepot.temp >= temp_threshold) lock(table_id, key, val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            LockElement<RWLock> *in_rtr_locklist;
            in_rtr_locklist = search_locklists(retrospective_locklists.get_table(table_id), key);

            if (in_rtr_locklist != nullptr) lock(table_id, key, val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            auto new_iter = rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(nullptr, ReadWriteType::DELETE, false, val));

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, new_iter);

            return val->rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            Epotemp loadepot;

            size_t epotemp_index;
            epotemp_index = key * sizeof(Value) / per_xx_temp;
            loadepot.obj = load_acquire(EpotempAry[epotemp_index].obj);

            // Get write lock
            if (loadepot.temp >= temp_threshold) lock(table_id, key, rw_iter->second.val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            LockElement<RWLock> *in_rtr_locklist;
            in_rtr_locklist = search_locklists(retrospective_locklists.get_table(table_id), key);

            if (in_rtr_locklist != nullptr) lock(table_id, key, rw_iter->second.val, LockType::WRITE);
            if (status == TransactionStatus::aborted) return nullptr;

            rw_iter->second.rwt = ReadWriteType::DELETE;

            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);

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

        LOG_INFO("  P1 (Lock WriteSet)");

        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            std::sort(w_table.begin(), w_table.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.first <= rhs.first;
            });
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                LOG_DEBUG("     LOCK (t: %lu, k: %lu)", table_id, w_iter->first);
                auto rw_iter = w_iter->second;
                if (rw_iter->second.rwt == ReadWriteType::INSERT) continue;
                lock(table_id, w_iter->first, rw_iter->second.val, LockType::WRITE);
                if (this->status == TransactionStatus::aborted) return false;
                if (rw_iter->second.rwt == ReadWriteType::UPDATE && rw_iter->second.val->tidword.absent) {
                    this->status = TransactionStatus::aborted;
                    return false;
                }

                this->max_wset = std::max(this->max_wset, rw_iter->second.val->tidword);
            }
        }

        asm volatile("":: : "memory");
        uint32_t epoch = load_acquire(EpochManager<MOCC<Index>>::get_global_epoch());
        LOG_INFO("  SERIAL POINT (se: %u, ce: %u)", starting_epoch, epoch);
        asm volatile("":: : "memory");

        // Phase 2.1 (Validate ReadSet)
        LOG_INFO("  P2.1 (Validate ReadSet)");
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                auto rwt = rw_iter->second.rwt;
                if (rwt != ReadWriteType::READ) continue;
                // rwt is READ
                TidWord current, expected;
                current.obj = load_acquire(rw_iter->second.val->tidword.obj);
                expected.obj = rw_iter->second.tw.obj;

                const int64_t write_locked = -1;
                if (!is_valid(current, expected)) {
                    rw_iter->second.failed_verification = true;
                    this->status = TransactionStatus::aborted;
                    return false;
                }

                if (rw_iter->second.val->rwl.get_lock_cnt() == write_locked 
                && search_writeset(table_id, rw_iter->first) == nullptr) {
                    rw_iter->second.failed_verification = true;
                    this->status = TransactionStatus::aborted;
                    return false;
                }

                this->max_rset = std::max(this->max_rset, current);
            }
        }

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
                    this->status = TransactionStatus::aborted;
                    return false;
                }
            }
        }

        // Generate tid that is bigger than that of read/writeset
        TidWord tid_a, tid_b, tid_c;
        tid_a = std::max(this->max_rset, this->max_wset);
        tid_a.tid++;

        tid_b = most_recently_chosen_tid;
        tid_b.tid++;

        tid_c.epoch = epoch;

        TidWord commit_tw = std::max({tid_a, tid_b, tid_c});
        LOG_INFO("  COMMIT TID: %d", commit_tw.tid);
        most_recently_chosen_tid = commit_tw;

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
                new_tw.absent = (rwt == ReadWriteType::DELETE);
                store_release(rw_iter->second.val->tidword.obj, new_tw.obj);
                GarbageCollector::collect(commit_tw.epoch, old);
                if (rwt == ReadWriteType::DELETE) {
                    idx.remove(table_id, w_iter->first);
                    GarbageCollector::collect(commit_tw.epoch, rw_iter->second.val);
                }
            }
        }

        unlock_current_locklists();
        // TODO: make sure MOCC destructor is called after commit
        // or else we need to clear readset writeset nodemap and retrospective_locklists
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
                    lock(table_id, w_iter->first, rw_iter->second.val, LockType::WRITE);
                    assert(load_acquire(rw_iter->second.val->rec) == nullptr);
                    TidWord tw;
                    tw.obj = load_acquire(rw_iter->second.val->tidword.obj);
                    tw.absent = 1;
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
        unlock_current_locklists();

    }

private:
    TxID txid;
    uint32_t starting_epoch;
    std::set<TableID> tables;
    ReadWriteSet<Key, Value> rws;
    WriteSet<Key, Value> ws;
    NodeSet ns;
    LockList<Key, RWLock> retrospective_locklists;
    LockList<Key, RWLock> current_locklists;
    TransactionStatus status;
    TidWord max_wset;
    TidWord max_rset;
    TidWord most_recently_chosen_tid;
    const uint64_t per_xx_temp = 4096;
    const uint64_t temp_threshold = 5;
    const int temp_max = 20;
    const int temp_reset_us = 100;

    void lock(TableID table_id, Key key, Value* val, LockType type) {
        unsigned int vioctr = 0;
        unsigned int threshold;
        bool upgrade = false;
        LockElement<RWLock>* le = nullptr;

        auto& cur_lock_list = current_locklists.get_table(table_id);
        sort(cur_lock_list.begin(), cur_lock_list.end());
        for (auto itr = cur_lock_list.begin(); itr != cur_lock_list.end(); ++itr) {
            // lock already exists in CLL_
            //    && its lock mode is equal to needed mode or it is stronger than needed
            //    mode.
            if ((*itr).key == key) {
                if (type == (*itr).type || type < (*itr).type)
                    return;
                else {
                    le = &(*itr);
                    upgrade = true;
                }
            }

            // collect violation
            if ((*itr).key >= key) {
                if (vioctr == 0) threshold = (*itr).key;

                vioctr++;
            }
        }
        if (vioctr == 0) threshold = -1;
        if ((vioctr > 100)) {
            if (type != LockType::READ) {
                if (upgrade) {
                    if (val->rwl.try_lock_upgrade()) {
                        le->type = LockType::WRITE;
                        return;
                    } else {
                        status = TransactionStatus::aborted;
                        return;
                    }
                } else if (val->rwl.try_lock()) {
                    cur_lock_list.push_back(LockElement<RWLock>(key, &(val->rwl), LockType::WRITE));
                    return;
                } else {
                    status = TransactionStatus::aborted;
                    return;
                }
            } else {
                if (val->rwl.try_lock_shared()) {
                    cur_lock_list.push_back(LockElement<RWLock>(key, &(val->rwl), LockType::READ));
                    return;
                } else {
                    status = TransactionStatus::aborted;
                    return;
                }
            }
        }

        if (vioctr != 0) {
            // not in canonical mode. restore.
            for (auto itr = cur_lock_list.begin() + (cur_lock_list.size() - vioctr);
                 itr != cur_lock_list.end(); ++itr) {
                if ((*itr).type == LockType::WRITE)
                    (*itr).lock->unlock();
                else
                    (*itr).lock->unlock_shared();
            }

            // delete from CLL_
            if (cur_lock_list.size() == vioctr)
                cur_lock_list.clear();
            else
                cur_lock_list.erase(
                    cur_lock_list.begin() + (cur_lock_list.size() - vioctr), cur_lock_list.end());
        }


        auto& retro_lock_list = retrospective_locklists.get_table(table_id);
        for (auto itr = retro_lock_list.begin(); itr != retro_lock_list.end(); ++itr) {
            if ((*itr).key <= threshold) continue;

            if ((*itr).key < key) {
                if ((*itr).type == LockType::WRITE)
                    (*itr).lock->lock();
                else
                    (*itr).lock->lock_shared();

                cur_lock_list.emplace_back((*itr).key, (*itr).lock, (*itr).type);
            } else {
                break;
            }
        }
    }

    void unlock_current_locklists() {
        for (TableID table_id: tables) {
            auto& cur_lock_list = current_locklists.get_table(table_id);
            for (auto itr = cur_lock_list.begin(); itr != cur_lock_list.end(); ++itr) {
                if ((*itr).type == LockType::WRITE)
                    (*itr).lock->unlock();
                else
                    (*itr).lock->unlock_shared();
            }
        }
    }

    void construct_retrospective_locklists() {
        retrospective_locklists.clear();
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                auto rw_iter = w_iter->second;
                retrospective_locklists.get_table(table_id).emplace_back(
                    w_iter->first, &(rw_iter->second.val->rwl), LockType::WRITE);
            }
        }
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            for (auto rw_iter = rw_table.begin(); rw_iter != rw_table.end(); ++rw_iter) {
                if (rw_iter->second.rwt == ReadWriteType::READ &&
                    rw_iter->second.failed_verification) {
                    Epotemp expected, desired;
                    expected.obj = load_acquire(rw_iter->second.val->epotemp.obj);

                    for (;;) {
                        if (expected.temp == temp_max) {
                            break;
                        } else if (urand_int(0, 0) % (1 << expected.temp) == 0) {
                            // TODO: make sure get_rand is a good random number generator
                            desired = expected;
                            desired.temp = expected.temp + 1;
                        } else {
                            break;
                        }
                        
                        if (compare_exchange(rw_iter->second.val->epotemp.obj, expected.obj, desired.obj)) {
                            break;
                        }
                    }
                }

                if (search_locklists(retrospective_locklists.get_table(table_id), rw_iter->first) != nullptr) {
                    continue;
                }

                Epotemp loadepot;
                loadepot.obj = load_acquire(rw_iter->second.val->epotemp.obj);
                if (loadepot.temp >= temp_threshold || rw_iter->second.failed_verification) {
                    retrospective_locklists.get_table(table_id).emplace_back(
                        rw_iter->first, &(rw_iter->second.val->rwl), LockType::READ);
                }
            }
            sort(retrospective_locklists.get_table(table_id).begin(), retrospective_locklists.get_table(table_id).end());
        }
    }

    template <typename Lock>
    LockElement<Lock>* search_locklists(std::vector<LockElement<Lock>>& lock_list, Key key) {
        // will do : binary search
        for (auto itr = lock_list.begin(); itr != lock_list.end(); ++itr) {
            if ((*itr).key == key) return &(*itr);
        }
        return nullptr;
    }

    ReadWriteElement<Value>* search_writeset(TableID table_id, Key key) {
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);
        if (rw_iter != rw_table.end() && rw_iter->second.rwt != ReadWriteType::READ) {
            return &rw_iter->second;
        }
        return nullptr;
    }

    bool is_valid(const TidWord& current, const TidWord& expected) {
        return current.epoch == expected.epoch && current.tid == expected.tid;
    }
};