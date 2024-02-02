#pragma once

#include <cassert>
#include <cstring>
#include <set>
#include <stdexcept>

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
    using LeafNode = typename Index::LeafNode;
    using NodeInfo = typename Index::NodeInfo;

    MOCC(TxID txid, uint32_t epoch)
        : txid(txid)
        , starting_epoch(epoch) {
        LOG_INFO("START Tx, e: %u", starting_epoch);
    }

    ~MOCC() {}

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
            const int per_xx_temp = 4096;
            const int temp_threshold = 5;
            const int temp_max = 20;
            const int temp_reset_us = 100;

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
        return nullptr;
        // LOG_INFO(
        //     "INSERT (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
        //     largest_ts, table_id, key);

        // const Schema& sch = Schema::get_schema();
        // Index& idx = Index::get_index();

        // size_t record_size = sch.get_record_size(table_id);
        // tables.insert(table_id);
        // auto& rw_table = rws.get_table(table_id);
        // auto rw_iter = rw_table.find(key);

        // if (rw_iter == rw_table.end()) {
        //     // Insert possible when
        //     // 1. Key exists in index with a deleted version as head of the version chain
        //     // 2. Key is not in index

        //     Value* val;
        //     typename Index::Result res = idx.find(table_id, key, val);

        //     if (res == Index::Result::OK) {
        //         val->lock();
        //         if (val->is_detached_from_tree()) {
        //             val->unlock();
        //             return nullptr;
        //         }
        //         if (val->is_empty()) {
        //             delete_from_tree(table_id, key, val);
        //             val->unlock();
        //             return nullptr;
        //         }
        //         Version* head_version = val->version;
        //         Version* version = get_correct_version(val);
        //         gc_version_chain(val);
        //         if (version == nullptr) {
        //             val->unlock();
        //             return nullptr;  // no visible version
        //         }
        //         version->update_readts(start_ts);  // update read timestamp
        //         val->unlock();
        //         if (!(head_version == version && version->deleted)) return nullptr;

        //         Rec* rec = MemoryAllocator::aligned_allocate(record_size);

        //         auto new_iter = rw_table.emplace_hint(
        //             rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
        //             std::forward_as_tuple(nullptr, rec, ReadWriteType::INSERT, false, val));
        //         auto& w_table = ws.get_table(table_id);
        //         w_table.emplace_back(key, new_iter);
        //         return rec;
        //     }

        //     // Create new value to insert
        //     Value* new_val =
        //         reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
        //     Version* version =
        //         reinterpret_cast<Version*>(MemoryAllocator::aligned_allocate(sizeof(Version)));
        //     Rec* rec = MemoryAllocator::aligned_allocate(record_size);
        //     new_val->initialize();
        //     new_val->version = version;
        //     version->read_ts = start_ts;
        //     version->write_ts = start_ts;
        //     version->prev = nullptr;
        //     version->rec = rec;
        //     version->deleted = false;
        //     auto new_iter = rw_table.emplace_hint(
        //         rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
        //         std::forward_as_tuple(nullptr, rec, ReadWriteType::INSERT, true, new_val));

        //     auto& w_table = ws.get_table(table_id);
        //     w_table.emplace_back(key, new_iter);
        //     return rec;
        // }

        // auto rwt = rw_iter->second.rwt;
        // if (rwt == ReadWriteType::READ || rwt == ReadWriteType::UPDATE
        //     || rwt == ReadWriteType::INSERT) {
        //     return nullptr;
        // } else if (rwt == ReadWriteType::DELETE) {
        //     rw_iter->second.rwt = ReadWriteType::UPDATE;
        //     return rw_iter->second.write_rec;
        // } else {
        //     throw std::runtime_error("invalid state");
        // }
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
            const int per_xx_temp = 4096;
            const int temp_threshold = 5;
            const int temp_max = 20;
            const int temp_reset_us = 100;

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
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, ReadWriteType::UPDATE, false, val));
            return rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Upgrade lock
            Epotemp loadepot;
            const int per_xx_temp = 4096;
            const int temp_threshold = 5;
            const int temp_max = 20;
            const int temp_reset_us = 100;

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
                rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, ReadWriteType::INSERT, true, new_val));
                return rec;


            } else if (res == Index::Result::OK) {
                // Update if found in index
                Epotemp loadepot;
                const int per_xx_temp = 4096;
                const int temp_threshold = 5;
                const int temp_max = 20;
                const int temp_reset_us = 100;

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
            Epotemp loadepot;
            const int per_xx_temp = 4096;
            const int temp_threshold = 5;
            const int temp_max = 20;
            const int temp_reset_us = 100;

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
        return false;
        // LOG_INFO(
        //     "READ_SCAN (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, lk: %lu, rk: %lu, c: %ld)",
        //     start_ts, smallest_ts, largest_ts, table_id, lkey, rkey, count);

        // Index& idx = Index::get_index();
        // tables.insert(table_id);
        // auto& rw_table = rws.get_table(table_id);

        // auto per_node_func = [&](LeafNode* leaf, uint64_t version, bool& continue_flag) {
        //     unused(version, continue_flag);
        //     leaf->update_ts(start_ts);
        // };
        // auto per_kv_func = [&](Key key, Value* val, bool& continue_flag) {
        //     auto rw_iter = rw_table.find(key);
        //     if (rw_iter == rw_table.end()) {
        //         val->lock();
        //         if (val->is_detached_from_tree()) {
        //             val->unlock();
        //             return;
        //         }
        //         if (val->is_empty()) {
        //             delete_from_tree(table_id, key, val);
        //             val->unlock();
        //             return;
        //         }
        //         Version* version = get_correct_version(val);
        //         gc_version_chain(val);
        //         if (version == nullptr) {
        //             val->unlock();
        //             return;
        //         }
        //         version->update_readts(start_ts);  // update read timestamp
        //         val->unlock();
        //         if (version->deleted == true) return;

        //         // Place it into readwriteset
        //         rw_table.emplace_hint(
        //             rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
        //             std::forward_as_tuple(version->rec, nullptr, ReadWriteType::READ, false,
        //             val));

        //         kr_map.emplace(key, version->rec);
        //     } else {
        //         auto rwt = rw_iter->second.rwt;
        //         if (rwt == ReadWriteType::READ) {
        //             kr_map.emplace(key, rw_iter->second.read_rec);
        //         } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
        //             kr_map.emplace(key, rw_iter->second.write_rec);
        //         } else if (rwt == ReadWriteType::DELETE) {
        //             throw std::runtime_error("deleted value");
        //         } else {
        //             throw std::runtime_error("invalid state");
        //         }
        //     }

        //     if (count != -1 && static_cast<int64_t>(kr_map.size()) >= count) continue_flag =
        //     false;
        // };

        // [[maybe_unused]] typename Index::Result res;
        // if (rev == true) {
        //     res = idx.get_kv_in_rev_range(table_id, lkey, rkey, per_node_func, per_kv_func);
        // } else {
        //     res = idx.get_kv_in_range(table_id, lkey, rkey, per_node_func, per_kv_func);
        // }
        // assert(res == Index::Result::OK);
        // return true;
    }


    bool update_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, bool rev,
        std::map<Key, Rec*>& kr_map) {
        return false;
        // LOG_INFO(
        //     "UPDATE_SCAN (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, lk: %lu, rk: %lu, c: %ld)",
        //     start_ts, smallest_ts, largest_ts, table_id, lkey, rkey, count);

        // const Schema& sch = Schema::get_schema();
        // size_t record_size = sch.get_record_size(table_id);
        // Index& idx = Index::get_index();
        // tables.insert(table_id);
        // auto& rw_table = rws.get_table(table_id);
        // auto& w_table = ws.get_table(table_id);

        // auto per_node_func = [&](LeafNode* leaf, uint64_t version, bool& continue_flag) {
        //     unused(version, continue_flag);
        //     leaf->update_ts(start_ts);
        // };
        // auto per_kv_func = [&](Key key, Value* val, bool& continue_flag) {
        //     auto rw_iter = rw_table.find(key);
        //     if (rw_iter == rw_table.end()) {
        //         // Read version chain and get the correct version
        //         val->lock();
        //         if (val->is_detached_from_tree()) {
        //             val->unlock();
        //             return;
        //         }
        //         if (val->is_empty()) {
        //             delete_from_tree(table_id, key, val);
        //             val->unlock();
        //             return;
        //         }
        //         Version* version = get_correct_version(val);
        //         gc_version_chain(val);
        //         if (version == nullptr) {
        //             val->unlock();
        //             return;
        //         }
        //         version->update_readts(start_ts);  // update read timestamp
        //         val->unlock();
        //         if (version->deleted == true) return;

        //         // Allocate memory for write
        //         Rec* rec = MemoryAllocator::aligned_allocate(record_size);
        //         memcpy(rec, version->rec, record_size);
        //         auto new_iter = rw_table.emplace_hint(
        //             rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
        //             std::forward_as_tuple(version->rec, rec, ReadWriteType::UPDATE, false, val));
        //         // Place it in writeset
        //         w_table.emplace_back(key, new_iter);
        //         kr_map.emplace(key, rec);
        //     } else {
        //         auto rwt = rw_iter->second.rwt;
        //         if (rwt == ReadWriteType::READ) {
        //             // Localset will point to allocated record
        //             Rec* rec = MemoryAllocator::aligned_allocate(record_size);
        //             memcpy(rec, rw_iter->second.read_rec, record_size);
        //             rw_iter->second.write_rec = rec;
        //             rw_iter->second.rwt = ReadWriteType::UPDATE;
        //             // Place it in writeset
        //             w_table.emplace_back(key, rw_iter);
        //             kr_map.emplace(key, rec);
        //         } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
        //             kr_map.emplace(key, rw_iter->second.write_rec);
        //         } else if (rwt == ReadWriteType::DELETE) {
        //             throw std::runtime_error("deleted value");
        //         } else {
        //             throw std::runtime_error("invalid state");
        //         }
        //     }

        //     if (count != -1 && static_cast<int64_t>(kr_map.size()) >= count) continue_flag =
        //     false;
        // };

        // [[maybe_unused]] typename Index::Result res;
        // if (rev == true) {
        //     res = idx.get_kv_in_rev_range(table_id, lkey, rkey, per_node_func, per_kv_func);
        // } else {
        //     res = idx.get_kv_in_range(table_id, lkey, rkey, per_node_func, per_kv_func);
        // }
        // assert(res == Index::Result::OK);
        // return true;
    }

    const Rec* remove(TableID table_id, Key key) {
        return nullptr;
        // LOG_INFO(
        //     "REMOVE (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
        //     largest_ts, table_id, key);
        // Index& idx = Index::get_index();

        // tables.insert(table_id);
        // auto& rw_table = rws.get_table(table_id);
        // auto rw_iter = rw_table.find(key);

        // if (rw_iter == rw_table.end()) {
        //     // Abort if not found in index
        //     Value* val;
        //     typename Index::Result res = idx.find(table_id, key, val);
        //     if (res == Index::Result::NOT_FOUND) return nullptr;

        //     // Read version chain and get the correct version
        //     val->lock();
        //     if (val->is_detached_from_tree()) {
        //         val->unlock();
        //         return nullptr;
        //     }
        //     if (val->is_empty()) {
        //         delete_from_tree(table_id, key, val);
        //         val->unlock();
        //         return nullptr;
        //     }
        //     Version* version = get_correct_version(val);
        //     gc_version_chain(val);
        //     if (version == nullptr) {
        //         val->unlock();
        //         return nullptr;  // no visible version
        //     }
        //     version->update_readts(start_ts);  // update read timestamp
        //     val->unlock();
        //     if (version->deleted == true) return nullptr;

        //     auto new_iter = rw_table.emplace_hint(
        //         rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
        //         std::forward_as_tuple(version->rec, nullptr, ReadWriteType::DELETE, false, val));
        //     // Place it in writeset
        //     auto& w_table = ws.get_table(table_id);
        //     w_table.emplace_back(key, new_iter);
        //     return version->rec;
        // }

        // auto rwt = rw_iter->second.rwt;
        // if (rwt == ReadWriteType::READ) {
        //     rw_iter->second.rwt = ReadWriteType::DELETE;

        //     // Place it in writeset
        //     auto& w_table = ws.get_table(table_id);
        //     w_table.emplace_back(key, rw_iter);

        //     return rw_iter->second.read_rec;
        // } else if (rwt == ReadWriteType::UPDATE) {
        //     assert(rw_iter->second.read_rec != nullptr);
        //     MemoryAllocator::deallocate(rw_iter->second.write_rec);
        //     rw_iter->second.write_rec = nullptr;
        //     return rw_iter->second.read_rec;
        // } else if (rwt == ReadWriteType::INSERT) {
        //     assert(rw_iter->second.read_rec == nullptr);
        //     MemoryAllocator::deallocate(rw_iter->second.write_rec);
        //     rw_table.erase(rw_iter);
        //     return nullptr;  // currently this aborts
        // } else {
        //     throw std::runtime_error("invalid state");
        // }
    }

    bool precommit() {
        return false;
        // LOG_INFO("PRECOMMIT, ts: %lu, s_ts: %lu, l_ts: %lu", start_ts, smallest_ts, largest_ts);
        // Index& idx = Index::get_index();

        // LOG_INFO("LOCKING RECORDS");
        // // Lock records
        // for (TableID table_id: tables) {
        //     auto& w_table = ws.get_table(table_id);
        //     std::sort(w_table.begin(), w_table.end(), [](const auto& lhs, const auto& rhs) {
        //         return lhs.first <= rhs.first;
        //     });
        //     for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
        //         LOG_DEBUG("     LOCK (t: %lu, k: %lu)", table_id, w_iter->first);
        //         auto rw_iter = w_iter->second;
        //         Value* val = rw_iter->second.val;
        //         val->lock();
        //         if (val->is_detached_from_tree()) {
        //             remove_already_inserted(table_id, w_iter->first, true);
        //             unlock_writeset(table_id, w_iter->first, false);
        //             return false;
        //         }
        //         auto rwt = rw_iter->second.rwt;
        //         bool is_new = rw_iter->second.is_new;
        //         if (rwt == ReadWriteType::INSERT && is_new) {
        //             // On INSERT(is_new=true), insert the record to shared index
        //             NodeInfo ni;
        //             auto res = idx.insert(table_id, w_iter->first, val, ni);
        //             if (res == Index::Result::NOT_INSERTED) {
        //                 remove_already_inserted(table_id, w_iter->first, true);
        //                 unlock_writeset(table_id, w_iter->first, false);
        //                 return false;
        //             } else if (res == Index::Result::OK) {
        //                 // to prevent phantoms, abort if timestamp of the node is larger than
        //                 // start_ts
        //                 LeafNode* leaf = reinterpret_cast<LeafNode*>(ni.node);
        //                 if (leaf->get_ts() > start_ts) {
        //                     remove_already_inserted(table_id, w_iter->first, false);
        //                     unlock_writeset(table_id, w_iter->first, false);
        //                     return false;
        //                 }
        //             }
        //         } else if (rwt == ReadWriteType::INSERT && !is_new) {
        //             // On INSERT(is_new=false), check the latest version timestamp and whether it
        //             is
        //             // deleted
        //             uint64_t read_ts = val->version->read_ts;
        //             uint64_t write_ts = val->version->write_ts;
        //             bool deleted = val->version->deleted;
        //             if (read_ts > start_ts || write_ts > start_ts || !deleted) {
        //                 remove_already_inserted(table_id, w_iter->first, true);
        //                 unlock_writeset(table_id, w_iter->first, false);
        //                 return false;
        //             }
        //         } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::DELETE) {
        //             // On UPDATE/DELETED, check the latest version timestamp and whether it is
        //             not
        //             // deleted
        //             uint64_t read_ts = val->version->read_ts;
        //             uint64_t write_ts = val->version->write_ts;
        //             bool deleted = val->version->deleted;
        //             if (read_ts > start_ts || write_ts > start_ts || deleted) {
        //                 remove_already_inserted(table_id, w_iter->first, true);
        //                 unlock_writeset(table_id, w_iter->first, false);
        //                 return false;
        //             }
        //         }
        //     }
        // }

        // LOG_INFO("APPLY CHANGES TO INDEX");
        // // Apply changes to index
        // for (TableID table_id: tables) {
        //     auto& w_table = ws.get_table(table_id);
        //     for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
        //         auto rw_iter = w_iter->second;
        //         Value* val = rw_iter->second.val;
        //         if (!rw_iter->second.is_new) {
        //             Version* version = reinterpret_cast<Version*>(
        //                 MemoryAllocator::aligned_allocate(sizeof(Version)));
        //             version->read_ts = start_ts;
        //             version->write_ts = start_ts;
        //             version->prev = val->version;
        //             version->rec = rw_iter->second.write_rec;
        //             version->deleted = (rw_iter->second.rwt == ReadWriteType::DELETE);
        //             val->version = version;
        //         }
        //         gc_version_chain(val);
        //         val->unlock();
        //     }
        // }
        // return true;
    }

    void abort() {
        // for (TableID table_id: tables) {
        //     auto& rw_table = rws.get_table(table_id);
        //     auto& w_table = ws.get_table(table_id);
        //     for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
        //         auto rw_iter = w_iter->second;
        //         auto rwt = rw_iter->second.rwt;
        //         bool is_new = rw_iter->second.is_new;
        //         if (rwt == ReadWriteType::INSERT && is_new) {
        //             Value* val = rw_iter->second.val;  // This points to locally allocated value
        //                                                // when Insert(is_new = true)
        //             if (val->version) {
        //                 // if version is not a nullptr, this means that no attempts have been
        //                 made
        //                 // to insert val to index. Thus, the version is not touched and not
        //                 // deallocated. Otherwise version is already deallocated by the
        //                 // remove_already_inserted function
        //                 MemoryAllocator::deallocate(val->version->rec);
        //                 MemoryAllocator::deallocate(val->version);
        //                 MemoryAllocator::deallocate(val);
        //             }
        //         } else {
        //             MemoryAllocator::deallocate(rw_iter->second.write_rec);
        //         }
        //     }
        //     rw_table.clear();
        //     w_table.clear();
        // }
        // tables.clear();
    }

private:
    TxID txid;
    uint32_t starting_epoch;
    std::set<TableID> tables;
    ReadWriteSet<Key, Value> rws;
    LockList<Key, RWLock> retrospective_locklists;
    LockList<Key, RWLock> current_locklists;
    TransactionStatus status;

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

    void remove_already_inserted(TableID end_table_id, Key end_key, bool end_exclusive) {
        // Index& idx = Index::get_index();
        // for (TableID table_id: tables) {
        //     auto& w_table = ws.get_table(table_id);
        //     for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
        //         Key key = w_iter->first;
        //         if (end_exclusive && table_id == end_table_id && key == end_key) return;
        //         auto rw_iter = w_iter->second;
        //         auto rwt = rw_iter->second.rwt;
        //         bool is_new = rw_iter->second.is_new;
        //         if (rwt == ReadWriteType::INSERT && is_new) {
        //             // On INSERT, insert the record to shared index
        //             Value* val = rw_iter->second.val;
        //             Version* version = val->version;
        //             idx.remove(table_id, key);
        //             val->version = nullptr;
        //             GarbageCollector::collect(largest_ts, val);
        //             MemoryAllocator::deallocate(version->rec);
        //             MemoryAllocator::deallocate(version);
        //         }
        //         if (!end_exclusive && table_id == end_table_id && key == end_key) return;
        //     }
        // }
    }

    void unlock_writeset(TableID end_table_id, Key end_key, bool end_exclusive) {
        // for (TableID table_id: tables) {
        //     auto& w_table = ws.get_table(table_id);
        //     for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
        //         if (end_exclusive && table_id == end_table_id && w_iter->first == end_key)
        //         return; auto rw_iter = w_iter->second; Value* val = rw_iter->second.val;
        //         val->unlock();
        //         if (!end_exclusive && table_id == end_table_id && w_iter->first == end_key)
        //         return;
        //     }
        // }
    }

    // Acquire val->lock() before calling this function
    void delete_from_tree(TableID table_id, Key key, Value* val) {
        // Index& idx = Index::get_index();
        // idx.remove(table_id, key);
        // Version* version = val->version;
        // val->version = nullptr;
        // GarbageCollector::collect(largest_ts, val);
        // MemoryAllocator::deallocate(version->rec);
        // MemoryAllocator::deallocate(version);
        // return;
    }

    // Acquire val->lock() before calling this function
    void gc_version_chain(Value* val) {
        // Version* gc_version_plus_one = nullptr;
        // Version* gc_version = val->version;

        // // look for the latest version with write_ts <= start_ts
        // while (gc_version != nullptr && smallest_ts < gc_version->write_ts) {
        //     gc_version_plus_one = gc_version;
        //     gc_version = gc_version->prev;
        // }

        // if (gc_version == nullptr) return;

        // // keep one
        // gc_version_plus_one = gc_version;
        // gc_version = gc_version->prev;

        // gc_version_plus_one->prev = nullptr;

        // Version* temp;
        // while (gc_version != nullptr) {
        //     temp = gc_version->prev;
        //     MemoryAllocator::deallocate(gc_version->rec);
        //     MemoryAllocator::deallocate(gc_version);
        //     gc_version = temp;
        // }
    }
    template <typename Lock>
    LockElement<Lock>* search_locklists(std::vector<LockElement<Lock>>& lock_list, Key key) {
        // will do : binary search
        for (auto itr = lock_list.begin(); itr != lock_list.end(); ++itr) {
            if ((*itr).key == key) return &(*itr);
        }
        return nullptr;
    }
};