#pragma once

#include <cassert>
#include <cstring>
#include <set>
#include <stdexcept>

#include "protocols/common/timestamp_manager.hpp"
#include "protocols/common/transaction_id.hpp"
#include "protocols/mvto/include/readwriteset.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"
#include "indexes/masstree.hpp"

template <typename Index>
class Serval {
public:
    using Key = typename Index::Key;
    using Value = typename Index::Value;
    using Version = typename Value::Version;
    using LeafNode = typename Index::LeafNode;
    using NodeInfo = typename Index::NodeInfo;

    Serval(TxID txid, uint64_t ts, uint64_t smallest_ts, uint64_t largest_ts)
        : txid(txid)
        , start_ts(ts)
        , smallest_ts(smallest_ts)
        , largest_ts(largest_ts) {
        LOG_INFO("START Tx, ts: %lu, s_ts: %lu, l_ts: %lu", start_ts, smallest_ts, largest_ts);
    }

    ~Serval() { GarbageCollector::remove(smallest_ts, largest_ts); }

    void set_new_ts(uint64_t start_ts_, uint64_t smallest_ts_, uint64_t largest_ts_) {
        start_ts = start_ts_;
        smallest_ts = smallest_ts_;
        largest_ts = largest_ts_;
    }

    Rec* append_pending_version(TableID table_id, Key key) {
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index(); 

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        // Case of not read and written
        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);  // find corresponding index in masstree
            if (res == NOT_FOUND){
                // To-Do
            } else if (res == OK) {
                // Got value from masstree
                if (val -> ptr_to_version_array == nullptr) { // Per-core version array does not exist (_0)
                    if (val->try_lock()){ // Successfully got the try lock (00)
                        // Create Version
                        Version* new_version = reinterpret_cast<Version*>(MemoryAllocator::aligned_allocate(sizeof(Version)));
                        // Append pending version
                        new_version->prev = val->version;
                        val->version = new_version;
                        return; // To-Do
                    }
                    // Failed to get the lock (10)
                    // Install pointer to per-core version array by __atomic_exchange
                    void* return_value;
                    __atomic_exchange(val->ptr_to_version_array, )
                    
                }
            }

        }
    }

    const Rec* read(TableID table_id, Key key) {
        // serval -> find the latest "1" from correpsonding bitmap
        LOG_INFO(
            "READ (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        Index& idx = Index::get_index();
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;

            // Read version chain and get the correct version
            val->lock();
            if (val->is_detached_from_tree()) {
                val->unlock();
                return nullptr;
            }
            if (val->is_empty()) {
                delete_from_tree(table_id, key, val);
                val->unlock();
                return nullptr;
            }
            Version* version = get_correct_version(val);
            gc_version_chain(val);
            if (version == nullptr) {
                val->unlock();
                return nullptr;  // no visible version
            }
            version->update_readts(start_ts);  // update read timestamp
            val->unlock();
            if (version->deleted == true) return nullptr;

            // Place it into readwriteset
            rw_table.emplace_hint(
                rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(version->rec, nullptr, ReadWriteType::READ, false, val));
            return version->rec;
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            return rw_iter->second.read_rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            return rw_iter->second.write_rec;
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
        // serval -> change corresponding bitmap 
        LOG_INFO(
            "UPDATE (ts: %lu, s_ts: %lu, l_ts: %lu, t: %lu, k: %lu)", start_ts, smallest_ts,
            largest_ts, table_id, key);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index(); // これでマス釣りを操る

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rw_table = rws.get_table(table_id);
        auto rw_iter = rw_table.find(key);

        if (rw_iter == rw_table.end()) { //まだよまれてないし書かれてないなら
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val); //masstreeでインデックスを見つける
            if (res == Index::Result::NOT_FOUND) {
                // Create new value to insert
                Value* new_val =
                    reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
                Version* version =
                    reinterpret_cast<Version*>(MemoryAllocator::aligned_allocate(sizeof(Version)));
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                new_val->initialize();
                new_val->version = version;
                version->read_ts = start_ts;
                version->write_ts = start_ts;
                version->prev = nullptr;
                version->rec = rec;
                version->deleted = false;
                auto new_iter = rw_table.emplace_hint(
                    rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(nullptr, rec, ReadWriteType::INSERT, true, new_val));

                auto& w_table = ws.get_table(table_id);
                w_table.emplace_back(key, new_iter);
                return rec;
            } else if (res == Index::Result::OK) {
                val->lock();
                if (val->is_detached_from_tree()) {
                    val->unlock();
                    return nullptr;
                }
                if (val->is_empty()) {
                    delete_from_tree(table_id, key, val);
                    val->unlock();
                    return nullptr;
                }
                Version* head_version = val->version;
                Version* version = get_correct_version(val);
                gc_version_chain(val);
                if (version == nullptr) {
                    val->unlock();
                    return nullptr;  // no visible version
                }
                version->update_readts(start_ts);  // update read timestamp
                val->unlock();
                if (head_version == version && version->deleted) {
                    // Insert
                    Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                    auto new_iter = rw_table.emplace_hint(
                        rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                        std::forward_as_tuple(nullptr, rec, ReadWriteType::INSERT, false, val));
                    auto& w_table = ws.get_table(table_id);
                    w_table.emplace_back(key, new_iter);
                    return rec;
                } else if (!version->deleted) {
                    // Update
                    Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                    memcpy(rec, version->rec, record_size);
                    auto new_iter = rw_table.emplace_hint(
                        rw_iter, std::piecewise_construct, std::forward_as_tuple(key),
                        std::forward_as_tuple(
                            version->rec, rec, ReadWriteType::UPDATE, false, val));
                    // Place it in writeset
                    auto& w_table = ws.get_table(table_id);
                    w_table.emplace_back(key, new_iter);
                    return rec;
                } else {
                    return nullptr;
                }
            } else {
                throw std::runtime_error("invalid state");
            }
        }

        auto rwt = rw_iter->second.rwt;
        if (rwt == ReadWriteType::READ) {
            // Localset will point to allocated record
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            memcpy(rec, rw_iter->second.read_rec, record_size);
            rw_iter->second.write_rec = rec;
            rw_iter->second.rwt = ReadWriteType::UPDATE;

            // Place it in writeset
            auto& w_table = ws.get_table(table_id);
            w_table.emplace_back(key, rw_iter);
            return rec;
        } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::INSERT) {
            return rw_iter->second.write_rec;
        } else if (rw_iter->second.rwt == ReadWriteType::DELETE) {
            rw_iter->second.rwt = ReadWriteType::UPDATE;
            return rw_iter->second.write_rec;
        } else {
            throw std::runtime_error("invalid state");
        }
    }

    bool precommit() {
        LOG_INFO("PRECOMMIT, ts: %lu, s_ts: %lu, l_ts: %lu", start_ts, smallest_ts, largest_ts);
        Index& idx = Index::get_index();

        LOG_INFO("LOCKING RECORDS");
        // Lock records
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            std::sort(w_table.begin(), w_table.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.first <= rhs.first;
            });
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                LOG_DEBUG("     LOCK (t: %lu, k: %lu)", table_id, w_iter->first);
                auto rw_iter = w_iter->second;
                Value* val = rw_iter->second.val;
                val->lock();
                if (val->is_detached_from_tree()) {
                    remove_already_inserted(table_id, w_iter->first, true);
                    unlock_writeset(table_id, w_iter->first, false);
                    return false;
                }
                auto rwt = rw_iter->second.rwt;
                bool is_new = rw_iter->second.is_new;
                if (rwt == ReadWriteType::INSERT && is_new) {
                    // On INSERT(is_new=true), insert the record to shared index
                    NodeInfo ni;
                    auto res = idx.insert(table_id, w_iter->first, val, ni);
                    if (res == Index::Result::NOT_INSERTED) {
                        remove_already_inserted(table_id, w_iter->first, true);
                        unlock_writeset(table_id, w_iter->first, false);
                        return false;
                    } else if (res == Index::Result::OK) {
                        // to prevent phantoms, abort if timestamp of the node is larger than
                        // start_ts
                        LeafNode* leaf = reinterpret_cast<LeafNode*>(ni.node);
                        if (leaf->get_ts() > start_ts) {
                            remove_already_inserted(table_id, w_iter->first, false);
                            unlock_writeset(table_id, w_iter->first, false);
                            return false;
                        }
                    }
                } else if (rwt == ReadWriteType::INSERT && !is_new) {
                    // On INSERT(is_new=false), check the latest version timestamp and whether it is
                    // deleted
                    uint64_t read_ts = val->version->read_ts;
                    uint64_t write_ts = val->version->write_ts;
                    bool deleted = val->version->deleted;
                    if (read_ts > start_ts || write_ts > start_ts || !deleted) {
                        remove_already_inserted(table_id, w_iter->first, true);
                        unlock_writeset(table_id, w_iter->first, false);
                        return false;
                    }
                } else if (rwt == ReadWriteType::UPDATE || rwt == ReadWriteType::DELETE) {
                    // On UPDATE/DELETED, check the latest version timestamp and whether it is not
                    // deleted
                    uint64_t read_ts = val->version->read_ts;
                    uint64_t write_ts = val->version->write_ts;
                    bool deleted = val->version->deleted;
                    if (read_ts > start_ts || write_ts > start_ts || deleted) {
                        remove_already_inserted(table_id, w_iter->first, true);
                        unlock_writeset(table_id, w_iter->first, false);
                        return false;
                    }
                }
            }
        }

        LOG_INFO("APPLY CHANGES TO INDEX");
        // Apply changes to index
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                auto rw_iter = w_iter->second;
                Value* val = rw_iter->second.val;
                if (!rw_iter->second.is_new) {
                    Version* version = reinterpret_cast<Version*>(
                        MemoryAllocator::aligned_allocate(sizeof(Version)));
                    version->read_ts = start_ts;
                    version->write_ts = start_ts;
                    version->prev = val->version;
                    version->rec = rw_iter->second.write_rec;
                    version->deleted = (rw_iter->second.rwt == ReadWriteType::DELETE);
                    val->version = version;
                }
                gc_version_chain(val);
                val->unlock();
            }
        }
        return true;
    }

    void abort() {
        for (TableID table_id: tables) {
            auto& rw_table = rws.get_table(table_id);
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                auto rw_iter = w_iter->second;
                auto rwt = rw_iter->second.rwt;
                bool is_new = rw_iter->second.is_new;
                if (rwt == ReadWriteType::INSERT && is_new) {
                    Value* val = rw_iter->second.val;  // This points to locally allocated value
                                                       // when Insert(is_new = true)
                    if (val->version) {
                        // if version is not a nullptr, this means that no attempts have been made
                        // to insert val to index. Thus, the version is not touched and not
                        // deallocated. Otherwise version is already deallocated by the
                        // remove_already_inserted function
                        MemoryAllocator::deallocate(val->version->rec);
                        MemoryAllocator::deallocate(val->version);
                        MemoryAllocator::deallocate(val);
                    }
                } else {
                    MemoryAllocator::deallocate(rw_iter->second.write_rec);
                }
            }
            rw_table.clear();
            w_table.clear();
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
    WriteSet<Key, Value> ws;

    void remove_already_inserted(TableID end_table_id, Key end_key, bool end_exclusive) {
        Index& idx = Index::get_index();
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                Key key = w_iter->first;
                if (end_exclusive && table_id == end_table_id && key == end_key) return;
                auto rw_iter = w_iter->second;
                auto rwt = rw_iter->second.rwt;
                bool is_new = rw_iter->second.is_new;
                if (rwt == ReadWriteType::INSERT && is_new) {
                    // On INSERT, insert the record to shared index
                    Value* val = rw_iter->second.val;
                    Version* version = val->version;
                    idx.remove(table_id, key);
                    val->version = nullptr;
                    GarbageCollector::collect(largest_ts, val);
                    MemoryAllocator::deallocate(version->rec);
                    MemoryAllocator::deallocate(version);
                }
                if (!end_exclusive && table_id == end_table_id && key == end_key) return;
            }
        }
    }

    void unlock_writeset(TableID end_table_id, Key end_key, bool end_exclusive) {
        for (TableID table_id: tables) {
            auto& w_table = ws.get_table(table_id);
            for (auto w_iter = w_table.begin(); w_iter != w_table.end(); ++w_iter) {
                if (end_exclusive && table_id == end_table_id && w_iter->first == end_key) return;
                auto rw_iter = w_iter->second;
                Value* val = rw_iter->second.val;
                val->unlock();
                if (!end_exclusive && table_id == end_table_id && w_iter->first == end_key) return;
            }
        }
    }

    // Acquire val->lock() before calling this function
    Version* get_correct_version(Value* val) {
        Version* version = val->version;

        // look for the latest version with write_ts <= start_ts
        while (version != nullptr && start_ts < version->write_ts) {
            version = version->prev;
        }

        return version;  // this could be nullptr
    }

    // Acquire val->lock() before calling this function
    void delete_from_tree(TableID table_id, Key key, Value* val) {
        Index& idx = Index::get_index();
        idx.remove(table_id, key);
        Version* version = val->version;
        val->version = nullptr;
        GarbageCollector::collect(largest_ts, val);
        MemoryAllocator::deallocate(version->rec);
        MemoryAllocator::deallocate(version);
        return;
    }

    // Acquire val->lock() before calling this function
    void gc_version_chain(Value* val) {
        Version* gc_version_plus_one = nullptr;
        Version* gc_version = val->version;

        // look for the latest version with write_ts <= start_ts
        while (gc_version != nullptr && smallest_ts < gc_version->write_ts) {
            gc_version_plus_one = gc_version;
            gc_version = gc_version->prev;
        }

        if (gc_version == nullptr) return;

        // keep one
        gc_version_plus_one = gc_version;
        gc_version = gc_version->prev;

        gc_version_plus_one->prev = nullptr;

        Version* temp;
        while (gc_version != nullptr) {
            temp = gc_version->prev;
            MemoryAllocator::deallocate(gc_version->rec);
            MemoryAllocator::deallocate(gc_version);
            gc_version = temp;
        }
    }
};