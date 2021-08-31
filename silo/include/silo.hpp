#pragma once

#include <cassert>
#include <map>
#include <set>
#include <unordered_map>
#include <utility>

#include "common/epoch_manager.hpp"
#include "common/garbage_collector.hpp"
#include "common/schema.hpp"
#include "silo/include/readwriteset.hpp"
#include "silo/include/value.hpp"

template <typename Index>
class Silo {
public:
    class NodeSet {
    public:
        typename Index::NodeMap& get_nodemap(TableID table_id) { return ns[table_id]; }

    private:
        std::unordered_map<TableID, typename Index::NodeMap> ns;
    };

    Silo(uint32_t epoch)
        : starting_epoch(epoch) {
        LOG_INFO("START Tx, e: %u", starting_epoch);
    }

    ~Silo() {
        for (TableID table_id: tables) {
            auto& rs_table = rs.get_table(table_id);
            auto& ws_table = ws.get_table(table_id);
            auto& vs_table = vs.get_table(table_id);
            auto& nm = ns.get_nodemap(table_id);

            for (auto r_iter = rs_table.begin(); r_iter != rs_table.end(); r_iter++) {
                // Deallocate ReadSet that is not in WriteSet
                if (ws_table.find(r_iter->first) == ws_table.end())
                    GarbageCollector::collect(starting_epoch, r_iter->second.rec);
            }

            ws_table.clear();
            rs_table.clear();
            vs_table.clear();
            nm.clear();
        }
        GarbageCollector::remove(starting_epoch);
    }

    const Rec* read(TableID table_id, Key key) {
        LOG_INFO("READ (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rs_table = rs.get_table(table_id);
        auto rs_iter = rs_table.find(key);
        auto& ws_table = ws.get_table(table_id);
        auto ws_iter = ws_table.find(key);
        auto& vs_table = vs.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
            // key is not in local set (read/write set)
            // 1. find key from shared index and abort if not found
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val, nm);

            if (res == Index::Result::NOT_FOUND) return nullptr;  // abort

            // 2. copy record and tidword from shared memory
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);

            TidWord tw;
            read_record_and_tidword(*val, rec, tw, record_size);

            if (!(tw.absent == 0 && tw.latest == 1)) {  // unable to read the value
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }

            // 3. place it in readset
            rs_table.emplace_hint(
                rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, tw));

            // 4. place it in validationset
            vs_table[key] = val;

            return rec;
        } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
            // key is not in readset but is in writeset
            // -> key is in validation set (whether it is insert or update)
            auto vs_iter = vs_table.find(key);
            assert(vs_iter != vs_table.end());
            Rec* rec = ws_iter->second.rec;
            TidWord tw;
            // copy tidword from shared memory
            read_tidword(*(vs_iter->second), tw);
            if (!(tw.absent == 0 && tw.latest == 1)) return nullptr;  // unable to read the value
            rs_table.emplace_hint(
                rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, tw));
            return rec;
        } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
            return rs_iter->second.rec;
        } else if (
            rs_iter != rs_table.end() && ws_iter != ws_table.end()
            && ws_iter->second.wt == WriteType::DELETE) {
            return nullptr;  // abort
        } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
            return rs_iter->second.rec;
        }
        assert(false);
        return nullptr;
    }

    Rec* insert(TableID table_id, Key key) {
        LOG_INFO("INSERT (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);

        auto& rs_table = rs.get_table(table_id);
        auto rs_iter = rs_table.find(key);
        auto& ws_table = ws.get_table(table_id);
        auto ws_iter = ws_table.find(key);
        auto& vs_table = vs.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) == vs_table.end());
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::OK) return nullptr;  // abort
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            Value* new_val =
                reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
            new_val->rec = nullptr;
            new_val->tidword.obj = 0;
            new_val->tidword.latest = 1;  // exist in index
            new_val->tidword.absent = 1;  // cannot be seen by others

            res = idx.insert(table_id, key, new_val, nm);
            if (res == Index::Result::NOT_INSERTED) {
                MemoryAllocator::deallocate(new_val);
                MemoryAllocator::deallocate(rec);
                return nullptr;  // abort
            } else if (res == Index::Result::BAD_INSERT) {
                return nullptr;  // abort
            }

            assert(res == Index::Result::OK);
            ws_table.emplace_hint(
                ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, WriteType::INSERT, true));
            vs_table.emplace(key, new_val);

            return rec;
        } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return nullptr;  // abort
        } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return nullptr;  // abort
        } else if (
            rs_iter != rs_table.end() && ws_iter != ws_table.end()
            && ws_iter->second.wt == WriteType::DELETE) {
            assert(vs_table.find(key) != vs_table.end());
            assert(rs_iter->second.rec == ws_iter->second.rec);
            Rec* rec = ws_iter->second.rec;
            memset(rec, 0, record_size);  // clear contents
            ws_iter->second.wt = WriteType::UPDATE;
            return rec;
        } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return nullptr;  // abort
        }

        assert(false);
        return nullptr;
    }

    Rec* update(TableID table_id, Key key) {
        LOG_INFO("UPDATE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rs_table = rs.get_table(table_id);
        auto rs_iter = rs_table.find(key);
        auto& ws_table = ws.get_table(table_id);
        auto ws_iter = ws_table.find(key);
        auto& vs_table = vs.get_table(table_id);

        if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) == vs_table.end());
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);

            if (res == Index::Result::NOT_FOUND) return nullptr;
            Rec* rec = MemoryAllocator::aligned_allocate(record_size);

            TidWord tw;

            read_record_and_tidword(*val, rec, tw, record_size);

            if (!(tw.absent == 0 && tw.latest == 1)) {  // unable to read the value
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }

            rs_table.emplace_hint(
                rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, tw));

            ws_table.emplace_hint(
                ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, WriteType::UPDATE, false));

            vs_table.emplace(key, val);

            return rec;
        } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return ws_iter->second.rec;
        } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            Rec* rec = rs_iter->second.rec;
            ws_table.emplace_hint(
                ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, WriteType::UPDATE, false));
            return rec;
        } else if (
            rs_iter != rs_table.end() && ws_iter != ws_table.end()
            && ws_iter->second.wt == WriteType::DELETE) {
            assert(vs_table.find(key) != vs_table.end());
            return nullptr;  // abort
        } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return ws_iter->second.rec;
        }

        assert(false);
        return nullptr;
    }

    Rec* upsert(TableID table_id, Key key) {
        LOG_INFO("UPSERT (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        size_t record_size = sch.get_record_size(table_id);
        tables.insert(table_id);
        auto& rs_table = rs.get_table(table_id);
        auto rs_iter = rs_table.find(key);
        auto& ws_table = ws.get_table(table_id);
        auto ws_iter = ws_table.find(key);
        auto& vs_table = vs.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val, nm);
            if (res == Index::Result::NOT_FOUND) {
                // INSERT into index
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                Value* new_val =
                    reinterpret_cast<Value*>(MemoryAllocator::aligned_allocate(sizeof(Value)));
                new_val->rec = nullptr;
                new_val->tidword.obj = 0;
                new_val->tidword.latest = 1;  // exist in index
                new_val->tidword.absent = 1;  // cannot be seen by others

                res = idx.insert(table_id, key, new_val, nm);
                if (res == Index::Result::NOT_INSERTED) {
                    MemoryAllocator::deallocate(new_val);
                    MemoryAllocator::deallocate(rec);
                    return nullptr;  // abort
                } else if (res == Index::Result::BAD_INSERT) {
                    return nullptr;  // abort
                }

                assert(res == Index::Result::OK);
                ws_table.emplace_hint(
                    ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, WriteType::INSERT, true));
                vs_table.emplace(key, new_val);

                return rec;
            } else {
                assert(res == Index::Result::OK);
                // UPDATE
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                read_record_and_tidword(*val, rec, tw, record_size);

                if (!(tw.absent == 0 && tw.latest == 1)) {  // unable to read the value
                    MemoryAllocator::deallocate(rec);
                    return nullptr;
                }

                rs_table.emplace_hint(
                    rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw));
                ws_table.emplace_hint(
                    ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, WriteType::UPDATE, false));
                vs_table.emplace(key, val);

                return rec;
            }
        } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return ws_iter->second.rec;
        } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            Rec* rec = rs_iter->second.rec;
            ws_table.emplace_hint(
                ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, WriteType::UPDATE, false));
            return rec;
        } else if (
            rs_iter != rs_table.end() && ws_iter != ws_table.end()
            && ws_iter->second.wt == WriteType::DELETE) {
            assert(vs_table.find(key) != vs_table.end());
            assert(rs_iter->second.rec == ws_iter->second.rec);
            Rec* rec = ws_iter->second.rec;
            memset(rec, 0, record_size);  // clear contents
            ws_iter->second.wt = WriteType::UPDATE;
            return rec;
        } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            return ws_iter->second.rec;
        }

        assert(false);
        return nullptr;
    }

    bool read_scan(
        TableID table_id, Key lkey, Key rkey, int64_t count, bool reverse,
        std::map<Key, Rec*>& kr_map) {
        LOG_INFO(
            "READ_SCAN (e: %u, t: %lu, lk: %lu, rk: %lu, c: %ld)", starting_epoch, table_id, lkey,
            rkey, count);
        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);

        size_t record_size = sch.get_record_size(table_id);
        auto& rs_table = rs.get_table(table_id);
        auto& ws_table = ws.get_table(table_id);
        auto& vs_table = vs.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        typename Index::KVMap kv_map;

        typename Index::Result res;
        if (reverse) {
            res = idx.get_kv_in_rev_range(table_id, lkey, rkey, count, kv_map, nm);
        } else {
            res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map, nm);
        }
        if (res == Index::Result::BAD_SCAN) return false;

        for (auto& [key, val]: kv_map) {
            auto rs_iter = rs_table.find(key);
            auto ws_iter = ws_table.find(key);

            // do read
            if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
                if (val == nullptr) return false;  // abort

                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                read_record_and_tidword(*val, rec, tw, record_size);
                if (!(tw.absent == 0 && tw.latest == 1)) {  // unable to read the value
                    MemoryAllocator::deallocate(rec);
                    return false;
                }
                // 3. place it in readset
                rs_table.emplace_hint(
                    rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw));

                // 4. place it in validationset
                vs_table[key] = val;

                kr_map.emplace(key, rec);
                continue;
            } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
                // key is not in readset but is in writeset
                Rec* rec = ws_iter->second.rec;
                TidWord tw;
                // copy tidword from shared memory
                read_tidword(*val, tw);
                if (!(tw.absent == 0 && tw.latest == 1)) return false;  // unable to read the value
                rs_table.emplace_hint(
                    rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw));
                kr_map.emplace(key, rec);
                continue;
            } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
                kr_map.emplace(key, rs_iter->second.rec);
                continue;
            } else if (
                rs_iter != rs_table.end() && ws_iter != ws_table.end()
                && ws_iter->second.wt == WriteType::DELETE) {
                return false;  // abort
            } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
                kr_map.emplace(key, rs_iter->second.rec);
                continue;
            }
            assert(false);
            return false;
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
        auto& rs_table = rs.get_table(table_id);
        auto& ws_table = ws.get_table(table_id);
        auto& vs_table = vs.get_table(table_id);
        auto& nm = ns.get_nodemap(table_id);

        typename Index::KVMap kv_map;

        typename Index::Result res;
        if (reverse) {
            res = idx.get_kv_in_rev_range(table_id, lkey, rkey, count, kv_map, nm);
        } else {
            res = idx.get_kv_in_range(table_id, lkey, rkey, count, kv_map, nm);
        }
        if (res == Index::Result::BAD_SCAN) return false;

        for (auto& [key, val]: kv_map) {
            auto rs_iter = rs_table.find(key);
            auto ws_iter = ws_table.find(key);

            // do update
            if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
                if (val == nullptr) return false;
                Rec* rec = MemoryAllocator::aligned_allocate(record_size);
                TidWord tw;
                read_record_and_tidword(*val, rec, tw, record_size);
                if (!(tw.absent == 0 && tw.latest == 1)) {  // unable to read the value
                    MemoryAllocator::deallocate(rec);
                    return false;
                }
                rs_table.emplace_hint(
                    rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, tw));

                ws_table.emplace_hint(
                    ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, WriteType::UPDATE, false));

                vs_table.emplace(key, val);
                kr_map.emplace(key, rec);
                continue;
            } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
                assert(vs_table.find(key) != vs_table.end());
                kr_map.emplace(key, ws_iter->second.rec);
                continue;
            } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
                assert(vs_table.find(key) != vs_table.end());
                Rec* rec = rs_iter->second.rec;
                ws_table.emplace_hint(
                    ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(rec, WriteType::UPDATE, false));
                kr_map.emplace(key, rec);
                continue;
            } else if (
                rs_iter != rs_table.end() && ws_iter != ws_table.end()
                && ws_iter->second.wt == WriteType::DELETE) {
                assert(vs_table.find(key) != vs_table.end());
                return false;  // abort
            } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
                assert(vs_table.find(key) != vs_table.end());
                kr_map.emplace(key, ws_iter->second.rec);
                continue;
            }
            assert(false);
            return false;
        }
        return true;
    }

    Rec* remove(TableID table_id, Key key) {
        LOG_INFO("REMOVE (e: %u, t: %lu, k: %lu)", starting_epoch, table_id, key);

        const Schema& sch = Schema::get_schema();
        Index& idx = Index::get_index();

        tables.insert(table_id);
        size_t record_size = sch.get_record_size(table_id);
        auto& rs_table = rs.get_table(table_id);
        auto rs_iter = rs_table.find(key);
        auto& ws_table = ws.get_table(table_id);
        auto ws_iter = ws_table.find(key);
        auto& vs_table = vs.get_table(table_id);

        if (rs_iter == rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) == vs_table.end());
            Value* val;
            typename Index::Result res = idx.find(table_id, key, val);
            if (res == Index::Result::NOT_FOUND) return nullptr;

            Rec* rec = MemoryAllocator::aligned_allocate(record_size);
            TidWord tw;
            read_record_and_tidword(*val, rec, tw, record_size);
            if (!(tw.absent == 0 && tw.latest == 1)) {  // unable to read the value
                MemoryAllocator::deallocate(rec);
                return nullptr;
            }

            rs_table.emplace_hint(
                rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, tw));
            ws_table.emplace_hint(
                ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, WriteType::DELETE, false));
            vs_table.emplace(key, val);
            return rec;
        } else if (rs_iter == rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            Value* val = vs_table.at(key);
            TidWord tw;
            read_tidword(*val, tw);
            assert(tw.absent == 1 && tw.latest == 1);  // Previous operation should be INSERT
            rs_table.emplace_hint(
                rs_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(ws_iter->second.rec, tw));
            ws_iter->second.wt = WriteType::DELETE;
            return ws_iter->second.rec;
        } else if (rs_iter != rs_table.end() && ws_iter == ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            Rec* rec = rs_iter->second.rec;
            ws_table.emplace_hint(
                ws_iter, std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(rec, WriteType::DELETE, false));
            return rec;
        } else if (
            rs_iter != rs_table.end() && ws_iter != ws_table.end()
            && ws_iter->second.wt == WriteType::DELETE) {
            assert(vs_table.find(key) != vs_table.end());
            return nullptr;  // abort
        } else if (rs_iter != rs_table.end() && ws_iter != ws_table.end()) {
            assert(vs_table.find(key) != vs_table.end());
            ws_iter->second.wt = WriteType::DELETE;
            return ws_iter->second.rec;
        }
        assert(false);
        return nullptr;
    }

    bool precommit() {
        LOG_INFO("PRECOMMIT, e: %u", starting_epoch);

        Index& idx = Index::get_index();

        TidWord commit_tw;
        commit_tw.obj = 0;

        LOG_INFO("  P1 (Lock WriteSet)");

        for (TableID table_id: tables) {
            auto& ws_table = ws.get_table(table_id);
            auto& vs_table = vs.get_table(table_id);
            for (auto w_iter = ws_table.begin(); w_iter != ws_table.end(); ++w_iter) {
                auto v_iter = vs_table.find(w_iter->first);
                assert(v_iter != vs_table.end());
                LOG_DEBUG("     LOCK (t: %lu, k: %lu)", table_id, w_iter->first);
                lock(*(v_iter->second));
                TidWord current;
                current.obj = load_acquire(v_iter->second->tidword.obj);

                if (!w_iter->second.is_new && !is_readable(current)) {
                    LOG_DEBUG("     UNREADABLE (t: %lu, k: %lu)", table_id, w_iter->first);
                    ++w_iter;
                    unlock_writeset(table_id, w_iter->first);
                    return false;
                }
                if (commit_tw.tid < current.tid) commit_tw.tid = current.tid;
            }
        }

        commit_tw.epoch = load_acquire(EpochManager<Silo>::get_global_epoch());
        LOG_INFO("  SERIAL POINT (se: %u, ce: %u)", starting_epoch, commit_tw.epoch);

        // Phase 2.1 (Validate ReadSet)
        LOG_INFO("  P2.1 (Validate ReadSet)");
        for (TableID table_id: tables) {
            auto& rs_table = rs.get_table(table_id);
            auto& ws_table = ws.get_table(table_id);
            auto& vs_table = vs.get_table(table_id);
            for (auto r_iter = rs_table.begin(); r_iter != rs_table.end(); ++r_iter) {
                auto v_iter = vs_table.find(r_iter->first);
                assert(v_iter != vs_table.end());
                TidWord current, expected;
                current.obj = load_acquire(v_iter->second->tidword.obj);
                expected.obj = r_iter->second.tw.obj;
                if (!is_valid(current, expected)
                    || (current.lock && ws_table.find(r_iter->first) == ws_table.end())) {
                    unlock_writeset();
                    return false;
                }
                if (commit_tw.tid < current.tid) commit_tw.tid = current.tid;
            }
        }

        // Generate tid that is bigger than that of read/writeset
        commit_tw.tid++;
        LOG_INFO("  COMMIT TID: %u", commit_tw.tid);

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
            auto& ws_table = ws.get_table(table_id);
            auto& vs_table = vs.get_table(table_id);
            for (auto w_iter = ws_table.begin(); w_iter != ws_table.end(); w_iter++) {
                auto v_iter = vs_table.find(w_iter->first);
                assert(v_iter != vs_table.end());
                Rec* old = exchange(v_iter->second->rec, w_iter->second.rec);
                TidWord new_tw;
                new_tw.epoch = commit_tw.epoch;
                new_tw.tid = commit_tw.tid;
                new_tw.latest = !(w_iter->second.wt == WriteType::DELETE);
                new_tw.absent = (w_iter->second.wt == WriteType::DELETE);
                new_tw.lock = 0;  // unlock
                store_release(v_iter->second->tidword.obj, new_tw.obj);
                GarbageCollector::collect(commit_tw.epoch, old);
                if (w_iter->second.wt == WriteType::DELETE) {
                    idx.remove(table_id, w_iter->first);
                    GarbageCollector::collect(commit_tw.epoch, load_acquire(v_iter->second->rec));
                    GarbageCollector::collect(commit_tw.epoch, load_acquire(v_iter->second));
                }
            }
        }

        LOG_INFO("PRECOMMIT SUCCESS");
        return true;
    }

    void abort() {
        Index& idx = Index::get_index();

        for (TableID table_id: tables) {
            auto& rs_table = rs.get_table(table_id);
            auto& ws_table = ws.get_table(table_id);
            auto& vs_table = vs.get_table(table_id);
            auto& nm = ns.get_nodemap(table_id);

            for (auto w_iter = ws_table.begin(); w_iter != ws_table.end(); w_iter++) {
                // For failed inserts
                if (w_iter->second.is_new) {
                    auto v_iter = vs_table.find(w_iter->first);
                    assert(v_iter != vs_table.end());
                    lock(*(v_iter->second));
                    TidWord tw;
                    tw.obj = load_acquire(v_iter->second->tidword.obj);
                    tw.absent = 1;
                    tw.latest = 0;
                    tw.lock = 0;
                    store_release(v_iter->second->tidword.obj, tw.obj);
                    idx.remove(table_id, w_iter->first);
                    GarbageCollector::collect(starting_epoch, load_acquire(v_iter->second->rec));
                    GarbageCollector::collect(starting_epoch, load_acquire(v_iter->second));
                }
                GarbageCollector::collect(starting_epoch, w_iter->second.rec);
            }

            for (auto r_iter = rs_table.begin(); r_iter != rs_table.end(); ++r_iter) {
                // Deallocate ReadSet that is not in WriteSet
                if (ws_table.find(r_iter->first) == ws_table.end())
                    GarbageCollector::collect(starting_epoch, r_iter->second.rec);
            }

            ws_table.clear();
            rs_table.clear();
            vs_table.clear();
            nm.clear();
        }
    }

private:
    uint32_t starting_epoch;
    std::set<TableID> tables;
    ReadSet rs;
    WriteSet ws;
    ValidationSet vs;
    NodeSet ns;

    void read_record_and_tidword(Value& val, Rec* rec, TidWord& tw, size_t rec_size) {
        TidWord expected, check;
        if (rec == nullptr) throw std::runtime_error("memory not allocated");
        expected.obj = load_acquire(val.tidword.obj);
        while (true) {
            while (expected.lock) {
                expected.obj = load_acquire(val.tidword.obj);
            }
            if (val.rec) memcpy(rec, val.rec, rec_size);
            check.obj = load_acquire(val.tidword.obj);
            if (expected.obj == check.obj) {
                tw.obj = check.obj;
                return;
            }
            expected.obj = check.obj;
        }
    }

    void read_tidword(Value& val, TidWord& tw) {
        TidWord expected, check;
        expected.obj = load_acquire(val.tidword.obj);
        while (true) {
            while (expected.lock) {
                expected.obj = load_acquire(val.tidword.obj);
            }
            // memcpy(rec, val.rec, rec_size);
            check.obj = load_acquire(val.tidword.obj);
            if (expected.obj == check.obj) {
                tw.obj = check.obj;
                return;
            }
            expected.obj = check.obj;
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
            auto& ws_table = ws.get_table(table_id);
            auto& vs_table = vs.get_table(table_id);
            for (auto w_iter = ws_table.begin(); w_iter != ws_table.end(); ++w_iter) {
                if (table_id == end_table_id && w_iter->first == end_key) return;
                LOG_DEBUG("UNLOCK (t: %lu, k: %lu)", table_id, w_iter->first);
                auto v_iter = vs_table.find(w_iter->first);
                assert(v_iter != vs_table.end());
                unlock(*(v_iter->second));
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

    bool is_readable(const TidWord& tw) { return !tw.absent && tw.latest; }

    bool is_valid(const TidWord& current, const TidWord& expected) {
        return current.epoch == expected.epoch && current.tid == expected.tid
            && is_readable(current);
    }
};