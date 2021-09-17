#pragma once
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "protocols/naive/include/cache.hpp"
#include "protocols/naive/include/database.hpp"
#include "protocols/naive/include/type_tuple.hpp"
#include "utils/logger.hpp"


enum LogType { INSERT, UPDATE, DELETE };

template <typename Record>
struct LogRecord {
    LogRecord() = default;
    LogRecord(LogType lt, std::unique_ptr<Record> rec_ptr)
        : lt(lt)
        , rec_ptr(std::move(rec_ptr))
        , it() {}
    LogType lt;
    std::unique_ptr<Record> rec_ptr;

    using Iterator = typename RecordToIterator<Record>::type;
    Iterator it;
};

template <typename Record>
struct RecordToWS {
#if 1  // std::map seems faster.
    using type = std::map<typename Record::Key, LogRecord<Record>>;
#else
    using type =
        std::unordered_map<typename Record::Key, LogRecord<Record>, Hash<typename Record::Key>>;
#endif
};

template <>
struct RecordToWS<History> {
    using type = std::deque<LogRecord<History>>;
};


class WriteSet {
public:
    explicit WriteSet(Database& db)
        : db(db)
        , ws_tuple() {}

    ~WriteSet() { clear_all(); }

    template <typename Record>
    typename RecordToWS<Record>::type& get_ws() {
        return get<typename RecordToWS<Record>::type>(ws_tuple);
    }

    template <typename Record>
    Record* apply_update_to_writeset(typename Record::Key rec_key) {
        // search into writeset
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT: return current_logrec_ptr->rec_ptr.get();
            case UPDATE: return current_logrec_ptr->rec_ptr.get();
            case DELETE: throw std::runtime_error("Record already deleted");
            default: throw std::runtime_error("Invalid LogType");
            }
        }

        const Record* rec_ptr_in_db;
        // search into database
        auto it = db.get_record<Record>(rec_ptr_in_db, rec_key);
        if (rec_ptr_in_db == nullptr) {
            throw std::runtime_error("No record to update in db");
        }
        // allocate memory in writeset
        LogRecord<Record>& lr =
            create_logrecord(LogType::UPDATE, rec_key, std::move(Cache::allocate<Record>()));
        Record* rec_ptr_in_ws = lr.rec_ptr.get();
        // copy from database
        rec_ptr_in_ws->deep_copy_from(*rec_ptr_in_db);
        lr.it = it;
        return rec_ptr_in_ws;
    }

    template <IsHistory Record>
    Record* apply_insert_to_writeset() {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        ws.emplace_back(LogType::INSERT, std::move(Cache::allocate<Record>()));
        return ws.back().rec_ptr.get();
    }

    template <typename Record>
    Record* apply_insert_to_writeset(typename Record::Key rec_key) {
        // Search into writeset
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT: throw std::runtime_error("Record already inserted");
            case UPDATE: throw std::runtime_error("Record already updated");
            case DELETE:
                current_logrec_ptr->lt = LogType::UPDATE;
                return current_logrec_ptr->rec_ptr.get();
            default: throw std::runtime_error("Invalid LogType");
            }
        }

#if 0  // Existance check is skipped.
       // Search into db
        const Record* rec_ptr_in_db;
        db.get_record<Record>(rec_ptr_in_db, rec_key);
        if (rec_ptr_in_db != nullptr) {
            throw std::runtime_error("Record already exists in db");
        }
#endif
        // allocate memory in writeset
        LogRecord<Record>& lr =
            create_logrecord(LogType::INSERT, rec_key, std::move(Cache::allocate<Record>()));
        return lr.rec_ptr.get();
    }

    template <typename Record>
    bool apply_delete_to_writeset(typename Record::Key rec_key) {
        // search into writeset
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT:
                remove_logrecord_with_logtype<Record>(rec_key, LogType::INSERT);
                return true;
            case UPDATE: current_logrec_ptr->lt = LogType::DELETE; return true;
            case DELETE: throw std::runtime_error("Record already deleted");
            default: throw std::runtime_error("Invalid LogType");
            }
        }

        // Search into db
        const Record* rec_ptr_in_db;
        auto it = db.get_record<Record>(rec_ptr_in_db, rec_key);
        if (rec_ptr_in_db == nullptr) {
            throw std::runtime_error("Record not found in db");
        }
        LogRecord<Record>& lr =
            create_logrecord(LogType::DELETE, rec_key, std::move(Cache::allocate<Record>()));
        lr.it = it;
        return true;
    }

    bool apply_to_database() {
        apply_writeset_to_database<Item>();
        apply_writeset_to_database<Warehouse>();
        apply_writeset_to_database<Stock>();
        apply_writeset_to_database<District>();
        apply_writeset_to_database<Customer>();
        apply_writeset_to_database<History>();
        apply_writeset_to_database<Order>();
        apply_writeset_to_database<NewOrder>();
        apply_writeset_to_database<OrderLine>();
        return true;
    }

    void clear_all() {
        clear_writeset<Item>();
        clear_writeset<Warehouse>();
        clear_writeset<Stock>();
        clear_writeset<District>();
        clear_writeset<Customer>();
        clear_writeset<History>();
        clear_writeset<Order>();
        clear_writeset<NewOrder>();
        clear_writeset<OrderLine>();
    }

private:
    Database& db;

    using TT = TypeTuple<
        RecordToWS<Item>::type, RecordToWS<Warehouse>::type, RecordToWS<Stock>::type,
        RecordToWS<District>::type, RecordToWS<Customer>::type, RecordToWS<History>::type,
        RecordToWS<Order>::type, RecordToWS<NewOrder>::type, RecordToWS<OrderLine>::type>;
    TT ws_tuple;

    template <typename Record>
    LogRecord<Record>* lookup_logrecord(typename Record::Key rec_key) {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        auto it = ws.find(rec_key);
        if (it == ws.end()) return nullptr;
        return &it->second;
    }

    template <typename Record>
    LogRecord<Record>& create_logrecord(
        LogType lt, typename Record::Key rec_key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        auto [it, ret] = ws.try_emplace(rec_key);
        assert(ret);
        LogRecord<Record>& lr = it->second;
        lr.lt = lt;
        lr.rec_ptr = std::move(rec_ptr);
        return lr;
    }

    template <typename Record>
    void remove_logrecord_with_logtype(typename Record::Key rec_key, LogType lt) {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        auto it = ws.find(rec_key);
        assert(it != ws.end());
        if (it->second.lt == lt) {
            Cache::deallocate<Record>(std::move(it->second.rec_ptr));
            ws.erase(it);
        }
    }

    template <IsHistory Record>
    void apply_writeset_to_database() {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            db.insert_record<Record>(std::move(it->rec_ptr));
        }
        ws.clear();
    }

    template <typename Record>
    void apply_writeset_to_database() {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            const typename Record::Key& key = it->first;
            LogRecord<Record>& lr = it->second;

            switch (lr.lt) {
            case LogType::INSERT: db.insert_record<Record>(key, std::move(lr.rec_ptr)); break;
            case LogType::UPDATE: db.update_record<Record>(lr.it, std::move(lr.rec_ptr)); break;
            case LogType::DELETE:
                db.delete_record<Record>(lr.it);
                Cache::deallocate<Record>(std::move(it->second.rec_ptr));
                break;
            }
        }
        ws.clear();
    }

    template <IsHistory Record>
    void clear_writeset() {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            if (it->rec_ptr) {
                Cache::deallocate<Record>(std::move(it->rec_ptr));
            } else {
                throw std::runtime_error("Null pointer found in writeset");
            }
        }
        ws.clear();
    }

    template <typename Record>
    void clear_writeset() {
        typename RecordToWS<Record>::type& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            if (it->second.rec_ptr) {
                Cache::deallocate<Record>(std::move(it->second.rec_ptr));
            } else {
                throw std::runtime_error("Null poitner found in writeset");
            }
        }
        ws.clear();
    }
};
