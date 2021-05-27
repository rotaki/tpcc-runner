#include "database.hpp"
#include "logger.hpp"
#include "record_layout.hpp"

enum LogType { INSERT, UPDATE, DELETE };

template <typename Record>
struct LogRecord {
    LogType lt;
    Record rec;
    LogRecord() = default;
};

template <typename Record>
struct RecordToWS {
    using WS = std::map<typename Record::Key, LogRecord<Record>>;
};

template <>
struct RecordToWS<History> {
    using WS = std::deque<LogRecord<History>>;
};

struct WriteSet {
    Database& db;

    WriteSet(Database& db)
        : db(db) {}

    template <typename Record>
    typename RecordToWS<Record>::WS& get_ws() {
        if constexpr (std::is_same<Record, Item>::value) {
            return items;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            return warehouses;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            return stocks;
        } else if constexpr (std::is_same<Record, District>::value) {
            return districts;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            return customers;
        } else if constexpr (std::is_same<Record, History>::value) {
            return histories;
        } else if constexpr (std::is_same<Record, Order>::value) {
            return orders;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            return neworders;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            return orderlines;
        } else {
            assert(false);
        }
    }

    template <typename Record>
    LogRecord<Record>* lookup_logrecord(typename Record::Key rec_key) {
        typename RecordToWS<Record>::WS& t = get_ws<Record>();
        if (t.find(rec_key) == t.end()) {
            return nullptr;
        } else {
            return &(t[rec_key]);
        }
    }

    template <
        typename Record,
        typename std::enable_if<std::is_same<Record, History>::value>::type* = nullptr>
    bool insert_logrecord(LogType lt, const Record* rec) {
        assert(lt == LogType::INSERT);
        typename RecordToWS<Record>::WS& t = get_ws<Record>();
        t.emplace_back();
        t.back().lt = lt;
        t.back().rec.deep_copy_from(*rec);
    }

    template <typename Record>
    bool insert_logrecord(LogType lt, typename Record::Key rec_key, const Record* rec_ptr) {
        switch (lt) {
        case LogType::INSERT:
            assert(rec_ptr != nullptr);
            return apply_insert_to_writeset(rec_key, *rec_ptr);
        case LogType::UPDATE:
            assert(rec_ptr != nullptr);
            return apply_update_to_writeset(rec_key, *rec_ptr);
        case LogType::DELETE:
            assert(rec_ptr == nullptr);
            return apply_delete_to_writeset<Record>(rec_key);
        default: assert(false);
        }
    }

    bool apply_to_database();

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
    RecordToWS<Item>::WS items;
    RecordToWS<Warehouse>::WS warehouses;
    RecordToWS<Stock>::WS stocks;
    RecordToWS<District>::WS districts;
    RecordToWS<Customer>::WS customers;
    RecordToWS<History>::WS histories;
    RecordToWS<Order>::WS orders;
    RecordToWS<NewOrder>::WS neworders;
    RecordToWS<OrderLine>::WS orderlines;

    template <typename Record>
    bool apply_insert_to_writeset(typename Record::Key rec_key, const Record& rec) {
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT: return false;
            case UPDATE: return false;
            case DELETE:
                current_logrec_ptr->lt = LogType::UPDATE;
                current_logrec_ptr->rec.deep_copy_from(rec);
                return true;
            default: assert(false);
            }
        }

        if (!db.lookup_record<Record>(rec_key)) {
            create_logrecord(LogType::INSERT, rec_key, &rec);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool apply_update_to_writeset(typename Record::Key rec_key, const Record& rec) {
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT: current_logrec_ptr->rec.deep_copy_from(rec); return true;
            case UPDATE: current_logrec_ptr->rec.deep_copy_from(rec); return true;
            case DELETE: return false;
            default: assert(false);
            }
        }

        if (db.lookup_record<Record>(rec_key)) {
            create_logrecord(LogType::UPDATE, rec_key, &rec);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool apply_delete_to_writeset(typename Record::Key rec_key) {
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT:
                remove_logrecord_with_logtype<Record>(rec_key, LogType::INSERT);
                return true;
            case UPDATE: current_logrec_ptr->lt = LogType::DELETE; return true;
            case DELETE: return false;
            default: assert(false);
            }
        }

        if (db.lookup_record<Record>(rec_key)) {
            Record* rec_ptr = nullptr;
            create_logrecord(LogType::DELETE, rec_key, rec_ptr);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    void create_logrecord(LogType lt, typename Record::Key rec_key, const Record* rec_ptr) {
        typename RecordToWS<Record>::WS& t = get_ws<Record>();
        if (rec_ptr) {
            assert(lt == LogType::INSERT || lt == LogType::UPDATE);
            t[rec_key].lt = lt;
            t[rec_key].rec.deep_copy_from(*rec_ptr);
        } else {
            assert(lt == LogType::DELETE);
            t[rec_key].lt = lt;
        }
    }

    template <typename Record>
    void remove_logrecord_with_logtype(typename Record::Key rec_key, LogType lt) {
        typename RecordToWS<Record>::WS& t = get_ws<Record>();
        if (t[rec_key].lt == lt) {
            t.erase(rec_key);
        }
    }

    template <typename Record>
    void clear_writeset() {
        typename RecordToWS<Record>::WS& t = get_ws<Record>();
        t.clear();
    }

    template <typename Record>
    void apply_writeset_to_database() {
        typename RecordToWS<Record>::WS& t = get_ws<Record>();
        for (const auto& [rec_key, logrecord]: t) {
            switch (logrecord.lt) {
            case LogType::INSERT: db.insert_record<Record>(logrecord.rec); break;
            case LogType::UPDATE: db.update_record<Record>(rec_key, logrecord.rec); break;
            case LogType::DELETE: db.delete_record<Record>(rec_key); break;
            default: LOG_TRACE("%d", static_cast<int>(logrecord.lt)); assert(false);
            }
        }
    }
};

template <>
inline void WriteSet::apply_writeset_to_database<History>() {
    RecordToWS<History>::WS& t = get_ws<History>();
    for (const auto& logrecord: t) {
        switch (logrecord.lt) {
        case INSERT: db.insert_record<History>(logrecord.rec); break;
        default: assert(false);
        }
    }
}

inline bool WriteSet::apply_to_database() {
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