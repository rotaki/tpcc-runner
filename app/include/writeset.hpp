#include "database.hpp"
#include "record_layout.hpp"

enum LogType { INSERT, UPDATE, DELETE };

template <typename Record>
struct LogRecord {
    LogType lt;
    Record rec;
    LogRecord() = default;
    LogRecord(LogType lt, Record rec_in)
        : lt(lt) {
        rec.deep_copy_from(rec_in);
    }
};

template <typename Record>
struct LogRecordToWS {
    using WS = std::map<typename Record::Key, LogRecord<Record>>;
};

template <>
struct LogRecordToWS<History> {
    using WS = std::deque<LogRecord<History>>;
};

struct WriteSet {
    Database& db;

    WriteSet(Database& db)
        : db(db) {}

    template <typename Record>
    typename LogRecordToWS<Record>::WS& get_ws() {
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
    Record* lookup_logrecord(LogType& lt, typename Record::Key rec_key) {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        if (t.find(rec_key) == t.end()) {
            return nullptr;
        } else {
            lt = t[rec_key].lt;
            return &(t[rec_key].rec);
        }
    }

    template <typename Record>
    bool create_logrecord(LogType lt, const Record& rec) {
        assert(lt == LogType::INSERT || lt == LogType::UPDATE);
        switch (lt) {
        case LogType::INSERT: return apply_insert_to_writeset(rec);
        case LogType::UPDATE: return apply_update_to_writeset(rec);
        default: assert(false);
        }
    }

    template <typename Record>
    bool create_logrecord(LogType lt, typename Record::Key rec_key) {
        assert(lt == LogType::DELETE);
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        return apply_delete_to_writeset(rec_key);
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
    LogRecordToWS<Item>::WS items;
    LogRecordToWS<Warehouse>::WS warehouses;
    LogRecordToWS<Stock>::WS stocks;
    LogRecordToWS<District>::WS districts;
    LogRecordToWS<Customer>::WS customers;
    LogRecordToWS<History>::WS histories;
    LogRecordToWS<Order>::WS orders;
    LogRecordToWS<NewOrder>::WS neworders;
    LogRecordToWS<OrderLine>::WS orderlines;

    template <typename Record>
    bool apply_insert_to_writeset(const Record& rec) {
        LogType current_lt;
        typename Record::Key rec_key = Record::Key::create_key(rec);
        if (lookup_logrecord(current_lt, rec_key)) {
            switch (current_lt) {
            case INSERT: return false;
            case UPDATE: return false;
            case DELETE: create_update_logrecord(rec); return true;
            default: assert(false);
            }
        }

        if (!db.lookup_record(rec_key)) {
            create_insert_logrecord(rec);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool apply_update_to_writeset(const Record& rec) {
        LogType current_lt;
        typename Record::Key rec_key = Record::Key::create_key(rec);
        if (lookup_logrecord(current_lt, rec_key)) {
            switch (current_lt) {
            case INSERT: create_insert_logrecord(rec); return true;
            case UPDATE: create_update_logrecord(rec); return true;
            case DELETE: return false;
            default: assert(false);
            }
        }

        if (db.lookup_record(rec_key)) {
            create_update_logrecord(rec);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    bool apply_delete_to_writeset(typename Record::Key rec_key) {
        LogType current_lt;
        if (lookup_logrecord(current_lt, rec_key)) {
            switch (current_lt) {
            case INSERT: remove_insert_logrecord(rec_key); return true;
            case UPDATE: create_delete_logrecord(rec_key); return true;
            case DELETE: return false;  // return FAIL
            default: assert(false);
            }
        }

        if (db.lookup_record(rec_key)) {
            create_delete_logrecord(rec_key);
            return true;
        } else {
            return false;
        }
    }

    template <typename Record>
    void create_insert_logrecord(const Record& rec) {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        typename Record::Key rec_key = Record::Key::create_key(rec);
        t[rec_key].lt = LogType::INSERT;
        t[rec_key].rec.deep_copy_from(rec);
    }

    template <typename Record>
    void remove_insert_logrecord(const Record& rec) {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        typename Record::Key rec_key = Record::Key::create_key(rec);
        assert(t[rec_key].lt == LogType::INSERT);
        t.erase(rec_key);
    }

    template <typename Record>
    void create_update_logrecord(const Record& rec) {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        typename Record::Key rec_key = Record::Key::create_key(rec);
        t[rec_key].lt = LogType::UPDATE;
        t[rec_key].rec.deep_copy_from(rec);
    }

    template <typename Record>
    void create_delete_logrecord(typename Record::Key rec_key) {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        t[rec_key].lt = LogType::DELETE;
    }

    template <typename Record>
    void clear_writeset() {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        t.clear();
    }

    template <typename Record>
    void apply_writeset_to_database() {
        typename LogRecordToWS<Record>::WS& t = get_ws<Record>();
        for (const auto& [rec_key, logrecord]: t) {
            switch (logrecord.lt) {
            case INSERT: db.insert_record<Record>(logrecord.rec);
            case UPDATE: db.update_record<Record>(rec_key, logrecord.rec);
            case DELETE: db.delete_record<Record>(rec_key);
            default: assert(false);
            }
        }
    }
};

template <>
inline bool WriteSet::create_logrecord<History>(LogType lt, const History& rec) {
    assert(lt == LogType::INSERT);
    LogRecordToWS<History>::WS& t = get_ws<History>();
    t.emplace_back(lt, rec);
    return true;
}

template <>
inline void WriteSet::apply_writeset_to_database<History>() {
    LogRecordToWS<History>::WS& t = get_ws<History>();
    for (const auto& logrecord: t) {
        switch (logrecord.lt) {
        case INSERT: db.insert_record<History>(logrecord.rec);
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