#include "cache.hpp"
#include "database.hpp"
#include "logger.hpp"
#include "record_layout.hpp"

enum LogType { INSERT, UPDATE, DELETE };

template <typename Record>
struct LogRecord {
    LogRecord() = default;
    LogRecord(LogType lt, std::unique_ptr<Record> rec_ptr)
        : lt(lt)
        , rec_ptr(std::move(rec_ptr)) {}
    LogType lt;
    std::unique_ptr<Record> rec_ptr;
};

template <typename Record>
struct RecordToWS {
    using WS = std::map<typename Record::Key, LogRecord<Record>>;
};

template <>
struct RecordToWS<History> {
    using WS = std::deque<LogRecord<History>>;
};


class WriteSet {
public:
    WriteSet(Database& db)
        : db(db) {}

    ~WriteSet() { clear_all(); }

    template <typename Record>
    typename RecordToWS<Record>::WS& get_ws() {
        if constexpr (std::is_same<Record, Item>::value) {
            return ws_i;
        } else if constexpr (std::is_same<Record, Warehouse>::value) {
            return ws_w;
        } else if constexpr (std::is_same<Record, Stock>::value) {
            return ws_s;
        } else if constexpr (std::is_same<Record, District>::value) {
            return ws_d;
        } else if constexpr (std::is_same<Record, Customer>::value) {
            return ws_c;
        } else if constexpr (std::is_same<Record, History>::value) {
            return ws_h;
        } else if constexpr (std::is_same<Record, Order>::value) {
            return ws_o;
        } else if constexpr (std::is_same<Record, NewOrder>::value) {
            return ws_no;
        } else if constexpr (std::is_same<Record, OrderLine>::value) {
            return ws_ol;
        } else {
            throw std::runtime_error("Undefined Record");
        }
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
        if (db.get_record<Record>(rec_ptr_in_db, rec_key)) {
            // allocate memory in writeset
            Record* rec_ptr_in_ws =
                create_logrecord(LogType::UPDATE, rec_key, std::move(Cache::allocate<Record>()));
            // copy from database
            rec_ptr_in_ws->deep_copy_from(*rec_ptr_in_db);
            return rec_ptr_in_ws;
        } else {
            throw std::runtime_error("No record to update in db");
        }
    }

    template <IsHistory Record>
    Record* apply_insert_to_writeset() {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
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

        // Search into db
        const Record* rec_ptr_in_db;
        if (!db.get_record<Record>(rec_ptr_in_db, rec_key)) {
            // allocate memory in writeset
            return create_logrecord(LogType::INSERT, rec_key, std::move(Cache::allocate<Record>()));
        } else {
            throw std::runtime_error("Record already exists in db");
        }
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
        if (db.get_record<Record>(rec_ptr_in_db, rec_key)) {
            create_logrecord(LogType::DELETE, rec_key, std::move(Cache::allocate<Record>()));
            return true;
        } else {
            throw std::runtime_error("Record not found in db");
        }
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

    RecordToWS<Item>::WS ws_i;
    RecordToWS<Warehouse>::WS ws_w;
    RecordToWS<Stock>::WS ws_s;
    RecordToWS<District>::WS ws_d;
    RecordToWS<Customer>::WS ws_c;
    RecordToWS<History>::WS ws_h;
    RecordToWS<Order>::WS ws_o;
    RecordToWS<NewOrder>::WS ws_no;
    RecordToWS<OrderLine>::WS ws_ol;

    template <typename Record>
    LogRecord<Record>* lookup_logrecord(typename Record::Key rec_key) {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        if (ws.find(rec_key) == ws.end()) {
            return nullptr;
        } else {
            return &(ws[rec_key]);
        }
    }

    template <typename Record>
    Record* create_logrecord(
        LogType lt, typename Record::Key rec_key, std::unique_ptr<Record> rec_ptr) {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        ws[rec_key].lt = lt;
        ws[rec_key].rec_ptr = std::move(rec_ptr);
        return ws[rec_key].rec_ptr.get();
    }

    template <typename Record>
    void remove_logrecord_with_logtype(typename Record::Key rec_key, LogType lt) {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        if (ws[rec_key].lt == lt) {
            Cache::deallocate<Record>(std::move(ws[rec_key].rec_ptr));
            ws.erase(rec_key);
        }
    }

    template <IsHistory Record>
    void apply_writeset_to_database() {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            db.insert_record<Record>(std::move(it->rec_ptr));
        }
        ws.clear();
    }

    template <typename Record>
    void apply_writeset_to_database() {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            switch (it->second.lt) {
            case LogType::INSERT:
                db.insert_record<Record>(it->first, std::move(it->second.rec_ptr));
                break;
            case LogType::UPDATE:
                db.update_record<Record>(it->first, std::move(it->second.rec_ptr));
                break;
            case LogType::DELETE:
                db.delete_record<Record>(it->first);
                Cache::deallocate<Record>(std::move(it->second.rec_ptr));
                break;
            }
        }
        ws.clear();
    }

    template <IsHistory Record>
    void clear_writeset() {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
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
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
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
