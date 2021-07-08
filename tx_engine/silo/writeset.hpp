
#include "database.hpp"
#include "logger.hpp"
#include "memory_manager.hpp"
#include "record_layout.hpp"
#include "record_with_header.hpp"

enum LogType { INSERT, UPDATE, DELETE };

template <typename Record>
struct LogRecord {
    LogRecord() = default;
    LogRecord(LogType lt, RecordWithHeader<Record>* rec_ptr)
        : lt(lt)
        , rec_ptr(rec_ptr) {}
    LogType lt;
    RecordWithHeader<Record>* rec_ptr;
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
    RecordWithHeader<Record>* apply_update_to_writeset(typename Record::Key rec_key) {
        // search into writeset
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT: return current_logrec_ptr->rec_ptr;
            case UPDATE: return current_logrec_ptr->rec_ptr;
            case DELETE: throw std::runtime_error("Record already deleted");
            default: throw std::runtime_error("Invalid LogType");
            }
        }

        const RecordWithHeader<Record>* rec_ptr_in_db = nullptr;
        typename MasstreeIndex<Record>::NodeInfo ni;
        // search into database
        if (db.get_record<Record>(rec_ptr_in_db, rec_key, ni)) {
            assert(rec_ptr_in_db != nullptr);
            // allocate memory in writeset
            RecordWithHeader<Record>* rec_ptr_in_ws =
                create_logrecord(LogType::UPDATE, rec_key, MemoryManager::allocate<Record>());
            // copy from database
            rec_ptr_in_ws->rec.deep_copy_from(rec_ptr_in_db->rec);
            return rec_ptr_in_ws;
        } else {
            throw std::runtime_error("No record to update in db");
        }
    }


    RecordWithHeader<History>* apply_insert_to_writeset() {
        typename RecordToWS<History>::WS& ws = get_ws<History>();
        ws.emplace_back(LogType::INSERT, MemoryManager::allocate<History>());
        return ws.back().rec_ptr;
    }

    template <typename Record>
    RecordWithHeader<Record>* apply_insert_to_writeset(typename Record::Key rec_key) {
        // Search into writeset
        LogRecord<Record>* current_logrec_ptr = lookup_logrecord<Record>(rec_key);
        if (current_logrec_ptr) {
            switch (current_logrec_ptr->lt) {
            case INSERT: throw std::runtime_error("Record already inserted");
            case UPDATE: throw std::runtime_error("Record already updated");
            case DELETE:
                current_logrec_ptr->lt = LogType::UPDATE;
                return current_logrec_ptr->rec_ptr;
            default: throw std::runtime_error("Invalid LogType");
            }
        }

        // Search into db
        const RecordWithHeader<Record>* rec_ptr_in_db;
        typename MasstreeIndex<Record>::NodeInfo ni;
        if (!db.get_record<Record>(rec_ptr_in_db, rec_key, ni)) {
            // allocate memory in writeset
            return create_logrecord(LogType::INSERT, rec_key, MemoryManager::allocate<Record>());
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
        const RecordWithHeader<Record>* rec_ptr_in_db;
        typename MasstreeIndex<Record>::NodeInfo ni;
        if (db.get_record<Record>(rec_ptr_in_db, rec_key, ni)) {
            create_logrecord(LogType::DELETE, rec_key, MemoryManager::allocate<Record>());
            return true;
        } else {
            throw std::runtime_error("Record not found in db");
        }
    }

    // definition moved to end to make template specialization work
    bool apply_to_database();

    // definition moved to end to make template specialization work
    void clear_all();

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
        auto it = ws.find(rec_key);
        return (it == ws.end() ? nullptr : &(it->second));
    }

    template <typename Record>
    RecordWithHeader<Record>* create_logrecord(
        LogType lt, typename Record::Key rec_key, RecordWithHeader<Record>* rec_ptr) {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        auto [it, ret] = ws.try_emplace(rec_key);
        assert(ret);
        LogRecord<Record>& lr = it->second;
        lr.lt = lt;
        lr.rec_ptr = rec_ptr;
        return rec_ptr;
    }

    template <typename Record>
    void remove_logrecord_with_logtype(typename Record::Key rec_key, LogType lt) {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        auto it = ws.find(rec_key);
        assert(it != ws.end());
        if (it->second.lt == lt) {
            MemoryManager::deallocate<Record>(it->second.rec_ptr);
            ws.erase(rec_key);
        }
    }

    template <typename Record>
    void apply_writeset_to_database() {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        typename MasstreeIndex<Record>::NodeInfo ni;
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            switch (it->second.lt) {
            case LogType::INSERT:
                db.insert_record<Record>(it->first, it->second.rec_ptr, ni);
                break;
            case LogType::UPDATE:
                db.update_record<Record>(it->first, it->second.rec_ptr, ni);
                break;
            case LogType::DELETE:
                db.delete_record<Record>(it->first, ni);
                MemoryManager::deallocate<Record>(it->second.rec_ptr);
                break;
            }
        }
        ws.clear();
    }

    template <typename Record>
    void clear_writeset() {
        typename RecordToWS<Record>::WS& ws = get_ws<Record>();
        for (auto it = ws.begin(); it != ws.end(); ++it) {
            if (it->second.rec_ptr)
                MemoryManager::deallocate<Record>(it->second.rec_ptr);
            else
                throw std::runtime_error("Null poitner found in writeset");
        }
        ws.clear();
    }
};

template <>
inline void WriteSet::apply_writeset_to_database<History>() {
    RecordToWS<History>::WS& ws = get_ws<History>();
    for (auto it = ws.begin(); it != ws.end(); ++it) db.insert_record(it->rec_ptr);
    ws.clear();
}

template <>
inline void WriteSet::clear_writeset<History>() {
    RecordToWS<History>::WS& ws = get_ws<History>();
    for (auto it = ws.begin(); it != ws.end(); ++it) {
        if (it->rec_ptr) MemoryManager::deallocate<History>(it->rec_ptr);
        throw std::runtime_error("Null pointer found in writeset");
    }
    ws.clear();
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

inline void WriteSet::clear_all() {
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
