#include "db_wrapper.hpp"

DBWrapper::DBWrapper() {}

DBWrapper::~DBWrapper() {}

DBWrapper& DBWrapper::get_db() {
    static DBWrapper db;
    return db;
}