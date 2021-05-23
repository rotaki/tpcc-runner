#include "database.hpp"

Database::Database() {}

Database::~Database() {}

Database& Database::get_db() {
    static Database db;
    return db;
}