#include "transaction.hpp"

Transaction::Transaction()
    : db(Database::get_db()) {}

void Transaction::abort() {
    // do nothing
}