#include "logger.hpp"
#include "config.hpp"
#include "utils.hpp"
#include "tx_utils.hpp"
#include "tx_runner.hpp"

#include "transaction.hpp"
#include "initializer.hpp"

#include <unistd.h>

#include <string>
#include <thread>

void run_tx(int* flag, ThreadLocalData& t_data) {
    while (__atomic_load_n(flag, __ATOMIC_ACQUIRE)) {
        Transaction tx(Database::get_db());

        Stat& stat = t_data.stat;
        Output& out = t_data.out;

        int x = urand_int(1, 100);
        if (x <= 4) {
            run_with_retry<StockLevelTx>(tx, stat, out);
        } else if (x <= 8) {
            run_with_retry<DeliveryTx>(tx, stat, out);
        } else if (x <= 12) {
            run_with_retry<OrderStatusTx>(tx, stat, out);
        } else if (x <= 12 + 43) {
            run_with_retry<PaymentTx>(tx, stat, out);
        } else {
            run_with_retry<NewOrderTx>(tx, stat, out);
        }
    }
}

int main(int argc, const char* argv[]) {
    if (argc != 4) {
        LOG_ERROR("num_warehouses num_threads seconds\n");
        exit(1);
    }

    uint16_t num_warehouse = static_cast<uint16_t>(std::stoi(argv[1], NULL, 10));
    int num_threads = std::stoi(argv[2], NULL, 10);
    int seconds = std::stoi(argv[3], NULL, 10);
    
    Config& c = get_mutable_config();
    c.set_num_warehouses(num_warehouse);

    LOG_INFO("Loading all tables with %" PRIu16 " warehouses", num_warehouse);
    Initializer::load_all_tables();
    LOG_INFO("Loaded");

    std::vector<std::thread> threads;

    alignas(64) int flag = 1;

    std::vector<ThreadLocalData> t_data(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(run_tx, &flag, std::ref(t_data[i]));
    }

    sleep(seconds);
    __atomic_store_n(&flag, 0, __ATOMIC_RELEASE);

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    int total_commits = 0;
    int total_usr_aborts = 0;
    int total_sys_aborts = 0;

    for (int i = 0; i < num_threads; i++) {
        total_commits += t_data[i].stat.num_commits;
        total_usr_aborts += t_data[i].stat.num_usr_aborts;
        total_sys_aborts += t_data[i].stat.num_sys_aborts;
    }

    printf("In %d seconds\n", seconds);
    printf("    num commits: %d\n", total_commits);
    printf("    num sys aborts: %d\n", total_sys_aborts);
    printf("    num usr aborts: %d\n", total_usr_aborts);
    printf("Throughput: %d\n", total_commits / seconds);
}