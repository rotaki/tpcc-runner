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
        printf("num_warehouses num_threads seconds\n");
        exit(1);
    }

    uint16_t num_warehouses = static_cast<uint16_t>(std::stoi(argv[1], NULL, 10));
    int num_threads = std::stoi(argv[2], NULL, 10);
    int seconds = std::stoi(argv[3], NULL, 10);

    assert(seconds > 0);
    
    Config& c = get_mutable_config();
    c.set_num_warehouses(num_warehouses);
    c.set_num_threads(num_threads);

    printf("Loading all tables with %" PRIu16 " warehouse(s)\n", num_warehouses);
    Initializer::load_all_tables();
    printf("Loaded\n");

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

    size_t commits[5] = {};
    size_t usr_aborts[5] = {};
    size_t sys_aborts[5] = {};

    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < 5; j++) {
            commits[j] += t_data[i].stat.num_commits[j];
            usr_aborts[j] += t_data[i].stat.num_usr_aborts[j];
            sys_aborts[j] += t_data[i].stat.num_sys_aborts[j];
        }
    }
    
    size_t total_commits = 0;
    size_t total_usr_aborts = 0;
    size_t total_sys_aborts = 0;

    for (int i = 0; i < 5; i++) {
        total_commits += commits[i];
        total_usr_aborts += usr_aborts[i];
        total_sys_aborts += sys_aborts[i];
    }


    printf("%d warehouse(s), %d thread(s), %d second(s)\n", num_warehouses, num_threads, seconds);
    printf("    commits: %lu\n", total_commits);
    printf("    sys aborts: %lu\n", total_sys_aborts);
    printf("    usr aborts: %lu\n", total_usr_aborts);
    printf("Throughput: %lu txns/s\n", total_commits / seconds);

    printf("\nDetails:\n");
    printf("    NewOrder    c:%lu(%.2f%%)   ua:%lu  sa:%lu\n", commits[0], 100*commits[0]/double(total_commits), usr_aborts[0], sys_aborts[0]);
    printf("    Payment     c:%lu(%.2f%%)   ua:%lu  sa:%lu\n", commits[1], 100*commits[1]/double(total_commits), usr_aborts[1], sys_aborts[1]);
    printf("    OrderStatus c:%lu(%.2f%%)   ua:%lu  sa:%lu\n", commits[2], 100*commits[2]/double(total_commits), usr_aborts[2], sys_aborts[2]);
    printf("    Delivery    c:%lu(%.2f%%)   ua:%lu  sa:%lu\n", commits[3], 100*commits[3]/double(total_commits), usr_aborts[3], sys_aborts[3]);
    printf("    StockLevel  c:%lu(%.2f%%)   ua:%lu  sa:%lu\n", commits[4], 100*commits[4]/double(total_commits), usr_aborts[4], sys_aborts[4]);
}