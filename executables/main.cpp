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

    uint16_t num_warehouses = static_cast<uint16_t>(std::stoi(argv[1], nullptr, 10));
    int num_threads = std::stoi(argv[2], nullptr, 10);
    int seconds = std::stoi(argv[3], nullptr, 10);

    assert(seconds > 0);

    Config& c = get_mutable_config();
    c.set_num_warehouses(num_warehouses);
    c.set_num_threads(num_threads);

    printf("Loading all tables with %" PRIu16 " warehouse(s)\n", num_warehouses);
    Initializer::load_all_tables();
    printf("Loaded\n");

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

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

    Stat stat;
    for (int i = 0; i < num_threads; i++) {
        stat.add(t_data[i].stat);
    }
    Stat::PerTxType total = stat.aggregate_perf();

    printf("%d warehouse(s), %d thread(s), %d second(s)\n", num_warehouses, num_threads, seconds);
    printf("    commits: %lu\n", total.num_commits);
    printf("    sys aborts: %lu\n", total.num_sys_aborts);
    printf("    usr aborts: %lu\n", total.num_usr_aborts);
    printf("Throughput: %lu txns/s\n", total.num_commits / seconds);

    printf("\nDetails:\n");
    for (size_t i = 0; i < TxType::Max; i++) {
        printf("    %-11s c:%8lu(%.2f%%)   ua:%6lu  sa:%6lu\n"
               , TxType::name(i)
               , stat[i].num_commits
               , stat[i].num_commits/(double)total.num_commits
               , stat[i].num_usr_aborts
               , stat[i].num_sys_aborts);
    }
}
