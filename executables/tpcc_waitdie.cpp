#include <inttypes.h>
#include <unistd.h>

#include <string>
#include <thread>

#include "benchmarks/tpcc/include/config.hpp"
#include "benchmarks/tpcc/include/tx_runner.hpp"
#include "benchmarks/tpcc/include/tx_utils.hpp"
#include "indexes/masstree.hpp"
#include "protocols/common/timestamp_manager.hpp"
#include "protocols/waitdie/include/value.hpp"
#include "protocols/waitdie/include/waitdie.hpp"
#include "protocols/waitdie/tpcc/initializer.hpp"
#include "protocols/waitdie/tpcc/transaction.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

volatile mrcu_epoch_type active_epoch = 1;
volatile std::uint64_t globalepoch = 1;
volatile bool recovering = false;

template <typename Protocol>
void run_tx(
    int* flag, ThreadLocalData& t_data, uint32_t worker_id, TimeStampManager<Protocol>& tsm) {
    Worker<Protocol> w(tsm, worker_id, 1);
    tsm.set_worker(worker_id, &w);
    while (__atomic_load_n(flag, __ATOMIC_ACQUIRE)) {
        Transaction tx(w);

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
    c.enable_fixed_warehouse_per_thread();

    printf("Loading all tables with %" PRIu16 " warehouse(s)\n", num_warehouses);

    using Index = MasstreeIndexes<Value>;
    using Protocol = WaitDie<Index>;

    Initializer<Index>::load_all_tables();
    printf("Loaded\n");

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    TimeStampManager<Protocol> tsm(num_threads, 5);

    alignas(64) int flag = 1;

    std::vector<ThreadLocalData> t_data(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(run_tx<Protocol>, &flag, std::ref(t_data[i]), i, std::ref(tsm));
    }

    tsm.start(seconds);


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
    printf("    usr_aborts: %lu\n", total.num_usr_aborts);
    printf("    sys_aborts: %lu\n", total.num_sys_aborts);
    printf("Throughput: %lu txns/s\n", total.num_commits / seconds);

    printf("\nDetails:\n");
    constexpr_for<TxProfileID::MAX>([&](auto i) {
        constexpr auto p = static_cast<TxProfileID>(i.value);
        using Profile = TxProfile<p>;
        double tries = stat[p].num_commits + stat[p].num_usr_aborts + stat[p].num_sys_aborts;
        printf(
            "    %-11s c[%.2f%%]:%10lu(%.2f%%)   ua:%10lu(%.2f%%)  sa:%10lu(%.2f%%)  avgl:%10.0lf  minl:%10" PRIu64
            "  maxl:%10" PRIu64 "\n",
            Profile::name, stat[p].num_commits / (double)total.num_commits, stat[p].num_commits,
            stat[p].num_commits / tries, stat[p].num_usr_aborts, stat[p].num_usr_aborts / tries,
            stat[p].num_sys_aborts, stat[p].num_sys_aborts / tries,
            stat[p].total_latency / (double)stat[p].num_commits, stat[p].min_latency,
            stat[p].max_latency);
    });

    printf("\nSystem Abort Details:\n");
    constexpr_for<TxProfileID::MAX>([&](auto i) {
        constexpr auto p = static_cast<TxProfileID>(i.value);
        using Profile = TxProfile<p>;
        printf("    %-11s\n", Profile::name);
        constexpr_for<Profile::AbortID::MAX>([&](auto j) {
            constexpr auto a = static_cast<typename Profile::AbortID>(j.value);
            printf(
                "        %-45s: %lu\n", Profile::template abort_reason<a>(),
                stat[p].abort_details[a]);
        });
    });
}
