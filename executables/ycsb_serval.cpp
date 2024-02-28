#include <unistd.h>

#include <string>
#include <thread>

#include "benchmarks/ycsb/include/config.hpp"
#include "benchmarks/ycsb/include/tx_runner.hpp"
#include "benchmarks/ycsb/include/tx_utils.hpp"
#include "indexes/masstree.hpp"
#include "protocols/common/timestamp_manager.hpp"
#include "protocols/mvto/include/mvto.hpp"
#include "protocols/mvto/include/value.hpp"
#include "protocols/mvto/ycsb/initializer.hpp"
#include "protocols/mvto/ycsb/transaction.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"
#include "utils/numa.hpp"

volatile mrcu_epoch_type active_epoch = 1;
volatile std::uint64_t globalepoch = 1;
volatile bool recovering = false;

#ifdef PAYLOAD_SIZE
using Record = Payload<PAYLOAD_SIZE>;
#else
#    define PAYLOAD_SIZE 1024
using Record = Payload<PAYLOAD_SIZE>;
#endif

// Global mutex -> make cout easier to read
std::mutex cout_mutex;

class Operation {
public:
    enum Ope { Read, Update };
    int index;
    int value;
    Ope ope;

    Operation(int idx, int val, Ope operation) : index(idx), value(val), ope(operation) {}
};

class Request {
public:
    std::vector<Operation> operations;  
    std::vector<Operation> write_set;

    void addOperation(const Operation& op) {
        operations.push_back(op);
    }
};

// Change tsm to em -> MVTO use timestamp manager, caracal and serval use epoch manager
template <typename Protocol>
void run_tx(int* flag, ThreadLocalData& t_data, uint32_t worker_id, TimeStampManager<Protocol>& tsm, std::vector<Request>& batch_txs) {
    Worker<Protocol> w(tsm, worker_id, 1);
    tsm.set_worker(worker_id, &w);

    // Pre-Initialization Phase
    // Core Assignment -> serval: sequential
    pid_t tid = gettid();
    Numa numa(tid, worker_id % LOGICAL_CORE_SIZE);
    unsigned int cpu, node;
    if (getcpu(&cpu, &node)) exit(EXIT_FAILURE);

    // Output the cpu and correspoonding node
    std::lock_guard<std::mutex> guard(cout_mutex);
    std::cout << "cpu: " << cpu << ", node: " << node << std::endl; 
    
    // Initialization Phase: Append Pending version
    Stat& stat = t_data.stat;

    for (size_t tx_id = 0; tx_id < batch_txs[cur_tx].write_set.size(); tx_id++) {
        Protocol serval(tx_id);
        serval.append_pending_version(write_set[i].index);
    }
}

int batch_size = 50000;

// make_global_transactions newest version
void make_global_transactions(std::vector<Request>& reqs) {
    const Config& c = get_config();
    int cur_tx = batch_size;

    for (int tx_index = 0; tx_index < cur_tx; ++tx_index) {
        Request newRequest;

        for (uint64_t i = 0; i < c.get_reps_per_txn(); ++i) {
            int operationType = urand_int(1, 100); 
            
            int key = zipf_int(c.get_contention(), c.get_num_records());
            int value = 0; 

            if (operationType <= c.get_read_propotion()) {
                newRequest.addOperation(Operation(key, value, Operation::Ope::Read));
            } else {
                newRequest.addOperation(Operation(key, value, Operation::Ope::Update));
            }
        }

        reqs.push_back(newRequest); 
    }
}

/*　Previous version
void make_global_transactions(std::vector<Request> &reqs){
    const Config& c = get_config();
    int r = c.get_read_propotion();
    int u = c.get_update_propotion();
    int rmw = c.get_readmodifywrite_propotion();
    int cur_tx = batch_size;

    // Make transactions
    while (cur_tx > 0) {
        const Config& c = get_config();
        uint64_t reps = c.get_reps_per_txn();

        // Make operations of each transactions
        for (uint64_t i = 0; i < reps; ++i) {
            // start making index
            reqs[cur_tx].key[i] = zipf_int(c.get_contention(), c.get_num_records());
            // end making index

            // start making operations
            using R = ReadTx<Record>;
            using U = UpdateTx<Record>;
            using RWM = ReadModifyWriteTx<Record>;

            int x = urand_int(1, 100);
            int p = 0;
            if (x <= (p += r)) {
                reqs[cur_tx].ope = Ope::Read;
            } else if (x <= (p += u)) {
                reqs[cur_tx].ope = Ope::Update;
            } else if (x <= (p += rmw)) {
                reqs[cur_tx].ope = Ope::Update;
            } else {
                throw std::runtime_error("No operation found");
            }
            // end making operations
        }
        cur_tx--;
    }
}
*/



int main(int argc, const char* argv[]) {
    if (argc != 7) {
        printf("workload_type(A,B,C,F) num_records num_threads seconds skew reps_per_txn\n");
        exit(1);
    }

    std::string workload_type = argv[1];
    uint64_t num_records = static_cast<uint64_t>(std::stoi(argv[2], nullptr, 10));
    int num_threads = std::stoi(argv[3], nullptr, 10);
    int seconds = std::stoi(argv[4], nullptr, 10);
    double skew = std::stod(argv[5]);
    int reps = std::stoi(argv[6], nullptr, 10);

    assert(seconds > 0);

    Config& c = get_mutable_config();
    c.set_workload_type(workload_type);
    c.set_num_records(num_records);
    c.set_num_threads(num_threads);
    c.set_contention(skew);
    c.set_reps_per_txn(reps);

    printf("Loading all tables with %lu record(s) each with %u bytes\n", num_records, PAYLOAD_SIZE);

    using Index = MasstreeIndexes<Value<Version>>;
    using Protocol = MVTO<Index>;

    Initializer<Index>::load_all_tables<Record>();
    printf("Loaded\n");

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    TimeStampManager<Protocol> tsm(num_threads, 5);

    alignas(64) int flag = 1;

    std::vector<ThreadLocalData> t_data(num_threads);

    std::vector<Request> batch_txs;
    make_global_transactions(batch_txs);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(run_tx<Protocol>, &flag, std::ref(t_data[i]), i, std::ref(tsm), std::ref(batch_txs));
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

    printf(
        "Workload: %s, Record(s): %lu, Thread(s): %d, Second(s): %d, Skew: %3.2f, RepsPerTxn: %u\n",
        workload_type.c_str(), num_records, num_threads, seconds, skew, reps);
    printf("    commits: %lu\n", total.num_commits);
    printf("    usr_aborts: %lu\n", total.num_usr_aborts);
    printf("    sys_aborts: %lu\n", total.num_sys_aborts);
    printf("Throughput: %lu txns/s\n", total.num_commits / seconds);

    printf("\nDetails:\n");
    constexpr_for<TxProfileID::MAX>([&](auto i) {
        constexpr auto p = static_cast<TxProfileID>(i.value);
        using Profile = TxProfile<p, Record>;
        printf(
            "    %-20s c:%10lu(%.2f%%)   ua:%10lu  sa:%10lu\n", Profile::name, stat[p].num_commits,
            stat[p].num_commits / (double)total.num_commits, stat[p].num_usr_aborts,
            stat[p].num_sys_aborts);
    });
}