#include <unistd.h>

#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "indexes/masstree.hpp"
#include "protocols/common/epoch_manager.hpp"
#include "protocols/silo/glue_code/initializer.hpp"
#include "protocols/silo/glue_code/transaction.hpp"
#include "protocols/silo/include/silo.hpp"
#include "protocols/silo/include/value.hpp"
#include "test/common/topological_sort.hpp"
#include "tpcc/include/config.hpp"
#include "tpcc/include/tx_runner.hpp"
#include "tpcc/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/utils.hpp"

volatile mrcu_epoch_type active_epoch = 1;
volatile std::uint64_t globalepoch = 1;
volatile bool recovering = false;

using Index = MasstreeIndexes<ValueTest>;
using Protocol = Silo<Index>;

template <typename Protocol>
using RWHistory = typename ThreadLocalRWTracker<typename Protocol::Key>::RWHistory;

template <typename Protocol>
RWHistory<Protocol>&& dump_rwsets() {
    return std::move(ThreadLocalRWTracker<typename Protocol::Key>::get_rwhistory());
}

template <typename Protocol>
using NodeHistory = typename ThreadLocalNodeTracker<typename Protocol::Index>::NodeHistory;

template <typename Protocol>
NodeHistory<Protocol>&& dump_nodesets() {
    return std::move(ThreadLocalNodeTracker<typename Protocol::Index>::get_nodehistory());
}

template <typename Protocol>
void run_tx(
    uint32_t cnt, ThreadLocalData& t_data, RWHistory<Protocol>& rwh, NodeHistory<Protocol>& nh,
    uint32_t worker_id, EpochManager<Protocol>& em) {
    Worker<Protocol> w(worker_id);
    em.set_worker(worker_id, &w);
    while (cnt != 0) {
        cnt--;
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
    rwh = dump_rwsets<Protocol>();
    nh = dump_nodesets<Protocol>();
}

void initialize_workload(uint16_t num_warehouses, uint32_t num_threads) {
    Config& c = get_mutable_config();
    c.set_num_warehouses(num_warehouses);
    c.set_num_threads(num_threads);
    c.enable_fixed_warehouse_per_thread();
    Initializer<Index>::load_all_tables();
}

TEST(SILO_SerializationTest, T15W15C100000) {
    uint16_t num_warehouses = 1;
    uint32_t num_threads = 5;
    initialize_workload(num_warehouses, num_threads);

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    EpochManager<Protocol> em(num_threads, 40);

    std::vector<ThreadLocalData> t_data(num_threads);
    std::vector<RWHistory<Protocol>> rw_data(num_threads);
    std::vector<NodeHistory<Protocol>> n_data(num_threads);

    uint32_t execution_per_thread = 5;

    for (uint32_t i = 0; i < num_threads; i++) {
        threads.emplace_back(
            run_tx<Protocol>, execution_per_thread, std::ref(t_data[i]), std::ref(rw_data[i]),
            std::ref(n_data[i]), i, std::ref(em));
    }

    em.start(10);

    for (uint32_t i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    // For conflict graph of both record and node
    Graph g;

    // For conflict graph of record
    Graph gr;
    for (uint32_t i = 0; i < num_threads; i++) {
        RWHistory<Protocol>& rwh = rw_data[i];
        for (auto iter = rwh.begin(); iter != rwh.end(); ++iter) {
            uint64_t dest = iter->first.id;
            auto& per_tx_rwhistory = iter->second;
            for (auto& tracked_rw_info: per_tx_rwhistory) {
                uint64_t src = tracked_rw_info.txid.id;
                gr.add_edge(src, dest);
                g.add_edge(src, dest);
            }
        }
    }
    ASSERT_TRUE(gr.topological_sort());
    // gr.visualize("record_conflict.dot", "Record Level Conflict Graph");

    // For conflict graph of node
    Graph gn;
    // Create a map to determine which tx wrote the version to leafnode
    std::map<uint32_t, std::unordered_map<uintptr_t, std::unordered_map<uint64_t, uint64_t>>>
        leafnode_tx_map;
    for (uint32_t i = 0; i < num_threads; i++) {
        NodeHistory<Protocol>& nh = n_data[i];
        for (auto iter = nh.begin(); iter != nh.end(); ++iter) {
            uint64_t src = iter->first.id;
            auto& per_tx_nodehistory = iter->second;
            for (auto& ni: per_tx_nodehistory) {
                for (auto& v: ni.created_versions) {
                    leafnode_tx_map[ni.epoch][ni.nodeid][v] = src;
                }
            }
        }
    }
    for (uint32_t i = 0; i < num_threads; i++) {
        NodeHistory<Protocol>& nh = n_data[i];
        for (auto iter = nh.begin(); iter != nh.end(); ++iter) {
            uint64_t dest = iter->first.id;
            auto& per_tx_nodehistory = iter->second;
            for (auto& ni: per_tx_nodehistory) {
                uint32_t read_epoch = ni.epoch;
                for (uint32_t i = read_epoch; i > 0; --i) {
                    auto& m = leafnode_tx_map[i][ni.nodeid];
                    auto m_iter = m.find(ni.old_version);
                    if (m_iter != m.end()) {
                        uint64_t src = m_iter->second;
                        gn.add_edge(src, dest);
                        g.add_edge(src, dest);
                        break;
                    }
                }
            }
        }
    }
    // dump data

    // FILE* pfile;
    // pfile = fopen("dump.log", "w");
    // for (uint32_t i = 0; i < num_threads; i++) {
    //     NodeHistory<Protocol>& nh = n_data[i];
    //     for (auto iter = nh.begin(); iter != nh.end(); ++iter) {
    //         uint64_t dest = iter->first.id;
    //         fprintf(pfile, "TxID: %lu\n", dest);
    //         auto& per_tx_nodehistory = iter->second;
    //         for (auto& ni: per_tx_nodehistory) {
    //             fprintf(pfile, "    (%lu, [%lu]", ni.nodeid, ni.old_version);
    //             for (auto& v: ni.created_versions) {
    //                 fprintf(pfile, ", %lu", v);
    //             }
    //             fprintf(pfile, ")\n");
    //         }
    //     }
    // }

    ASSERT_TRUE(gn.topological_sort());
    gn.visualize("node_conflict.dot", "Node Level Conflict Graph");

    ASSERT_TRUE(g.topological_sort());
    g.visualize("conflict_graph.dot", "Conflict Graph");
}