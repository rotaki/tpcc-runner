#pragma once

#include <cstdint>
#include <cstdio>
#include <deque>
#include <set>
#include <unordered_map>
#include <vector>

class Graph {
public:
    Graph(uint32_t num_nodes)
        : num_nodes(num_nodes) {
        serialized_order.reserve(num_nodes);
    }

    Graph() {}

    void visualize(const std::string& filename, const std::string& title) {
        if (serialized_order.empty()) return;
        FILE* pfile;
        pfile = fopen(filename.c_str(), "w");
        fprintf(pfile, "digraph G {\n");
        // fprintf(pfile, "    node [colorscheme=set19]\n");
        // for (auto iter = indegree.begin(); iter != indegree.end(); ++iter) {
        //     uint64_t dest = iter->first;
        //     if (dest != 0)
        //         fprintf(pfile, "    %u, %u;\n", dest & UINT32_MAX, (dest >> 32) + 1);
        // }
        fprintf(
            pfile,
            "    graph [label=\"%s.\nEach node represents a unique transaction: (ThreadID, TxCount). ",
            title.c_str());
        fprintf(pfile, "Each edge represents a W->R or W->W conflict.\n");
        fprintf(
            pfile, "Serialized Order: (%u,%u)", get_thread(serialized_order[0]),
            get_txnum(serialized_order[0]));
        for (uint32_t i = 1; i < serialized_order.size(); i++) {
            fprintf(
                pfile, "->(%u,%u)", get_thread(serialized_order[i]),
                get_txnum(serialized_order[i]));
        }
        fprintf(pfile, "\"];\n");
        for (auto iter = graph.begin(); iter != graph.end(); ++iter) {
            uint64_t src = iter->first;
            auto& dests = iter->second;
            for (auto& dest: dests) {
                fprintf(
                    pfile, "    \"(%u,%u)\" -> \"(%u,%u)\";\n", get_thread(src), get_txnum(src),
                    get_thread(dest), get_txnum(dest));
            }
        }
        fprintf(pfile, "}\n");
        fclose(pfile);
    }

    bool topological_sort() {
        // set of all nodes with no incoming edges
        std::vector<uint64_t> S;
        for (auto iter = indegree.begin(); iter != indegree.end(); ++iter) {
            if (iter->second == 0) S.emplace_back(iter->first);
        }

        while (!S.empty()) {
            // remove node n from S
            uint64_t n = S.back();
            S.pop_back();
            serialized_order.emplace_back(n);
            // find node with 0 degree after removing n
            for (uint64_t m: graph[n]) {
                indegree[m]--;
                if (indegree[m] == 0) {
                    S.emplace_back(m);
                }
            }
        }

        for (auto iter = indegree.begin(); iter != indegree.end(); ++iter) {
            if (iter->second > 0) {
                printf("bad dest: %lu\n", iter->first);
                return false;
            }
        }

        return true;
    }

    // can only be used if topological_sort returns true
    void print_serialized_order() {
        if (serialized_order.empty()) return;
        printf("%lu ", serialized_order[0]);
        for (uint32_t i = 1; i < serialized_order.size(); i++) {
            printf("-> %lu", serialized_order[i]);
        }
        printf(";\n");
    }

    void add_edge(uint64_t src, uint64_t dest) {
        if (indegree.find(src) == indegree.end()) indegree[src] = 0;
        if (indegree.find(dest) == indegree.end()) indegree[dest] = 0;

        auto& dests = graph[src];
        const auto& [iter, inserted] = dests.insert(dest);
        if (inserted) {
            indegree[dest]++;
        }
    }

private:
    uint32_t num_nodes;
    std::vector<uint64_t> serialized_order;
    std::unordered_map<uint64_t, std::set<uint64_t>> graph;
    std::unordered_map<uint64_t, uint64_t> indegree;

    uint32_t get_thread(uint64_t txid) { return static_cast<uint32_t>(txid >> 32); }

    uint32_t get_txnum(uint64_t txid) { return static_cast<uint32_t>(txid & UINT32_MAX); }
};