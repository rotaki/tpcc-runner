#!/usr/bin/env python3

import os
from sys import executable
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# EXECUTE THIS SCRIPT IN BASE DIRECTORY!!!

NUM_EXPERIMENTS_PER_SETUP = 5
NUM_SECONDS = 10


def get_filename(protocol, payload, workload, record, thread, skew, reps, second, i):
    return "YCSB" + protocol + "P" + str(payload) + "W" + workload + "R" + str(record) + "T" + str(thread) + "S" + str(second) + "Theta" + str(skew).replace('.', '') + "Reps" + str(reps) + ".log" + str(i)


def gen_build_setups():
    protocols = ["silo", "nowait", "mvto"]
    payloads = [4, 100, 1024]
    return [[protocol, payload] for protocol in protocols for payload in payloads]


def build():
    if not os.path.exists("./build"):
        os.mkdir("./build")  # create build
    os.chdir("./build")
    if not os.path.exists("./log"):
        os.mkdir("./log")  # compile logs
    for setup in gen_build_setups():
        protocol = setup[0]
        payload = setup[1]
        title = "ycsb" + str(payload) + "_" + protocol
        print("Compiling " + title)
        os.system(
            "cmake .. -DLOG_LEVEL=0 -DCMAKE_BUILD_TYPE=Release -DBENCHMARK=YCSB -DCC_ALG=" +
            protocol.upper() + " -DPAYLOAD_SIZE=" + str(payload))

        logfile = title + ".compile_log"
        ret = os.system("make -j$(nproc) > ./log/" + logfile + " 2>&1")
        if ret != 0:
            print("Error. Stopping")
            exit(0)
    os.chdir("../")  # go back to base directory


def gen_setups():
    protocols = ["silo", "nowait", "mvto"]
    threads = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
    setups = [
        # Cicada
        [100, "B", 10000000, 0.99, 16],
        [100, "A", 10000000, 0.99, 1],
        # TicToc ("A" is not exactly the same)
        [1024, "C", 10000000, 0, 2],
        [1024, "A", 10000000, 0.8, 16],
        [1024, "B", 10000000, 0.9, 16]
        # MOCC (Skip)
    ]
    return [[protocol, thread, *setup]
            for protocol in protocols
            for thread in threads
            for setup in setups]


def run_all():
    os.chdir("./build/bin")  # move to bin
    if not os.path.exists("./res"):
        os.mkdir("./res")  # create result directory inside bin
    for setup in gen_setups():
        protocol = setup[0]
        thread = setup[1]
        payload = setup[2]
        workload = setup[3]
        record = setup[4]
        skew = setup[5]
        reps = setup[6]
        second = NUM_SECONDS

        title = "ycsb" + str(payload) + "_" + protocol
        args = workload + " " + \
            str(record) + " " + str(thread) + " " + \
            str(second) + " " + str(skew) + " " + str(reps)

        print("[{}: {}]".format(title, args))

        for i in range(NUM_EXPERIMENTS_PER_SETUP):
            result_file = get_filename(
                protocol, payload, workload, record, thread, skew, reps, second, i)
            print(" Trial:" + str(i))
            ret = os.system("./" + title + " " + args +
                            " > ./res/" + result_file + " 2>&1")
            if ret != 0:
                print("Error. Stopping")
                exit(0)
    os.chdir("../../")  # back to base directory


def get_stats_from_file(result_file):
    f = open(result_file)
    for line in f:
        line = line.strip().split()
        # print(line)
        if not line:
            continue
        if line[0] == "commits:":
            txn_cnt = float(line[1])
        if line[0] == "sys_aborts:":
            abort_cnt = float(line[1])
        if line[0] == "Throughput:":
            throughput = float(line[1])
    f.close()
    return txn_cnt, abort_cnt, throughput


def tuple_to_string(tup):
    payload, workload, record, skew, reps = tup
    return "YCSB({})P{}R{}THETA{}REPS{}".format(workload, payload, record, str(skew).replace('.', ''), reps)


def plot_all():

    # plot throughput
    os.chdir("./build/bin/res")  # move to result file
    if not os.path.exists("./plots"):
        os.mkdir("./plots")  # create plot directory inside res
    throughputs = {}
    abort_rates = {}
    for setup in gen_setups():
        protocol = setup[0]
        thread = setup[1]
        payload = setup[2]
        workload = setup[3]
        record = setup[4]
        skew = setup[5]
        reps = setup[6]
        second = NUM_SECONDS

        graph_line = tuple([payload, workload, record, skew, reps])
        if graph_line not in throughputs:
            throughputs[graph_line] = {}
        if graph_line not in abort_rates:
            abort_rates[graph_line] = {}
        if protocol not in throughputs[graph_line]:
            throughputs[graph_line][protocol] = []
        if protocol not in abort_rates[graph_line]:
            abort_rates[graph_line][protocol] = []

        average_throughput = 0
        average_abort_rate = 0
        for i in range(NUM_EXPERIMENTS_PER_SETUP):
            result_file = get_filename(
                protocol, payload, workload, record, thread, skew, reps, second, i)
            txn_cnt, abort_cnt, throughput = get_stats_from_file(result_file)
            abort_rate = abort_cnt / (abort_cnt + txn_cnt)
            average_throughput += throughput
            average_abort_rate += abort_rate
        average_throughput /= NUM_EXPERIMENTS_PER_SETUP
        average_abort_rate /= NUM_EXPERIMENTS_PER_SETUP
        throughputs[graph_line][protocol].append([thread, average_throughput])
        abort_rates[graph_line][protocol].append([thread, average_abort_rate])

    for key in throughputs:
        payload, workload, record, skew, reps = key

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4))

        markers = ['o', 'v', 's', 'p', 'P', '*', 'X', 'D', 'd', '|', '_']

        marker_choice = 0
        for protocol, res in throughputs[key].items():
            res = np.array(res).T
            ax1.plot(res[0], res[1]/(10**6),
                     markers[marker_choice] + '-', label=protocol)
            marker_choice += 1

        marker_choice = 0
        for protocol, res in abort_rates[key].items():
            res = np.array(res).T
            ax2.plot(res[0], res[1], markers[marker_choice] + '-')
            marker_choice += 1

        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax2.xaxis.set_major_locator(MaxNLocator(integer=True))

        ax1.set_xlabel(
            "Thread Count ({} seconds)".format(NUM_SECONDS))
        ax2.set_xlabel(
            "Thread Count ({} seconds)".format(NUM_SECONDS))
        ax1.set_ylabel("Throughput (Million txns/s)")
        ax2.set_ylabel("Abort Rate")
        ax1.grid()
        ax2.grid()
        fig.legend(loc="lower center", bbox_to_anchor=(
            0.5, 0.84), ncol=len(throughputs.keys()))
        fig.suptitle("YCSB-{}, {} records each with {} bytes, $\\theta$ = {}, {} reps per txn".format(
            workload, record, payload, skew, reps))
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.savefig("./plots/{}.png".format(tuple_to_string(key)))
        print("{}.pdf is saved in ./build/bin/res/plots/".format(tuple_to_string(key)))

    os.chdir("../../../")  # go back to base directory


if __name__ == "__main__":
    build()
    run_all()
    plot_all()
