#!/usr/bin/env python3

import os
from sys import executable
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# EXECUTE THIS SCRIPT IN BASE DIRECTORY!!!

NUM_EXPERIMENTS_PER_SETUP = 5
NUM_SECONDS = 10

def get_filename(protocol, thread, warehouse, second, i):
    return "TPCC" + protocol + "T" + str(thread) + "W" + str(warehouse) + "S" + str(second) + ".log" + str(i)

def gen_setups():
    protocols = ["silo", "nowait", "mvto"]
    threads = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
    return [[protocol, thread] for protocol in protocols for thread in threads]

def build():
    if not os.path.exists("./build"):
        os.mkdir("./build")  # create build
    os.chdir("./build")
    if not os.path.exists("./log"):
        os.mkdir("./log")  # compile logs
    compiled_protocol = []
    for setup in gen_setups():
        protocol = setup[0]
        if (protocol not in compiled_protocol):
            compiled_protocol.append(protocol)
        else:
            continue
        print("Compiling " + protocol)
        os.system(
            "cmake .. -DLOG_LEVEL=0 -DCMAKE_BUILD_TYPE=Release -DBENCHMARK=TPCC -DCC_ALG=" + protocol.upper())
        logfile = protocol + ".compile_log"
        ret = os.system("make -j > ./log/" + logfile + " 2>&1")
        if ret != 0:
            print("Error. Stopping")
            exit(0)
    os.chdir("../")  # go back to base directory


def run_all():
    os.chdir("./build/bin")  # move to bin
    if not os.path.exists("./res"):
        os.mkdir("./res")  # create result directory inside bin
    for setup in gen_setups():
        protocol = setup[0]
        thread = setup[1]
        warehouse = thread
        second = NUM_SECONDS
        args = " " + str(warehouse) + " " + str(thread) + " " + str(second)
        print("[" + protocol + "]" + " W:" + str(warehouse) +
              " T:" + str(thread) + " S:" + str(second))
        for i in range(NUM_EXPERIMENTS_PER_SETUP):
            result_file = get_filename(protocol, thread, warehouse, second, i)
            print(" Trial:" + str(i))
            ret = os.system("./tpcc_" + protocol + args +
                            " > ./res/" + result_file + " 2>&1")
            if ret != 0:
                print("Error. Stopping")
                exit(0)
    os.chdir("../../")  # back to base directory


def plot_all():
    # plot throughput
    os.chdir("./build/bin/res")  # move to result file
    if not os.path.exists("./plots"):
        os.mkdir("./plots")  # create plot directory inside res
    throughputs = {}
    abort_rates = {}
    for setup in gen_setups():
        protocol = setup[0]
        if protocol not in throughputs:
            throughputs[protocol] = []
        if protocol not in abort_rates:
            abort_rates[protocol] = []
        thread = setup[1]
        warehouse = thread
        second = NUM_SECONDS
        average_throughput = 0
        average_abort_rate = 0
        for i in range(NUM_EXPERIMENTS_PER_SETUP):
            result_file = get_filename(protocol, thread, warehouse, second, i) 
            result_file = open(result_file)
            for line in result_file:
                line = line.strip().split()
                if not line:
                    continue
                if line[0] == "commits:":
                    txn_cnt = float(line[1])
                if line[0] == "sys_aborts:":
                    abort_cnt = float(line[1])
                if line[0] == "Throughput:":
                    throughput = float(line[1])
            result_file.close()
            abort_rate = abort_cnt / (abort_cnt + txn_cnt)
            average_throughput += throughput
            average_abort_rate += abort_rate
        average_throughput /= NUM_EXPERIMENTS_PER_SETUP
        average_abort_rate /= NUM_EXPERIMENTS_PER_SETUP
        throughputs[protocol].append([thread, average_throughput])
        abort_rates[protocol].append([thread, average_abort_rate])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4))

    markers = ['o', 'v', 's', 'p', 'P', '*', 'X', 'D', 'd', '|', '_']

    marker_choice = 0
    for protocol, res in throughputs.items():
        res = np.array(res).T
        ax1.plot(res[0], res[1]/(10**6),
                 markers[marker_choice] + '-', label=protocol)
        marker_choice += 1

    marker_choice = 0
    for protocol, res in abort_rates.items():
        res = np.array(res).T
        ax2.plot(res[0], res[1], markers[marker_choice] + '-')
        marker_choice += 1

    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax2.xaxis.set_major_locator(MaxNLocator(integer=True))

    ax1.set_xlabel(
        "Thread Count = Num Warehouse ({} seconds)".format(NUM_SECONDS))
    ax2.set_xlabel(
        "Thread Count = Num Warehouse ({} seconds)".format(NUM_SECONDS))
    ax1.set_ylabel("Throughput (Million txns/s)")
    ax2.set_ylabel("Abort Rate")
    ax1.grid()
    ax2.grid()
    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.94), ncol=len(throughputs.keys()))
    fig.suptitle("(full) TPC-C")
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig("./plots/warehouse_threadcount.png")
    print("warehouse_threadcount.png is saved in ./build/bin/res/plots/")
    os.chdir("../../../")  # go back to base directory


if __name__ == "__main__":
    build()
    run_all()
    plot_all()
