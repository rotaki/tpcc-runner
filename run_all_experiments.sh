#!/bin/bash
set -euxvo pipefail

# install necessary
apt-get update
apt-get install -y g++-10 cmake python3-pip
pip3 install matplotlib numpy
update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 10

# simbolic link
ln -s scripts/ycsb/various_measurements.py various_measurements.py
ln -s scripts/tpcc/warehouse_threadcount.py warehouse_threadcount.py

# execute
python3 various_measurements.py
python3 warehouse_threadcount.py