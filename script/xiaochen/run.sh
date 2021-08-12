#!/bin/bash

# check sudo
sudo true

# build target benchmark
cd build
make slot_iterator_benchmark
cd ..

# run
binary="build/benchmark/slot_iterator_benchmark"
FlameGraphDir="../FlameGraph"
export PATH=$FlameGraphDir:$PATH
sudo perf record -F 99 -a -g -- ${binary}
sudo chmod 666 perf.data
perf script | stackcollapse-perf.pl > out.perf-folded
flamegraph.pl out.perf-folded > perf-kernel.svg
python3 -m http.server

printf "graph: http://localhost:8000/perf-kernel.svg"

