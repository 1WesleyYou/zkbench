#!/usr/bin/env bash

set -euo pipefail
par_dir="$(dirname "$(readlink -f "$0")")"
cd "$par_dir" || exit 1

echo "Running main workload (bench_normal.conf)..."
rm -f $par_dir/*.png
rm -f $par_dir/*.dat
"$par_dir/zkbench" -conf bench_create.conf
"$par_dir/zkbench" -conf bench_mixed.conf 