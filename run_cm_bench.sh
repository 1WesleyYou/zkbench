#!/usr/bin/env bash

set -euo pipefail
par_dir="$(dirname "$(readlink -f "$0")")"
cd "$par_dir" || exit 1

echo "Running main workload (bench_normal.conf)..."
rm -f $par_dir/*.png
rm -f $par_dir/*.dat
"$par_dir/zkbench" -conf bench_create.conf

# Mark injection start with 10s delay for startup
current_time=$(date +"%Y-%m-%d %H:%M:%S.%3N")
injection_time=$(date -d "$current_time 10 seconds" +"%Y-%m-%d %H:%M:%S.%3N")
injection_file="$par_dir/../../agent/metrics/main_injection_timestamp.txt"
echo "inj,$injection_time" >> "$injection_file"
echo "Marked injection start (in 10s): $injection_time"

"$par_dir/zkbench" -conf bench_mixed.conf 