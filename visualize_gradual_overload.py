#!/usr/bin/env python3
"""
visualize_gradual_overload.py

Standalone visualization script extracted from main_gradual_overload.go.
Usage:
    python3 visualize_gradual_overload.py --prefix ./results/zkresult-gradual-2025-01-01-00_00_00-

It expects two CSV files at <prefix>gradual_overload_metrics.csv and <prefix>phase_transitions.csv

Requires: pandas, matplotlib, numpy
"""
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main(prefix: str):
    metrics_file = prefix + "gradual_overload_metrics.csv"
    phases_file = prefix + "phase_transitions.csv"

    metrics_df = pd.read_csv(metrics_file)
    phases_df = pd.read_csv(phases_file)

    metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'])
    phases_df['timestamp'] = pd.to_datetime(phases_df['timestamp'])

    start_time = metrics_df['timestamp'].min()
    metrics_df['time_seconds'] = (metrics_df['timestamp'] - start_time).dt.total_seconds()

    fig, axes = plt.subplots(3, 1, figsize=(14, 10))
    fig.suptitle('Gradual Overload Test: |Init|Warmup|Load Increase|Failure|Mitigation|', fontsize=16)

    phase_colors = {
        'INIT': '#90EE90',
        'WARMUP': '#FFD700',
        'LOAD_INCREASE': '#FFA500',
        'FAILURE': '#FF6347',
        'MITIGATION': '#87CEEB',
        'RECOVERED': '#98FB98',
        'STABLE': '#32CD32',
        'PARTIAL_RECOVERY': '#F0E68C'
    }

    ax1 = axes[0]
    ax1.plot(metrics_df['time_seconds'], metrics_df['throughput'], 'b-', linewidth=2, label='Throughput')
    ax1.set_ylabel('Throughput (ops/sec)')
    ax1.set_xlabel('Time (seconds)')
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    ax2 = axes[1]
    ax2.plot(metrics_df['time_seconds'], metrics_df['avg_latency_ms'], 'r-', linewidth=2, label='Avg Latency')
    if 'p99_latency_ms' in metrics_df.columns:
        ax2.plot(metrics_df['time_seconds'], metrics_df['p99_latency_ms'], 'r--', alpha=0.5, label='P99 Latency')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_xlabel('Time (seconds)')
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    ax3 = axes[2]
    ax3.plot(metrics_df['time_seconds'], metrics_df['workload'], 'g-', linewidth=2, label='Workload')
    ax3.set_ylabel('Workload (requests)')
    ax3.set_xlabel('Time (seconds)')
    ax3.grid(True, alpha=0.3)
    ax3.legend()

    # annotate phases
    for _, phase in phases_df.iterrows():
        phase_time = (phase['timestamp'] - start_time).total_seconds()
        phase_name = phase['phase']
        for ax in axes:
            ax.axvline(x=phase_time, color='gray', linestyle='--', alpha=0.5)
            ax.text(phase_time, ax.get_ylim()[1] * 0.95, phase_name, rotation=45, fontsize=9, ha='right')

    # colored backgrounds
    for i in range(len(phases_df) - 1):
        current_phase = phases_df.iloc[i]
        next_phase = phases_df.iloc[i + 1]
        start = (current_phase['timestamp'] - start_time).total_seconds()
        end = (next_phase['timestamp'] - start_time).total_seconds()
        phase_name = current_phase['phase']
        color = phase_colors.get(phase_name, '#FFFFFF')
        for ax in axes:
            ax.axvspan(start, end, alpha=0.2, color=color)

    if len(phases_df) > 0:
        last_phase = phases_df.iloc[-1]
        start = (last_phase['timestamp'] - start_time).total_seconds()
        end = metrics_df['time_seconds'].max()
        phase_name = last_phase['phase']
        color = phase_colors.get(phase_name, '#FFFFFF')
        for ax in axes:
            ax.axvspan(start, end, alpha=0.2, color=color)

    failure_point = metrics_df[metrics_df['phase'] == 'FAILURE'] if 'phase' in metrics_df.columns else pd.DataFrame()
    if not failure_point.empty:
        fp = failure_point.iloc[0]
        ax1.annotate(f"Failure\n{int(fp['workload'])} req", xy=(fp['time_seconds'], fp['throughput']), xytext=(fp['time_seconds'] - 10, fp['throughput'] * 1.2), arrowprops=dict(arrowstyle='->', color='red', lw=2), fontsize=10, color='red')

    plt.tight_layout()
    out_png = prefix + 'visualization.png'
    plt.savefig(out_png, dpi=150, bbox_inches='tight')
    print(f"Visualization written to {out_png}")

    # summary
    print('\n=== Test Summary ===')
    print(f"Total test duration: {metrics_df['time_seconds'].max():.1f} seconds")
    print(f"Peak throughput: {metrics_df['throughput'].max():.2f} ops/sec")
    if 'avg_latency_ms' in metrics_df.columns:
        print(f"Maximum latency: {metrics_df['avg_latency_ms'].max():.2f} ms")
    if not failure_point.empty:
        print(f"Failure detected at workload: {failure_point.iloc[0]['workload']} requests")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Visualize gradual overload test results')
    parser.add_argument('--prefix', required=True, help='Output prefix used by the benchmark (including trailing - if used)')
    args = parser.parse_args()
    main(args.prefix)
