#!/usr/bin/env python3
"""
Visualizer for zkbench summary.dat file
Plots: avg latency, max latency, p99 latency, throughput over time, separate by bench_type(READ/WRITE)
"""
import sys
import csv
from datetime import datetime
import matplotlib.pyplot as plt
from collections import defaultdict

def main():
    if len(sys.argv) < 2:
        print("Usage: python plot_bench_metrics.py <summary.dat>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    # Aggregate by bench_type and timestamp
    data_by_type = defaultdict(lambda: defaultdict(lambda: {
        'avg_lat': [], 'max_lat': [], 'p99_lat': [], 'throughput': []
    }))
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            btype = row['bench_type']
            
            # Only process READ and WRITE
            if btype not in ['READ', 'WRITE']:
                continue
            
            # Parse timestamp (default 8-digit microseconds from Go)
            ts_str = row['group_start_time'].replace('Z', '+00:00')
            if '.' in ts_str and '+' in ts_str:
                base, rest = ts_str.split('.')
                microsec, tz = rest.split('+')
                ts_str = f"{base}.{microsec[:6].ljust(6, '0')}+{tz}"
            start_time = datetime.fromisoformat(ts_str)
            
            # Parse latencies (nanoseconds -> milliseconds)
            avg_lat = float(row['average_latency']) / 1e6
            max_lat = float(row['max_latency']) / 1e6
            p99_lat = float(row['99th_latency']) / 1e6
            throughput = float(row['throughput'])
            
            # Group by bench_type and timestamp
            data_by_type[btype][start_time]['avg_lat'].append(avg_lat)
            data_by_type[btype][start_time]['max_lat'].append(max_lat)
            data_by_type[btype][start_time]['p99_lat'].append(p99_lat)
            data_by_type[btype][start_time]['throughput'].append(throughput)
    
    if not data_by_type:
        print("No READ/WRITE test data found")
        sys.exit(0)
    
    # Process each bench_type separately
    for btype in sorted(data_by_type.keys()):
        data_by_timestamp = data_by_type[btype]
        
        # Sort timestamps and aggregate metrics
        sorted_timestamps = sorted(data_by_timestamp.keys())
        
        # For each timestamp: average latencies, sum throughput
        avg_lats = []
        max_lats = []
        p99_lats = []
        throughputs = []
        timestamps = []
        
        for ts in sorted_timestamps:
            data = data_by_timestamp[ts]
            
            # Average latencies across all clients at this timestamp
            avg_lats.append(sum(data['avg_lat']) / len(data['avg_lat']))
            max_lats.append(sum(data['max_lat']) / len(data['max_lat']))
            p99_lats.append(sum(data['p99_lat']) / len(data['p99_lat']))
            
            # Sum throughput across all clients at this timestamp
            throughputs.append(sum(data['throughput']))
            timestamps.append(ts)
        
        # Convert timestamps to relative seconds from start
        if timestamps:
            start_ts = timestamps[0]
            time_seconds = [(ts - start_ts).total_seconds() for ts in timestamps]
        else:
            continue
        
        # Create 2x2 subplot
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f'ZKBench Metrics - {btype} Operations', fontsize=14, fontweight='bold')
        
        # Plot 1: Average Latency over time
        ax1 = axes[0, 0]
        ax1.plot(time_seconds, avg_lats, marker='o', markersize=4, color='blue', linewidth=2, label='Avg Latency')
        ax1.set_title('Average Latency (Mean across clients)')
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('Latency (ms)')
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        # Plot 2: Max Latency over time
        ax2 = axes[0, 1]
        ax2.plot(time_seconds, max_lats, marker='o', markersize=4, color='red', linewidth=2, label='Max Latency')
        ax2.set_title('Max Latency (Mean across clients)')
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Latency (ms)')
        ax2.grid(True, alpha=0.3)
        ax2.legend()
        
        # Plot 3: P99 Latency over time
        ax3 = axes[1, 0]
        ax3.plot(time_seconds, p99_lats, marker='o', markersize=4, color='orange', linewidth=2, label='P99 Latency')
        ax3.set_title('99th Percentile Latency (Mean across clients)')
        ax3.set_xlabel('Time (seconds)')
        ax3.set_ylabel('Latency (ms)')
        ax3.grid(True, alpha=0.3)
        ax3.legend()
        
        # Plot 4: Throughput (sum across all clients at each timestamp)
        ax4 = axes[1, 1]
        ax4.plot(time_seconds, throughputs, marker='o', markersize=4, color='green', linewidth=2, label='Throughput (sum)')
        ax4.set_title('Throughput (Sum across all clients)')
        ax4.set_xlabel('Time (seconds)')
        ax4.set_ylabel('Operations/second')
        ax4.grid(True, alpha=0.3)
        ax4.legend()
        
        plt.tight_layout()
        
        # Save figure
        output = csv_file.replace('.dat', f'_{btype}_metrics.png')
        plt.savefig(output, dpi=150, bbox_inches='tight')
        print(f"Saved: {output}")
        
        plt.close()

if __name__ == '__main__':
    main()
