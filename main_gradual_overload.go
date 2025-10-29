package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	
	zkb "github.com/OrderLab/zkbench/bench"
)

var (
	conf      = flag.String("conf", "bench_gradual_overload.conf", "Benchmark configuration file")
	outprefix = flag.String("outprefix", "zkresult", "Benchmark stat filename prefix")
	visualize = flag.Bool("viz", true, "Generate visualization data")
)

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " " + string(bytes))
}

func main() {
	flag.Parse()
	
	// Parse base configuration
	config, err := zkb.ParseConfig(*conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fail to parse config: %v\n", err)
		os.Exit(1)
	}
	
	// Set up logging
	log.SetFlags(0)
	log.SetOutput(new(logWriter))
	
	// Create gradual overload configuration
	gradualConfig := zkb.GradualOverloadConfig{
		InitialRequests:   config.NRequests / 20,  // Start with 5% of max
		MaxRequests:       config.NRequests * 5,    // Go up to 5x configured
		StepSize:          config.NRequests / 10,   // Increase by 10% each step
		StepDuration:      10,                      // 10 seconds per step
		WarmupSteps:       5,                       // 5 warmup steps
		LatencyThreshold:  50.0,                    // 50ms latency threshold
		ThroughputDrop:    30.0,                    // 30% throughput drop threshold
		StabilizationTime: 5,                       // 5 seconds stabilization
	}
	
	// Override with command line flags if provided
	initialReq := flag.Int64("initial", gradualConfig.InitialRequests, "Initial requests per second")
	maxReq := flag.Int64("max", gradualConfig.MaxRequests, "Maximum requests to test")
	stepSize := flag.Int64("step", gradualConfig.StepSize, "Step size for load increase")
	stepDuration := flag.Int("duration", gradualConfig.StepDuration, "Duration of each step in seconds")
	warmupSteps := flag.Int("warmup", gradualConfig.WarmupSteps, "Number of warmup steps")
	latencyThreshold := flag.Float64("latency", gradualConfig.LatencyThreshold, "Latency threshold in ms")
	throughputDrop := flag.Float64("throughput", gradualConfig.ThroughputDrop, "Throughput drop percentage threshold")
	
	flag.Parse()
	
	// Apply overrides
	gradualConfig.InitialRequests = *initialReq
	gradualConfig.MaxRequests = *maxReq
	gradualConfig.StepSize = *stepSize
	gradualConfig.StepDuration = *stepDuration
	gradualConfig.WarmupSteps = *warmupSteps
	gradualConfig.LatencyThreshold = *latencyThreshold
	gradualConfig.ThroughputDrop = *throughputDrop
	
	// Create base benchmark
	b := new(zkb.Benchmark)
	b.BenchConfig = *config
	b.Init()
	
	// Smoke test
	b.SmokeTest()
	
	// Create gradual overload benchmark
	gb := zkb.NewGradualOverloadBenchmark(b, gradualConfig)
	
	// Generate output prefix with timestamp
	current := time.Now()
	prefix := *outprefix + "-gradual-" + current.Format("2006-01-02-15_04_05") + "-"
	
	// Run the gradual overload test
	log.Println("===========================================")
	log.Println("Starting Gradual Overload Test")
	log.Println("===========================================")
	
	if err := gb.RunGradualOverload(prefix); err != nil {
		log.Fatalf("Gradual overload test failed: %v", err)
	}
	
	log.Println("===========================================")
	log.Println("Gradual Overload Test Complete")
	log.Println("===========================================")
	
	// Generate visualization script if requested
	if *visualize {
		generateVisualizationScript(prefix)
	}
	
	// Cleanup if configured
	if b.Cleanup {
		b.Done()
	}
}

// generateVisualizationScript creates a Python script for visualizing the results
func generateVisualizationScript(prefix string) {
	scriptPath := prefix + "visualize.py"
	script := `#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from datetime import datetime

# Read data files
metrics_file = '` + prefix + `gradual_overload_metrics.csv'
phases_file = '` + prefix + `phase_transitions.csv'

# Load data
metrics_df = pd.read_csv(metrics_file)
phases_df = pd.read_csv(phases_file)

# Convert timestamps
metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'])
phases_df['timestamp'] = pd.to_datetime(phases_df['timestamp'])

# Calculate time relative to start
start_time = metrics_df['timestamp'].min()
metrics_df['time_seconds'] = (metrics_df['timestamp'] - start_time).dt.total_seconds()

# Create figure with subplots
fig, axes = plt.subplots(3, 1, figsize=(14, 10))
fig.suptitle('Gradual Overload Test: |Init|Warmup|Load Increase|Failure|Mitigation|', fontsize=16)

# Color map for phases
phase_colors = {
    'INIT': '#90EE90',           # Light green
    'WARMUP': '#FFD700',          # Gold
    'LOAD_INCREASE': '#FFA500',   # Orange
    'FAILURE': '#FF6347',         # Tomato red
    'MITIGATION': '#87CEEB',      # Sky blue
    'RECOVERED': '#98FB98',       # Pale green
    'STABLE': '#32CD32',          # Lime green
    'PARTIAL_RECOVERY': '#F0E68C' # Khaki
}

# Plot 1: Throughput over time
ax1 = axes[0]
ax1.plot(metrics_df['time_seconds'], metrics_df['throughput'], 'b-', linewidth=2, label='Throughput')
ax1.set_ylabel('Throughput (ops/sec)', fontsize=12)
ax1.set_xlabel('Time (seconds)', fontsize=12)
ax1.grid(True, alpha=0.3)
ax1.legend()

# Plot 2: Latency over time
ax2 = axes[1]
ax2.plot(metrics_df['time_seconds'], metrics_df['avg_latency_ms'], 'r-', linewidth=2, label='Avg Latency')
ax2.plot(metrics_df['time_seconds'], metrics_df['p99_latency_ms'], 'r--', alpha=0.5, label='P99 Latency')
ax2.set_ylabel('Latency (ms)', fontsize=12)
ax2.set_xlabel('Time (seconds)', fontsize=12)
ax2.grid(True, alpha=0.3)
ax2.legend()

# Add latency threshold line
if 'latency_threshold' in metrics_df.columns or True:
    ax2.axhline(y=50, color='orange', linestyle=':', label='Threshold')

# Plot 3: Workload over time
ax3 = axes[2]
ax3.plot(metrics_df['time_seconds'], metrics_df['workload'], 'g-', linewidth=2, label='Workload')
ax3.set_ylabel('Workload (requests)', fontsize=12)
ax3.set_xlabel('Time (seconds)', fontsize=12)
ax3.grid(True, alpha=0.3)
ax3.legend()

# Add phase annotations to all plots
for i, phase in phases_df.iterrows():
    phase_time = (phase['timestamp'] - start_time).total_seconds()
    phase_name = phase['phase']
    
    for ax in axes:
        ax.axvline(x=phase_time, color='gray', linestyle='--', alpha=0.5)
        ax.text(phase_time, ax.get_ylim()[1] * 0.95, phase_name, 
                rotation=45, fontsize=9, ha='right')

# Add colored backgrounds for phases
for i in range(len(phases_df) - 1):
    current_phase = phases_df.iloc[i]
    next_phase = phases_df.iloc[i + 1]
    
    start = (current_phase['timestamp'] - start_time).total_seconds()
    end = (next_phase['timestamp'] - start_time).total_seconds()
    phase_name = current_phase['phase']
    
    color = phase_colors.get(phase_name, '#FFFFFF')
    
    for ax in axes:
        ax.axvspan(start, end, alpha=0.2, color=color)

# Handle last phase
if len(phases_df) > 0:
    last_phase = phases_df.iloc[-1]
    start = (last_phase['timestamp'] - start_time).total_seconds()
    end = metrics_df['time_seconds'].max()
    phase_name = last_phase['phase']
    color = phase_colors.get(phase_name, '#FFFFFF')
    
    for ax in axes:
        ax.axvspan(start, end, alpha=0.2, color=color)

# Add annotations for key events
failure_point = metrics_df[metrics_df['phase'] == 'FAILURE']
if not failure_point.empty:
    fp = failure_point.iloc[0]
    ax1.annotate(f'Failure\n{fp["workload"]} req',
                xy=(fp['time_seconds'], fp['throughput']),
                xytext=(fp['time_seconds'] - 10, fp['throughput'] * 1.2),
                arrowprops=dict(arrowstyle='->', color='red', lw=2),
                fontsize=10, color='red')

plt.tight_layout()
plt.savefig('` + prefix + `visualization.png', dpi=150, bbox_inches='tight')
plt.show()

# Print summary statistics
print("\n=== Test Summary ===")
print(f"Total test duration: {metrics_df['time_seconds'].max():.1f} seconds")
print(f"Peak throughput: {metrics_df['throughput'].max():.2f} ops/sec")
print(f"Maximum latency: {metrics_df['avg_latency_ms'].max():.2f} ms")

if not failure_point.empty:
    print(f"Failure detected at workload: {failure_point.iloc[0]['workload']} requests")
    print(f"Failure latency: {failure_point.iloc[0]['avg_latency_ms']:.2f} ms")
    print(f"Failure throughput: {failure_point.iloc[0]['throughput']:.2f} ops/sec")

print("\nPhase transitions:")
for _, phase in phases_df.iterrows():
    print(f"  {phase['phase']}: {phase['timestamp']} (workload: {phase['workload']})")
`

	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		log.Printf("Failed to create visualization script: %v", err)
	} else {
		log.Printf("Visualization script created: %s", scriptPath)
		log.Printf("Run 'python3 %s' to generate graphs", scriptPath)
	}
}