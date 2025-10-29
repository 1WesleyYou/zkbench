# Gradual Overload Test Implementation for Distributed Systems

## Overview

This implementation provides a comprehensive testing framework for identifying critical failure points in distributed systems (specifically ZooKeeper) by gradually increasing workload. The test follows the pattern:

**|Init|Warmup|Load Increase|Failure|Mitigation|**

## Architecture

### Core Components

1. **bench_gradual_overload.go** - Main implementation of the gradual overload benchmark
   - Extends the existing `Benchmark` struct with gradual load capabilities
   - Implements phase-based testing with automatic failure detection
   - Records detailed metrics and phase transitions

2. **main_gradual_overload.go** - Entry point for the gradual overload test
   - Parses configuration and command-line parameters
   - Generates visualization scripts automatically

3. **Configuration Files**
   - `bench_gradual_overload.conf` - Test configuration
   - Customizable parameters for workload patterns

4. **Execution Scripts**
   - `run_gradual_overload_test.sh` - Shell script for running tests
   - `Makefile` - Build and execution automation

## Test Phases

### 1. INIT Phase
- Establishes baseline metrics with minimal load
- Determines baseline latency and initial throughput
- Duration: Configurable stabilization time

### 2. WARMUP Phase
- Gradually increases load to warm up the system
- Allows caches, connection pools, and JIT compilation to optimize
- Default: 5 steps with incremental load increase

### 3. LOAD_INCREASE Phase
- Systematically increases workload to find critical point
- Monitors for failure conditions:
  - Latency exceeding threshold (default: 50ms)
  - Throughput drop > 30% from peak
  - Error rate > 10%
- Continues until failure or max workload reached

### 4. FAILURE Phase
- Triggered when failure conditions are detected
- Records the critical workload value
- Marks exact timestamp of failure

### 5. MITIGATION Phase
- Automatically reduces load to 70% of failure point
- Applies mitigation strategies (integrates with your agent)
- Verifies system recovery

## Key Features

### Automatic Failure Detection
```go
func (gb *GradualOverloadBenchmark) detectFailure(current, previous StepMetrics) bool {
    // Check latency threshold
    if current.AvgLatency > gb.Config.LatencyThreshold {
        return true
    }
    
    // Check throughput drop from peak
    throughputDropPercent := (1.0 - current.Throughput/gb.PeakThroughput) * 100
    if throughputDropPercent > gb.Config.ThroughputDrop {
        return true
    }
    
    // Check for significant error rate
    errorRate := float64(current.Errors) / float64(current.TotalRequests)
    if errorRate > 0.1 { // More than 10% errors
        return true
    }
    
    return false
}
```

### Integration with Monitoring System
- Creates markers in `../../agent/metrics/` directory:
  - `main_injection_timestamp.txt` - Marks start of main workload
  - `mitigation_trigger.txt` - Marks when mitigation is needed
- These files can trigger your AI agent for automatic mitigation

### Comprehensive Metrics Collection
- Throughput (ops/sec)
- Average, Max, and P99 latency
- Error rates
- Phase transitions with timestamps
- Per-second throughput tracking

## Usage

### Basic Usage

```bash
# Using Makefile
make bench-gradual

# With custom parameters
make bench-gradual MAX_REQUESTS=10000 LATENCY_THRESHOLD=100

# Direct execution
./run_gradual_overload_test.sh
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| INITIAL_REQUESTS | Starting requests per second | 50 |
| MAX_REQUESTS | Maximum requests to test | 5000 |
| STEP_SIZE | Request increase per step | 100 |
| STEP_DURATION | Duration of each step (seconds) | 10 |
| WARMUP_STEPS | Number of warmup steps | 5 |
| LATENCY_THRESHOLD | Latency failure threshold (ms) | 50.0 |
| THROUGHPUT_DROP | Throughput drop threshold (%) | 30.0 |

### Advanced Configuration

Edit `bench_gradual_overload.conf`:

```conf
namespace = zkGradualTest
requests = 1000          # Base request count
clients = 100            # Concurrent clients
parallelism = 20         # Request parallelism
read_percent = 0.7       # Read percentage for mixed workload
write_percent = 0.3      # Write percentage
```

## Output Files

The test generates several output files in the `results/` directory:

1. **gradual_overload_metrics.csv** - Detailed metrics for each step
2. **phase_transitions.csv** - Phase change timestamps
3. **test_summary.txt** - Human-readable summary
4. **visualize.py** - Auto-generated Python visualization script

## Visualization

The test automatically generates a Python script for visualization:

```bash
# Run visualization (requires pandas, matplotlib)
python3 results/zkresult-gradual-*-visualize.py
```

This creates a multi-panel graph showing:
- Throughput over time with phase markers
- Latency trends (average and P99)
- Workload progression
- Color-coded phase backgrounds

## Integration with Your System

### 1. Prometheus Integration
The test works with your existing Prometheus setup:
- Metrics are compatible with PromQL queries
- Can trigger alerts based on phase transitions

### 2. AI Agent Integration
The test creates marker files that your AI agent can monitor:
```python
# In your agent code
def check_for_mitigation():
    if os.path.exists("agent/metrics/mitigation_trigger.txt"):
        with open("agent/metrics/mitigation_trigger.txt") as f:
            data = f.read()
            # Parse workload value and trigger mitigation
            workload = extract_workload(data)
            apply_mitigation(workload)
```

### 3. HAProxy Integration
Works with your HAProxy configuration:
- Tests through load balancer (10.10.1.4:2181)
- Can trigger HAProxy-based mitigation (connection limits, weight adjustment)

## Example Test Run

```bash
$ make bench-gradual
=== Running Gradual Overload Test ====
Test Configuration:
  Initial requests: 50
  Max requests: 5000
  Step size: 100
  Latency threshold: 50.0ms
  Throughput drop threshold: 30.0%

Running gradual overload test...
2024-10-29T10:15:00.000Z Phase: INIT - Establishing baseline with 50 requests
2024-10-29T10:15:10.000Z Baseline established - Latency: 12.5ms, Throughput: 450.2 ops/s
2024-10-29T10:15:15.000Z Phase: WARMUP - Gradually increasing load over 5 steps
2024-10-29T10:16:00.000Z Warmup complete - Peak throughput: 1850.5 ops/s
2024-10-29T10:16:05.000Z Phase: LOAD_INCREASE - Increasing load to find critical point
2024-10-29T10:18:30.000Z FAILURE DETECTED at workload 2300!
  Latency: 85.3ms (threshold: 50.0ms)
  Throughput: 1100.2 ops/s (40.5% drop from peak 1850.5)
2024-10-29T10:18:35.000Z Phase: MITIGATION - Applying mitigation strategies
2024-10-29T10:19:00.000Z Mitigation SUCCESSFUL - System recovered
  Latency: 28.4ms, Throughput: 1520.8 ops/s

Test Summary:
  Critical Failure Point: 2300 requests
  Safe Operating Range: 50-2200 requests
  Peak Throughput: 1850.5 ops/s
  Baseline Latency: 12.5 ms
```

## Troubleshooting

### Common Issues

1. **Build Errors**
   ```bash
   # Install dependencies
   go get github.com/samuel/go-zookeeper/zk
   ```

2. **Visualization Errors**
   ```bash
   # Install Python dependencies
   pip3 install pandas matplotlib numpy
   ```

3. **Permission Errors**
   ```bash
   # Make scripts executable
   chmod +x run_gradual_overload_test.sh
   ```

## Extending the Test

### Adding Custom Failure Conditions

Edit `detectFailure` function in `bench_gradual_overload.go`:

```go
// Add custom metric check
if current.CustomMetric > threshold {
    return true
}
```

### Integrating with New Mitigation Tools

Edit `runMitigationPhase` function:

```go
// Add custom mitigation
if gb.Config.EnableChaosBlades {
    applyChaosBladesMitigation()
}
```

### Custom Workload Patterns

Modify request generation in `runWorkloadStep`:

```go
// Custom request pattern
generator := func(iter int64) *Request {
    // Your custom logic here
    return customRequest(iter)
}
```

## Best Practices

1. **Start Conservative**: Begin with low `INITIAL_REQUESTS` to establish stable baseline
2. **Adequate Step Duration**: Use at least 10 seconds per step for stability
3. **Multiple Runs**: Run test multiple times to identify consistent failure points
4. **Monitor Resources**: Watch CPU, memory, and network during tests
5. **Gradual Increases**: Use smaller `STEP_SIZE` for more precise failure detection

## Next Steps

1. **Integration with your AI Agent**: 
   - Modify agent to read phase markers
   - Implement automatic mitigation based on failure detection

2. **Custom Mitigation Strategies**:
   - Integrate with resilience4j for circuit breaking
   - Add ChaosBlade functions for complex scenarios

3. **Enhanced Monitoring**:
   - Export metrics to Prometheus
   - Create Grafana dashboards for real-time visualization

4. **Automated Testing Pipeline**:
   - Add to CI/CD pipeline
   - Create regression tests for failure points

## Support

For questions or issues with the implementation, refer to:
- The inline code documentation
- The generated test_summary.txt after each run
- The visualization graphs for patterns

This implementation provides a robust foundation for understanding your distributed system's failure characteristics and validating mitigation strategies.