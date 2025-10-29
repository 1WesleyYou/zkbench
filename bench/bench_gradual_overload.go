package bench

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"math"
)

// GradualOverloadConfig holds configuration for gradual overload testing
type GradualOverloadConfig struct {
	InitialRequests   int64   // Starting number of requests per second
	MaxRequests       int64   // Maximum requests to try
	StepSize          int64   // How much to increase requests each step
	StepDuration      int     // Duration of each step in seconds
	WarmupSteps       int     // Number of steps for warmup phase
	LatencyThreshold  float64 // Latency threshold in ms to detect failure
	ThroughputDrop    float64 // Percentage drop in throughput to detect failure
	StabilizationTime int     // Time to wait for stabilization in seconds
}

// PhaseMarker represents different test phases
type PhaseMarker struct {
	Phase     string
	Timestamp time.Time
	Workload  int64
	AvgLatency float64
	Throughput float64
}

// GradualOverloadBenchmark extends the base Benchmark for gradual load testing
type GradualOverloadBenchmark struct {
	*Benchmark
	Config      GradualOverloadConfig
	PhaseMarkers []PhaseMarker
	CurrentPhase string
	FailurePoint int64
	PeakThroughput float64
	BaselineLatency float64
	metricsFile *os.File
	phaseFile   *os.File
	mu          sync.Mutex
}

// NewGradualOverloadBenchmark creates a new gradual overload benchmark
func NewGradualOverloadBenchmark(bench *Benchmark, config GradualOverloadConfig) *GradualOverloadBenchmark {
	return &GradualOverloadBenchmark{
		Benchmark:    bench,
		Config:       config,
		PhaseMarkers: make([]PhaseMarker, 0),
		CurrentPhase: "INIT",
	}
}

// RunGradualOverload executes the gradual overload test
func (gb *GradualOverloadBenchmark) RunGradualOverload(outprefix string) error {
	// Initialize output files
	if err := gb.initOutputFiles(outprefix); err != nil {
		return fmt.Errorf("failed to initialize output files: %v", err)
	}
	defer gb.closeOutputFiles()

	// Initialize benchmark
	if !gb.initialized {
		gb.Init()
	}

	log.Printf("Starting Gradual Overload Test")
	log.Printf("Configuration: Initial=%d, Max=%d, Step=%d, StepDuration=%ds",
		gb.Config.InitialRequests, gb.Config.MaxRequests, 
		gb.Config.StepSize, gb.Config.StepDuration)

	// Phase 1: INIT - Establish baseline
	if err := gb.runInitPhase(); err != nil {
		return fmt.Errorf("init phase failed: %v", err)
	}

	// Phase 2: WARM-UP - Gradual increase
	if err := gb.runWarmupPhase(); err != nil {
		return fmt.Errorf("warmup phase failed: %v", err)
	}

	// Phase 3: LOAD INCREASE - Find failure point
	failureDetected, err := gb.runLoadIncreasePhase()
	if err != nil {
		return fmt.Errorf("load increase phase failed: %v", err)
	}

	// Phase 4: FAILURE or STABLE
	if failureDetected {
		log.Printf("Failure detected at workload: %d requests", gb.FailurePoint)
		gb.markPhase("FAILURE", gb.FailurePoint)
		
		// Phase 5: MITIGATION
		if err := gb.runMitigationPhase(); err != nil {
			return fmt.Errorf("mitigation phase failed: %v", err)
		}
	} else {
		log.Printf("System remained stable up to maximum workload")
		gb.markPhase("STABLE", gb.Config.MaxRequests)
	}

	// Generate summary report
	gb.generateSummaryReport(outprefix)
	
	return nil
}

// runInitPhase establishes baseline metrics
func (gb *GradualOverloadBenchmark) runInitPhase() error {
	gb.markPhase("INIT", gb.Config.InitialRequests)
	log.Printf("Phase: INIT - Establishing baseline with %d requests", gb.Config.InitialRequests)
	
	// Run initial low workload to establish baseline
	metrics := gb.runWorkloadStep(gb.Config.InitialRequests, gb.Config.StepDuration)
	gb.BaselineLatency = metrics.AvgLatency
	gb.PeakThroughput = metrics.Throughput
	
	gb.recordMetrics("INIT", gb.Config.InitialRequests, metrics)
	log.Printf("Baseline established - Latency: %.2fms, Throughput: %.2f ops/s", 
		gb.BaselineLatency, gb.PeakThroughput)
	
	// Stabilization period
	time.Sleep(time.Duration(gb.Config.StabilizationTime) * time.Second)
	
	return nil
}

// runWarmupPhase gradually increases load during warmup
func (gb *GradualOverloadBenchmark) runWarmupPhase() error {
	gb.markPhase("WARMUP", gb.Config.InitialRequests)
	log.Printf("Phase: WARMUP - Gradually increasing load over %d steps", gb.Config.WarmupSteps)
	
	currentLoad := gb.Config.InitialRequests
	stepIncrement := gb.Config.StepSize / int64(gb.Config.WarmupSteps)
	if stepIncrement < 1 {
		stepIncrement = 1
	}
	
	for i := 0; i < gb.Config.WarmupSteps; i++ {
		currentLoad += stepIncrement
		log.Printf("Warmup step %d/%d: %d requests", i+1, gb.Config.WarmupSteps, currentLoad)
		
		metrics := gb.runWorkloadStep(currentLoad, gb.Config.StepDuration)
		gb.recordMetrics("WARMUP", currentLoad, metrics)
		
		// Update peak throughput if higher
		if metrics.Throughput > gb.PeakThroughput {
			gb.PeakThroughput = metrics.Throughput
		}
		
		// Small delay between steps
		time.Sleep(2 * time.Second)
	}
	
	log.Printf("Warmup complete - Peak throughput: %.2f ops/s", gb.PeakThroughput)
	time.Sleep(time.Duration(gb.Config.StabilizationTime) * time.Second)
	
	return nil
}

// runLoadIncreasePhase increases load until failure or max
func (gb *GradualOverloadBenchmark) runLoadIncreasePhase() (bool, error) {
	gb.markPhase("LOAD_INCREASE", 0)
	log.Printf("Phase: LOAD_INCREASE - Increasing load to find critical point")
	
	currentLoad := gb.Config.InitialRequests + (gb.Config.StepSize * int64(gb.Config.WarmupSteps))
	previousMetrics := StepMetrics{
		Throughput: gb.PeakThroughput,
		AvgLatency: gb.BaselineLatency,
	}
	
	for currentLoad <= gb.Config.MaxRequests {
		log.Printf("Testing workload: %d requests", currentLoad)
		
		metrics := gb.runWorkloadStep(currentLoad, gb.Config.StepDuration)
		gb.recordMetrics("LOAD_INCREASE", currentLoad, metrics)
		
		// Check for failure conditions
		if gb.detectFailure(metrics, previousMetrics) {
			gb.FailurePoint = currentLoad
			log.Printf("FAILURE DETECTED at workload %d!", currentLoad)
			log.Printf("  Latency: %.2fms (threshold: %.2fms)", 
				metrics.AvgLatency, gb.Config.LatencyThreshold)
			log.Printf("  Throughput: %.2f ops/s (%.2f%% drop from peak %.2f)", 
				metrics.Throughput, 
				(1.0 - metrics.Throughput/gb.PeakThroughput) * 100,
				gb.PeakThroughput)
			return true, nil
		}
		
		// Update peak if necessary
		if metrics.Throughput > gb.PeakThroughput {
			gb.PeakThroughput = metrics.Throughput
		}
		
		previousMetrics = metrics
		currentLoad += gb.Config.StepSize
		
		// Brief stabilization between steps
		time.Sleep(3 * time.Second)
	}
	
	return false, nil
}

// runMitigationPhase attempts to mitigate the detected failure
func (gb *GradualOverloadBenchmark) runMitigationPhase() error {
	gb.markPhase("MITIGATION", gb.FailurePoint)
	log.Printf("Phase: MITIGATION - Applying mitigation strategies")
	
	// Write mitigation marker for the monitoring system
	gb.writeMitigationMarker()
	
	// Reduce load to 70% of failure point
	mitigatedLoad := int64(float64(gb.FailurePoint) * 0.7)
	log.Printf("Reducing load to %d requests (70%% of failure point)", mitigatedLoad)
	
	// Wait for mitigation to take effect
	time.Sleep(time.Duration(gb.Config.StabilizationTime) * time.Second)
	
	// Test with reduced load
	metrics := gb.runWorkloadStep(mitigatedLoad, gb.Config.StepDuration*2)
	gb.recordMetrics("MITIGATION", mitigatedLoad, metrics)
	
	// Check if mitigation was successful
	if metrics.AvgLatency < gb.Config.LatencyThreshold &&
	   metrics.Throughput > gb.PeakThroughput*0.8 {
		log.Printf("Mitigation SUCCESSFUL - System recovered")
		log.Printf("  Latency: %.2fms, Throughput: %.2f ops/s", 
			metrics.AvgLatency, metrics.Throughput)
		gb.markPhase("RECOVERED", mitigatedLoad)
	} else {
		log.Printf("Mitigation PARTIAL - System partially recovered")
		gb.markPhase("PARTIAL_RECOVERY", mitigatedLoad)
	}
	
	return nil
}

// StepMetrics holds metrics for a single workload step
type StepMetrics struct {
	Throughput     float64
	AvgLatency     float64
	MaxLatency     float64
	P99Latency     float64
	Errors         int64
	TotalRequests  int64
}

// runWorkloadStep executes a single workload step and returns metrics
func (gb *GradualOverloadBenchmark) runWorkloadStep(requestsPerSec int64, duration int) StepMetrics {
	var metrics StepMetrics
	totalRequests := requestsPerSec * int64(duration)
	
	// Calculate requests per client
	requestsPerClient := totalRequests / int64(len(gb.clients))
	if requestsPerClient < 1 {
		requestsPerClient = 1
	}
	
	// Use multiple goroutines to generate load
	parallelism := int(math.Min(float64(gb.Parallelism), float64(requestsPerSec/100)))
	if parallelism < 1 {
		parallelism = 1
	}
	
	var wg sync.WaitGroup
	var mu sync.Mutex
	aggregatedStats := &BenchStat{}
	
	// Distribute load across clients
	for _, client := range gb.clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			
			// Create request generator
			generator := func(iter int64) *Request {
				key := sequentialKey(gb.KeySizeBytes, iter)
				val := randBytes(nil, gb.ValueSizeBytes)
				return &Request{key, val}
			}
			
			// Create handler (mixed read/write)
			handler := func(client *Client, r *Request) error {
				if iter%2 == 0 {
					_, _, err := client.Read(r.key)
					return err
				}
				return client.Write(r.key, r.value)
			}
			
			// Process requests with rate limiting
			gb.processRequestsWithRateLimit(c, "STEP", requestsPerClient, 
				parallelism, false, false, generator, handler, duration)
			
			// Aggregate stats
			if c.Stat != nil {
				mu.Lock()
				aggregatedStats.Merge(c.Stat)
				mu.Unlock()
			}
		}(client)
	}
	
	wg.Wait()
	
	// Calculate metrics from aggregated stats
	if aggregatedStats.Ops > 0 {
		metrics.TotalRequests = aggregatedStats.Ops
		metrics.Throughput = float64(aggregatedStats.Ops) / float64(duration)
		metrics.AvgLatency = float64(aggregatedStats.AvgLatency.Nanoseconds()) / 1e6 // Convert to ms
		metrics.MaxLatency = float64(aggregatedStats.MaxLatency.Nanoseconds()) / 1e6
		metrics.P99Latency = float64(aggregatedStats.NinetyNinethLatency) / 1e6
		metrics.Errors = aggregatedStats.Errors
	}
	
	return metrics
}

// processRequestsWithRateLimit is similar to processRequests but with rate limiting
func (gb *GradualOverloadBenchmark) processRequestsWithRateLimit(client *Client, optype string,
	nrequests int64, parallelism int, random bool, same bool,
	generator ReqGenerator, handler ReqHandler, duration int) {
	
	// Use the existing processRequests with timing control
	startTime := time.Now()
	endTime := startTime.Add(time.Duration(duration) * time.Second)
	
	// Reset client stats
	client.Stat = &BenchStat{
		OpType: optype,
		Latencies: make([]BenchLatency, 0, nrequests),
	}
	
	requestsPerSecond := nrequests / int64(duration)
	ticker := time.NewTicker(time.Second / time.Duration(requestsPerSecond))
	defer ticker.Stop()
	
	var reqCount int64
	for time.Now().Before(endTime) && reqCount < nrequests {
		select {
		case <-ticker.C:
			req := generator(reqCount)
			begin := time.Now()
			err := handler(client, req)
			latency := time.Since(begin)
			
			client.Stat.Ops++
			if err != nil {
				client.Stat.Errors++
			} else {
				client.Stat.TotalLatency += latency
				if client.Stat.MinLatency == 0 || latency < client.Stat.MinLatency {
					client.Stat.MinLatency = latency
				}
				if latency > client.Stat.MaxLatency {
					client.Stat.MaxLatency = latency
				}
			}
			reqCount++
			
		case <-time.After(endTime.Sub(time.Now())):
			break
		}
	}
	
	if client.Stat.Ops > 0 {
		client.Stat.AvgLatency = client.Stat.TotalLatency / time.Duration(client.Stat.Ops)
		client.Stat.Throughput = float64(client.Stat.Ops) / time.Since(startTime).Seconds()
	}
}

// detectFailure checks if failure conditions are met
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

// markPhase records a phase transition
func (gb *GradualOverloadBenchmark) markPhase(phase string, workload int64) {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	
	gb.CurrentPhase = phase
	marker := PhaseMarker{
		Phase:     phase,
		Timestamp: time.Now(),
		Workload:  workload,
	}
	gb.PhaseMarkers = append(gb.PhaseMarkers, marker)
	
	// Write to phase file
	if gb.phaseFile != nil {
		timestamp := marker.Timestamp.Format("2006-01-02 15:04:05.000")
		fmt.Fprintf(gb.phaseFile, "%s,%s,%d\n", phase, timestamp, workload)
		gb.phaseFile.Sync()
	}
	
	log.Printf("=== PHASE TRANSITION: %s at workload %d ===", phase, workload)
}

// recordMetrics writes metrics to file
func (gb *GradualOverloadBenchmark) recordMetrics(phase string, workload int64, metrics StepMetrics) {
	if gb.metricsFile != nil {
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		fmt.Fprintf(gb.metricsFile, "%s,%s,%d,%.2f,%.2f,%.2f,%.2f,%d,%d\n",
			timestamp, phase, workload,
			metrics.Throughput, metrics.AvgLatency, metrics.MaxLatency,
			metrics.P99Latency, metrics.Errors, metrics.TotalRequests)
		gb.metricsFile.Sync()
	}
}

// initOutputFiles initializes output files for metrics and phases
func (gb *GradualOverloadBenchmark) initOutputFiles(prefix string) error {
	var err error
	
	// Create metrics file
	metricsPath := prefix + "gradual_overload_metrics.csv"
	gb.metricsFile, err = os.Create(metricsPath)
	if err != nil {
		return err
	}
	gb.metricsFile.WriteString("timestamp,phase,workload,throughput,avg_latency_ms,max_latency_ms,p99_latency_ms,errors,total_requests\n")
	
	// Create phase transitions file
	phasePath := prefix + "phase_transitions.csv"
	gb.phaseFile, err = os.Create(phasePath)
	if err != nil {
		gb.metricsFile.Close()
		return err
	}
	gb.phaseFile.WriteString("phase,timestamp,workload\n")
	
	return nil
}

// closeOutputFiles closes all output files
func (gb *GradualOverloadBenchmark) closeOutputFiles() {
	if gb.metricsFile != nil {
		gb.metricsFile.Close()
	}
	if gb.phaseFile != nil {
		gb.phaseFile.Close()
	}
}

// writeMitigationMarker writes a marker for the mitigation system
func (gb *GradualOverloadBenchmark) writeMitigationMarker() {
	exePath, err := os.Executable()
	if err != nil {
		return
	}
	exeDir := filepath.Dir(exePath)
	outPath := filepath.Clean(filepath.Join(exeDir, "../../agent/metrics/mitigation_trigger.txt"))
	
	os.MkdirAll(filepath.Dir(outPath), 0755)
	
	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	
	now := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Fprintf(f, "mit,%s,workload=%d\n", now, gb.FailurePoint)
}

// generateSummaryReport creates a summary of the test
func (gb *GradualOverloadBenchmark) generateSummaryReport(prefix string) {
	reportPath := prefix + "test_summary.txt"
	f, err := os.Create(reportPath)
	if err != nil {
		log.Printf("Failed to create summary report: %v", err)
		return
	}
	defer f.Close()
	
	fmt.Fprintln(f, "=== Gradual Overload Test Summary ===")
	fmt.Fprintln(f)
	fmt.Fprintf(f, "Test Configuration:\n")
	fmt.Fprintf(f, "  Initial Requests: %d\n", gb.Config.InitialRequests)
	fmt.Fprintf(f, "  Max Requests: %d\n", gb.Config.MaxRequests)
	fmt.Fprintf(f, "  Step Size: %d\n", gb.Config.StepSize)
	fmt.Fprintf(f, "  Step Duration: %d seconds\n", gb.Config.StepDuration)
	fmt.Fprintf(f, "  Latency Threshold: %.2f ms\n", gb.Config.LatencyThreshold)
	fmt.Fprintf(f, "  Throughput Drop Threshold: %.2f%%\n", gb.Config.ThroughputDrop)
	fmt.Fprintln(f)
	
	fmt.Fprintf(f, "Test Results:\n")
	fmt.Fprintf(f, "  Baseline Latency: %.2f ms\n", gb.BaselineLatency)
	fmt.Fprintf(f, "  Peak Throughput: %.2f ops/s\n", gb.PeakThroughput)
	if gb.FailurePoint > 0 {
		fmt.Fprintf(f, "  Critical Failure Point: %d requests\n", gb.FailurePoint)
		fmt.Fprintf(f, "  Safe Operating Range: %d-%d requests\n", 
			gb.Config.InitialRequests, gb.FailurePoint-gb.Config.StepSize)
	} else {
		fmt.Fprintf(f, "  No failure detected up to %d requests\n", gb.Config.MaxRequests)
	}
	fmt.Fprintln(f)
	
	fmt.Fprintf(f, "Phase Transitions:\n")
	for _, marker := range gb.PhaseMarkers {
		fmt.Fprintf(f, "  %s: %s (workload: %d)\n", 
			marker.Phase, 
			marker.Timestamp.Format("15:04:05"), 
			marker.Workload)
	}
	
	log.Printf("Test summary written to %s", reportPath)
}