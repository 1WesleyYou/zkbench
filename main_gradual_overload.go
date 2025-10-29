package gradual

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	
	zkb "github.com/OrderLab/zkbench/bench"
)

// var (
// 	conf      = flag.String("conf", "bench_gradual_overload.conf", "Benchmark configuration file")
// 	outprefix = flag.String("outprefix", "zkresult", "Benchmark stat filename prefix")
// 	visualize = flag.Bool("viz", true, "Generate visualization data")
// )

// type logWriter struct {
// }

// func (writer logWriter) Write(bytes []byte) (int, error) {
// 	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " " + string(bytes))
// }

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
	
	// // Generate visualization script if requested
	// if *visualize {
	// 	generateVisualizationScript(prefix)
	// }
	
	// Cleanup if configured
	if b.Cleanup {
		b.Done()
	}
}

// Visualization script was extracted to a standalone Python file
// at tools/zkbench_haoran/visualize_gradual_overload.py.
// The Go-side generator was removed per request.