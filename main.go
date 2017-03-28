package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	zkb "zkbench/bench"
)

var (
	conf      = flag.String("conf", "bench.conf", "Benchmark configuration file")
	outprefix = flag.String("outprefix", "zkbench", "Benchmark stat filename prefix")
)

func main() {
	flag.Parse()
	config, err := zkb.ParseConfig(*conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fail to parse config: %v\n", err)
		os.Exit(1)
	}
	b := new(zkb.Benchmark)
	b.BenchmarkConfig = *config
	b.Init()
	b.SmokeTest()
	current := time.Now()
	prefix := *outprefix + "-" + current.Format("2006-01-02-15_04_05") + "-"
	b.Run(prefix)
}
