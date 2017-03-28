package main

import (
	"flag"
	"fmt"
	"os"

	zkb "zkbench/bench"
)

var (
	conf = flag.String("conf", "bench.conf", "Benchmark configuration file")
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
	b.Run()
	b.Done()
}
