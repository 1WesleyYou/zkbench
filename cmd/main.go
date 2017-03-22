package main

import (
	"flag"
	"fmt"
	"os"

	"zkbench"
	"zkbench/bench"
)

var (
	conf = flag.String("conf", "bench.conf", "Benchmark configuration file")
)

func main() {
	flag.Parse()
	config, err := zkbench.ParseConfig(*conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fail to parse config: %v\n", err)
		os.Exit(1)
	}
	var b bench.Benchmark
	b.Config = config
	b.Init()
	b.SmokeTest()
}
