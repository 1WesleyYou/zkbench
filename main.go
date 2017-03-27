package main

import (
	"flag"
	"fmt"
	"os"

	zkb "zkbench/bench"
	zkc "zkbench/config"
)

var (
	conf = flag.String("conf", "bench.conf", "Benchmark configuration file")
)

func main() {
	flag.Parse()
	config, err := zkc.ParseConfig(*conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fail to parse config: %v\n", err)
		os.Exit(1)
	}
	var b zkb.Benchmark
	b.Config = config
	b.Init()
	// b.SmokeTest()
	b.Run()
	// b.Done()
}
