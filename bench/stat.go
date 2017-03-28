package bench

import (
	"time"
)

type BenchStat struct {
	Ops          int64
	Errors       int64
	Latencies    []time.Duration
	MinLatency   time.Duration
	MaxLatency   time.Duration
	AvgLatency   time.Duration
	TotalLatency time.Duration
	Throughput   float64
}
