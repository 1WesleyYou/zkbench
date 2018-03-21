package bench

import (
	"time"
)

type BenchLatency struct {
  Start     time.Time
  Latency   time.Duration
}

type BenchStat struct {
	Ops          int64
	Errors       int64
  StartTime    time.Time
  EndTime      time.Time
	Latencies    []BenchLatency
	MinLatency   time.Duration
	MaxLatency   time.Duration
	AvgLatency   time.Duration
	TotalLatency time.Duration
	Throughput   float64
}
