package bench

import (
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type BenchType uint32

const (
	WARM_UP BenchType = 1 << iota
	READ              = 1 << iota
	WRITE             = 1 << iota
	CREATE            = 1 << iota
	DELETE            = 1 << iota
)

type Request struct {
	key   string
	value []byte
}

type ReqHandler func(c *Client, r *Request) error
type ReqGenerator func(iter int64) *Request

type Benchmark struct {
	clients     []*Client
	root_client *Client
	initialized bool
	BenchConfig
}

func (self BenchType) String() string {
	switch self {
	case WARM_UP:
		return "WARM_UP"
	case READ:
		return "READ"
	case WRITE:
		return "WRITE"
	case CREATE:
		return "CREATE"
	default:
		return "UNKNOWN"
	}
}

func (self *Benchmark) Init() {
	clients, err := NewClients(self.Servers, self.Endpoints, self.NClients, self.Namespace)
	if err != nil {
		log.Fatal("Error:", err)
	}
	self.clients = clients
	if len(self.Servers) > 0 {
		self.root_client, _ = NewClient("root", self.Servers[0], self.Endpoints[0], self.Namespace)
		err := self.root_client.Setup()
		if err != nil {
			self.root_client.Log("error in initializing root client: %v", err)
		}
	} else {
		self.root_client = nil
	}
	for _, client := range self.clients {
		err := client.Setup()
		if err != nil {
			client.Log("error in initializing client %s: %v", client.Id, err)
			// log.Fatal(err)
		}
	}

	self.initialized = true
}

func (self *Benchmark) Run(outprefix string, raw bool) {
	if !self.initialized {
		log.Fatal("Must initialize benchmark first")
	}
	summaryf, err := os.OpenFile(outprefix+"summary.dat", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	summaryf.WriteString("client_id,bench_test,operations,errors,average_latency,min_latency,max_latency,total_latency,throughput\n")
  var rawf *os.File
  if raw {
    rawf, err = os.OpenFile(outprefix+"raw.dat", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
      panic(err)
    }
	  rawf.WriteString("client_id,bench_test,time,op_id,error,latency\n")
  }
	self.runBench(WARM_UP, 1, summaryf, rawf)
	if self.Type&CREATE != 0 {
		self.runBench(CREATE, 1, summaryf, rawf)
	}
	if self.Type&WRITE != 0 {
		self.runBench(WRITE, 1, summaryf, rawf)
	}
	if self.Type&READ != 0 {
		self.runBench(READ, 1, summaryf, rawf)
	}
	if self.Type&WRITE != 0 {
		self.runBench(WRITE, 2, summaryf, rawf)
		self.runBench(WRITE, 3, summaryf, rawf)
	}
	summaryf.Close()
  if rawf != nil {
	  rawf.Close()
  }
}

func (self *Benchmark) processRequests(client *Client, btype BenchType, same bool, generator ReqGenerator, handler ReqHandler) *BenchStat {
	var req *Request
	var stat BenchStat
	stat.Latencies = make([]BenchLatency, self.NRequests)
	if same {
		req = generator(-1)
	}
	bstr := btype.String()
  stat.StartTime = time.Now()
	for i := int64(0); i < self.NRequests; i++ {
		stat.Ops++
		if !same {
			req = generator(i)
		}
		begin := time.Now()
		err := handler(client, req)
		d := time.Since(begin)
    stat.Latencies[i].Start = begin
		if err != nil {
			stat.Errors++
			client.Log("error in processing %s request for key %s: %v", bstr, req.key, err)
			if err == zk.ErrNoServer {
				client.Reconnect()
			}
      stat.Latencies[i].Latency = -1
		} else {
      stat.Latencies[i].Latency = d
      if i == 0 || d < stat.MinLatency {
        stat.MinLatency = d
      }
      if i == 0 || d > stat.MaxLatency {
        stat.MaxLatency = d
      }
      stat.TotalLatency += d
    }
	}
  stat.EndTime = time.Now()
	stat.AvgLatency = stat.TotalLatency / time.Duration(stat.Ops)
	stat.Throughput = float64(stat.Ops) / stat.TotalLatency.Seconds()
	return &stat
}

func (self *Benchmark) runBench(btype BenchType, run int, statf *os.File, rawf *os.File) {
	var wg sync.WaitGroup
	key := sameKey(self.KeySizeBytes)
	val := randBytes(self.ValueSizeBytes)
	var empty []byte
	clientStats := make([]*BenchStat, self.NClients)
	for cid := range self.clients {
		var handler ReqHandler
		var generator ReqGenerator
		switch btype {
		case WARM_UP:
			generator = func(iter int64) *Request { return &Request{} }
			handler = func(c *Client, r *Request) error {
				_, _, err := c.Read(r.key)
				return err
			}
		case READ:
			if self.SameKey {
				generator = func(iter int64) *Request { return &Request{key, empty} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
			}
			handler = func(c *Client, r *Request) error {
				_, _, err := c.Read(r.key)
				return err
			}
		case WRITE:
			if self.SameKey {
				generator = func(iter int64) *Request { return &Request{key, val} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), val} }
			}
			handler = func(c *Client, r *Request) error {
				return c.Write(r.key, r.value)
			}
		case CREATE:
			if self.SameKey {
				generator = func(iter int64) *Request { return &Request{key, empty} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
			}
			handler = func(c *Client, r *Request) error {
				return c.Create(r.key, r.value)
			}
		case DELETE:
			if self.SameKey {
				generator = func(iter int64) *Request { return &Request{key, empty} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
			}
			handler = func(c *Client, r *Request) error {
				return c.Delete(r.key)
			}
		}
		wg.Add(1)
		go func(cid int, generator ReqGenerator, handler ReqHandler) {
			client := self.clients[cid]
			stat := self.processRequests(client, btype, self.SameKey, generator, handler)
			client.Log("done bench %s", btype.String())
			clientStats[cid] = stat
			wg.Done()
		}(cid, generator, handler)
	}
	wg.Wait()
	bstr := fmt.Sprintf("%s.%d", btype.String(), run)
	for cid, stat := range clientStats {
		statf.WriteString(fmt.Sprintf("%d,%s,%d,%d,%d,%d,%d,%s,%f\n", cid, bstr, stat.Ops, stat.Errors, stat.AvgLatency.Nanoseconds(), stat.MinLatency.Nanoseconds(), stat.MaxLatency.Nanoseconds(), stat.TotalLatency.String(), stat.Throughput))
	}
  if rawf != nil {
	  for cid, stat := range clientStats {
      for opid, latency := range stat.Latencies {
        latency_error := 0
        if latency.Latency < 0 {
          latency_error = 1
        }
		    rawf.WriteString(fmt.Sprintf("%d,%s,%s,%d,%d,%d\n", cid, bstr, latency.Start.Format("15:04:05.00000"), opid, latency_error, latency.Latency.Nanoseconds()))
      }
    }
  }
}

func (self *Benchmark) SmokeTest() {
	for _, client := range self.clients {
		children, stat, _, err := client.Conn.ChildrenW(self.Namespace)
		if err != nil {
			log.Println(err)
			// panic(err)
		}
		client.Log("children: %+v; stat: %+v", children, stat)
	}
}

func (self *Benchmark) Done() {
	var client *Client
	var current []*Client = self.clients

	for i := 0; i < 3; i = i + 1 {
		var leftover []*Client
		for _, client = range current {
			client.Log("clean up")
			err := client.Cleanup()
			if err != nil {
				client.Log("error in clean up: %v", err)
				leftover = append(leftover, client)
			}
		}
		if len(leftover) == 0 {
			break
		}
		current = leftover
	}
	if self.root_client != nil {
		self.root_client.Log("clean up")
		err := self.root_client.Cleanup()
		if err != nil {
			self.root_client.Log("error in clean up root directory: %v", err)
		}
	}
}

func sameKey(size int64) string {
	return strings.Repeat("x", int(size))
}

func sequentialKey(size, num int64) string {
	txt := fmt.Sprintf("%d", num)
	if len(txt) > int(size) {
		return txt
	}
	delta := int(size) - len(txt)
	return strings.Repeat("0", delta) + txt
}

func randBytes(bytesN int64) []byte {
	// source: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
	const (
		letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	src := mrand.NewSource(time.Now().UnixNano())
	b := make([]byte, bytesN)
	for i, cache, remain := bytesN-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return b
}
