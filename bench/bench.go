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
	FILL              = 1 << iota
	READ              = 1 << iota
	WRITE             = 1 << iota
	CREATE            = 1 << iota
	DELETE            = 1 << iota
	MIXED             = 1 << iota
)

const (
	ZIPF_SKEW = 1.3
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
	case FILL:
		return "FILL"
	case READ:
		return "READ"
	case WRITE:
		return "WRITE"
	case CREATE:
		return "CREATE"
	case DELETE:
		return "DELETE"
	case MIXED:
		return "MIXED"
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
		self.root_client, _ = NewClient(0, "root", self.Servers[0], self.Endpoints[0], self.Namespace)
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
		self.runBench(CREATE, 1, summaryf, rawf) // create key space
		self.runBench(FILL, 1, summaryf, rawf)   // fill in data
	}
	// runs only apply to the actual benchmark
	for i := 0; i < self.Runs; i++ {
		if self.Type&READ != 0 {
			self.runBench(READ, i+1, summaryf, rawf) // read
		}
		if self.Type&WRITE != 0 {
			self.runBench(WRITE, i+1, summaryf, rawf) // write
		}
		if self.Type&MIXED != 0 {
			self.runBench(MIXED, i+1, summaryf, rawf) // r/w
		}
	}
	summaryf.Close()
	if rawf != nil {
		rawf.Close()
	}
}

func (self *Benchmark) processRequests(client *Client, optype string, nrequests int64,
	parallelism int, random bool, same bool, generator ReqGenerator, handler ReqHandler) {

	var req *Request
	var stat BenchStat
	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}

	stat.OpType = optype
	stat.Latencies = make([]BenchLatency, nrequests)
	if same {
		req = generator(-1)
	}
	start := int64(0)
	end := start
	group := nrequests / int64(parallelism)
	if parallelism > 1 {
		client.AddChildren(parallelism)
	}
	reqf := func(client *Client, zipf *mrand.Zipf, start, end int64, parallel bool) {
		for j := start; j < end; j++ {
			if !same {
				if zipf != nil {
					var key int64 = int64(zipf.Uint64())
					// fmt.Printf("random key %d\n\n", key)
					req = generator(key)
				} else {
					req = generator(j)
				}
			}
			begin := time.Now()
			err := handler(client, req)
			d := time.Since(begin)
			if parallel {
				mutex.Lock()
			}
			stat.Ops++
			stat.Latencies[j].Start = begin
			if err != nil {
				stat.Errors++
				client.Log("error in processing %s request for key %s: %v", optype, req.key, err)
				if err == zk.ErrNoServer {
					client.Reconnect()
				}
				stat.Latencies[j].Latency = -1
			} else {
				stat.Latencies[j].Latency = d
				if j == 0 || d < stat.MinLatency {
					stat.MinLatency = d
				}
				if j == 0 || d > stat.MaxLatency {
					stat.MaxLatency = d
				}
				stat.TotalLatency += d
			}
			if parallel {
				mutex.Unlock()
			}
		}
		if parallel {
			wg.Done()
		}
	}
	stat.StartTime = time.Now()
	if parallelism > 1 {
		for p := 0; p < parallelism; p++ {
			// fmt.Printf("Launching parallel request group %d of %s\n", p, btype)
			if start >= nrequests {
				break
			}
			end = start + group
			if end > nrequests {
				end = nrequests // cannot exceed more than nrequests
			}
			wg.Add(1)
			c := client.GetChild(p)
			if c == nil {
				client.Log("failed to get child for parallel request group %d\n", p)
				c = client
			}
			var zipf *mrand.Zipf
			if random {
				rd := mrand.New(mrand.NewSource(time.Now().UnixNano()))
				zipf = mrand.NewZipf(rd, ZIPF_SKEW, 1.0, uint64(nrequests))
			}
			go reqf(c, zipf, start, end, true)
			start = end
		}
		wg.Wait()
		client.CloseChildren()
	} else {
		var zipf *mrand.Zipf
		if random {
			rd := mrand.New(mrand.NewSource(time.Now().UnixNano()))
			zipf = mrand.NewZipf(rd, ZIPF_SKEW, 1.0, uint64(nrequests))
		}
		reqf(client, zipf, 0, nrequests, false)
	}
	stat.EndTime = time.Now()
	stat.AvgLatency = stat.TotalLatency / time.Duration(stat.Ops)
	stat.Throughput = float64(stat.Ops) / stat.TotalLatency.Seconds()

	if client.Stat != nil {
		// if the client already has stats, merge the stat
		client.Stat.Merge(&stat)
	} else {
		// otherwise, directly use this stat
		client.Stat = &stat
	}
}

func (self *Benchmark) runBench(btype BenchType, run int, statf *os.File, rawf *os.File) {
	var empty []byte
	var wg sync.WaitGroup

	src := mrand.NewSource(time.Now().UnixNano())
	key := sameKey(self.KeySizeBytes)
	val := randBytes(src, self.ValueSizeBytes)
	fillVal := []byte("whosyourdaddy")

	// at most two concurrent request types (r/w)
	generators := make([]ReqGenerator, 2)
	handlers := make([]ReqHandler, 2)
	nrequests := make([]int64, 2)
	subtypes := make([]BenchType, 2)
	random := false
	concurrency := 1 // by default one outstanding request type
	parallelism := 1 // by default each request is sent synchronously

	switch btype {
	case WARM_UP:
		generators[0] = func(iter int64) *Request { return &Request{} }
		handlers[0] = func(c *Client, r *Request) error {
			_, _, err := c.Read(r.key)
			return err
		}
		nrequests[0] = self.NRequests / 10 // warm up n/10 iterations
	case READ:
		if self.SameKey {
			generators[0] = func(iter int64) *Request { return &Request{key, empty} }
		} else {
			generators[0] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
		}
		handlers[0] = func(c *Client, r *Request) error {
			_, _, err := c.Read(r.key)
			return err
		}
		if self.ReadPercent > 0 {
			nrequests[0] = int64(float64(self.ReadPercent) * float64(self.NRequests))
		} else {
			nrequests[0] = self.NRequests // full requests
		}
		// depending on if user specified random access
		random = self.RandomAccess
	case WRITE:
		if self.SameKey {
			generators[0] = func(iter int64) *Request { return &Request{key, val} }
		} else {
			generators[0] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), val} }
		}
		handlers[0] = func(c *Client, r *Request) error {
			return c.Write(r.key, r.value)
		}
		if self.WritePercent > 0 {
			nrequests[0] = int64(float64(self.WritePercent) * float64(self.NRequests))
		} else {
			nrequests[0] = self.NRequests // full requests
		}
		// depending on if user specified random access
		random = self.RandomAccess
	case CREATE:
		if self.SameKey {
			generators[0] = func(iter int64) *Request { return &Request{key, empty} }
		} else {
			generators[0] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
		}
		handlers[0] = func(c *Client, r *Request) error {
			return c.Create(r.key, r.value)
		}
		nrequests[0] = self.NRequests // full key space
	case FILL:
		if self.SameKey {
			generators[0] = func(iter int64) *Request { return &Request{key, fillVal} }
		} else {
			generators[0] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), fillVal} }
		}
		handlers[0] = func(c *Client, r *Request) error {
			return c.Write(r.key, r.value)
		}
		nrequests[0] = self.NRequests // full key space
	case DELETE:
		if self.SameKey {
			generators[0] = func(iter int64) *Request { return &Request{key, empty} }
		} else {
			generators[0] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
		}
		handlers[0] = func(c *Client, r *Request) error {
			return c.Delete(r.key)
		}
		nrequests[0] = self.NRequests // full requests
	case MIXED:
		if self.SameKey {
			generators[0] = func(iter int64) *Request { return &Request{key, empty} }
			generators[1] = func(iter int64) *Request { return &Request{key, val} }
		} else {
			generators[0] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), empty} }
			generators[1] = func(iter int64) *Request { return &Request{sequentialKey(self.KeySizeBytes, iter), val} }
		}
		handlers[0] = func(c *Client, r *Request) error {
			_, _, err := c.Read(r.key)
			return err
		}
		handlers[1] = func(c *Client, r *Request) error {
			return c.Write(r.key, r.value)
		}
		if self.ReadPercent > 0 {
			nrequests[0] = int64(float64(self.ReadPercent) * float64(self.NRequests))
		} else {
			nrequests[0] = self.NRequests // full requests
		}
		if self.WritePercent > 0 {
			nrequests[1] = int64(float64(self.WritePercent) * float64(self.NRequests))
		} else {
			nrequests[1] = self.NRequests // full requests
		}
		subtypes[0] = READ
		subtypes[1] = WRITE
		// depending on if user specified random access
		random = self.RandomAccess
		concurrency = 2
		parallelism = self.Parallelism
	}

	reqf := func(client *Client, nrequests int64, optype string, parallelims int, random bool, generator ReqGenerator, handler ReqHandler) {
		client.Log("start bench %s", optype)
		self.processRequests(client, optype, nrequests, parallelism, random, self.SameKey, generator, handler)
		client.Log("done bench %s", optype)
		wg.Done()
	}

	for _, client := range self.clients {
		// since each run of a benchmark type is independent
		// and that at the end of this function stat will be
		// saved, we should reset the stat each time
		client.Stat = nil
		if concurrency > 1 {
			// if the concurrency level is larger than 1
			// need to create multiple clients to launch concurrent requests
			// otherwise there will be data races
			client.AddChildren(concurrency)
			for i := 0; i < concurrency; i++ {
				child := client.GetChild(i)
				if child != nil {
					wg.Add(1)
					bstr := fmt.Sprintf("%s.%s.%d", btype.String(), subtypes[i].String(), run)
					go reqf(child, nrequests[i], bstr, parallelism, random, generators[i], handlers[i])
				}
			}
		} else {
			wg.Add(1)
			bstr := fmt.Sprintf("%s.%d", btype.String(), run)
			go reqf(client, nrequests[0], bstr, parallelism, random, generators[0], handlers[0])
		}
	}
	wg.Wait()

	// aggregate child request stats
	// then destroy child clients
	for _, client := range self.clients {
		if client.Children == nil {
			continue
		}
		for _, child := range client.Children {
			if child.Stat == nil {
				continue
			}
			if client.Stat != nil {
				client.Log("merge child stats")
				client.Stat.Merge(child.Stat)
			} else {
				client.Stat = child.Stat
				// reset the optype
				client.Stat.OpType = fmt.Sprintf("%s.%d", btype.String(), run)
			}
			child.Conn.Close()
			child.Conn = nil
		}
		client.Children = nil
	}

	// dump client stats
	for _, client := range self.clients {
		stat := client.Stat
		statf.WriteString(fmt.Sprintf("%d,%s,%d,%d,%d,%d,%d,%s,%f\n", client.Id, stat.OpType, stat.Ops,
			stat.Errors, stat.AvgLatency.Nanoseconds(), stat.MinLatency.Nanoseconds(),
			stat.MaxLatency.Nanoseconds(), stat.TotalLatency.String(), stat.Throughput))
	}
	if rawf != nil {
		for _, client := range self.clients {
			cid := client.Id
			stat := client.Stat
			for opid, latency := range stat.Latencies {
				latency_error := 0
				if latency.Latency < 0 {
					latency_error = 1
				}
				rawf.WriteString(fmt.Sprintf("%d,%s,%s,%d,%d,%d\n", cid, stat.OpType, latency.Start.UTC().Format("2006-01-02T15:04:05.000Z07:00"), opid, latency_error, latency.Latency.Nanoseconds()))
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

func randBytes(src mrand.Source, bytesN int64) []byte {
	// source: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
	const (
		letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
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
