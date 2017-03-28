package bench

import (
	"fmt"
	"log"
	mrand "math/rand"
	"strings"
	"sync"
	"time"
)

type BenchType uint32

const (
	WARM_UP BenchType = 1 << iota
	READ              = 1 << iota
	WRITE             = 1 << iota
	CREATE            = 1 << iota
	DELETE            = 1 << iota
)

type benchConfig struct {
	namespace        string
	nclients         int
	servers          []string
	endpoints        []string
	cleanup          bool
	nrequests        int64
	key_size_bytes   int64
	value_size_bytes int64
}

type Request struct {
	key   string
	value []byte
}

type request struct {
	key   string
	value []byte
}

type benchRun struct {
	mu       sync.RWMutex
	rqch     chan request
	handlers []ReqHandler
	wg       sync.WaitGroup
	reqGen   func(chan<- request)
}

type ReqHandler func(c *Client, r *Request) error
type ReqGenerator func(iter int64) *Request

type Benchmark struct {
	clients     []*Client
	initialized bool
	BenchmarkConfig
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
	clients, err := NewClients(self.servers, self.endpoints, self.nclients, self.namespace)
	checkErr(err)
	self.clients = clients

	for _, client := range self.clients {
		err := client.Setup()
		if err != nil {
			panic(err)
		}
	}

	self.initialized = true
}

func (self *Benchmark) Run() {
	if !self.initialized {
		log.Fatal("Must initialize benchmark first")
	}
	self.runBench(WARM_UP)
	self.runBench(CREATE)
	self.runBench(WRITE)
	self.runBench(READ)
	/*
		rqch := make(chan request, self.nclients)
		defer close(rqch)
		chs := self.newCreateHandlers()
		reqGen := func(rqch chan<- request) { self.seqRequests(rqch) }
		br := &benchRun{
			rqch:     rqch,
			handlers: chs,
			wg:       sync.WaitGroup{},
			reqGen:   reqGen,
		}
		self.startRequests(br)
		fmt.Println("Created")

		br.handlers = self.newWriteHandlers()
		self.startRequests(br)
		fmt.Println("Written")
	*/
}

func (self *Benchmark) processRequests(client *Client, btype BenchType, same bool, generator ReqGenerator, handler ReqHandler) *BenchStat {
	var req *Request
	var stat BenchStat
	stat.Latencies = make([]time.Duration, self.nrequests)
	if same {
		req = generator(-1)
	}
	bstr := btype.String()
	for i := int64(0); i < self.nrequests; i++ {
		stat.Ops++
		if !same {
			req = generator(i)
		}
		begin := time.Now()
		err := handler(client, req)
		d := time.Since(begin)
		if err != nil {
			stat.Errors++
			fmt.Printf("Error in processing %s request for key %s: %v\n", bstr, req.key, err)
		}
		stat.Latencies[i] = d
		if i == 0 || d < stat.MinLatency {
			stat.MinLatency = d
		}
		if i == 0 || d > stat.MaxLatency {
			stat.MaxLatency = d
		}
		stat.TotalLatency += d
	}
	stat.AvgLatency = stat.TotalLatency / time.Duration(stat.Ops)
	stat.Throughput = float64(stat.Ops) / stat.TotalLatency.Seconds()
	return &stat
}

func (self *Benchmark) runBench(btype BenchType) {
	var wg sync.WaitGroup
	key := sameKey(self.key_size_bytes)
	val := randBytes(self.value_size_bytes)
	var empty []byte
	for _, client := range self.clients {
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
			if self.samekey {
				generator = func(iter int64) *Request { return &Request{key, empty} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.key_size_bytes, iter), empty} }
			}
			handler = func(c *Client, r *Request) error {
				_, _, err := c.Read(r.key)
				return err
			}
		case WRITE:
			if self.samekey {
				generator = func(iter int64) *Request { return &Request{key, val} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.key_size_bytes, iter), val} }
			}
			handler = func(c *Client, r *Request) error {
				return c.Write(r.key, r.value)
			}
		case CREATE:
			if self.samekey {
				generator = func(iter int64) *Request { return &Request{key, empty} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.key_size_bytes, iter), empty} }
			}
			handler = func(c *Client, r *Request) error {
				return c.Create(r.key, r.value)
			}
		case DELETE:
			if self.samekey {
				generator = func(iter int64) *Request { return &Request{key, empty} }
			} else {
				generator = func(iter int64) *Request { return &Request{sequentialKey(self.key_size_bytes, iter), empty} }
			}
			handler = func(c *Client, r *Request) error {
				return c.Delete(r.key)
			}
		}
		wg.Add(1)
		go func(client *Client, generator ReqGenerator, handler ReqHandler) {
			stat := self.processRequests(client, btype, self.samekey, generator, handler)
			log.Printf("[Client %s]: done bench %s, %v\n", client.Id, btype.String(), *stat)
			wg.Done()
		}(client, generator, handler)
	}
	wg.Wait()
}

func (self *Benchmark) startRequests(br *benchRun) {
	/*
		for i := range br.handlers {
			br.wg.Add(1)
			go func(handler ReqHandler) {
				defer br.wg.Done()
				for req := range br.rqch {
					fmt.Println(req.key + ":" + string(req.value))
					err := handler(&Request{req.key, req.value})
					if err != nil {
						log.Println("Error:", err)
					}
				}
			}(br.handlers[i])
		}
		br.reqGen(br.rqch)
		br.wg.Wait()
	*/
}

func (self *Benchmark) SmokeTest() {
	for _, client := range self.clients {
		children, stat, _, err := client.Conn.ChildrenW(self.namespace)
		if err != nil {
			panic(err)
		}
		log.Printf("[Client %s]: %+v %+v\n", client.Id, children, stat)
	}
}

func (self *Benchmark) Done() {
	var client *Client
	for _, client = range self.clients {
		err := client.Cleanup()
		log.Printf("Clean up client " + client.Id)
		if err != nil {
			log.Println("Error: ", err)
		}
	}
}

func (self *Benchmark) seqRequests(rqch chan<- request) {
	val := randBytes(self.value_size_bytes)

	for i := int64(0); i < self.nrequests; i++ {
		k := sequentialKey(self.key_size_bytes, i)
		rqch <- request{key: self.namespace + "/" + k, value: val}
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

func checkErr(err error) {
	if err != nil {
		log.Fatal("Error:", err)
	}
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
