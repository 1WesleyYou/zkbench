package bench

import (
	"fmt"
	"log"
	mrand "math/rand"
	"sort"
	"strings"
	"time"

	"sync"

	zkc "zkbench/config"
)

type BenchType uint32

const (
	WARM_UP BenchType = 1 << iota
	READ              = 1 << iota
	WRITE             = 1 << iota
	CREATE            = 1 << iota
	DELETE            = 1 << iota
	CLEANUP           = 1 << iota
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

type ReqHandler func(key string, value []byte) error

type Benchmark struct {
	Config *zkc.Config

	clients     []*Client
	initialized bool

	benchConfig
}

func (self *Benchmark) parseConfig() {
	namespace, err := self.Config.GetString("namespace")
	checkErr(err)
	nclients := checkPosInt(self.Config, "clients")
	nrequests := checkPosInt64(self.Config, "requests")
	key_size_bytes := checkPosInt64(self.Config, "key_size_bytes")
	value_size_bytes := checkPosInt64(self.Config, "value_size_bytes")
	cleanup, err := self.Config.GetBool("cleanup")
	checkErr(err)
	servers := self.Config.GetKeys("server")
	sort.Strings(servers)
	endpoints := make([]string, len(servers))
	for i, server := range servers {
		endpoints[i], _ = self.Config.GetString(server)
		fmt.Println(server + "=" + endpoints[i])
	}
	checkErr(err)

	self.namespace = "/" + namespace
	self.nclients = nclients
	self.servers = servers
	self.endpoints = endpoints
	self.cleanup = cleanup
	self.nrequests = nrequests
	self.key_size_bytes = key_size_bytes
	self.value_size_bytes = value_size_bytes
	fmt.Println(string(randBytes(self.value_size_bytes)))
}

func (self *Benchmark) Init() {
	self.parseConfig()
	clients, err := NewClients(self.servers, self.endpoints, self.nclients, self.namespace)
	checkErr(err)
	self.clients = clients

	self.initialized = true
}

func (self *Benchmark) Run() {
	if !self.initialized {
		log.Fatal("Must initialize benchmark first")
	}
	for _, client := range self.clients {
		err := client.Setup()
		if err != nil {
			panic(err)
		}
	}

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

func (self *Benchmark) runBench(btype BenchType) {
	val := randBytes(self.value_size_bytes)
	var wg sync.WaitGroup
	for _, client := range self.clients {
		wg.Add(1)
		var handler ReqHandler
		go func(c *Client, data []byte, handler ReqHandler) {
			defer wg.Done()
			for i := int64(0); i < self.nrequests; i++ {
				k := sequentialKey(self.key_size_bytes, i)
				handler(c.Namespace+"/"+k, data)
			}
		}(client, val, handler)
	}
	wg.Wait()

	/*
		switch btype {
		case WARM_UP:

		case READ:

		case WRITE:
			handler = newWriteHandler(client)

		case CREATE:

		case DELETE:

		case CLEANUP:
		}
	*/
}

func (self *Benchmark) startRequests(br *benchRun) {
	for i := range br.handlers {
		br.wg.Add(1)
		go func(handler ReqHandler) {
			defer br.wg.Done()
			for req := range br.rqch {
				fmt.Println(req.key + ":" + string(req.value))
				err := handler(req.key, req.value)
				if err != nil {
					log.Println("Error:", err)
				}
			}
		}(br.handlers[i])
	}
	br.reqGen(br.rqch)
	br.wg.Wait()
}

func (self *Benchmark) SmokeTest() {
	/*
		tryCreate := true
		var err error
		for _, client := range self.clients {
			if tryCreate {
				_, err = client.Create(self.namespace, []byte(client.Id))
			}
			if err == nil {
				tryCreate = false
			} else {
				exists, _, _ := client.Conn.Exists(self.namespace)
				if exists {
					tryCreate = false
				}
			}
			if !tryCreate {
				client.Conn.Create(client.Namespace, []byte(""))
			}
			children, stat, _, err := client.Conn.ChildrenW(self.namespace)
			if err != nil {
				panic(err)
			}
			fmt.Printf("[client %s]: %+v %+v\n", client.Id, children, stat)
		}
	*/
}

func (self *Benchmark) Done() {
	for _, client := range self.clients {
		client.Cleanup()
	}
}

func (self *Benchmark) seqRequests(rqch chan<- request) {
	val := randBytes(self.value_size_bytes)

	for i := int64(0); i < self.nrequests; i++ {
		k := sequentialKey(self.key_size_bytes, i)
		rqch <- request{key: self.namespace + "/" + k, value: val}
	}
}

func (self *Benchmark) newWriteHandlers() []ReqHandler {
	handlers := make([]ReqHandler, self.nclients)
	for i, client := range self.clients {
		handlers[i] = client.Set
	}
	return handlers
}

func (self *Benchmark) newCreateHandlers() []ReqHandler {
	handlers := make([]ReqHandler, self.nclients)
	for i, client := range self.clients {
		handlers[i] = client.Create
	}
	return handlers
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

func checkPosInt64(config *zkc.Config, key string) int64 {
	val, err := config.GetInt64(key)
	if err != nil {
		log.Fatal("Error:", err)
	}
	if val <= 0 {
		log.Fatalf("parameter '%s' must be positive\n", key)
	}
	return val
}

func checkPosInt(config *zkc.Config, key string) int {
	val, err := config.GetInt(key)
	if err != nil {
		log.Fatal("Error:", err)
	}
	if val <= 0 {
		log.Fatalf("parameter '%s' must be positive\n", key)
	}
	return val
}
