package bench

import (
	"fmt"
	"log"
	mrand "math/rand"
	"sort"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"sync"

	"zkbench"
)

const (
	WARM_UP = 1 << iota
	READ    = 1 << iota
	WRITE   = 1 << iota
	CREATE  = 1 << iota
	DELETE  = 1 << iota
	CLEANUP = 1 << iota
)

var (
	zkCreateFlags = int32(0)
	zkCreateACL   = zk.WorldACL(zk.PermAll)
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

type ReqHandler func(req *request) error

type Benchmark struct {
	Config *zkbench.Config

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
	clients, err := NewClients(self.servers, self.endpoints, self.nclients)
	checkErr(err)
	self.clients = clients
	self.initialized = true
}

func (self *Benchmark) Run() {
	if !self.initialized {
		log.Fatal("Must initialize benchmark first")
	}

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
}

func (self *Benchmark) startRequests(br *benchRun) {
	for i := range br.handlers {
		br.wg.Add(1)
		go func(handler ReqHandler) {
			defer br.wg.Done()
			for req := range br.rqch {
				fmt.Println(req.key + ":" + string(req.value))
				err := handler(&req)
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
	for _, client := range self.clients {
		children, stat, _, err := client.Conn.ChildrenW(self.namespace)
		if err != nil {
			panic(err)
		}
		fmt.Printf("[client %s]: %+v %+v\n", client.Id, children, stat)
	}
}

func (self *Benchmark) seqRequests(rqch chan<- request) {
	val := randBytes(self.value_size_bytes)

	for i := int64(0); i < self.nrequests; i++ {
		k := sequentialKey(self.key_size_bytes, i)
		rqch <- request{key: self.namespace + "/" + k, value: val}
	}
}

func newWriteHandler(client *Client) ReqHandler {
	return func(req *request) error {
		_, err := client.Conn.Set(req.key, req.value, int32(-1))
		return err
	}
}

func (self *Benchmark) newWriteHandlers() []ReqHandler {
	handlers := make([]ReqHandler, self.nclients)
	for i, client := range self.clients {
		handlers[i] = newWriteHandler(client)
	}
	return handlers
}

func newCreateHandler(client *Client) ReqHandler {
	return func(req *request) error {
		_, err := client.Conn.Create(req.key, req.value, zkCreateFlags, zkCreateACL)
		return err
	}
}

func (self *Benchmark) newCreateHandlers() []ReqHandler {
	handlers := make([]ReqHandler, self.nclients)
	for i, client := range self.clients {
		handlers[i] = newCreateHandler(client)
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

func checkPosInt64(config *zkbench.Config, key string) int64 {
	val, err := config.GetInt64(key)
	if err != nil {
		log.Fatal("Error:", err)
	}
	if val <= 0 {
		log.Fatalf("parameter '%s' must be positive\n", key)
	}
	return val
}

func checkPosInt(config *zkbench.Config, key string) int {
	val, err := config.GetInt(key)
	if err != nil {
		log.Fatal("Error:", err)
	}
	if val <= 0 {
		log.Fatalf("parameter '%s' must be positive\n", key)
	}
	return val
}
