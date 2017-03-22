package bench

import (
	"fmt"
	"log"
	mrand "math/rand"
	"sort"
	"strings"
	"time"

	"sync"
	// "golang.org/x/net/context"

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

type operand struct {
	key   string
	value string
}

type benchRun struct {
}

type Benchmark struct {
	Config *zkbench.Config

	clients     []*Client
	initialized bool
	wg          sync.WaitGroup

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

func writeRequests(b *Benchmark, c *Client, opch chan<- operand) {
	val := randBytes(b.value_size_bytes)
	sval := string(val)

	var wg sync.WaitGroup
	defer func() {
		close(opch)
		wg.Wait()
	}()

	for i := int64(0); i < b.nrequests; i++ {
		k := sequentialKey(b.key_size_bytes, i)
		fmt.Println(k)
		opch <- operand{key: b.namespace + "/" + k, value: sval}
	}
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
