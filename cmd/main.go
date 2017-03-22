package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"zkbench"
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
	servers := config.GetKeys("server")
	sort.Strings(servers)
	endpoints := make([]string, len(servers))
	for i, server := range servers {
		endpoints[i], _ = config.GetString(server)
		fmt.Println(endpoints[i])
	}

	for _, endpoint := range endpoints {
		conn, _, err := zk.Connect([]string{endpoint}, time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fail to connect to %s\n", endpoint)
			os.Exit(1)
		}
		children, stat, _, err := conn.ChildrenW("/zkTest")
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v %+v\n", children, stat)
	}
}
