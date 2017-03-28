package bench

import (
	"fmt"
	"sort"

	zkc "zkbench/config"
)

type BenchmarkConfig struct {
	namespace        string
	nclients         int
	servers          []string
	endpoints        []string
	nrequests        int64
	key_size_bytes   int64
	value_size_bytes int64
	samekey          bool
	cleanup          bool
}

func ParseConfig(path string) (*BenchmarkConfig, error) {
	config, err := zkc.ParseConfig(path)
	if err != nil {
		return nil, fmt.Errorf("Fail to parse config: %v\n", err)
	}
	namespace, err := config.GetString("namespace")
	if err != nil {
		return nil, err
	}
	nclients, err := checkPosInt(config, "clients")
	if err != nil {
		return nil, err
	}
	nrequests, err := checkPosInt64(config, "requests")
	if err != nil {
		return nil, err
	}
	key_size_bytes, err := checkPosInt64(config, "key_size_bytes")
	if err != nil {
		return nil, err
	}
	value_size_bytes, err := checkPosInt64(config, "value_size_bytes")
	if err != nil {
		return nil, err
	}
	cleanup, err := config.GetBool("cleanup")
	if err != nil {
		return nil, err
	}
	samekey, err := config.GetBool("same_key")
	if err != nil {
		return nil, err
	}
	servers := config.GetKeys("server")
	if err != nil {
		return nil, err
	}
	sort.Strings(servers)
	endpoints := make([]string, len(servers))
	for i, server := range servers {
		endpoints[i], _ = config.GetString(server)
		fmt.Println(server + "=" + endpoints[i])
	}
	benchconf := &BenchmarkConfig{
		namespace:        "/" + namespace,
		nclients:         nclients,
		servers:          servers,
		endpoints:        endpoints,
		nrequests:        nrequests,
		key_size_bytes:   key_size_bytes,
		value_size_bytes: value_size_bytes,
		samekey:          samekey,
		cleanup:          cleanup,
	}
	fmt.Println(string(randBytes(value_size_bytes)))
	return benchconf, nil
}

func checkPosInt64(config *zkc.Config, key string) (int64, error) {
	val, err := config.GetInt64(key)
	if err != nil {
		return 0, err
	}
	if val <= 0 {
		return 0, fmt.Errorf("parameter '%s' must be positive\n", key)
	}
	return val, nil
}

func checkPosInt(config *zkc.Config, key string) (int, error) {
	val, err := config.GetInt(key)
	if err != nil {
		return 0, err
	}
	if val <= 0 {
		return 0, fmt.Errorf("parameter '%s' must be positive\n", key)
	}
	return val, nil
}
