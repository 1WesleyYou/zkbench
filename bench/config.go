package bench

import (
	"fmt"
	"sort"

	zkc "zkbench/config"
)

type BenchConfig struct {
	Namespace      string
	NClients       int
	Servers        []string
	Endpoints      []string
	Type           uint32
	NRequests      int64
	KeySizeBytes   int64
	ValueSizeBytes int64
	SameKey        bool
	Cleanup        bool
}

var (
	BENCHTYPEMAP map[rune]BenchType = map[rune]BenchType{
		'c': CREATE,
		'r': READ,
		'u': WRITE,
		'd': DELETE,
	}
)

func TypeStr(btype uint32) string {
	var types [4]byte
	i := 0
	if btype&CREATE != 0 {
		types[i], i = 'c', i+1
	}
	if btype&READ != 0 {
		types[i], i = 'r', i+1
	}
	if btype&WRITE != 0 {
		types[i], i = 'u', i+1
	}
	if btype&DELETE != 0 {
		types[i], i = 'd', i+1
	}
	return string(types[:i])
}

func ParseConfig(path string) (*BenchConfig, error) {
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
	btypestr, err := config.GetString("type")
	if err != nil {
		return nil, err
	}
	if len(btypestr) > 4 {
		return nil, fmt.Errorf("Bench type should be at most 4-char\n")
	}
	var btype uint32 = 0
	for _, c := range btypestr {
		t, ok := BENCHTYPEMAP[c]
		if !ok {
			return nil, fmt.Errorf("Unrecognized bench type\n")
		}
		btype = btype | uint32(t)
	}

	sort.Strings(servers)
	endpoints := make([]string, len(servers))
	for i, server := range servers {
		endpoints[i], _ = config.GetString(server)
		fmt.Println(server + "=" + endpoints[i])
	}
	benchconf := &BenchConfig{
		Namespace:      "/" + namespace,
		NClients:       nclients,
		Servers:        servers,
		Endpoints:      endpoints,
		Type:           btype,
		NRequests:      nrequests,
		KeySizeBytes:   key_size_bytes,
		ValueSizeBytes: value_size_bytes,
		SameKey:        samekey,
		Cleanup:        cleanup,
	}
	fmt.Println("Random value: " + string(randBytes(value_size_bytes)))
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
