package bench

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Client struct {
	Id       string
	Server   string
	EndPoint string
	Conn     *zk.Conn
}

func NewClient(id int, server string, endpoint string) (*Client, error) {
	conn, _, err := zk.Connect([]string{endpoint}, time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{Id: fmt.Sprintf("%d", id), Server: server, EndPoint: endpoint, Conn: conn}, nil
}

func NewClients(servers []string, endpoints []string, nclients int) ([]*Client, error) {
	clients := make([]*Client, nclients)
	for i := 0; i < nclients; i++ {
		client, err := NewClient(i+1, servers[i%len(servers)], endpoints[i%len(endpoints)])
		if err != nil {
			return nil, err
		}
		clients[i] = client
	}
	return clients, nil
}
