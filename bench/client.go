package bench

import (
	"fmt"
	"path"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Client struct {
	Id        string
	Server    string
	Namespace string
	EndPoint  string
	Conn      *zk.Conn
}

var (
	zkCreateFlags = int32(0)
	zkCreateACL   = zk.WorldACL(zk.PermAll)
)

func (self *Client) CreateR(rpath string, data []byte) error {
	var subps []string
	if len(rpath) > 0 && rpath != "/" {
		subps = append(subps, rpath)
	}
	for d := path.Dir(rpath); d != "." && d != "/"; {
		subps = append(subps, d)
		d = path.Dir(d)
	}
	l := len(subps) - 1
	var err error
	for i := range subps {
		if i != l {
			_, err = self.CreateIfNotExist(subps[l-i], []byte(""))
		} else {
			_, err = self.Conn.Create(subps[0], data, zkCreateFlags, zkCreateACL)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Client) Create(rpath string, data []byte) error {
	_, err := self.Conn.Create(self.Namespace+"/"+rpath, data, zkCreateFlags, zkCreateACL)
	return err
}

func (self *Client) Set(rpath string, data []byte) error {
	_, err := self.Conn.Set(rpath, data, int32(-1))
	return err
}

func (self *Client) CreateIfNotExist(rpath string, data []byte) (bool, error) {
	exists, _, err := self.Conn.Exists(rpath)
	if err != nil {
		return false, err
	}
	if !exists {
		_, err = self.Conn.Create(rpath, data, zkCreateFlags, zkCreateACL)
		return false, err
	}
	return true, nil
}

func (self *Client) Setup() error {
	exists, _, err := self.Conn.Exists(self.Namespace)
	if err != nil {
		return err
	}
	if !exists {
		err = self.CreateR(self.Namespace, []byte("I am client "+self.Id))
	}
	return err
}

func (self *Client) Cleanup() {
	self.Conn.Delete(self.Namespace, 0)
	self.Conn.Close()
}

func NewClient(id int, server string, endpoint string, namespace string) (*Client, error) {
	conn, _, err := zk.Connect([]string{endpoint}, time.Second)
	if err != nil {
		return nil, err
	}
	sid := fmt.Sprintf("%d", id)
	return &Client{Id: sid, Server: server, Namespace: namespace + "/client" + sid, EndPoint: endpoint, Conn: conn}, nil
}

func NewClients(servers []string, endpoints []string, nclients int, namespace string) ([]*Client, error) {
	clients := make([]*Client, nclients)
	for i := 0; i < nclients; i++ {
		client, err := NewClient(i+1, servers[i%len(servers)], endpoints[i%len(endpoints)], namespace)
		if err != nil {
			return nil, err
		}
		clients[i] = client
	}
	return clients, nil
}
