package zk_client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

/* Service Node Information, will be marshalled and stored in zk-cluster as Value.*/
type ServiceNode struct {
	Name     string `json:"name"` // 服务名称，这里是user
	IP       string `json:"host"`
	Port     int    `json:"port"`
	Hostname string `json:"hostname"` // hostname of this service, not exactly like hostname
}
func (s *ServiceNode) ServerString() string {
	return s.IP + ":" + strconv.Itoa(s.Port)
}
// 在定义一个服务发现的客户端结构体SdClient。
/* client information,
a client connect to a list of servers.
at a root node
with a connection.
*/
type Client struct {
	zkServers []string // 多个节点地址
	zkRoot    string   // 服务根节点，这里是/api
	conn      *zk.Conn // zk的客户端连接
}

func CreateClientFromConfigurationFile(filename string) (*Client, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil,err
	}

	var conf Conf
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return nil, err
	}

	zkClient, err := NewClient(conf.ServerStrings(), conf.ZkRoot,conf.ZkTimeout)
	if err != nil {
		return nil,err
	}

	return zkClient,nil
}

/* client constructor */
func NewClient(zkServers []string, zkRoot string, timeout int) (*Client, error) {
	// 连接服务器
	conn, _, err := zk.Connect(zkServers, time.Duration(timeout)*time.Second)
	if err != nil {
		return nil, err
	}
	client := Client{
		zkServers: zkServers,
		zkRoot:    zkRoot,
		conn:      conn,
	}
	// 创建服务根节点
	if err := client.ensureRoot(); err != nil {
		client.Close()
		return nil, err
	}
	return &client, nil
}

/* client methods */

/* close client connection */ // 关闭连接，释放临时节点
func (s *Client) Close() {
	s.conn.Close()
}

/* create root if not exist */
func (s *Client) ensureRoot() error {
	exists, _, err := s.conn.Exists(s.zkRoot)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Create(s.zkRoot, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

// 值得注意的是代码中的Create调用可能会返回节点已存在错误，这是正常现象，因为会存在多进程同时创建节点的可能。如果创建根节点出错，还需要及时关闭连接。我们不关心节点的权限控制，所以使用zk.WorldACL(zk.PermAll)表示该节点没有权限限制。Create参数中的flag=0表示这是一个持久化的普通节点。

// 接下来我们编写服务注册方法
/* client register a service, protected temporal sequential add */
func (s *Client) Register(node ServiceNode) (string,error) {
	if err := s.ensureName(node.Name); err != nil {
		return "",err
	}
	path := s.zkRoot + "/" + node.Name + "/n"
	data, err := json.Marshal(node)
	if err != nil {
		return "", err
	}
	newPath, err := s.conn.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", err
	}

	return newPath,nil
}

func (s *Client) TryPrimary(node ServiceNode)error {
	if err := s.ensureName(node.Name); err != nil {
		return err
	}
	path := s.zkRoot + "/" + node.Name + "/n"
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	_, err = s.conn.Create(path, data,zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

/* create name entry if not exist */
func (s *Client) ensureName(name string) error {
	path := s.zkRoot + "/" + name
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

// 先要创建/api/user节点作为服务列表的父节点。然后创建一个保护顺序临时(ProtectedEphemeralSequential)子节点，同时将地址信息存储在节点中。什么叫保护顺序临时节点，首先它是一个临时节点，会话关闭后节点自动消失。其它它是个顺序节点，zookeeper自动在名称后面增加自增后缀，确保节点名称的唯一性。同时还是个保护性节点，节点前缀增加了GUID字段，确保断开重连后临时节点可以和客户端状态对接上。

// 接下来我们实现消费者获取服务列表方法
/* get service node list

 */
func (s *Client) GetNodes(name string) ([]*ServiceNode, error) {
	path := s.zkRoot + "/" + name
	// 获取字节点名称
	childs, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return []*ServiceNode{}, nil
		}
		return nil, err
	}
	var nodes []*ServiceNode

	for _, child := range childs {
		fullPath := path + "/" + child
		data, _, err := s.conn.Get(fullPath)
		if err != nil {
			if err == zk.ErrNoNode {
				continue
			}
			return nil, err
		}
		node := new(ServiceNode)
		err = json.Unmarshal(data, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}
/*
SubscribeNodes : return a channel that emit a []*ServiceNode whenever the children under "name"path change
*/
func (s *Client) SubscribeNodes(name string,done chan struct{}) (chan []*ServiceNode, error){
	path := s.zkRoot + "/" + name
	err := s.ensureName(name)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	// 获取字节点名称
	nodesChan := make(chan []*ServiceNode)
	go func() {
		for {
			select {
			case _,ok := <- done:
				if !ok {
					log.Printf("done send, stop subscribing channel of %s\n", name)
					close(nodesChan)
					return
				}
			default:
			}
			childs, _,ch, err := s.conn.ChildrenW(path)
			if err != nil {
				fmt.Println(err.Error())
				panic(err)
				return
			}
			var nodes []*ServiceNode
			for _, child := range childs {
				fullPath := path + "/" + child
				data, _, err := s.conn.Get(fullPath)
				if err != nil {
					if err == zk.ErrNoNode {
						continue
					}
					log.Println(err.Error())
				}
				node := new(ServiceNode)
				err = json.Unmarshal(data, node)
				if err != nil {
					log.Println(err.Error())
				}
				nodes = append(nodes, node)
			}
			/* send new nodes to chan */
			nodesChan <- nodes
			/* when receiving events*/
			e := <- ch
			if e.Type == zk.EventNodeChildrenChanged {
				log.Printf("zk Path: %s, children changed\n", name)
			}else {
				log.Printf("zk Path: %s, unexpected events\n",name)
			}

		}
	}()
	return nodesChan, nil
}
/*
Subscribe1Node :
Watch the status of one node, and return a channel
channel will close if the node no longer exist
*/
func (s *Client) Subscribe1Node(name string) (chan ServiceNode, error){
	resChan := make(chan ServiceNode)
	done := make(chan struct{})
	nodeChan, _ := s.SubscribeNodes(name,done)

	go func() {
		for nodes := range nodeChan {
			if len(nodes) == 0 {
			close(resChan)
			close(done)
			}else if len(nodes) != 1{
				log.Printf("not suppose to see multiple primary nodes")
			}else {
				resChan <- *nodes[0]
			}
		}
	}()
	return resChan,nil
}
//func (s *Client)GetStat(name string, hostname string) error
/*
Delete Node: delete node with hostname under the path name
*/
func (s *Client)DeleteNode(path string) error {


	//path := s.zkRoot + "/" + name
	//// 获取字节点名称
	//childs, _, err := s.conn.Children(path)
	//log.Printf("hostname %s, get children %+v\n",hostname, childs)
	//if err != nil {
	//	return err
	//}
	//for _, child := range childs {
	//	fullPath := path + "/" + child
	//	data, stat, err := s.conn.Get(fullPath)
	//	if err != nil {
	//		if err == zk.ErrNoNode {
	//			continue
	//		}
	//		return err
	//	}
	//	node := new(ServiceNode)
	//	err = json.Unmarshal(data, node)
	//	log.Printf("path %s, node info %+v,stat %+v\n",fullPath,*node,stat)
	//	if err != nil {
	//		return err
	//	}
	//	if node.hostname == hostname{
	//		log.Printf("delete emit\n")
	//		s.conn.Delete(name, 0)
	//		return nil
	//	}
	//}
	if err := s.conn.Delete(path,0); err != nil {
		return err
	}
	return nil
}
/*
	get service node under "name", and assert only one result
*/
var ErrNotFound = errors.New("not found any node under the path")
var ErrMultipleNodes = errors.New("found multiple nodes, supposed to be one")
func (s *Client) Get1Node(name string) (*ServiceNode, error){
	if nodes, err := s.GetNodes(name); err != nil {
		return nil,err
	}else if len(nodes) == 0 {
		return nil, ErrNotFound
	}else if len(nodes) > 1 {
		return nil,ErrMultipleNodes
	}else {
		return nodes[0],nil
	}

}
/* ZkServer information  */
type ZkServer struct {
	Ip   string `json:"ip"`
	Port string `json:"port"`
}

func (s *ZkServer) ToString() string {
	return s.Ip + ":" + s.Port
}

/* configuration.json data structure*/
type Conf struct {
	Servers   []ZkServer `json:"zookeepers"`
	ZkRoot    string     `json:"zkRoot"`
	ZkTimeout int        `json:"zkTimeout"`
}

func (c *Conf) ServerStrings() []string {
	strings := make([]string, 0)
	for _, server := range c.Servers {
		strings = append(strings, server.ToString())
	}
	return strings
}
