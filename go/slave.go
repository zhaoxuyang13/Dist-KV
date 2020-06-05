package main

import (
	. "ds/go/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

/* move to a global util */
type ZkServer struct{
	Ip   string `json:"ip"`
	Port string `json:"port"`
}
func (s *ZkServer) toString() string{
	return s.Ip + ":" + s.Port
}
type Conf struct {
	Servers []ZkServer `json:"zookeepers"`
}
func (c* Conf) serversString() []string{
	strings := make([]string, 0)
	for _, server := range c.Servers {
		strings = append(strings, server.toString())
	}
	return strings
}


// type Slave struct {
// 	nodeInfo ServiceNode    // for info marshalling
// 	zkClient SdClient  // interacting with Zkpr
// }

// func NewSlave(name string, host string, Port int, zkServers []string, zkRoot string, timeout int) (*Slave,err){
// 	nodeInfo := ServiceNode{
// 		Name: name,
// 		Host: host,
// 		Port: Port,
// 	}
// 	zkClient,err := NewClient(zkServers, zkRoot, timeout);
// 	if err != nil {
// 		return nil, err
// 	}
// 	slave := Slave{
// 		nodeInfo: nodeInfo,
// 		zkClient: zkClient,
// 	}
// 	return &slave, nil
// }

func main()  {

	args := os.Args[1:] // args without program name, determine Ip:Port of this slave.
	ip := args[0]
	port,err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal(err)
	}

	content, err := ioutil.ReadFile("./configuration.json")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("File contents: %s", content) // remove when correct

	var conf Conf
	err = json.Unmarshal([]byte(content), &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- t:\n%v\n\n", conf) // remove when correct

	client,err := NewClient(conf.serversString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	slave := ServiceNode{Name: "slave", Host: ip, Port: port}

	if err := client.Register(&slave); err != nil {
		panic(err)
	}

	nodes, err := client.GetNodes("slave")
	if err != nil {
		panic(err) 
	}
	for _, node := range nodes {
		fmt.Println(node.Host, node.Port)
	}
	
}