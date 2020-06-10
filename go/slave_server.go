package main

import (
	"ds/go/common/zk_client"
	"ds/go/slave"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"
)

func main() { // start RPC Service and register Service according to cmdline arguments

	/* read cmdline arguments and load configuration file*/
	args := os.Args[1:] // args without program name, determine Ip:Port hostname of this slave.
	ip := args[0]
	port, err := strconv.Atoi(args[1])
	hostname := args[2]
	groupID, err := strconv.Atoi(args[3])

	if err != nil {
		log.Fatal(err)
	}

	content, err := ioutil.ReadFile("./configuration.json")
	if err != nil {
		log.Fatal(err)
	}

	//fmt.Printf("File contents: %s", content) // remove when correct

	var conf zk_client.Conf
	err = json.Unmarshal([]byte(content), &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	//fmt.Printf("--- t:\n%v\n\n", conf) // remove when correct

	/* start RPC Service on ip:port */
	grpcServer := grpc.NewServer()
	slaveServer := new(slave.Slave)
	slave.RegisterKVServiceServer(grpcServer, slaveServer)

	listen, err := net.Listen("tcp", ip+":"+args[1]) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}

	client, err := zk_client.NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	/* defer execute in LIFO, close connection at last*/
	defer func() {
		fmt.Println("serving at ip:port " + ip + ":" + strconv.Itoa(port))
		grpcServer.Serve(listen)
	}()
	primaryPath := "slave_primary/" + strconv.Itoa(groupID)
	backupPath := "slave_backup/" + strconv.Itoa(groupID)
	slaveNode := zk_client.ServiceNode{Name: primaryPath, Host: ip, Port: port, Hostname: hostname}
	fmt.Printf("slavenode %+v\n", slaveNode)
	if err := client.TryPrimary(&slaveNode); err != nil {
		if err.Error() != "zk: node already exists" {
			panic(err)
		}
	} else {
		/*select as primary node, just return and wait for rpc service to start*/
		nodes, err := client.GetNodes(primaryPath)
		if err != nil {
			panic(err)
		}
		for _, node := range nodes {
			fmt.Println(node.Host, node.Port, node.Hostname)
		}
		return
	}

	/* all ready exist a primary*/
	slaveNode = zk_client.ServiceNode{Name: backupPath, Host: ip, Port: port, Hostname: hostname}
	if err := client.Register(&slaveNode); err != nil {
		panic(err)
	}
	nodes, err := client.GetNodes(backupPath)
	if err != nil {
		panic(err)
	}
	for _, node := range nodes {
		fmt.Println(node.Host, node.Port, node.Hostname)
	}

}
