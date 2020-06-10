package main

import (
	"ds/go/common/zk_client"
	"ds/go/master"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
)

func main() { // start shardMaster Service and register Service according to cmdline arguments

	/* read cmdline arguments and load configuration file*/
	args := os.Args[1:] // args without program name, determine Ip:Port hostname of this master.
	ip := args[0]
	port, err := strconv.Atoi(args[1])
	hostname := args[2]

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
	master.RegisterShardingServiceServer(grpcServer, new(master.ShardMaster))
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
		fmt.Println("master serving at ip:port " + ip + ":" + strconv.Itoa(port))
		grpcServer.Serve(listen)
	}()

	master := zk_client.ServiceNode{Name: "master", Host: ip, Port: port,Hostname: hostname}

	/* only one master allow, seems ok to use tryPrimary logic*/
	if err := client.TryPrimary(&master); err != nil {
		log.Fatalf("error: %v", err)
	}

	nodes, err := client.GetNodes("master")
	if err != nil {
		panic(err)
	}
	for _, node := range nodes {
		fmt.Println(node.Host, node.Port,node.Hostname)
	}

}
