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
	args := os.Args[1:] // args without program name, determine Ip:Port name of this master.
	ip := args[0]
	port, err := strconv.Atoi(args[1])

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
	grpcServer.Serve(listen)


	client, err := zk_client.NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	slave := zk_client.ServiceNode{Name: "slave", Host: ip, Port: port}

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
