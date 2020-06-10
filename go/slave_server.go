package main

import (
	"context"
	"ds/go/common/zk_client"
	"ds/go/master"
	"ds/go/slave"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"time"
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
	err = json.Unmarshal(content, &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	//fmt.Printf("--- t:\n%v\n\n", conf) // remove when correct

	/* start RPC Service on ip:port */
	grpcServer := grpc.NewServer()
	slaveServer := &slave.Slave{
		LocalStorages: make(map[int32]*slave.LocalStorage),
		Version: 	   0,
		GroupId:       int32(groupID),
		ShardList:     make([]int32,0),
	}
	slave.RegisterKVServiceServer(grpcServer, slaveServer)

	listen, err := net.Listen("tcp", ip+":"+args[1]) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}

	sdClient, err := zk_client.NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	defer sdClient.Close()
	/* defer execute in LIFO, close connection at last*/
	defer func() {
		fmt.Println("slave server start serving requests at " + ip + ":" + strconv.Itoa(port))
		if err := grpcServer.Serve(listen); err != nil {
			log.Fatal(err)
		}
	}()
	/* start timer, slave server will retrieve configuration from master every time period*/
	defer func(){

		if masterNodes,err := sdClient.GetNodes("master"); err != nil{
			fmt.Println("Error: get master information error "  + err.Error())
			log.Fatal(err)
		}else if len(masterNodes) > 1{
			fmt.Printf("Error: not supposed to see multiple master %d\n", len(masterNodes))
			log.Fatal(masterNodes)
		}else {
			serverString := masterNodes[0].Host + ":" + strconv.Itoa(masterNodes[0].Port)
			fmt.Println("master server String : " + serverString)
			conn, err := grpc.Dial("127.0.0.1:4100", grpc.WithInsecure())
			if err != nil {
				log.Fatal(err)
			}
			masterClient :=  master.NewShardingServiceClient(conn)
			/* start a separate timer, conn should wait until timer end. */
			go func() {
				defer conn.Close()
				fmt.Println("	update configuration every period time")
				for range time.Tick(time.Millisecond * 200){
					fmt.Println("	querying configuration")
					conf, err := masterClient.Query(context.Background(), &master.QueryRequest{ConfVersion: -1})
					if err != nil{
						log.Fatal(err)
					}
					if conf.Version < slaveServer.Version {
						fmt.Println("Error: local version larger than remote version")
					}else if conf.Version == slaveServer.Version{
						// dont do anything, just skip
					}else {
						slaveServer.ShardList = conf.Mapping[slaveServer.GroupId].Shards
						slaveServer.Version = conf.Version

						/* TODO: remove those shards not used */
					}
				}
				fmt.Println("should never reach here")
			}()

		}
	}()

	primaryPath := "slave_primary/" + strconv.Itoa(groupID)
	backupPath := "slave_backup/" + strconv.Itoa(groupID)
	slaveNode := zk_client.ServiceNode{Name: primaryPath, Host: ip, Port: port, Hostname: hostname}
	//fmt.Printf("slavenode %+v\n", slaveNode)
	if err := sdClient.TryPrimary(&slaveNode); err != nil {
		if err.Error() != "zk: node already exists" {
			panic(err)
		}
	} else {
		/*select as primary node, just return and wait for rpc service to start*/
		/*nodes, err := sdClient.GetNodes(primaryPath)
		if err != nil {
			panic(err)
		}
		for _, node := range nodes {
			fmt.Println(node.Host, node.Port, node.Hostname)
		}*/
		return
	}
	/* all ready exist a primary*/
	slaveNode = zk_client.ServiceNode{Name: backupPath, Host: ip, Port: port, Hostname: hostname}
	if err := sdClient.Register(&slaveNode); err != nil {
		panic(err)
	}
	/*
	nodes, err := sdClient.GetNodes(backupPath)
	if err != nil {
		panic(err)
	}
	for _, node := range nodes {
		fmt.Println(node.Host, node.Port, node.Hostname)
	}*/

}
