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
/* fetch group information from zookeeper cluster,
	and then RPC-call master to update master configuration */
func updateConf(args []string,client *zk_client.SdClient){
	command := args[0]
	groupIDs := make([]int32, 0)
	for i := 1;i < len(args); i ++ {
		id, _ := strconv.Atoi(args[i])
		groupIDs = append(groupIDs, int32(id))
	}
	masterClient,conn,_ := master.GetMasterRPCClient(client)
	defer conn.Close()
	mappings := make(map[int32]*master.JoinRequest_ServerConfs)
	for _, gid := range groupIDs{
		conf := master.JoinRequest_ServerConfs{
			Names: make([]string,0),
		}
		if primaries, err := client.GetNodes("slave_primary/" + strconv.Itoa(int(gid))); err != nil || len(primaries) != 1 {
			fmt.Println("get primary node error :" + err.Error())
		}else {
			conf.Names = append(conf.Names, primaries[0].Hostname)
		}
		if backups, err := client.GetNodes("slave_backup/" + args[1]); err != nil {
			fmt.Println("get primary node error :" + err.Error())
		}else {
			for _,backup := range backups{
				conf.Names = append(conf.Names, backup.Hostname)
			}
		}
		mappings[gid] = &conf
	}

	if command == "join-group"{
		masterClient.Join(context.Background(),  &master.JoinRequest{
			Mapping: mappings,
		})
	}else if command == "leave-group" {
		_, err := masterClient.Leave(context.Background(), &master.LeaveRequest{GidList: groupIDs })
		if err != nil {
			fmt.Println("leave-group rpc failed: " + err.Error())
		}
	}
}


func main() { // start RPC Service and register Service according to cmdline arguments

	/* read configuration from json file, and start connection with zookeeper cluster */
	content, err := ioutil.ReadFile("./configuration.json")
	if err != nil {
		log.Fatal(err)
	}
	var conf zk_client.Conf
	err = json.Unmarshal(content, &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	sdClient, err := zk_client.NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	defer sdClient.Close()


	/* read cmdline arguments and load configuration file*/
	args := os.Args[1:] // args without program name, determine Ip:Port hostname of this slave.
	if args[0] == "join-group" || args[0] == "leave-group" { // if only 2 args provided, treat it as a configuration change
		updateConf(args,sdClient)
		return
	}
	ip := args[0]  // cannot use 0.0.0.0 as localhost, but 127.0.0.1, not a problem in multi computer scenario
	port, err := strconv.Atoi(args[1])
	hostname := args[2]
	groupID, err := strconv.Atoi(args[3])

	/* start RPC Service on ip:port */
	grpcServer := grpc.NewServer()
	slaveServer := &slave.Slave{
		LocalStorages: make(map[int]*slave.LocalStorage),
		Version: 	   0,
		GroupInfo: master.Group{
			Gid:     groupID,
			Servers: make([]string, 0),
			Shards:  make([]int,0),
		},
	}
	slave.RegisterKVServiceServer(grpcServer, slaveServer)
	listen, err := net.Listen("tcp", ip+":"+args[1]) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}
	/* defer execute in LIFO, close connection at last*/
	defer func() {
		fmt.Println("slave server start serving requests at " + ip + ":" + strconv.Itoa(port))
		if err := grpcServer.Serve(listen); err != nil {
			log.Fatal(err)
		}
	}()

	/* start timer, slave server will retrieve configuration from master every time period*/
	/* Wrap it up with function in module slave */
	defer func(){

			masterClient,conn,err := master.GetMasterRPCClient(sdClient)
			if err !=nil {
				fmt.Println(err.Error())
				return
			}
			/* start a separate timer, conn should wait until timer end. */
			go func() {
				defer conn.Close()
				fmt.Println("	update configuration every period time")
				for range time.Tick(time.Millisecond * 200){
					//fmt.Println(".")
					conf, err := masterClient.Query(context.Background(), &master.QueryRequest{ConfVersion: -1})
					if err != nil{
						log.Fatal(err)
					}
					if int(conf.Version) < slaveServer.Version {
						fmt.Println("Error: local version larger than remote version")
					}else if int(conf.Version) == slaveServer.Version{
						// dont do anything, just skip
					}else {
						// deep copy
						slaveServer.GroupInfo.Shards = make([]int, 0)
						slaveServer.GroupInfo.Servers = make([]string,0)
						slaveServer.Version = int(conf.Version)
						if group, exist := conf.Mapping[int32(groupID)]; exist{
							for _,shard := range group.Shards{
								slaveServer.GroupInfo.Shards = append(slaveServer.GroupInfo.Shards, int(shard))
							}
							for _,server := range group.Servers{
								slaveServer.GroupInfo.Servers = append(slaveServer.GroupInfo.Servers, server)
							}
							fmt.Printf("update to: %+v\n",slaveServer)
						}else {
							// this group not belongs to confs, not copy configuration,
						}
						/* TODO: remove those shards not used */
					}
				}
				fmt.Println("should never reach here")
			}()
	}()

	primaryPath := "slave_primary/" + strconv.Itoa(groupID)
	backupPath := "slave_backup/" + strconv.Itoa(groupID)
	slaveNode := zk_client.ServiceNode{Name: primaryPath, Host: ip, Port: port, Hostname: hostname}
	//fmt.Printf("slavenode %+v\n", slaveNode)
	if err := sdClient.TryPrimary(&slaveNode); err != nil {
		slaveServer.Primary = false
		if err.Error() != "zk: node already exists" {
			panic(err)
		}
	} else {
		/*select as primary node, just return and wait for rpc service to start*/
		slaveServer.Primary = true
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
