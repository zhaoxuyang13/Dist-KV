package main

import (
	"context"
	"ds/go/common/zk_client"
	"ds/go/master"
	"ds/go/slave"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

/* fetch group information from zookeeper cluster,
and then RPC-call master to update master configuration */
func updateConf(args []string, client *zk_client.SdClient) {
	/* parse arguments */
	command := args[0]
	groupIDs := make([]int32, 0)
	for i := 1; i < len(args); i++ {
		id, _ := strconv.Atoi(args[i])
		groupIDs = append(groupIDs, int32(id))
	}

	/* read registered servers from /node/slave_primary/gid and /node/slave_backup/gid,
	and save to mappings, for join-group request.
	*/
	mappings := make(map[int32]*master.JoinRequest_ServerConfs)
	for _, gid := range groupIDs {
		conf := master.JoinRequest_ServerConfs{
			Names: make([]string, 0),
		}
		
		if primary, err := client.Get1Node("slave_primary/" + strconv.Itoa(int(gid))); err != nil  {
			fmt.Println("get primary node error :" + err.Error())
		} else {
			conf.Names = append(conf.Names, primary.Hostname)
		}
		if backups, err := client.GetNodes("slave_backup/" + args[1]); err != nil {
			fmt.Println("get primary node error :" + err.Error())
		} else {
			for _, backup := range backups {
				conf.Names = append(conf.Names, backup.Hostname)
			}
		}
		mappings[gid] = &conf
	}

	/* connect to master, do Join/Leave Request */
	masterClient, conn, _ := master.GetMasterRPCClient(client)
	defer conn.Close()
	if command == "join-group" {
		masterClient.Join(context.Background(), &master.JoinRequest{
			Mapping: mappings,
		})
	} else if command == "leave-group" {
		_, err := masterClient.Leave(context.Background(), &master.LeaveRequest{GidList: groupIDs})
		if err != nil {
			fmt.Println("leave-group rpc failed: " + err.Error())
		}
	}
}

func startZkFromFile(filename string) (*zk_client.SdClient, error) {
	/* read configuration from json file, and start connection with zookeeper cluster */
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var conf zk_client.Conf
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return nil, err
	}
	sdClient, err := zk_client.NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	return sdClient, nil
}

func main() { // start RPC Service and register Service according to cmdline arguments

	sdClient, err := startZkFromFile("./configuration.json")
	if err != nil {
		log.Fatal(err)
	}

	defer sdClient.Close()
	/* read cmdline arguments and load configuration file*/
	args := os.Args[1:]                                      // args without program name, determine Ip:Port hostname of this slave.
	if args[0] == "join-group" || args[0] == "leave-group" { // if only 2 args provided, treat it as a configuration change
		updateConf(args, sdClient)
		return
	}

	ip := args[0] // cannot use 0.0.0.0 as localhost, but 127.0.0.1, not a problem in multi computer scenario
	port, err := strconv.Atoi(args[1])
	hostname := args[2]
	groupID, err := strconv.Atoi(args[3])

	/* start RPC Service on ip:port */
	slaveServer := slave.CreateSlave(sdClient, groupID,hostname,ip,port)
	slaveServer.StartService(ip, port, hostname)

}
