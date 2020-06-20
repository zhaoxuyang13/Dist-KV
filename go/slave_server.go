package main

import (
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
	groupIDs := make([]int, 0)
	for i := 1; i < len(args); i++ {
		id, _ := strconv.Atoi(args[i])
		groupIDs = append(groupIDs, id)
	}

	masterNode, err := client.Get1Node("master")
	if err != nil {
		log.Fatal("error: master died\n")
	}
	masterClient, err := master.NewRPCClient(master.ServerConf{
		IP:       masterNode.IP,
		Port:     masterNode.Port,
		Hostname: masterNode.Hostname,
	})
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer masterClient.Close()
	/* read registered servers from /node/slave_primary/gid and /node/slave_backup/gid,
	and save to mappings, for join-group request.
	*/
	if command == "join-group" {
		newGroups := make(map[int][]string)
		for _, gid := range groupIDs {
			servers := make([]string, 0)
			if primary, err := client.Get1Node("slave_primary/" + strconv.Itoa(gid)); err != nil {
				fmt.Println("get primary node error :" + err.Error())
			} else {
				servers = append(servers, primary.Hostname)
			}
			if backups, err := client.GetNodes("slave_backup/" + args[1]); err != nil {
				fmt.Println("get primary node error :" + err.Error())
			} else {
				for _, backup := range backups {
					servers = append(servers, backup.Hostname)
				}
			}
			newGroups[gid] = servers
		}
		err := masterClient.Join(newGroups)
		if err != nil {
			log.Println("join-group rpc failed : " + err.Error())
		}
	} else if command == "leave-group" {
		err := masterClient.Leave(groupIDs)
		if err != nil {
			log.Println("leave-group rpc failed: " + err.Error())
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
	sdClient, err := zk_client.NewClient(conf.ServerStrings(), "/node", 10)
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
	args := os.Args[1:]                                      // args without program name, determine IP:Port hostname of this slave.
	if args[0] == "join-group" || args[0] == "leave-group" { // if only 2 args provided, treat it as a configuration change
		updateConf(args, sdClient)
		return
	}

	ip := args[0] // cannot use 0.0.0.0 as localhost, but 127.0.0.1, not a problem in multi computer scenario
	port, err := strconv.Atoi(args[1])
	hostname := args[2]
	groupID, err := strconv.Atoi(args[3])

	/* start RPC Service on ip:port */
	slaveServer := slave.NewSlave(slave.ServerConf{
		Hostname: hostname,
		IP:       ip,
		Port:     port,
		GroupID:  groupID,
	},sdClient)
	slaveServer.StartService(ip, port, hostname)

}
