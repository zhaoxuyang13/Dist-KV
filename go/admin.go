package main

import (
	"ds/go/common/zk_client"
	"ds/go/master"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]

	zkClient,err := zk_client.CreateClientFromConfigurationFile("./configuration.json")
	if err != nil {
		log.Printf(err.Error())
		return
	}
	defer func() {
		if err := recover(); err !=nil {
			log.Printf("ultimate reason of crash %+v\n",err)
		}
		log.Println("closing zookeeper client")
		zkClient.Close()
	}()

	/* read cmdline arguments and load configuration file*/
	/* parse arguments */
	command := args[0]
	groupIDs := make([]int, 0)
	for i := 1; i < len(args); i++ {
		id, _ := strconv.Atoi(args[i])
		groupIDs = append(groupIDs, id)
	}

	masterNode, err := zkClient.Get1Node("master")
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
			if primary, err := zkClient.Get1Node("slave_primary/" + strconv.Itoa(gid)); err != nil {
				fmt.Println("get primary node error :" + err.Error())
			} else {
				servers = append(servers, primary.Hostname)
			}
			if backups, err := zkClient.GetNodes("slave_backup/" + args[1]); err != nil {
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
