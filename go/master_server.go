package main

import (
	"ds/go/common/zk_client"
	"ds/go/master"
	"log"
	"os"
	"strconv"
)

func main() { // start shardMaster Service and register Service according to cmdline arguments

	/* read cmdline arguments and load configuration file*/
	args := os.Args[1:] // args without program name, determine IP:Port hostname of this master.
	ip := args[0]
	port, _ := strconv.Atoi(args[1])
	hostname := args[2]
	shardNum, _ := strconv.Atoi(args[3])

	zkClient,err := zk_client.CreateClientFromConfigurationFile("./configuration.json")
	if err != nil {
		log.Fatal(err)
	}

	masterServer := master.NewServer(master.ServerConf{
		IP:       ip,
		Port:     port,
		ShardNum: shardNum,
		Hostname: hostname,
	},zkClient)
	defer masterServer.Serve()

}
