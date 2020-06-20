package main

import (
	"ds/go/common/zk_client"
	"ds/go/slave"
	"log"
	"os"
	"strconv"
)

func main() {

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
	args := os.Args[1:]                                      // args without program name, determine IP:Port hostname of this slave.

	ip := args[0] // cannot use 0.0.0.0 as localhost, but 127.0.0.1, not a problem in multi computer scenario
	port, err := strconv.Atoi(args[1])
	hostname := args[2]
	groupID, err := strconv.Atoi(args[3])

	/* start RPC Service on ip:port */
	slaveServer := slave.NewServer(slave.ServerConf{
		Hostname: hostname,
		IP:       ip,
		Port:     port,
		GroupID:  groupID,
	},zkClient)
	/* serve will block */
	slaveServer.Serve()

}
