package main

import (
	"bufio"
	"context"
	"ds/go/common/Utils"
	"ds/go/common/zk_client"
	"ds/go/master"
	"ds/go/slave"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

/* REPL interface of client */
func help() {
	print("$ > Dist-KV V0.0, ZXY\n" +
		"$ > Commands: \n" +
		"$ >	put [key] [value]\n" +
		"$ > 	get [key] \n" +
		"$ > 	del [key] \n" +
		"$ > key,value are strings\n" +
		"$ > print 'exit' to exit\n")
}
func printRepl() {
	print("$ > ")
}
func get(r *bufio.Reader) string {
	t, _ := r.ReadString('\n')
	return strings.TrimSpace(t)
}
func shouldContinue(text string) bool {
	if strings.EqualFold("exit", text) {
		return false
	}
	return true
}

func printInvalidCmd(text string) {
	print("invalid cmd\n")
}

var ErrInvalidConf = errors.New("problematic configuration file")
var MaxRetry = 3
func (c *UserClient) Key2Server(key string) (string,int, error) {
	if c.conf.Version == 0 {
		return "",0,ErrInvalidConf
	}
	shardID := Utils.Key2shard(key, c.conf.ShardNum)
	gid, exist := c.conf.Assignment[shardID]
	if !exist {
		return "",0, ErrInvalidConf
	}
	serviceNode, exist := c.primaries[gid]
	if !exist{
		return "",0, ErrInvalidConf
	}
	serverString := serviceNode.IP + ":" + strconv.Itoa(serviceNode.Port)
	return serverString,shardID, nil
}

/* Put a key-value  pair
1. ask local configuration for key's server
2. on fail, update configuration and retry.
		fail1: primary-backup change => cannot connect to server.
		fail2: configuration change => shard not managed by me
	solution: update configuration, update primary information, and retry.
*/
func (c *UserClient) Put(args []string) {
	key := args[0]
	value := args[1]
	try := 0
	for try < MaxRetry {
		err := c.SimplePut(args)
		if err == nil {
			fmt.Printf("put success: \"%s\" = \"%s\" \n",key,value)
			return
		}else {
			if err == ErrInvalidConf{
				c.updateConf()
			}else {
				errStatus, _ := status.FromError(err)
				try++
				switch errStatus.Code() {
				case codes.Unavailable:
					fallthrough
				case codes.PermissionDenied: // TODO : not primary case
					c.updateConf()
				default:
					fmt.Println("unhandled err: " + err.Error())

				}
			}
		}
	}

}
/* simple put don't handle error */
func (c *UserClient) SimplePut(args []string) error {
	key := args[0]
	value := args[1]
	print("put " + key + " - " + value + "\n")
	if targetServer, shardID, err := c.Key2Server(args[0]); err != nil {
		return err
	} else {
		if conn, err := grpc.Dial(targetServer, grpc.WithInsecure()); err != nil {
			fmt.Println("client: grpc dialing failed, error: " + err.Error())
			return err
		} else {
			defer conn.Close()
			client := slave.NewKVServiceClient(conn)
			if _, err := client.Put(context.Background(), &slave.Request{
				ShardID: int32(shardID),
				Key:     key,
				Value:   value,
			}); err != nil {
				return err
			}else {
				return nil
			}
		}
	}
}
func (c *UserClient) Get(args []string) {
	key := args[0]
	try := 0
	for try < MaxRetry {
		value, err := c.SimpleGet(args)
		if err == nil {
			fmt.Printf("\"%s\" = \"%s\" \n",key,value)
			return
		}else {
			errStatus, _ := status.FromError(err)
			try ++
			switch errStatus.Code() {
			case codes.NotFound:
				fmt.Printf("\"%s\" not exist \n",key)
				return
			case codes.Unavailable: fallthrough
			case codes.PermissionDenied: // TODO : not primary case
				c.updateConf()
			default:
				fmt.Println("unhandled err: " + err.Error())
			}
		}
	}
}
func (c *UserClient) SimpleGet(args []string) (string,error){
	key := args[0]
	print("get " + key + "\n")
	if targetServer,shardID,err := c.Key2Server(args[0]); err!=nil {
		panic(err)
	}else {
		if conn, err := grpc.Dial(targetServer, grpc.WithInsecure());err != nil {
			fmt.Println("client: grpc dialing failed, error: " + err.Error())
			return "",err
		}else {
			defer conn.Close()
			client := slave.NewKVServiceClient(conn)
			if response, err := client.Get(context.Background(), &slave.Request{
				ShardID: int32(shardID),
				Key: key,
			}); err != nil {
				return "",err
			}else {
				return response.Value,nil
			}
		}
	}
}
func (c *UserClient) Del(args []string) {
	key := args[0]
	try := 0
	for try < MaxRetry {
		err := c.SimpleDel(args)
		if err == nil {
			fmt.Printf("\"%s\" deleted\n",key)
			return
		}else {
			errStatus, _ := status.FromError(err)
			try ++
			switch errStatus.Code() {
			case codes.NotFound:
				fmt.Printf("\"%s\" not exist \n",key)
				return
			case codes.Unavailable: fallthrough
			case codes.PermissionDenied: // TODO : not primary case
				c.updateConf()
			default:
				fmt.Println("unhandled err: " + err.Error())
			}
		}
	}
}
func (c *UserClient) SimpleDel(args []string) error {
	key := args[0]
	print("del " + key + "\n")
	if targetServer,shardID,err := c.Key2Server(args[0]); err!=nil {
		panic(err)
	}else {
		if conn, err := grpc.Dial(targetServer, grpc.WithInsecure());err != nil {
			fmt.Println("client: grpc dialing failed, error: " + err.Error())
			return err
		}else {
			defer conn.Close()
			client := slave.NewKVServiceClient(conn)
			if _, err := client.Del(context.Background(), &slave.Request{
				ShardID: int32(shardID),
				Key: key,
			}); err != nil {
				return err
			}else {
				return nil
			}
		}
	}
}
type UserClient struct {
	conf      master.Configuration
	primaries map[int]zk_client.ServiceNode
	zkClient  *zk_client.Client
}

func (c *UserClient) initZkClient() error {
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
	c.zkClient, err = zk_client.NewClient(conf.ServerStrings(), "/node", 10)
	if err != nil {
		panic(err)
	}
	return nil
}

var ErrInvalidVersion = errors.New("local version larger than remote version")

/* fetch configuration from master server, only call it after initMasterRpc */
func (c *UserClient) updateConf() (bool, error) {

	masterNode,err := c.zkClient.Get1Node("master")
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
		return false, err
	}
	defer masterClient.Close()
	fmt.Printf("updating conf...")
	if conf, err := masterClient.Query(-1); err != nil {
		log.Fatal(err)
		return false, err
	} else if conf.Version < c.conf.Version {
		log.Printf("invalid version num %+v\n", conf)
		return false, ErrInvalidVersion
	} else {
		c.conf = *conf
		if err := c.getPrimaries(); err != nil {
			return false, nil
		}
		fmt.Printf("updated conf %+v\n",c.conf)
		return true, nil
	}
}

var ErrDuplicatePrimary = errors.New("not supposed to get duplicate primary entries for one group ")

/* fetch primaries' ip:port from zookeeper cluster */
func (c *UserClient) getPrimaries() error {
	fmt.Println("	updating primary information ...")
	for gid := range c.conf.Id2Groups {
		if primaries, err := c.zkClient.GetNodes("slave_primary/" + strconv.Itoa(gid)); err != nil || len(primaries) != 1 {
			if len(primaries) != 1 {
				err = ErrDuplicatePrimary
			}
			return err
		} else {
			c.primaries[gid] = *primaries[0]
		}
	}
	return nil
}
func main() {
	client := UserClient{
		conf: master.Configuration{Version: 0},
		primaries: make(map[int]zk_client.ServiceNode),
		zkClient: nil,
	}
	if err := client.initZkClient(); err != nil {
		fmt.Println(err.Error())
		return
	}else {
		defer client.zkClient.Close()
	}
	if updated, err := client.updateConf(); err != nil {
		fmt.Println(err.Error())
		return
	} else if !updated {
		fmt.Println("no slave servers configuration detected")
	}
	reader := bufio.NewReader(os.Stdin)
	help()
	printRepl()
	text := get(reader)
	for ; shouldContinue(text); text = get(reader) {
		switch args := strings.Split(text, string(' ')); args[0] {
		case "put":
			if len(args) != 3 {
				printInvalidCmd(text)
			} else {
				client.Put(args[1:])
			}
		case "get":
			if len(args) != 2 {
				printInvalidCmd(text)
			} else {
				client.Get(args[1:])
			}
		case "del":
			if len(args) != 2 {
				printInvalidCmd(text)
			} else {
				client.Del(args[1:])
			}
		default:
			printInvalidCmd(text)
		}
		printRepl()
	}
	fmt.Println("Bye!")

}
