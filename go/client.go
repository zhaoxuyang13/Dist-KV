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

func (c *UserClient) Key2Server(key string) (string,int, error) {
	shardID := Utils.Key2shard(key, c.conf.ShardNum)
	gid, exist := c.conf.Assignment[shardID]
	if !exist {
		return "",0, ErrInvalidConf
	}
	serviceNode, exist := c.primaries[gid]
	if !exist{
		return "",0, ErrInvalidConf
	}
	serverString := serviceNode.Host + ":" + strconv.Itoa(serviceNode.Port)
	return serverString,gid, nil
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
	print("put " + key + " - " + value + "\n")

	if targetServer,gid,err := c.Key2Server(args[0]); err!=nil {
		panic(err)
	}else {
		if conn, err := grpc.Dial(targetServer, grpc.WithInsecure());err != nil {
			panic(err)
		}else {
			defer conn.Close()
			client := slave.NewKVServiceClient(conn)
			if _, err := client.Put(context.Background(), &slave.Request{
				ShardID: int32(gid),
				Key: key,
				Value: value,
			}); err != nil {
				panic(err)
			}else {
				fmt.Printf("\"%s\" = \"%s\"\n",key,value)
			}
		}
	}

}
func (c *UserClient) Get(args []string) {
	key := args[0]
	print("get " + key + "\n")
	if targetServer,gid,err := c.Key2Server(args[0]); err!=nil {
		//fmt.Println(err.Error())
		panic(err)
	}else {
		if conn, err := grpc.Dial(targetServer, grpc.WithInsecure());err != nil {
			//fmt.Println(err.Error())
			panic(err)
		}else {
			defer conn.Close()
			client := slave.NewKVServiceClient(conn)
			if response, err := client.Get(context.Background(), &slave.Request{
				ShardID: int32(gid),
				Key: key,
			}); err != nil {
				panic(err)
			}else {
				fmt.Printf("\"%s\" = \"%s\" \n",key,response.Value)
			}
		}
	}
}
func (c *UserClient) Del(args []string) {
	key := args[0]
	print("del " + key + "\n")
	if targetServer,gid,err := c.Key2Server(args[0]); err!=nil {
		//fmt.Println(err.Error())
		panic(err)
	}else {
		if conn, err := grpc.Dial(targetServer, grpc.WithInsecure());err != nil {
			//fmt.Println(err.Error())
			panic(err)
		}else {
			defer conn.Close()
			client := slave.NewKVServiceClient(conn)
			if _, err := client.Del(context.Background(), &slave.Request{
				ShardID: int32(gid),
				Key: key,
			}); err != nil {
				panic(err)
			}else {
				fmt.Printf("\"%s\" deleted\n",key)
			}
		}
	}
}

type UserClient struct {
	conf      master.Configuration
	primaries map[int]zk_client.ServiceNode
	zkClient  *zk_client.SdClient
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
	c.zkClient, err = zk_client.NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	return nil
}

var ErrInvalidVersion = errors.New("local version larger than remote version")

/* fetch configuration from master server, only call it after initMasterRpc */
func (c *UserClient) updateConf() (bool, error) {
	msClient,conn,err := master.GetMasterRPCClient(c.zkClient)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	fmt.Println("updating conf...")
	if conf, err := msClient.Query(context.Background(), &master.QueryRequest{ConfVersion: -1}); err != nil {
		log.Fatal(err)
		return false, err
	} else if int(conf.Version) < c.conf.Version {
		return false, ErrInvalidVersion
	} else if int(conf.Version) == c.conf.Version {
		return false, nil
	} else {
		c.conf = *master.NewConf(conf)
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
	for gid := range c.conf.Groups {
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

	/* TODO: move this to test file, and build a client according to it after master done.
	conn, err := grpc.Dial("0.0.0.0:4000", grpc.WithInsecure()) // only for testing
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := NewKVServiceClient(conn)

	reply, err := client.Put(context.Background(), &Request{Value: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
	reply, err = client.Get(context.Background(), &Request{Value: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
	reply, err = client.Del(context.Background(), &Request{Value: "hello"})

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
	*/
}
