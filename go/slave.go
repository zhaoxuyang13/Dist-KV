package main

//noinspection ALL
import (
	"context"
	. "ds/go/utils"
	. "ds/go/slave"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
)

type Slave struct{}
/*
	Filename : Slave_file_node.json
	{
		"key": "value"
	}
*/
type LocalStorage struct{
	storage map[string]string
}
var localStorages map[int32]*LocalStorage = make(map[int32]*LocalStorage) // map from vnodeNum to LocalStorage

/* TODO add check for Vnode number's validation, slave should maintain valid vnode list. and respond error if not */
func (s *Slave) Put(ctx context.Context, args *Request) (*Response, error) {

	/* TODO : acquire lock for the key*/

	/* create if not exist, append if exist*/
	if localStorage,ok := localStorages[args.VnodeNum]; ok{
		localStorage.storage[args.GetKey()] = args.GetValue()
	}else {
		ls := LocalStorage {
			storage: make(map[string]string),
		}
		ls.storage[args.GetKey()] = args.GetValue()
		localStorages[args.VnodeNum] = &ls
	}


	/* TODO : release lock for the key*/

	reply := Response{
		Value: "put "+ strconv.Itoa(int(args.VnodeNum)) + " of key " + args.GetKey() + " of value " + args.GetValue(),
	} // echo server primitive

	return &reply, nil
}
func (s *Slave) Get(ctx context.Context, args *Request) (*Response, error) {


	/* TODO : acquire lock for the key, but is it necessary to put a lock on read ?*/
	/* create if not exist, append if exist*/
	localStorage,ok := localStorages[args.VnodeNum]
	if !ok {
		return nil,errors.New("Vnode not exist on this machine\n")
	}

	res,ok := localStorage.storage[args.GetKey()]
	if !ok {
		return nil,errors.New("Key not exist on this machine's vnode\n")
	}

	reply := Response{
		Value: res,
	} // echo server primitive

	/* TODO : release lock for the key*/
	return &reply, nil
}
func (s *Slave) Del(ctx context.Context, args *Request) (*Response, error) {

	/* TODO : acquire lock for the key*/
	/* create if not exist, append if exist*/
	localStorage, ok := localStorages[args.VnodeNum]
	if !ok {
		return nil,errors.New("Vnode not exist on this machine\n")
	}

	_,ok = localStorage.storage[args.GetKey()]
	if ok {
		delete(localStorage.storage, args.GetKey())
	}else{
		return nil,errors.New("Key not exist on this machine's vnode\n")
	}

	reply := Response{
		Value: "successful delete " + args.GetKey(),
	} // echo server primitive

	/* TODO : release lock for the key*/

	return &reply, nil
}

func main() { // start RPC Service and register Service according to cmdline arguments

	/* read cmdline arguments and load configuration file*/
	args := os.Args[1:] // args without program name, determine Ip:Port of this slave.
	ip := args[0]
	port, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal(err)
	}

	content, err := ioutil.ReadFile("./configuration.json")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("File contents: %s", content) // remove when correct

	var conf Conf
	err = json.Unmarshal([]byte(content), &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- t:\n%v\n\n", conf) // remove when correct

	/* start RPC Service on ip:port */
	grpcServer := grpc.NewServer()
	RegisterKVServiceServer(grpcServer, new(Slave))

	listen, err := net.Listen("tcp", ip+":"+args[1]) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}
	grpcServer.Serve(listen)

	client, err := NewClient(conf.ServersString(), "/node", 10)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	slave := ServiceNode{Name: "slave", Host: ip, Port: port}

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
