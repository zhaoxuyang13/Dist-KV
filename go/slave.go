package main

//noinspection ALL
import (
	"context"
	. "ds/go/utils"
	. "ds/go/slave"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
)

type Slave struct{}

func (s *Slave) Put(ctx context.Context, args *String) (*String, error) {
	reply := String{
		Value: "put " + args.GetValue(),
	} // echo server primitive
	return &reply, nil
}
func (s *Slave) Get(ctx context.Context, args *String) (*String, error) {
	reply := String{
		Value: "Get " + args.GetValue(),
	} // echo server primitive
	return &reply, nil
}
func (s *Slave) Del(ctx context.Context, args *String) (*String, error) {
	reply := String{
		Value: "Del " + args.GetValue(),
	} // echo server primitive
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
