package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	. "ds/go/slave"
)

func main() {

	conn, err := grpc.Dial("0.0.0.0:4000", grpc.WithInsecure()) // only for testing
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := NewKVServiceClient(conn)
	reply, err := client.Put(context.Background(), &String{Value: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
	reply, err = client.Get(context.Background(), &String{Value: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
	reply, err = client.Del(context.Background(), &String{Value: "hello"})

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
}
