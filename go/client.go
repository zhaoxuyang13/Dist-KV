package main

import (
	"bufio"
	"strings"

	//"context"
	//. "ds/go/slave"
	"fmt"
	//"google.golang.org/grpc"
	//"log"
	"os"
)

/* REPL interface of client */
func help(){
	print("$ > Dist-KV V0.0, ZXY\n" +
		"$ > Commands: \n" +
		"$ >	put [key] [value]\n" +
		"$ > 	get [key] [value]\n" +
		"$ > 	del [key] [value]\n" +
		"$ > key,value are strings\n" +
		"$ > print 'exit' to exit\n" )
}
func printRepl(){
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

/* put a key-value  pair
1. go to master and ask for key's ip/port
2. go to client and put the value.
*/
func Put(args []string){
	print("put " + args[0] + " - " +args[1] +"\n")
}
func Get(args []string){
	print("get " + args[0] +"\n")
}
func Del(args []string){
	print("del " + args[0] +"\n")
}




func main() {

	reader := bufio.NewReader(os.Stdin)
	help()
	printRepl()
	text := get(reader)
	for ; shouldContinue(text); text = get(reader) {
		switch args := strings.Split(text, string(' ')); args[0] {
		case "put":
			if len(args) != 3 {
				printInvalidCmd(text)
			}else {
				Put(args[1:])
			}
		case "get":
			if len(args) != 2 {
				printInvalidCmd(text)
			}else {
				Get(args[1:])
			}
		case "del":
			if len(args) != 2 {
				printInvalidCmd(text)
			}else {
				Del(args[1:])
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
