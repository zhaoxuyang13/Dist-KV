package slave

//noinspection ALL
import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	//"fmt"
	"net"
)



type RPCServer struct {
	slave *Slave

}
func NewRPCServer(slave *Slave) *RPCServer {
	return &RPCServer{
		slave: slave,
	}
}

func (s *RPCServer) Serve(){
	/* start RPC Service on ip:Port */
	grpcServer := grpc.NewServer()
	RegisterKVServiceServer(grpcServer, s)
	serverString := s.slave.ServerString()
	//TODO extract net.Listen out.
	listen, err := net.Listen("tcp", serverString) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("slave rpc service serving at ip:Port " + serverString)
	grpcServer.Serve(listen)

}

/*
Put : RPC call to Put a key value
*/
func (s *RPCServer) Put(ctx context.Context, args *Request) (*Empty, error) {
	shardID := int(args.ShardID)
	return s.slave.Put(args.Key,args.Value,shardID)
}
/*
Get : RPC call to Get a key */
func (s *RPCServer) Get(ctx context.Context, args *Request) (*Response, error) {
	shardID := int(args.ShardID)

	return s.slave.Get(args.Key,shardID)
}
/*
Del: RPC call to delete a key
*/
func (s *RPCServer) Del(ctx context.Context, args *Request) (*Empty, error) {
	shardID := int(args.ShardID)

	return s.slave.Del(args.Key, shardID)
}
/*
SyncRequest
*/
func (s*RPCServer)Sync(ctx context.Context, req *SyncRequest)(*Empty, error) {
	/* no need to acquire lock when sync because primary already has lock */
	return s.slave.Sync(
		request{
			Key:     req.Key,
			Value:   req.Value,
			ShardID: int(req.ShardID),
			ReqCode: int(req.ReqCode),
		})
}
/*
RPC call to Transfer a Shard to here
*/
func (s *RPCServer) TransferShard(ctx context.Context, req *ShardRequest) (*Empty, error) {

	shardID := int(req.ShardID)
	storage := req.Storage

	return s.slave.TransferShard(shardID,storage)
}