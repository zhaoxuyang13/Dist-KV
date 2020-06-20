package master

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
)


type RPCServer struct {
	master *ShardMaster
}
/* todo: change some of thess class and method to private*/
func NewRPCServer(master *ShardMaster) RPCServer {
	return RPCServer{
		master: master,
	}
}
/* when start is called, the current process is blocked, and waiting for requests */
func (s *RPCServer) Serve(){

	/* start RPC Service on ip:port */
	grpcServer := grpc.NewServer()
	RegisterShardingServiceServer(grpcServer, s)
	serverString := s.master.ServerString()
	listen, err := net.Listen("tcp", serverString) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}

	/* defer execute in LIFO, close connection at last*/
	defer func() {
		fmt.Println("master rpc service serving at ip:port " + serverString)
		grpcServer.Serve(listen)
	}()
}


/*
Join : join several new groups to shardMaster, and re-balance
*/
func (s *RPCServer) Join(ctx context.Context, req *JoinRequest) (*Empty, error) {
	log.Printf("receive a Join RPC, %v\n", req.NewGroups)

	newGroups := make(map[int][]string)
	for gid, serverConfs := range req.NewGroups{
		servers := make([]string,0)
		servers = append(servers, serverConfs.Names...)
		newGroups[int(gid)] = servers
	}
	if err := s.master.Join(newGroups); err != nil {
		return &Empty{},err
	}else {
		return &Empty{}, nil
	}
}

/*
Leave : leave several old groups from shardMaster, and re-balance
*/
func (s *RPCServer) Leave(ctx context.Context, req *LeaveRequest) (*Empty, error) {
	log.Printf("receive a Leave RPC, %v\n", req.GroupIDs)
	GroupIDs := make([]int,0)
	for _,groupID := range req.GroupIDs {
		GroupIDs = append(GroupIDs, int(groupID))
	}
	if err := s.master.Leave(GroupIDs); err != nil {
		return &Empty{}, err
	}else {
		return &Empty{}, nil
	}
}

/*
Query : Query for configuration giving a version
*/
func (s *RPCServer) Query(ctx context.Context, req *QueryRequest) (*Conf, error) {
	confVersion := int(req.ConfVersion)

	if configuration,err := s.master.Query(confVersion); err != nil {
		return nil, err
	}else {
		return configuration.ToConf()
	}

}
