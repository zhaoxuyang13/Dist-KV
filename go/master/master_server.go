package master

import (
	"ds/go/common/zk_client"
	"log"
)

type Server struct {
	master *ShardMaster
	rpcServer RPCServer
	zkClient *zk_client.Client
}
func NewServer(conf ServerConf, zkClient *zk_client.Client) *Server {
	master := NewShardMaster(conf)
	return &Server{
		master:    master,
		rpcServer: NewRPCServer(master),
		zkClient:  zkClient,
	}
}
func (s *Server)Serve(){

	masterNode := zk_client.ServiceNode{Name: "master", IP: s.master.ip, Port: s.master.port,Hostname: s.master.hostname}

	/* only one master allow, seems ok to use tryPrimary logic*/
	if err := s.zkClient.TryPrimary(masterNode); err != nil {
		log.Fatalf("error: %v", err)
	}

	s.rpcServer.Serve()
}