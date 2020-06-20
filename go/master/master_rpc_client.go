package master

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"strconv"
)


type RPCClient struct {
	client ShardingServiceClient
	conn *grpc.ClientConn
}

func NewRPCClient(conf ServerConf) (*RPCClient,error) {
	serverString := conf.IP + ":" + strconv.Itoa(conf.Port)
	/* connect to master */
	conn, err := grpc.Dial(serverString, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return &RPCClient{
		client: NewShardingServiceClient(conn),
		conn : conn,
	},nil
}

func (c *RPCClient)Close() error {
	return c.conn.Close()
}


func (c *RPCClient)Query(version int)(*Configuration,error){

	conf , err := c.client.Query(context.Background(), &QueryRequest{
		ConfVersion:  int32(version),
	})
	return NewConf(conf),err
}

func (c *RPCClient)Join(newGroups map[int][]string) error {

	confNewGroups := make(map[int32]*JoinRequest_ServerConfs)
	for gid, servers := range newGroups {
		confServers := make([]string,0)
		confServers = append(confServers, servers...)
		confNewGroups[int32(gid)] = &JoinRequest_ServerConfs{
			Names: confServers,
		}
	}
	_, err := c.client.Join(context.Background(), &JoinRequest{
		NewGroups: confNewGroups,
	})
	return err
}

func (c *RPCClient)Leave(groupIDs []int) error {

	confGroupIDs := make([]int32,0)
	for _, groupID := range groupIDs {
		confGroupIDs = append(confGroupIDs, int32(groupID))
	}
	_, err := c.client.Leave(context.Background(), &LeaveRequest{
		GroupIDs: confGroupIDs,
	})
	return err
}
