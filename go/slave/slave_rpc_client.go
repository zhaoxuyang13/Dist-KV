package slave

//noinspection ALL
import (
	"context"
	"google.golang.org/grpc"
	"log"
)



type RPCClient struct {
	hostname string
	client KVServiceClient
	conn *grpc.ClientConn
}
func NewRPCClient(conf ServerConf) (*RPCClient,error) {

	serverString := conf.ServerString()
	/* connect to master */
	conn, err := grpc.Dial(serverString, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return &RPCClient{
		client: NewKVServiceClient(conn),
		conn : conn,
	},nil
}


func (c *RPCClient)Close() error {
	return c.conn.Close()
}

/*
Put : RPC call to Put a key value
*/
func (c *RPCClient) Put(key string, value string, shardID int) (*Empty, error) {
	return c.client.Put(context.Background(), &Request{
		ShardID: int32(shardID),
		Key:     key,
		Value:   value,
	})
}
/*
Get : RPC call to Get a key */
func (c *RPCClient) Get(key string, shardID int) (*Response, error) {

	return c.client.Get(context.Background(),&Request{
		ShardID: int32(shardID),
		Key:     key,
	})
}
/*
Del: RPC call to delete a key
*/
func (c *RPCClient) Del(key string, shardID int) (*Empty, error) {
	return c.client.Del(context.Background(),&Request{
		ShardID: int32(shardID),
		Key:     key,
	})
}
/*
SyncRequest
*/
func (c*RPCClient)Sync(req request)(*Empty, error) {
	return c.client.Sync(context.Background(),&SyncRequest{
		Key:     req.Key,
		Value:   req.Value,
		ShardID: int32(req.ShardID),
		ReqCode: int32(req.ReqCode),
	})
}
/*
RPC call to Transfer a Shard to here
*/
func (c *RPCClient) TransferShard(shardID int, storage map[string]string) (*Empty, error) {
	return c.client.TransferShard(context.Background(),&ShardRequest{
		Storage: storage,
		ShardID: int32(shardID),
	})
}