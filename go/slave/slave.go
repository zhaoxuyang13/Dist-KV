package slave

//noinspection ALL
import (
	"context"
	"ds/go/common/Utils"
	"ds/go/common/zk_client"
	"ds/go/master"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strconv"
	"sync"
	"time"
)
var (
	/* when request is for gid not belong to this group*/

	ErrWrongGroup = errors.New("key not managed by this group")
	/* when slave is backup and request is not from primary*/
	ErrNotPrimary = errors.New("don't send request to a back up server")
	/* for del and get operation, key is managed by this group, but not in the storage */
	ErrNotFound = errors.New("key not exist on this machine's storage")
)



type StorageState int
const (
	UNREADY StorageState = iota
	READY
	EXPIRED
)

/*
LocalStorages : in-memory kv store using map
*/
type LocalStorage struct {
	cond *sync.Cond
	lock *sync.RWMutex
	storage map[string]string
	state StorageState
}

/*
Slave : slave structure implement slave RPC semantics
*/
/* maybe convert to sync.Map */
type Slave struct{
	/* connection to zookeeper */
	ZkClient *zk_client.SdClient

	/* Am I primary node ?*/
	Primary bool

	/* the version number storage updated to */
	LocalVersion int

	/* lock for groupInfo.shards structure, acquire when modifying or reading*/
	ShardsLock *sync.RWMutex
	GroupInfo master.Group
		// Gid 	  : the group id this slave belongs to, used to check configuration
		// Shards : list of shards this slave will manage ,get from master every 100 milliseconds

	/* Configuration lock, lock when updating group info, RLock when accessing*/
	ConfLock sync.RWMutex
	/* local copy of the latest configuration got from master */
	Conf         master.Configuration

	/* lock for shardID -> *localstorage map */
	StorageLock *sync.RWMutex
	LocalStorages map[int]*LocalStorage // map from ShardID to LocalStorages

	/* wake up channel: contains threads that need to be wake up */

}

func (s *Slave) Put(ctx context.Context, args *Request) (*Empty, error) {
	fmt.Println("put")

	shardID := int(args.ShardID)

	s.ShardsLock.RLock()
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	s.ShardsLock.RUnlock()

	localStorage := s.assureStorage(shardID,READY)


	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()
	switch localStorage.state {
	case EXPIRED:
		log.Fatal("impossible to be expired and reach here")
		return nil,status.Errorf(codes.Unknown,"impossible to be expired and reach here" )
	case UNREADY:
	for localStorage.state != READY {
		localStorage.cond.Wait()
	}
	default:
	}

	localStorage.storage[args.GetKey()] = args.GetValue()

	/* release lock for the key, done by defer*/
	return &Empty{}, nil
}
func (s *Slave) Get(ctx context.Context, args *Request) (*Response, error) {
	fmt.Println("get")
	shardID := int(args.ShardID)

	s.ShardsLock.RLock()
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	s.ShardsLock.RUnlock()

	/* acquire r lock when read storage map*/
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	if localStorage, ok := s.LocalStorages[shardID]; !ok {
		return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
	}else {
		/* acquire r lock when read storage */
		localStorage.lock.RLock()
		defer localStorage.lock.RUnlock()
		if res, ok := localStorage.storage[args.GetKey()]; !ok {
			return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
		}else {
			return &Response{
				Value: res,
			}, nil
		}
	}
}
func (s *Slave) Del(ctx context.Context, args *Request) (*Empty, error) {
	fmt.Println("del")
	shardID := int(args.ShardID)

	s.ShardsLock.RLock()
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	s.ShardsLock.RUnlock()

	/* acquire r lock when read storage map*/
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	if localStorage, ok := s.LocalStorages[shardID]; !ok {
		/* localStorage not exist */
		return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
	}else {
		localStorage.lock.Lock()
		defer localStorage.lock.Unlock()
		if _, ok = localStorage.storage[args.GetKey()]; !ok {
			/* storage entry not exist*/
			return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
		} else {
			delete(localStorage.storage, args.GetKey())
			return &Empty{}, nil
		}
	}
	/*release lock for the key, done by defer*/
}
/*
assureStorage: atomic
*/
func (s *Slave) assureStorage(shard int,state StorageState) *LocalStorage{
	s.StorageLock.Lock()
	defer s.StorageLock.Unlock()
	if _,exist := s.LocalStorages[shard]; !exist{
		rwLock := new(sync.RWMutex)
		s.LocalStorages[shard] = &LocalStorage{
			storage: make(map[string]string),
			state: state,
			lock: rwLock,
			cond: sync.NewCond(rwLock),
		}
	}
	return s.LocalStorages[shard]
}
/*
acquireStorage: atomic
*/
func (s *Slave) acquireStorage(shard int) *LocalStorage{
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	return s.LocalStorages[shard]
}
func (s *Slave) TransferShard(ctx context.Context, req *ShardRequest) (*Empty, error) {

	shardID := int(req.ShardID)

	localStorage := s.assureStorage(shardID, UNREADY)
	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()

	for key,value := range req.Storage {
		localStorage.storage[key] = value
	}
	localStorage.state = READY
	localStorage.cond.Broadcast()
	//s.GroupInfo.Shards = append(s.GroupInfo.Shards, shardID)
	//switch s.LocalStorages[shardID].state {
	//case WAITING :
	//	s.LocalStorages[shardID].state = NORMAL
	//	s.ShardLocks[shardID].Unlock()
	//case UNREADY : fallthrough
	//case EXPIRED :
	//	s.LocalStorages[shardID].state = PENDING
	//default:
	//	log.Fatal("not possible")
	//}
	return &Empty{},nil
}
/*
diffShards return the difference of two shards,
1: new shards added
2: old shards deleted
*/
func diffShards(old []int, new[]int) ([]int,[]int){
	added := make([]int,0)
	removed := make([]int,0)
	set := make(map[int]struct{})
	for _,shard := range old {
		set[shard] = struct{}{}
	}
	for _,shard := range new {
		if _,exist := set[shard]; exist{
			delete(set, shard)
		}else {
			added = append(added, shard)
		}
	}
	for shard,_ := range set {
		removed = append(removed, shard)
	}
	return added,removed
}
func (s* Slave)sendShard(shard int,gid int) error {

	/* get RPC client of primary node */
	primaryClient,conn, err := GetSlavePrimaryRPCClient(s.ZkClient, gid)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer conn.Close()
	/* rpc call to transfer */
	/* deep copy corresponding storage */
	localstorage := s.acquireStorage(shard)
	localstorage.lock.RLock()
	storageCopy := make(map[string]string)
	for k,v := range localstorage.storage{
		storageCopy[k] = v
	}
	localstorage.lock.RUnlock()

	if _,err := primaryClient.TransferShard(context.Background(), &ShardRequest{
		Storage:       storageCopy,
		ShardID:       int32(gid),
	});err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
/*
acquireShardLock : atomic
*/
func (s* Slave)acquireShardLock(shard int) *sync.RWMutex{
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	return s.LocalStorages[shard].lock
}
/*
动态更改configuration设计：
1. 每个server自行更新configuration
2. 定期检测configuration和本地存储的区别（排除本地有人在访问conf, conf.Lock）
	- added: 配置对应的shard
	- removed: （排除本地访问该组）设置成invalid，并RPC到远端。RPC会改state为ready
	- not changed : up-to-date = true
*/
func (s *Slave)CheckConfEveryPeriod(milliseconds time.Duration){
	/* start a separate timer, check configuration every duration
	for new shard: block all access and  waiting until shard came by RPC
	for old shard: send to target using RPC, and reject all access.
	*/

	go func() {
		fmt.Println("	check configuration every period of time")
		for range time.Tick(time.Millisecond * milliseconds){
			if s.LocalVersion == s.Conf.Version {
				continue // if up2date, not check conf
			}
			/*deep copy latest conf received*/
			s.ConfLock.RLock() /* TODO: reason about all these concurrent issues*/
			var shards []int
			if group, exist := s.Conf.Groups[s.GroupInfo.Gid]; exist {
				shards = make([]int,len(group.Shards))
				copy(shards,group.Shards)
			} else {
				// this group not belongs to confs, not copy configuration,
				shards = make([]int,0)
			}
			version := s.Conf.Version
			s.ConfLock.RUnlock()
			added, removed := diffShards(s.GroupInfo.Shards,shards)
			if len(added) == 0 && len(removed) == 0 {
				s.LocalVersion = version
				continue
			}
			/* handling added shard, and removed shard */
			/* send all shard to it's current machine,just do it concurrently */
			counter := sync.WaitGroup{}

			counter.Add(len(removed))
			for _, shard := range removed {
				go func() {
					/* invalidate local before sending shard to target */

					wg := sync.WaitGroup{}
					wg.Add(2)
					go func() {
						localStorage := s.acquireStorage(shard)
						localStorage.lock.Lock()
						defer localStorage.lock.Unlock()
						localStorage.state = EXPIRED
						wg.Done()
					}()
					go func () {
						s.ShardsLock.Lock()
						s.GroupInfo.Shards = Utils.Delete(s.GroupInfo.Shards, shard)
						s.ShardsLock.Unlock()
						wg.Done()
					}()
					wg.Wait()
					counter.Done()
					/* send shard to remote anyway*/
					s.ConfLock.RLock()
					gid := s.Conf.Assignment[shard]
					s.ConfLock.RUnlock()
					if err := s.sendShard(shard,gid ); err != nil{
						fmt.Println(err.Error())
					}
				}()
			}
			for _,shard := range added{
				s.assureStorage(shard, UNREADY)
				s.ShardsLock.Lock()
				s.GroupInfo.Shards = append(s.GroupInfo.Shards, shard)
				s.ShardsLock.Unlock()
			}
			counter.Wait() // only continue when all the removed shard are invalid
			/* TODO: remove those shards not used */
		}
	}()
}
func (s* Slave)UpdateConfEveryPeriod(milliseconds time.Duration) {

	masterClient, conn, err := master.GetMasterRPCClient(s.ZkClient)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	/* start a separate timer, conn should wait until timer end. */
	go func() {
		defer conn.Close()
		fmt.Println("	update configuration every period of time")
		for range time.Tick(time.Millisecond * milliseconds) {
			conf, err := masterClient.Query(context.Background(), &master.QueryRequest{ConfVersion: -1})
			if err != nil {
				log.Fatal(err)
			}
			if int(conf.Version) < s.Conf.Version {
				fmt.Println("Error: local conf version larger than remote version")
			} else if int(conf.Version) == s.Conf.Version {
				// dont do anything, just skip
			} else {
				// deep copy
				s.ConfLock.Lock()
				s.Conf = *master.NewConf(conf)
				s.ConfLock.Unlock()
			}
		}
		fmt.Println("should never reach here")
	}()
}


/* return RPC client of primary slave of group gid */
func GetSlavePrimaryRPCClient (sdClient *zk_client.SdClient,gid int) (KVServiceClient,*grpc.ClientConn,error){
	/* connect to master */
	if primaryNode,err := sdClient.Get1Node("slave_primary/" + strconv.Itoa(gid)); err != nil {
		return nil,nil,err
	}else {
		serverString := primaryNode.Host + ":" + strconv.Itoa(primaryNode.Port)
		fmt.Println("slave server String : " + serverString)
		conn, err := grpc.Dial(serverString, grpc.WithInsecure())
		if err != nil {
			return nil,nil,err
		}
		return NewKVServiceClient(conn),conn,nil
	}
}