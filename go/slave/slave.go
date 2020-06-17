package slave

//noinspection ALL
import (
	"context"
	"ds/go/common/Utils"
	"ds/go/common/zk_client"
	"ds/go/master"
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	//"fmt"
	"net"
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
	cond    *sync.Cond
	lock    *sync.RWMutex
	storage map[string]string
	state   StorageState
}

type KVClient struct {
	serviceClient KVServiceClient
	hostname string
	conn *grpc.ClientConn
}
/*
Slave : slave structure implement slave RPC semantics
*/
/* maybe convert to sync.Map */
type Slave struct {
	/* connection to zookeeper */
	ZkClient *zk_client.SdClient

	/* Am I primary node ?*/
	Primary bool
	/*hostname,ip,port of current slave */
	ip string
	port int
	hostname string
	path string
	backupConfLock *sync.RWMutex
	/* link to all back up servers of this cluster */
	backupServers []KVClient
	/* the version number storage updated to */
	LocalVersion int

	/*
	ShardsLock : lock for groupInfo.shards(what shards current group serves),
	acquire when modifying or reading*/
	ShardsLock *sync.RWMutex
	GroupInfo  master.Group
	// Gid 	  : the group id this slave belongs to, used to check configuration
	// Shards : list of shards this slave will manage ,get from master every 100 milliseconds

	/*
	StorageLock: lock for shardID -> *localstorage map
	lock for individual Local Shard Storage is in LocalStorage Structure.
	*/
	StorageLock   *sync.RWMutex
	LocalStorages map[int]*LocalStorage // map from ShardID to LocalStorages

	/*
	SyncReqs: channel for requests from primary for processing
	*/
	SyncReqs chan SyncRequest
}

const SyncReqChanLength = 100
func CreateSlave(client *zk_client.SdClient, gid int,hostname string,ip string, port int) *Slave {
	return &Slave{
		ZkClient:     client,
		Primary:      false,
		ip: ip,
		port: port,
		hostname: hostname,
		path : "",
		backupConfLock: new(sync.RWMutex),
		backupServers: make([]KVClient,0),
		LocalVersion: 0,
		ShardsLock:   new(sync.RWMutex),
		GroupInfo: master.Group{
			Gid:     gid,
			Servers: make([]string, 0),
			Shards:  make([]int, 0),
		},
		//ConfLock: new(sync.RWMutex),
		//Conf: master.Configuration{
		//	Version: 0,
		//},
		StorageLock:   new(sync.RWMutex),
		LocalStorages: make(map[int]*LocalStorage),
		//SyncReqs: make(chan SyncRequest, SyncReqChanLength),
		SyncReqs: nil, // will only be init before registered as a backup,  if the node fail to be the primary
	}
}
/*
Put : RPC call to put a key value
*/
func (s *Slave) Put(ctx context.Context, args *Request) (*Empty, error) {
	shardID := int(args.ShardID)
	return s.put(args.Key,args.Value,shardID)
}
func (s *Slave) put(key string , value string , shardID int ) (*Empty,error){
	log.Printf("put %s-%s in shard %d\n", key, value, shardID)
	s.ShardsLock.RLock()
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		s.ShardsLock.RUnlock()
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	s.ShardsLock.RUnlock()

	localStorage := s.assureStorage(shardID, READY)

	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()
	switch localStorage.state {
	case EXPIRED:
		log.Fatal("impossible to be expired and reach here")
		return nil, status.Errorf(codes.Unknown, "impossible to be expired and reach here")
	case UNREADY:
		for localStorage.state != READY {
			localStorage.cond.Wait()
		}
	default:
	}
	localStorage.storage[key] = value

	if s.Primary {
		if err := s.forwardRequest(&SyncRequest{
			Key:     key,
			Value:   value,
			ShardID: int32(shardID),
			ReqCode: PutReq,
		}); err != nil {
			log.Printf("forwarding error\n")
			return nil, ErrNotPrimary //TODO: this is wrong error code
		}
	}
	/* release lock for the key, done by defer*/
	return &Empty{}, nil
}
/*
Get : RPC call to get a key */
func (s *Slave) Get(ctx context.Context, args *Request) (*Response, error) {
	shardID := int(args.ShardID)

	return s.get(args.Key,shardID)
}
func (s *Slave)get(key string , shardID int)  (*Response,error){
	log.Printf("get %s in shard %d\n", key, shardID)
	s.ShardsLock.RLock()
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		s.ShardsLock.RUnlock()
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	s.ShardsLock.RUnlock()

	/* acquire r lock when read storage map*/
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	if localStorage, ok := s.LocalStorages[shardID]; !ok {
		return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
	} else {
		/* acquire r lock when read storage */
		localStorage.lock.RLock()
		defer localStorage.lock.RUnlock()
		if res, ok := localStorage.storage[key]; !ok {
			return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
		} else {
			return &Response{
				Value: res,
			}, nil
		}
	}
}
/*
Del: RPC call to delete a key
*/
func (s *Slave) Del(ctx context.Context, args *Request) (*Empty, error) {
	shardID := int(args.ShardID)

	return s.del(args.Key, shardID)
}

func (s *Slave) del(key string, shardID int) (*Empty, error) {

	log.Printf("del %s in shard %d\n", key, shardID)
	s.ShardsLock.RLock()
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		s.ShardsLock.RUnlock()
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	s.ShardsLock.RUnlock()

	/* acquire r lock when read storage map*/
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	if localStorage, ok := s.LocalStorages[shardID]; !ok {
		/* localStorage not exist */
		return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
	} else {
		localStorage.lock.Lock()
		defer localStorage.lock.Unlock()
		if _, ok = localStorage.storage[key]; !ok {
			/* storage entry not exist*/
			return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
		} else {
			delete(localStorage.storage, key)
		}
	}
	if s.Primary{
		if err := s.forwardRequest(&SyncRequest{
			Key:           key,
			ShardID:       int32(shardID),
			ReqCode:       DelReq,
		}); err != nil {
			log.Printf("forwarding error\n")
			return nil,ErrNotPrimary //TODO: this is wrong error code
		}
	}

	return &Empty{}, nil
	/*release lock for the key, done by defer*/
}
const (
	PutReq = iota
	DelReq
)
/*
SyncRequest
*/
func (s* Slave)Sync(ctx context.Context, req *SyncRequest)(*Empty, error) {
	/* no need to acquire lock when sync because primary already has lock */
	s.SyncReqs <- *req
	return &Empty{},nil
}
func (s *Slave)forwardRequest(req *SyncRequest) error {
	s.backupConfLock.RLock()
	defer s.backupConfLock.RUnlock()
	wg := sync.WaitGroup{}
	wg.Add(len(s.backupServers))
	for _,backup := range s.backupServers {
		go func(backup KVClient) {
			log.Printf("forward request to %v\n",backup.hostname)
			if _, err := backup.serviceClient.Sync(context.Background(), req); err != nil {
				log.Printf(err.Error())
			}
			wg.Done()
		}(backup)
	}
	wg.Wait()
	return nil
}
/**/
func (s *Slave)ProcessSyncRequests(){
	for req := range s.SyncReqs {
		shardID := int(req.ShardID)
		log.Printf("receive request:  %+v\n",req)
		switch req.ReqCode { // todo: fault handling, backup conf may be lagged behind
		case PutReq:
			if _,err := s.put(req.Key,req.Value,shardID); err != nil {
				log.Printf(err.Error())
			}
		case DelReq:
			if _,err :=s.del(req.Key,shardID); err != nil {
				log.Printf(err.Error())
			}
		}
	}
}
/*
assureStorage: atomic
*/
func (s *Slave) assureStorage(shard int, state StorageState) *LocalStorage {
	s.StorageLock.Lock()
	defer s.StorageLock.Unlock()
	if _, exist := s.LocalStorages[shard]; !exist {
		rwLock := new(sync.RWMutex)
		s.LocalStorages[shard] = &LocalStorage{
			storage: make(map[string]string),
			state:   state,
			lock:    rwLock,
			cond:    sync.NewCond(rwLock),
		}
	}
	return s.LocalStorages[shard]
}

/*
acquireStorage: atomic
*/
func (s *Slave) acquireStorage(shard int) *LocalStorage {
	s.StorageLock.RLock()
	defer s.StorageLock.RUnlock()
	return s.LocalStorages[shard]
}
/*
RPC call to Transfer a Shard to here
*/
func (s *Slave) TransferShard(ctx context.Context, req *ShardRequest) (*Empty, error) {

	shardID := int(req.ShardID)

	log.Printf("receive shard %d\n", shardID)
	localStorage := s.assureStorage(shardID, UNREADY)
	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()

	for key, value := range req.Storage {
		localStorage.storage[key] = value
	}
	localStorage.state = READY
	localStorage.cond.Broadcast()

	if s.Primary{
		s.backupConfLock.RLock()
		defer s.backupConfLock.RUnlock()
		wg := sync.WaitGroup{}
		wg.Add(len(s.backupServers))
		for _,backup := range s.backupServers {
			go func(backup KVClient) {
				log.Printf("forward transfer shard to %v\n",backup.hostname)
				if _, err := backup.serviceClient.TransferShard(context.Background(), req); err != nil {
					log.Printf(err.Error())
				}
				wg.Done()
			}(backup)
		}
		wg.Wait()
	}

	return &Empty{}, nil
}

/*
diffShards return the difference of two shards,
1: new shards added
2: old shards deleted
*/
func diffShards(old []int, new []int) ([]int, []int) {
	added := make([]int, 0)
	removed := make([]int, 0)
	set := make(map[int]struct{})
	for _, shard := range old {
		set[shard] = struct{}{}
	}
	for _, shard := range new {
		if _, exist := set[shard]; exist {
			delete(set, shard)
		} else {
			added = append(added, shard)
		}
	}
	for shard := range set {
		removed = append(removed, shard)
	}
	return added, removed
}
func (s *Slave) sendShard(shard int, gid int) error {

	log.Printf("send shard %d to gid %d\n", shard, gid)
	/* get RPC client of primary node */
	primaryClient, conn, err := s.getSlavePrimaryRPCClient(gid)
	if err != nil {
		log.Println(err)
		return err
	}
	defer conn.Close()
	/* rpc call to transfer */
	/* deep copy corresponding storage */
	localstorage := s.acquireStorage(shard)
	localstorage.lock.RLock()
	storageCopy := make(map[string]string)
	for k, v := range localstorage.storage {
		storageCopy[k] = v
	}
	localstorage.lock.RUnlock()
	/* backup only start receiving sync request on this shard, after primary start*/
	if _, err := primaryClient.TransferShard(context.Background(), &ShardRequest{
		Storage: storageCopy,
		ShardID: int32(shard),
	}); err != nil {
		log.Println(err)
		return err
	}
	return nil
}


/*
动态更改configuration设计：
1. 每个server自行更新configuration
2. 定期检测configuration和本地存储的区别（排除本地有人在访问conf, conf.Lock）
	- added: 配置对应的shard
	- removed: （排除本地访问该组）设置成invalid，并RPC到远端。RPC会改state为ready
	- not changed : up-to-date = true
*/
func (s *Slave) ProcessNewConf(confChan chan master.Configuration) {
	for conf := range confChan{
		log.Printf("processing conf Version %d\n",conf.Version)
		if s.LocalVersion == conf.Version {
			continue // if up2date, not check conf
		}
		/* server should init the shard if it is first to be assigned
		When issuing first join, many concurrency shouldn't be allowed
		*/
		log.Printf("new configuration found %+v\n", conf)
		if s.LocalVersion == 0 && conf.Version == 1 {
			for _, shard := range conf.Groups[s.GroupInfo.Gid].Shards {
				s.GroupInfo.Shards = append(s.GroupInfo.Shards, shard)
				s.assureStorage(shard, READY)
			}
			s.LocalVersion = 1
			continue
		}
		/*deep copy latest conf received*/
		var shards []int
		if group, exist := conf.Groups[s.GroupInfo.Gid]; exist {
			shards = make([]int, len(group.Shards))
			copy(shards, group.Shards)
		} else {
			// this group not belongs to confs, not copy configuration,
			shards = make([]int, 0)
		}
		version := conf.Version
		log.Printf("old shards, %v, new conf %v, new shards %v\n", s.GroupInfo.Shards, conf.Groups[s.GroupInfo.Gid], shards)
		added, removed := diffShards(s.GroupInfo.Shards, shards)
		if len(added) == 0 && len(removed) == 0 {
			s.LocalVersion = version
			continue
		}
		/* handling added shard, and removed shard */
		if s.Primary {
			/* send all shard to it's current machine,just do it concurrently */
			counter := sync.WaitGroup{}
			counter.Add(len(removed))
			log.Printf("about to send %v  out\n", removed)
			for _, shard := range removed {
				go func(shardID int) {
					/* invalidate local before sending shard to target */
					wg := sync.WaitGroup{}
					wg.Add(2)
					go func() {
						localStorage := s.acquireStorage(shardID)
						localStorage.lock.Lock()
						defer localStorage.lock.Unlock()
						localStorage.state = EXPIRED
						wg.Done()
					}()
					go func() {
						s.ShardsLock.Lock()
						s.GroupInfo.Shards = Utils.Delete(s.GroupInfo.Shards, shardID)
						s.ShardsLock.Unlock()
						wg.Done()
					}()
					wg.Wait()
					counter.Done()
					/* send shard to remote anyway*/
					gid := conf.Assignment[shardID]
					if err := s.sendShard(shardID, gid); err != nil {
						log.Println(err.Error())
					}
				}(shard)
			}
			counter.Wait() // only continue when all the removed shard are invalid
		}
		log.Printf("about to add %v \n", added)
		for _, shard := range added {
			s.assureStorage(shard, UNREADY)
			s.ShardsLock.Lock()
			s.GroupInfo.Shards = append(s.GroupInfo.Shards, shard)
			s.ShardsLock.Unlock()
		}
		log.Printf("shard after send and added:  %+v\n", s.GroupInfo.Shards)

		/* TODO: remove those shards not used */
	}
}
func (s *Slave) UpdateConfEveryPeriod(milliseconds time.Duration) {

	masterClient, conn, err := master.GetMasterRPCClient(s.ZkClient)
	if err != nil {
		log.Println(err.Error())
		return
	}
	confChan := make(chan master.Configuration)
	/* start a separate timer, conn should wait until timer end. */
	go func() {
		defer conn.Close()
		log.Println("	update configuration every period of time")
		currentVersion := 0
		for range time.Tick(time.Millisecond * milliseconds) {
			conf, err := masterClient.Query(context.Background(), &master.QueryRequest{ConfVersion: -1})
			if err != nil {
				log.Fatal(err)
			}
			if int(conf.Version) < currentVersion {
				log.Println("Error: local conf version larger than remote version")
			} else if int(conf.Version) == currentVersion {
				// dont do anything, just skip
			} else {
				// deep copy
				log.Printf("received new Conf %d\n",conf.Version)
				currentVersion = int(conf.Version)
				confChan <- *master.NewConf(conf)
			}
		}
		log.Println("should never reach here")
	}()
	go s.ProcessNewConf(confChan)
}

/* return RPC client of primary slave of group gid */
func (s *Slave)getSlavePrimaryRPCClient(gid int) (KVServiceClient, *grpc.ClientConn, error) {
	/* connect to master */
	if primaryNode, err := s.ZkClient.Get1Node("slave_primary/" + strconv.Itoa(gid)); err != nil {
		return nil, nil, err
	} else {
		serverString := primaryNode.Host + ":" + strconv.Itoa(primaryNode.Port)
		conn, err := grpc.Dial(serverString, grpc.WithInsecure())
		if err != nil {
			return nil, nil, err
		}
		return NewKVServiceClient(conn), conn, nil
	}
}
/*
*/
func (s* Slave) processBackupConfs(serverChan chan []*zk_client.ServiceNode){
	/* iterate until channel closed */
	for backups := range serverChan {
		backupServersNew := make([]KVClient,0)
		/* check for new servers and start connection */
		findServer := func (s []KVClient, e string) int {
			for index, a := range s {
				if a.hostname == e {
					return index
				}
			}
			return -1
		}
		for _,backup := range backups {
			index := findServer(s.backupServers, backup.Hostname)
			if index == -1 {
				serverString := backup.ServerString()
				conn, err := grpc.Dial(serverString, grpc.WithInsecure())
				if err != nil {
					log.Printf("grpc dial error:" + err.Error())
				} else {
					backupServersNew = append(backupServersNew, KVClient{
						serviceClient: NewKVServiceClient(conn),
						hostname:      backup.Hostname,
						conn:          conn,
					})
				}
			}else {
				backupServersNew = append(backupServersNew, s.backupServers[index])
			}
		}
		s.backupConfLock.Lock()
		s.backupServers = backupServersNew
		s.backupConfLock.Unlock()
		log.Printf("updated backup server %v\n",backupServersNew)
	}
}
func (s *Slave) ElectThenWork(){

	primaryPath := "slave_primary/" + strconv.Itoa(s.GroupInfo.Gid)
	backupPath := "slave_backup/" + strconv.Itoa(s.GroupInfo.Gid)
	slaveNode := zk_client.ServiceNode{Name: primaryPath, Host: s.ip, Port: s.port, Hostname: s.hostname}
	if err := s.ZkClient.TryPrimary(&slaveNode); err != nil {
		if err != zk.ErrNodeExists {
			log.Printf(err.Error())
			panic(err)
		}
		s.Primary = false
		/* create sync chan before register */
		s.SyncReqs = make(chan SyncRequest,SyncReqChanLength)

		/* already exist a primary*/
		slaveNode = zk_client.ServiceNode{Name: backupPath, Host: s.ip, Port: s.port, Hostname: s.hostname}
		if path,err := s.ZkClient.Register(&slaveNode); err != nil {
			panic(err)
		}else {
			s.path = path
		}
		log.Printf("I'm one of backup nodes of group %d\n",s.GroupInfo.Gid)
		nodeChan, _ := s.ZkClient.Subscribe1Node(primaryPath)
		go s.WatchPrimary(nodeChan)
		go s.ProcessSyncRequests()
	} else {
		/*select as primary node, just return and wait for rpc service to start*/
		s.Primary = true
		log.Printf("I'm the primary node of group %d\n",s.GroupInfo.Gid)
		/* primary should connect to back up servers */
		serverChan, _ := s.ZkClient.SubscribeNodes(backupPath, make(chan struct{})) // pass in a non-closing channel
		go s.processBackupConfs(serverChan)
	}
}
/*
WatchPrimary : Watching the primary and ready to take over
*/
func (s *Slave) WatchPrimary(nodeChan chan zk_client.ServiceNode) {
	for node := range nodeChan {
		log.Printf("current primary Node of this group is %+v\n",node)
	}
	log.Printf("primary node seems to die\n")
	/* primary node seems to be offline, re-election ! */
	close(s.SyncReqs)
	s.SyncReqs = nil
	//s.ZkClient.DeleteNode("slave_backup/" + strconv.Itoa(s.GroupInfo.Gid), s.hostname)
	s.ZkClient.DeleteNode(s.path)
	log.Printf("backup node deleted, path %s\n", s.path)

	s.ElectThenWork()
}
/*
StartService : start KV service at ip:port as hostname
wrapper function for slave server to run*/
func (s *Slave) StartService(ip string, port int, hostname string) {

	/* start RPC Service on ip:port */

	grpcServer := grpc.NewServer()
	RegisterKVServiceServer(grpcServer, s)
	listen, err := net.Listen("tcp", ip+":"+strconv.Itoa(port)) // hard configure TCP
	if err != nil {
		log.Fatal(err)
	}
	/* defer execute in LIFO, close connection at last*/
	defer func() {
		log.Println("slave server start serving requests at " + ip + ":" + strconv.Itoa(port))
		if err := grpcServer.Serve(listen); err != nil {
			log.Fatal(err)
		}
	}()

	/* start timer, slave server will retrieve configuration from master every time period*/
	defer s.UpdateConfEveryPeriod(100)
	/* primary election, and work as the role*/
	s.ElectThenWork()
}

