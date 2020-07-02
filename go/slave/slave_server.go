package slave

import (
	"ds/go/common/zk_client"
	"ds/go/master"
	"github.com/samuel/go-zookeeper/zk"
	"log"

	"strconv"
	"sync"
	"time"
)


type ServerConf struct {
	Hostname string
	IP       string
	Port     int
	GroupID  int
}

func (conf *ServerConf) ServerString() string {
	return conf.IP+ ":" + strconv.Itoa(conf.Port)
}
func (s *Server) ServerString () string {
	return s.conf.ServerString()
}

const SyncReqChanLength = 100
const (
	PutReq = iota
	DelReq
	InvalidateReq
)
type request struct {
	Key  string
	Value string
	ShardID int
	ReqCode int // PutReq, DelReq
}

type Server struct {
	zkClient *zk_client.Client
	path string
	/* the version number storage updated to */
	localVersion int

	conf ServerConf

	primary        bool
	backupConfLock *sync.RWMutex
	backupServers []*RPCClient
	syncReqs chan request

	slave *Slave
	rpcServer *RPCServer
}

func NewServer(conf ServerConf, client *zk_client.Client) *Server {
	slave := NewSlave()
	server := Server{
		zkClient:     client,

		conf : conf,
		primary:  false,
		backupConfLock: &sync.RWMutex{},
		backupServers: make([]*RPCClient,0),
		syncReqs: nil,

		slave:        slave,
		rpcServer:    nil,
		localVersion: 0,
	}
	server.rpcServer = NewRPCServer(&server)
	return &server
}

/*
Serve : start KV service
*/
func (s *Server) Serve() {
	/* primary election, then register and work as the role*/
	s.electThenWork()

	/* start timer, slave server will retrieve configuration from master every time period*/
	confChan := s.getConfEveryPeriod(100)
	go s.processConfs(confChan)
	s.rpcServer.Serve()

}

func (s *Server) Put(key string, value string, shardID int) error {

	log.Printf("Put %s-%s in shard %d\n", key,value, shardID)
	err := s.slave.put(key,value,shardID)
	if err != nil {
		return err
	}

	if s.primary {
		if err := s.forwardRequest(request{
			Key:     key,
			Value:   value,
			ShardID: shardID,
			ReqCode: PutReq,
		}); err != nil {
			log.Printf("forwarding error\n")
			return ErrNotPrimary //TODO: this is wrong error code
		}
	}
	return nil
}

func (s *Server) Get(key string , shardID int)  (string,error) {
	log.Printf("Get %s in shard %d\n", key, shardID)
	if value, err := s.slave.get(key,shardID); err != nil {
		return "",err
	}else {
		return value, nil
	}
}


func (s *Server) Del(key string, shardID int) error {

	log.Printf("Del %s in shard %d\n", key, shardID)
	err := s.slave.del(key,shardID)
	if err != nil {
		return err
	}
	if s.primary{
		if err := s.forwardRequest(request{
			Key:     key,
			ShardID: shardID,
			ReqCode: DelReq,
		}); err != nil {
			log.Printf("forwarding error\n")
			return ErrNotPrimary //TODO: this is wrong error code
		}
	}

	return nil
	/*release lock for the key, done by defer*/
}
func (s* Server)Sync(req request) error {
	s.syncReqs <- req
	return nil
}

/*
RPC call to Transfer a Shard to here
*/
func (s *Server) TransferShard(shardID int,storage map[string]string) error {

	log.Printf("receive shard %d\n", shardID)
	if err := s.slave.addStorage(shardID,storage); err != nil {
		return err
	}

	if s.primary{
		s.backupConfLock.RLock()
		defer s.backupConfLock.RUnlock()
		wg := sync.WaitGroup{}
		wg.Add(len(s.backupServers))
		for _,backup := range s.backupServers {
			// localize backup, or the backup variable will always be the last one in the iteration
			backup := backup
			go func() {
				log.Printf("forward shard %d to %v\n",shardID,backup.hostname)
				if  err := backup.TransferShard(shardID,storage); err != nil {
					log.Printf(err.Error())
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
	return nil
}

func (s *Server)forwardRequest(req request) error {
	s.backupConfLock.RLock()
	defer s.backupConfLock.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(s.backupServers))
	for _,backup := range s.backupServers {
		backup := backup
		go func() {
			log.Printf("forward request to %v\n",backup.hostname)
			if err := backup.Sync(req); err != nil {
				log.Printf(err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}
func (s *Server)ProcessSyncRequests(){
	for req := range s.syncReqs {
		shardID := req.ShardID
		switch req.ReqCode { // todo: fault handling, backup conf may be lagged behind
		case PutReq:
			log.Printf("sync:  put %s, %s\n", req.Key, req.Value)
			if err := s.Put(req.Key,req.Value,shardID); err != nil {
				log.Printf(err.Error())
				panic(err)
			}
		case DelReq:
			log.Printf("sync: del %s\n", req.Key)
			if err :=s.Del(req.Key,shardID); err != nil {
				log.Printf(err.Error())
				panic(err)
			}
		case InvalidateReq:
			log.Printf("sync: invalidate %d\n", shardID)
			if err := s.InvalidateShard(shardID); err != nil {
				log.Printf(err.Error())
				panic(err)
			}
		}
	}
}


func (s *Server) InvalidateShard(shardID int) error {
	s.slave.deleteStorage(shardID)
	return nil
}

func (s *Server) electThenWork(){

	log.Printf("try to be the primary node %+v\n",s.primaryNode())
	if err := s.zkClient.TryPrimary(s.primaryNode()); err != nil {
		if err != zk.ErrNodeExists {
			log.Printf(err.Error())
			panic(err)
		}
		s.primary = false
		/* create sync chan before register */
		s.syncReqs = make(chan request,SyncReqChanLength)

		if path,err := s.zkClient.Register(s.backupNode()); err != nil {
			panic(err)
		}else {
			s.path = path
		}
		log.Printf("I'm one of backup nodes of group %d\n",s.conf.GroupID)
		nodeChan, _ := s.zkClient.Subscribe1Node(s.primaryPath())
		go s.watchPrimary(nodeChan)
		go s.ProcessSyncRequests()
	} else {
		/*select as primary node, just return and wait for rpc service to start*/
		s.primary = true
		log.Printf("I'm the primary node of group %d\n",s.conf.GroupID)
		/* primary should connect to back up servers */
		serverChan, _ := s.zkClient.SubscribeNodes(s.backupPath(), make(chan struct{})) // pass in a non-closing channel
		go s.processBackupConfs(serverChan)
	}
}
/*
watchPrimary : Watching the primary and ready to take over
*/
func (s *Server) watchPrimary(nodeChan chan zk_client.ServiceNode) {
	for node := range nodeChan {
		log.Printf("current primary Node of this group is %+v\n",node)
	}
	log.Printf("primary node seems to die\n")
	/* primary node seems to be offline, re-election ! */
	close(s.syncReqs)
	s.syncReqs = nil
	//s.ZkClient.DeleteNode("slave_backup/" + strconv.Itoa(s.GroupInfo.Gid), s.hostname)
	if err := s.zkClient.DeleteNode(s.path); err != nil {
		log.Println("delete node err ", err)
	}
	log.Printf("backup node deleted, path %s\n", s.path)

	s.electThenWork()
}

func (s *Server) getConfEveryPeriod(milliseconds time.Duration) chan master.Configuration {

	confChan := make(chan master.Configuration)
	/* start a separate timer, conn should wait until timer end. */
	go func() {
		/* get master information */
		masterNode,err := s.zkClient.Get1Node("master")
		if err != nil {
			log.Fatal("error: master died\n")
		}
		/* connect to master */
		masterClient, err := master.NewRPCClient(master.ServerConf{
			IP:       masterNode.IP,
			Port:     masterNode.Port,
			Hostname: masterNode.Hostname,
		})
		if err != nil {
			panic(err.Error())
		}
		defer masterClient.Close()

		/* get configuration changes and put it on confChan*/
		log.Println("	update configuration every period of time")
		currentVersion := 0
		for range time.Tick(time.Millisecond * milliseconds) {
			conf, err := masterClient.Query(-1)
			if err != nil {
				log.Fatal(err)
			}
			if conf.Version < currentVersion {
				log.Println("Error: local conf version larger than remote version")
			} else if conf.Version == currentVersion {
				// configuration of same version, just skip
			} else {
				// deep copy
				log.Printf("received new Conf %d\n",conf.Version)
				currentVersion = conf.Version
				confChan <- *conf
			}
		}
		log.Println("should never reach here")
	}()
	return confChan
}


/*
动态更改configuration设计：
1. 每个server自行更新configuration
2. 定期检测configuration和本地存储的区别（排除本地有人在访问conf, conf.Lock）
*/
func (s *Server) processConfs(confChan chan master.Configuration) {
	for conf := range confChan{
		log.Printf("processing conf Version %d\n",conf.Version)
		if s.localVersion == conf.Version {
			continue // if up2date, not check conf
		}
		log.Printf("new configuration found %+v\n", conf)


		/* server should init the shard if it is the first configuration. */
		if s.localVersion == 0 && conf.Version == 1 {
			for _, shard := range conf.Id2Groups[s.conf.GroupID].Shards {
				s.slave.shards = append(s.slave.shards, shard)
				s.slave.assureStorage(shard, READY)
			}
			s.localVersion = 1
			continue
		}

		/* server should compare the two configurations, and react to changes */

		/* deep copy last configuration */
		var shards []int
		if group, exist := conf.Id2Groups[s.conf.GroupID]; exist {
			shards = make([]int, len(group.Shards))
			copy(shards, group.Shards)
		} else {
			// this group not belongs to confs, not copy configuration,
			shards = make([]int, 0)
		}
		version := conf.Version
		log.Printf("old shards, %v, new conf %v, new shards %v\n", s.slave.shards, conf.Id2Groups[s.conf.GroupID], shards)

		added, removed := s.slave.compareShards(shards)
		if len(added) == 0 && len(removed) == 0 {
			/* no difference, already up2date*/
			s.localVersion = version
			continue
		}

		/* for shards that has been removed, send it to its new manager */
		if s.primary {
			/* send all shardID to it's current manager, do it concurrently */
			counter := sync.WaitGroup{}
			counter.Add(len(removed))
			log.Printf("about to send %v  out\n", removed)
			for _, shardID := range removed {
				shardID := shardID
				go func() {
					s.slave.deleteShard(shardID)
					counter.Done()

					gid := conf.Assignment[shardID]
					if err := s.sendShard(shardID, gid); err != nil {
						log.Println(err.Error())
					}
					s.slave.deleteStorage(shardID)
					/* inform backups that this shard has been successfully send. */
					s.backupConfLock.RLock()
					defer s.backupConfLock.RUnlock()
					for _,backup := range s.backupServers {
						backup.Sync(request{
							ShardID: shardID,
							ReqCode: InvalidateReq,
						})
					}
				}()
			}
			counter.Wait()
		}else {
			for _, shardID := range removed {
				s.slave.deleteShard(shardID)
				s.slave.expireStorage(shardID)
			}
		}
		/* for shards that's new, allocate entry if not allocated. */
		log.Printf("about to add %v \n", added)
		for _, shardID := range added {
			s.slave.assureStorage(shardID, UNREADY)
			s.slave.addShard(shardID)
		}
		log.Printf("shard after send and added:  %+v\n", s.slave.shards)
		/* TODO: remove those shards not used */
	}
}

func (s *Server) sendShard(shard int, gid int) error {

	log.Printf("send shard %d to gid %d\n", shard, gid)

	/* Get RPC client of primary node */
	primaryClient, err := s.getSlavePrimaryRPCClient(gid)
	if err != nil {
		log.Println(err)
		return err
	}
	defer primaryClient.Close()
	/* rpc call to transfer */
	/* deep copy corresponding storage */
	localstorage := s.slave.assureStorage(shard,READY)
	storageCopy := localstorage.copyStorageAtomic()
	/* backup only start receiving sync request on this shard, after primary start*/
	if err := primaryClient.TransferShard(shard, storageCopy); err != nil {
		log.Println(err)
		return err
	}
	return nil
}


/* return RPC client of primary slave of group gid */
func (s *Server)getSlavePrimaryRPCClient(gid int) (*RPCClient, error) {
	/* connect to master */
	if primaryNode, err := s.zkClient.Get1Node("slave_primary/" + strconv.Itoa(gid)); err != nil {
		return nil,err
	} else {
		return NewRPCClient(ServerConf{
			Hostname: primaryNode.Hostname,
			IP:       primaryNode.IP,
			Port:     primaryNode.Port,
		})
	}
}
func (s *Server) transferAllShardsTo(backupClient *RPCClient) error{

	s.slave.storageLock.RLock()
	defer s.slave.storageLock.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(s.slave.localStorages))
	for shardID,storage := range s.slave.localStorages{
		shardID := shardID
		storage := storage
		go func() {
			storage.lock.RLock()
			defer storage.lock.RUnlock()
			if err := backupClient.TransferShard(shardID,storage.storage); err != nil {
				log.Println(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func (s* Server) processBackupConfs(serverChan chan []*zk_client.ServiceNode){
	/* iterate until channel closed */
	for backups := range serverChan {
		backupServersNew := make([]*RPCClient,0)
		/* check for new servers and start connection */
		findServer := func (s []*RPCClient, e string) int {
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
				if server, err := NewRPCClient(ServerConf{
					Hostname: backup.Hostname,
					IP:       backup.IP,
					Port:     backup.Port,
				}); err != nil {
					log.Printf("create rpc client to %s,  error:" + err.Error(), backup.ServerString())
				} else {

					if err := s.transferAllShardsTo(server); err != nil {
						log.Println(err)
					}

					backupServersNew = append(backupServersNew,server)
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

var (
	PrimaryPrefix = "slave_primary"
	BackupPrefix = "slave_backup"
)
func (s *Server)primaryPath() string {
	return PrimaryPrefix + "/" + strconv.Itoa(s.conf.GroupID)
}
func (s *Server)backupPath() string {
	return BackupPrefix + "/" + strconv.Itoa(s.conf.GroupID)
}
func (s *Server)primaryNode() zk_client.ServiceNode {
	return	zk_client.ServiceNode{
		Name:     s.primaryPath(),
		IP:       s.conf.IP,
		Port:     s.conf.Port,
		Hostname: s.conf.Hostname,
	}
}
func (s *Server)backupNode() zk_client.ServiceNode {
	return	zk_client.ServiceNode{
		Name:     s.backupPath(),
		IP:       s.conf.IP,
		Port:     s.conf.Port,
		Hostname: s.conf.Hostname,
	}
}
