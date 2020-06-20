package slave

//noinspection ALL
import (
	"ds/go/common/Utils"
	"ds/go/common/zk_client"
	"ds/go/master"
	"github.com/samuel/go-zookeeper/zk"
	"log"

	"strconv"
	"sync"
	"time"
)



type Server struct {
	zkClient *zk_client.Client
	path string
	/* the version number storage updated to */
	localVersion int

	slave *Slave
	rpcServer *RPCServer
}

func NewServer(conf ServerConf, client *zk_client.Client) *Server {
	slave := NewSlave(conf)
	return &Server{
		zkClient:     client,
		slave:        slave,
		rpcServer:    NewRPCServer(slave),
		localVersion: 0,
	}
}

/*
Serve : start KV service
*/
func (s *Server) Serve() {
	/* primary election, then register and work as the role*/
	s.ElectThenWork()

	/* start timer, slave server will retrieve configuration from master every time period*/
	confChan := s.getConfEveryPeriod(100)
	go s.ProcessConfs(confChan)
	s.rpcServer.Serve()

}

func (s *Server) ElectThenWork(){

	if err := s.zkClient.TryPrimary(s.primaryNode()); err != nil {
		if err != zk.ErrNodeExists {
			log.Printf(err.Error())
			panic(err)
		}
		s.slave.Primary = false
		/* create sync chan before register */
		s.slave.syncReqs = make(chan request,SyncReqChanLength)

		if path,err := s.zkClient.Register(s.backupNode()); err != nil {
			panic(err)
		}else {
			s.path = path
		}
		log.Printf("I'm one of backup nodes of group %d\n",s.slave.conf.GroupID)
		nodeChan, _ := s.zkClient.Subscribe1Node(s.primaryPath())
		go s.WatchPrimary(nodeChan)
		go s.slave.ProcessSyncRequests()
	} else {
		/*select as primary node, just return and wait for rpc service to start*/
		s.slave.Primary = true
		log.Printf("I'm the primary node of group %d\n",s.slave.conf.GroupID)
		/* primary should connect to back up servers */
		serverChan, _ := s.zkClient.SubscribeNodes(s.backupPath(), make(chan struct{})) // pass in a non-closing channel
		go s.processBackupConfs(serverChan)
	}
}
/*
WatchPrimary : Watching the primary and ready to take over
*/
func (s *Server) WatchPrimary(nodeChan chan zk_client.ServiceNode) {
	for node := range nodeChan {
		log.Printf("current primary Node of this group is %+v\n",node)
	}
	log.Printf("primary node seems to die\n")
	/* primary node seems to be offline, re-election ! */
	close(s.slave.syncReqs)
	s.slave.syncReqs = nil
	//s.ZkClient.DeleteNode("slave_backup/" + strconv.Itoa(s.GroupInfo.Gid), s.hostname)
	s.zkClient.DeleteNode(s.path)
	log.Printf("backup node deleted, path %s\n", s.path)

	s.ElectThenWork()
}

func (s *Server) getConfEveryPeriod(milliseconds time.Duration) chan master.Configuration {

	confChan := make(chan master.Configuration)
	/* start a separate timer, conn should wait until timer end. */
	go func() {
		masterNode,err := s.zkClient.Get1Node("master")
		if err != nil {
			log.Fatal("error: master died\n")
		}

		masterClient, err := master.NewRPCClient(master.ServerConf{
			IP:       masterNode.IP,
			Port:     masterNode.Port,
			Hostname: masterNode.Hostname,
		})
		if err != nil {
			log.Println(err.Error())
		}

		defer masterClient.Close()
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
	- added: 配置对应的shard
	- removed: （排除本地访问该组）设置成invalid，并RPC到远端。RPC会改state为ready
	- not changed : up-to-date = true
*/
func (s *Server) ProcessConfs(confChan chan master.Configuration) {
	for conf := range confChan{
		log.Printf("processing conf Version %d\n",conf.Version)
		if s.localVersion == conf.Version {
			continue // if up2date, not check conf
		}
		/* server should init the shard if it is first to be assigned
		When issuing first join, many concurrency shouldn't be allowed
		*/
		log.Printf("new configuration found %+v\n", conf)
		if s.localVersion == 0 && conf.Version == 1 {
			for _, shard := range conf.Id2Groups[s.slave.conf.GroupID].Shards {
				s.slave.shards = append(s.slave.shards, shard)
				s.slave.assureStorage(shard, READY)
			}
			s.localVersion = 1
			continue
		}
		/*deep copy latest conf received*/
		var shards []int
		if group, exist := conf.Id2Groups[s.slave.conf.GroupID]; exist {
			shards = make([]int, len(group.Shards))
			copy(shards, group.Shards)
		} else {
			// this group not belongs to confs, not copy configuration,
			shards = make([]int, 0)
		}
		version := conf.Version
		log.Printf("old shards, %v, new conf %v, new shards %v\n", s.slave.shards, conf.Id2Groups[s.slave.conf.GroupID], shards)
		added, removed := diffShards(s.slave.shards, shards)
		if len(added) == 0 && len(removed) == 0 {
			s.localVersion = version
			continue
		}
		/* handling added shard, and removed shard */
		if s.slave.Primary {
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
						localStorage := s.slave.acquireStorage(shardID)
						localStorage.lock.Lock()
						defer localStorage.lock.Unlock()
						localStorage.state = EXPIRED
						wg.Done()
					}()
					go func() {
						s.slave.ShardsLock.Lock()
						s.slave.shards = Utils.Delete(s.slave.shards, shardID)
						s.slave.ShardsLock.Unlock()
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
			s.slave.assureStorage(shard, UNREADY)
			s.slave.ShardsLock.Lock()
			s.slave.shards = append(s.slave.shards, shard)
			s.slave.ShardsLock.Unlock()
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
	localstorage := s.slave.acquireStorage(shard)
	storageCopy := localstorage.copyStorageAtomic()
	/* backup only start receiving sync request on this shard, after primary start*/
	if _, err := primaryClient.TransferShard(shard, storageCopy); err != nil {
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
/*
*/
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
			index := findServer(s.slave.backupServers, backup.Hostname)
			if index == -1 {
				if server, err := NewRPCClient(ServerConf{
					Hostname: backup.Hostname,
					IP:       backup.IP,
					Port:     backup.Port,
				}); err != nil {
					log.Printf("create rpc client to %s,  error:" + err.Error(), backup.ServerString())
				} else {
					backupServersNew = append(backupServersNew,server)
				}
			}else {
				backupServersNew = append(backupServersNew, s.slave.backupServers[index])
			}
		}
		s.slave.backupConfLock.Lock()
		s.slave.backupServers = backupServersNew
		s.slave.backupConfLock.Unlock()
		log.Printf("updated backup server %v\n",backupServersNew)
	}
}

var (
	PrimaryPrefix = "slave_primary"
	BackupPrefix = "slave_backup"
)
func (s *Server)primaryPath() string {
	return PrimaryPrefix + "/" + strconv.Itoa(s.slave.conf.GroupID)
}
func (s *Server)backupPath() string {
	return BackupPrefix + "/" + strconv.Itoa(s.slave.conf.GroupID)
}
func (s *Server)primaryNode() zk_client.ServiceNode {
	return	zk_client.ServiceNode{
		Name:     s.primaryPath(),
		IP:       s.slave.conf.IP,
		Port:     s.slave.conf.Port,
		Hostname: s.slave.conf.Hostname,
	}
}
func (s *Server)backupNode() zk_client.ServiceNode {
	return	zk_client.ServiceNode{
		Name:     s.backupPath(),
		IP:       s.slave.conf.IP,
		Port:     s.slave.conf.Port,
		Hostname: s.slave.conf.Hostname,
	}
}
