package slave

//noinspection ALL
import (
	"ds/go/common/Utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strconv"
	"sync"
)

var (
	/* when request is for gid not belong to this group*/
	ErrWrongGroup = status.Errorf(codes.Unavailable, "key not managed by this group")
	/* when slave is backup and request is not from primary*/
	ErrNotPrimary = status.Errorf(codes.PermissionDenied,"don't send request to a back up server")
	/* for Del and Get operation, key is managed by this group, but not in the storage */
	ErrNotFound = status.Errorf(codes.NotFound, "key not exist on this machine's storage")
)

type StorageState int

const (
	UNREADY StorageState = iota
	READY
	EXPIRED
)

/*
localStorages : in-memory kv store using map
*/
type LocalStorage struct {
	cond    *sync.Cond
	lock    *sync.RWMutex
	storage map[string]string
	state   StorageState
}
func (s *LocalStorage)copyStorage() map[string]string {
	storageCopy := make(map[string]string)
	for k, v := range s.storage {
		storageCopy[k] = v
	}
	return storageCopy
}
/* copy storage with read lock*/
func (s *LocalStorage)copyStorageAtomic() map[string]string{
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.copyStorage()
}

type ServerConf struct {
	Hostname string
	IP       string
	Port     int
	GroupID  int
}

func (conf *ServerConf) ServerString() string {
	return conf.IP+ ":" + strconv.Itoa(conf.Port)
}

/*
Slave : slave structure implement slave RPC semantics
*/
/* maybe convert to sync.Map */
type Slave struct {
	conf ServerConf
	/* Am I primary node ?*/
	Primary        bool
	backupConfLock *sync.RWMutex
	/* link to all back up servers of this cluster */
	backupServers []*RPCClient

	/*
		ShardsLock : lock for groupInfo.shards(what shards current group serves),
		acquire when modifying or reading*/
	ShardsLock *sync.RWMutex
	shards 		[]int
	/*
		storageLock: lock for shardID -> *localstorage map
		lock for individual Local Shard Storage is in LocalStorage Structure.
	*/
	storageLock   *sync.RWMutex
	localStorages map[int]*LocalStorage // map from ShardID to localStorages
	/*
		syncReqs: channel for requests from primary for processing
	*/
	syncReqs chan request
}

const SyncReqChanLength = 100
func NewSlave(conf ServerConf) *Slave {
	return &Slave{
		Primary:        false,
		conf :          conf,
		backupConfLock: new(sync.RWMutex),
		backupServers:  make([]*RPCClient,0),
		ShardsLock:     new(sync.RWMutex),
		shards:         make([]int,0),
		storageLock:    new(sync.RWMutex),
		localStorages:  make(map[int]*LocalStorage),
	}
}
func (s *Slave)ServerString() string {
	return s.conf.ServerString()
}
func (s *Slave) Put(key string , value string , shardID int ) error {
	log.Printf("Put %s-%s in shard %d\n", key, value, shardID)
	s.ShardsLock.RLock()
	if Utils.ContainsInt(s.shards, shardID) == false {
		s.ShardsLock.RUnlock()
		return ErrWrongGroup
	}
	s.ShardsLock.RUnlock()

	localStorage := s.assureStorage(shardID, READY)

	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()
	switch localStorage.state {
	case EXPIRED:
		log.Fatal("impossible to be expired and reach here")
	case UNREADY:
		for localStorage.state != READY {
			localStorage.cond.Wait()
		}
	default:
	}
	localStorage.storage[key] = value

	if s.Primary {
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

func (s *Slave) Get(key string , shardID int)  (string,error) {
	log.Printf("Get %s in shard %d\n", key, shardID)
	s.ShardsLock.RLock()
	if Utils.ContainsInt(s.shards, shardID) == false {
		s.ShardsLock.RUnlock()
		return "",ErrWrongGroup
	}
	s.ShardsLock.RUnlock()

	/* acquire r lock when read storage map*/
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()
	if localStorage, ok := s.localStorages[shardID]; !ok {
		return "",ErrNotFound
	} else {
		/* acquire r lock when read storage */
		localStorage.lock.RLock()
		defer localStorage.lock.RUnlock()
		if res, ok := localStorage.storage[key]; !ok {
			return "",ErrNotFound
		} else {
			return res,nil
		}
	}
}

func (s *Slave) Del(key string, shardID int) error {

	log.Printf("Del %s in shard %d\n", key, shardID)
	s.ShardsLock.RLock()
	if Utils.ContainsInt(s.shards, shardID) == false {
		s.ShardsLock.RUnlock()
		return ErrWrongGroup
	}
	s.ShardsLock.RUnlock()

	/* acquire r lock when read storage map*/
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()
	if localStorage, ok := s.localStorages[shardID]; !ok {
		/* localStorage not exist */
		return ErrNotFound
	} else {
		localStorage.lock.Lock()
		defer localStorage.lock.Unlock()
		if _, ok = localStorage.storage[key]; !ok {
			/* storage entry not exist*/
			return ErrNotFound
		} else {
			delete(localStorage.storage, key)
		}
	}
	if s.Primary{
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
const (
	PutReq = iota
	DelReq
)

type request struct {
	Key  string
	Value string
	ShardID int
	ReqCode int
}
/*
SyncRequest
*/
func (s* Slave)Sync(req request) error {
	/* no need to acquire lock when sync because primary already has lock */
	s.syncReqs <- req
	return nil
}
func (s *Slave)forwardRequest(req request) error {
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
/**/
func (s *Slave)ProcessSyncRequests(){
	for req := range s.syncReqs {
		shardID := req.ShardID
		log.Printf("receive request:  %+v\n",req)
		switch req.ReqCode { // todo: fault handling, backup conf may be lagged behind
		case PutReq:
			if err := s.Put(req.Key,req.Value,shardID); err != nil {
				log.Printf(err.Error())
				panic(err)
			}
		case DelReq:
			if err :=s.Del(req.Key,shardID); err != nil {
				log.Printf(err.Error())
				panic(err)
			}
		}
	}
}
/*
assureStorage: atomic
*/
func (s *Slave) assureStorage(shard int, state StorageState) *LocalStorage {
	s.storageLock.Lock()
	defer s.storageLock.Unlock()
	if _, exist := s.localStorages[shard]; !exist {
		rwLock := new(sync.RWMutex)
		s.localStorages[shard] = &LocalStorage{
			storage: make(map[string]string),
			state:   state,
			lock:    rwLock,
			cond:    sync.NewCond(rwLock),
		}
	}
	return s.localStorages[shard]
}

/*
acquireStorage: atomic
*/
func (s *Slave) acquireStorage(shard int) *LocalStorage {
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()
	return s.localStorages[shard]
}

/*
RPC call to Transfer a Shard to here
*/
func (s *Slave) TransferShard(shardID int,storage map[string]string) error {

	log.Printf("receive shard %d\n", shardID)
	localStorage := s.assureStorage(shardID, UNREADY)
	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()

	for key, value := range storage {
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
	log.Printf("	shard loaded %+v\n", s.localStorages[shardID])
	return nil
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