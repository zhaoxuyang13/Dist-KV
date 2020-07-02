package slave

//noinspection ALL
import (
	"ds/go/common/Utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
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
	REMOVABLE
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

/*
Slave : slave structure implement slave RPC semantics
*/
type Slave struct {
	/*
		shardsLock : lock for groupInfo.shards(what shards current group serves),
		acquire when modifying or reading*/
	shardsLock *sync.RWMutex
	shards     []int

	/*
		storageLock: lock for shardID -> *localstorage map
		lock for individual Local Shard Storage is in LocalStorage Structure.*/
	storageLock   *sync.RWMutex
	localStorages map[int]*LocalStorage // map from ShardID to localStorages

}

func NewSlave() *Slave {
	return &Slave{
		shardsLock:    new(sync.RWMutex),
		shards:        make([]int,0),
		storageLock:   new(sync.RWMutex),
		localStorages: make(map[int]*LocalStorage),
	}
}
func (s *Slave) put(key string , value string , shardID int ) error {
	s.shardsLock.RLock()
	defer s.shardsLock.RUnlock()
	if Utils.ContainsInt(s.shards, shardID) == false {
		s.shardsLock.RUnlock()
		return ErrWrongGroup
	}

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
	return nil
}

func (s *Slave) get(key string , shardID int)  (string,error) {
	s.shardsLock.RLock()
	defer s.shardsLock.RUnlock()
	if Utils.ContainsInt(s.shards, shardID) == false {
		s.shardsLock.RUnlock()
		return "",ErrWrongGroup
	}

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

func (s *Slave) del(key string, shardID int) error {

	s.shardsLock.RLock()
	defer s.shardsLock.RUnlock()
	if Utils.ContainsInt(s.shards, shardID) == false {
		s.shardsLock.RUnlock()
		return ErrWrongGroup
	}

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
	return nil
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
add a new shard
*/
func (s *Slave) addStorage(shardID int,storage map[string]string) error {

	localStorage := s.assureStorage(shardID, UNREADY)
	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()

	for key, value := range storage {
		localStorage.storage[key] = value
	}
	localStorage.state = READY
	localStorage.cond.Broadcast()

	log.Printf("	shard loaded %d\n",shardID )
	return nil
}

func (s *Slave) expireStorage(shardID int) {
	localStorage := s.assureStorage(shardID, EXPIRED)
	localStorage.lock.Lock()
	defer localStorage.lock.Unlock()
	localStorage.state = EXPIRED
}
func (s *Slave) deleteStorage(shardID int){
	s.storageLock.Lock()
	defer s.storageLock.Unlock()
	delete(s.localStorages, shardID)
}

func (s *Slave) deleteShard(shardID int){
	s.shardsLock.Lock()
	defer s.shardsLock.Unlock()
	s.shards = Utils.Delete(s.shards, shardID)
}
func (s *Slave) addShard(shardID int){
	s.shardsLock.Lock()
	defer s.shardsLock.Unlock()
	s.shards = append(s.shards, shardID)
}
/* safely with a read lock */
func (s *Slave)compareShards(newShards []int) ([]int, []int) {
	s.shardsLock.RLock()
	defer s.shardsLock.RUnlock()
	return diffShards(s.shards,newShards)
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