package slave

//noinspection ALL
import (
	"context"
	"ds/go/master"
	"errors"
	"fmt"
)

var ErrWrongGroup = errors.New("key not managed by this group")
var ErrNotPrimary = errors.New("don't send request to a back up server")
/*
LocalStorages : in-memory kv store using map
*/
type LocalStorage struct {
	storage map[string]string
}

/*
Slave : slave structure implement slave RPC semantics
*/
type Slave struct{
	//bigLock sync.Mutex // for keyLocks map
	//keyLocks map[string]*sync.Mutex
	Primary bool
	LocalStorages map[int]*LocalStorage // map from ShardID to LocalStorages
	Version int // local version number
	//GroupId int32 // the group id this slave belongs to, used to check configuration
	GroupInfo master.Group
	//ShardList []int32 // list of shards this slave will manage ,get from master every 100 milliseconds
}


//var localStorages = make(map[int32]*LocalStorages)

/* TODO add check for Vnode number's validation, slave should maintain valid vnode list. and respond error if not */
func (s *Slave) Put(ctx context.Context, args *Request) (*Empty, error) {
	fmt.Println("put")
	/* TODO : is it need to acquire lock for the key ? */
	//s.bigLock.Lock()
	//if mutex,exist := s.keyLocks[args.GetKey()]; !exist {
	//	s.keyLocks[args.GetKey()] = &sync.Mutex{}
	//}else {
	//	mutex.Lock()
	//	defer mutex.Unlock()
	//}
	//s.bigLock.Unlock()

	/* create if not exist, append if exist*/
	shardID := int(args.ShardID)
	if localStorage, ok := s.LocalStorages[shardID]; ok {
		localStorage.storage[args.GetKey()] = args.GetValue()

	} else {
		ls := LocalStorage{
			storage: make(map[string]string),
		}
		ls.storage[args.GetKey()] = args.GetValue()
		s.LocalStorages[shardID] = &ls
	}

	/* release lock for the key, done by defer*/

	//reply := Response{
	//	Value: "put " + strconv.Itoa(int(args.ShardID)) + " of key " + args.GetKey() + " of value " + args.GetValue(),
	//} // echo server primitive

	return &Empty{}, nil
}
func (s *Slave) Get(ctx context.Context, args *Request) (*Response, error) {
	fmt.Println("get")
	/* TODO : acquire lock for the key, but is it necessary to put a lock on read ?*/
	/* create if not exist, append if exist*/
	shardID := int(args.ShardID)
	localStorage, ok := s.LocalStorages[shardID]
	if !ok {
		return nil, errors.New("Vnode not exist on this machine\n")
	}

	res, ok := localStorage.storage[args.GetKey()]
	if !ok {
		return nil, errors.New("Key not exist on this machine's vnode\n")
	}

	reply := Response{
		Value: res,
	}

	return &reply, nil
}
func (s *Slave) Del(ctx context.Context, args *Request) (*Empty, error) {
	fmt.Println("del")
	//s.bigLock.Lock()
	//if mutex,exist := s.keyLocks[args.GetKey()]; !exist {
	//	s.keyLocks[args.GetKey()] = &sync.Mutex{}
	//}else {
	//	mutex.Lock()
	//	defer mutex.Unlock()
	//}
	//s.bigLock.Unlock()

	/* create if not exist, append if exist*/
	shardID := int(args.ShardID)
	localStorage, ok := s.LocalStorages[shardID]
	if !ok {
		return nil, errors.New("Vnode not exist on this machine\n")
	}

	_, ok = localStorage.storage[args.GetKey()]
	if ok {
		delete(localStorage.storage, args.GetKey())
	} else {
		return nil, errors.New("Key not exist on this machine's vnode\n")
	}

	//reply := Response{
	//	Value: "successful delete " + args.GetKey(),
	//} // echo server primitive

	/*release lock for the key, done by defer*/

	return &Empty{}, nil
}
