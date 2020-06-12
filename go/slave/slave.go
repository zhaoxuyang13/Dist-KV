package slave

//noinspection ALL
import (
	"context"
	"ds/go/common/Utils"
	"ds/go/master"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)
var (
	/* when request is for gid not belong to this group*/

	ErrWrongGroup = errors.New("key not managed by this group")
	/* when slave is backup and request is not from primary*/
	ErrNotPrimary = errors.New("don't send request to a back up server")
	/* for del and get operation, key is managed by this group, but not in the storage */
	ErrNotFound = errors.New("key not exist on this machine's storage")
)

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
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		//fmt.Printf("%+v, %d\n", s, shardID )
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
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

	return &Empty{}, nil
}
func (s *Slave) Get(ctx context.Context, args *Request) (*Response, error) {
	fmt.Println("get")
	/* TODO : acquire lock for the key, but is it necessary to put a lock on read ?*/
	/* create if not exist, append if exist*/
	shardID := int(args.ShardID)
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	if localStorage, ok := s.LocalStorages[shardID]; !ok {
		return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
	}else {
		if res, ok := localStorage.storage[args.GetKey()]; !ok {
			return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
		}else {
			reply := Response{
				Value: res,
			}
			return &reply, nil
		}
	}
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
	if Utils.Contains(s.GroupInfo.Shards, shardID) == false {
		return nil, status.Errorf(codes.Unavailable, ErrWrongGroup.Error())
	}
	if localStorage, ok := s.LocalStorages[shardID]; !ok {
		/* localStorage not exist */
		return nil, status.Errorf(codes.NotFound, ErrNotFound.Error())
	}else {
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
