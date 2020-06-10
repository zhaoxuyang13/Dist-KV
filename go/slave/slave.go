package slave

//noinspection ALL
import (
	"context"
	"errors"
	"strconv"
)

/*
Slave : slave structure implement slave RPC semantics
*/
type Slave struct{}

/*
LocalStorage : Slave_file_node.json
	{
		"key": "value"
	}
*/
type LocalStorage struct {
	storage map[string]string
}

var localStorages = make(map[int32]*LocalStorage) // map from vnodeNum to LocalStorage

/* TODO add check for Vnode number's validation, slave should maintain valid vnode list. and respond error if not */
func (s *Slave) Put(ctx context.Context, args *Request) (*Response, error) {

	/* TODO : acquire lock for the key*/

	/* create if not exist, append if exist*/
	if localStorage, ok := localStorages[args.VnodeNum]; ok {
		localStorage.storage[args.GetKey()] = args.GetValue()
	} else {
		ls := LocalStorage{
			storage: make(map[string]string),
		}
		ls.storage[args.GetKey()] = args.GetValue()
		localStorages[args.VnodeNum] = &ls
	}

	/* TODO : release lock for the key*/

	reply := Response{
		Value: "put " + strconv.Itoa(int(args.VnodeNum)) + " of key " + args.GetKey() + " of value " + args.GetValue(),
	} // echo server primitive

	return &reply, nil
}
func (s *Slave) Get(ctx context.Context, args *Request) (*Response, error) {

	/* TODO : acquire lock for the key, but is it necessary to put a lock on read ?*/
	/* create if not exist, append if exist*/
	localStorage, ok := localStorages[args.VnodeNum]
	if !ok {
		return nil, errors.New("Vnode not exist on this machine\n")
	}

	res, ok := localStorage.storage[args.GetKey()]
	if !ok {
		return nil, errors.New("Key not exist on this machine's vnode\n")
	}

	reply := Response{
		Value: res,
	} // echo server primitive

	/* TODO : release lock for the key*/
	return &reply, nil
}
func (s *Slave) Del(ctx context.Context, args *Request) (*Response, error) {

	/* TODO : acquire lock for the key*/
	/* create if not exist, append if exist*/
	localStorage, ok := localStorages[args.VnodeNum]
	if !ok {
		return nil, errors.New("Vnode not exist on this machine\n")
	}

	_, ok = localStorage.storage[args.GetKey()]
	if ok {
		delete(localStorage.storage, args.GetKey())
	} else {
		return nil, errors.New("Key not exist on this machine's vnode\n")
	}

	reply := Response{
		Value: "successful delete " + args.GetKey(),
	} // echo server primitive

	/* TODO : release lock for the key*/

	return &reply, nil
}
