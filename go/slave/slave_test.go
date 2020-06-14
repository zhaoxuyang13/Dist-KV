package slave

import (
	"context"
	"fmt"

	//"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlave_KV(t *testing.T) {
	assert := assert.New(t)
	slave := Slave{
		//bigLock: sync.Mutex{},
		//keyLocks: make(map[string]*sync.Mutex),
		LocalStorages: make(map[int]*LocalStorage),
	}
	var ctx context.Context
	args := Request{
		ShardID : 0,
		Key : "test Key",
		Value : "test Value",
	}

	if _, err := slave.Put(ctx, &args); err != nil {
		assert.Fail("Put operation failed \n" + err.Error())
	}else {
		print("put K-V: " + args.GetKey() )
	}
	if res, err := slave.Get(ctx, &args);err != nil {
		assert.Fail("Get operation failed \n" + err.Error())
	}else {
		print("get Value " + res.GetValue() + "for key " + args.GetKey())
	}
	if _, err := slave.Del(ctx, &args); err != nil {
		assert.Fail("delete operation failed \n" + err.Error())
	}

	if res, err := slave.Get(ctx, &args); err == nil && res != nil{
		assert.Fail("delete operation failed")
	}else {
		print("key deleted \n" + err.Error())
	}
	/* immulate RPC call by directly calling */
}
/*
Access single key concurrently
*/
//func TestKV_Concurrent_Single(t *testing.T){
//	assert := assert.New(t)
//	slave := Slave{
//		//bigLock: sync.Mutex{},
//		//keyLocks: make(map[string]*sync.Mutex),
//		LocalStorages: make(map[int32]*LocalStorage),
//	}
//	var ctx context.Context
//	args := Request{
//		ShardID : 0,
//		Key : "key1",
//		Value : "value1",
//	}
//
//
//}

func TestDiffShards(t *testing.T){
	assert := assert.New(t)
	old := []int{2,3,4,5,6}
	new := []int{4,5,6,7,8}
	added,removed := diffShards(old,new)

	assert.Len(added,2)
	assert.Len(removed,2)
	assert.Contains(added, 7)
	assert.Contains(added, 8)
	assert.Contains(removed, 2)
	assert.Contains(removed, 3)
	fmt.Printf("add %v, remove %v\n",added,removed)
}