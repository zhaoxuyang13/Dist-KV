package slave

import (
	"ds/go/common/Utils"
	"fmt"
	"sync"

	"testing"

	"github.com/stretchr/testify/assert"
)

func ContainsKey(reqs []request,key string) bool{
	for _,entry := range reqs {
		if entry.Key == key {
			return true
		}
	}
	return false
}
func TestSlave_Concurrent(t *testing.T) {
	assert := assert.New(t)
	slave := NewSlave(ServerConf{
		Hostname: "slave-1",
		IP:       "127.0.0.1",
		Port:     4001,
		GroupID:  1,
	})
	slave.Primary = false
	shardNum := 20
	for i:=0 ; i < shardNum; i ++{
		slave.shards = append(slave.shards, i)
	}


	maxStringLength := 10
	reqs := make([]request,0)
	for i := 0; i < 100; {
		key := Utils.RandString(maxStringLength)
		if ContainsKey(reqs,key){
			continue
		}
		req := request{
			Key:     key,
			Value:   Utils.RandString(maxStringLength),
			ShardID: Utils.Key2shard(key,shardNum),
		}
		reqs = append(reqs, req)
		i ++
	}

	/* concurrent put */
	wg := sync.WaitGroup{}
	wg.Add(len(reqs))
	for _,req := range reqs {
		req := req
		go func() {
			err := slave.Put(req.Key,req.Value,req.ShardID)
			assert.True(err == nil)
			wg.Done()
		}()
	}
	wg.Wait()


	/* concurrent get */
	wg.Add(len(reqs))
	for _,req := range reqs{
		req := req
		go func() {
			v,err := slave.Get(req.Key,req.ShardID)
			assert.True(err == nil)
			assert.Equal(req.Value,v)
			wg.Done()
		}()
	}
	wg.Wait()

	/* concurrent del */
	wg.Add(len(reqs))
	for _,req := range reqs{
		req := req
		go func() {
			err := slave.Del(req.Key,req.ShardID)
			assert.True(err == nil)
			wg.Done()
		}()
	}
	wg.Wait()


	/* should not exist */
	wg.Add(len(reqs))
	for _,req := range reqs{
		req := req
		go func() {
			v,err := slave.Get(req.Key,req.ShardID)
			assert.True(err != nil)
			assert.Equal("",v)
			wg.Done()
		}()
	}
	wg.Wait()

	/* Transfer 20 shards */
	storages := make(map[int] map[string]string)
	for i:=0 ;i < shardNum;i ++{
		storages[i] = make(map[string]string)
	}
	for _,req := range reqs{
		storages[req.ShardID][req.Key] = req.Value
	}
	wg.Add(shardNum)
	for shardID, storage := range storages{
		shardID := shardID
		storage := storage
		go func() {
			err := slave.TransferShard(shardID,storage)
			assert.True(err == nil)
			wg.Done()
		}()
	}
	wg.Wait()

	/* should exist */
	wg.Add(len(reqs))
	for _,req := range reqs{
		req := req
		go func() {
			v,err := slave.Get(req.Key,req.ShardID)
			assert.True(err == nil)
			assert.True(v == req.Value)
			wg.Done()
		}()
	}
	wg.Wait()
}


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