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
		localStorages: make(map[int]*LocalStorage),
	}

	var ctx context.Context
	args := Request{
		ShardID : 0,
		Key : "test Key",
		Value : "test Value",
	}

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