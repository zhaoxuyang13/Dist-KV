package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardMaster_Simple_Join_Leave(t *testing.T) {
	assert := assert.New(t)
	shardNum := 20
	shardMaster := NewShardMaster(ServerConf{
		IP:        "127.0.0.1",
		Port:      4100,
		ShardNum:  shardNum,
		Hostname : "master1",
	})

	mapping1 := map[int][]string{
		1: {"1-1", "1-2", "1-3"},
	}
	mapping2 := map[int][]string{
		2: {"2-1", "2-2", "2-3"},
	}
	mapping34 := map[int][]string{
		3: {"3-1", "3-2", "3-3"},
		4: {"4-1", "4-2", "4-3"},
	}
	if err := shardMaster.Join(mapping1); err != nil {
		assert.Fail(err.Error())
	}

	if conf, err := shardMaster.Query(-1); err != nil {
		assert.Fail(err.Error())
	}else if !CheckConf(shardNum,1, conf){
		assert.Fail("Configuration not correct\n")
	}

	if err := shardMaster.Join(mapping2); err != nil {
		assert.Fail(err.Error())
	}

	if conf, err := shardMaster.Query(-1); err != nil {
		assert.Fail(err.Error())
	}else if !CheckConf(shardNum,2, conf){
		assert.Fail("Configuration not correct\n")
	}

	if err := shardMaster.Leave([]int{1}); err != nil {
		assert.Fail(err.Error())
	}
	if conf, err := shardMaster.Query(-1); err != nil {
		assert.Fail(err.Error())
	}else if !CheckConf(shardNum,1, conf){
		assert.Fail("Configuration not correct\n")
	}

	if err := shardMaster.Join(mapping34); err != nil {
		assert.Fail(err.Error())
	}

	if conf, err := shardMaster.Query(-1); err != nil {
		assert.Fail(err.Error())
	}else if !CheckConf(shardNum,3, conf){
		assert.Fail("Configuration not correct\n")
	}

}
func CheckConf(shardNum int, groupNum int, conf *Configuration) bool{
	high := shardNum / groupNum + 1
	highNum := shardNum % groupNum
	if len(conf.Id2Groups) != groupNum {
		return false
	}
	countHigh := 0
	for _, group := range conf.Id2Groups {
		if len(group.Shards) == high {
			countHigh ++
		}
	}
	if countHigh != highNum {
		return false
	}
	// not checking assignments, todo
	return true
}