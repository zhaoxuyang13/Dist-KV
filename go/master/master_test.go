package master

import (
	"context"
	. "ds/go/common/sharding"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardMaster_Join_Leave(t *testing.T) {
	assert := assert.New(t)
	sm := NewShardMaster(20)

	/* assert sm has 20 shards and first config. with all 0*/

	assert.True(sm.ShardNum == 20 && sm.latest == 0)
	assert.True(len(sm.Confs) == 1)
	for _, gid := range sm.Confs[0].Assignment {
		assert.True(gid == 0)
	}

	var ctx context.Context
	group1 := JoinRequest{
		Mapping: map[int32]*JoinRequest_ServerConfs{
			1: {
				Names: []string{"1a", "1b", "1c"},
			},
		},
	}
	group2 := JoinRequest{
		Mapping: map[int32]*JoinRequest_ServerConfs{
			2: {
				Names: []string{"2a", "2b", "2c"},
			},
		},
	}
	group34 := JoinRequest{
		Mapping: map[int32]*JoinRequest_ServerConfs{
			3: {
				Names: []string{"3a", "3b", "3c"},
			},
			4: {
				Names: []string{"4a", "4b", "4c"},
			},
		},
	}
	gid1 := LeaveRequest{
		GidList: []int32{1},
	}
	gid2 := LeaveRequest{
		GidList: []int32{2},
	}
	gid23 := LeaveRequest{
		GidList: []int32{2, 3},
	}
	/* add group 1*/
	_, err := sm.Join(ctx, &group1)
	if err != nil {
		assert.Fail(err.Error())
	}

	fmt.Printf("conf %+v\n", sm.Confs[1])

	assert.True(sm.ShardNum == 20 && sm.latest == 1)
	assert.True(len(sm.Confs) == 2)
	for _, gid := range sm.Confs[1].Assignment {
		assert.True(gid == 1)
	}

	/* add group 2*/
	_, err = sm.Join(ctx, &group2)
	if err != nil {
		assert.Fail(err.Error())
	}

	fmt.Printf("conf %+v\n", sm.Confs[2])

	assert.True(sm.ShardNum == 20 && sm.latest == 2 && len(sm.Confs) == 3)
	counters := []int{0, 0, 0}
	for _, gid := range sm.Confs[2].Assignment {
		assert.True(gid == 1 || gid == 2)
		counters[gid]++
	}
	assert.True(counters[1] == counters[2] && counters[1] == 10)

	/* leave group 1*/
	_, err = sm.Leave(ctx, &gid1)
	if err != nil {
		assert.Fail(err.Error())
	}
	assert.True(sm.ShardNum == 20 && sm.latest == 3 && len(sm.Confs) == 4)
	for _, gid := range sm.Confs[3].Assignment {
		assert.True(gid == 2)
	}

	sm.Join(ctx, &group34)
	sm.Leave(ctx, &gid23)
	assert.True(sm.ShardNum == 20 && sm.latest == 5 && len(sm.Confs) == 6)
	for _, gid := range sm.Confs[5].Assignment {
		assert.True(gid == 4)
	}

	fmt.Printf("conf %+v\n", sm.Confs[5])

	/* leave last gid, will fail, and take no effect */
	_, err = sm.Leave(ctx, &gid2)
	assert.True(err != nil)

	conf, err := sm.Query(ctx, &QueryRequest{
		ConfVersion: -1,
	})
	if err != nil {
		assert.Fail(err.Error())
	}
	c := NewConf(conf)
	assert.True(c.Version == sm.latest)
	assert.True(len(c.Groups[4].Shards) == sm.ShardNum)
	assert.True(len(c.Groups) == 1)

	fmt.Printf("configuration read: %+v", c)
	for _, gid := range c.Assignment {
		assert.True(gid == 4)
	}

}
