package master

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

/*
ShardMaster : RPC Interface of a ShardMaster
*/
type ShardMaster struct {
	confLock sync.Mutex // big lock for configuration modification.
	latest   int        // latest version number
	ShardNum int        // total number of shards to be split across slave groups
	Confs    []Configuration
}

/*
NewShardMaster : creat the ShardMaster with numbers of shard
*/
func NewShardMaster(shardNum int) *ShardMaster {
	sm := ShardMaster{
		ShardNum: shardNum,
		latest:   0,
		confLock: sync.Mutex{},
		Confs:    make([]Configuration, 1),
	}

	firstConf := Configuration{
		Version:    0,
		Groups:     make(map[int]Group),
		Assignment: make(map[int]int),
	}
	/* init with first configuration map all shards to GID 0, which is not valid */
	for i := 0; i < shardNum; i++ {
		firstConf.Assignment[i] = 0
	}
	sm.Confs[0] = firstConf
	return &sm
}

/* split work load for groups in circumstance of
M shards, N groups
return how many shards each group will have
average groups has "avg" , and the first "num" groups have avg + 1
*/
func splitWork(M int, N int) []int {
	average := M / N
	left := M % N
	proportion := make([]int, N)
	for i := 0; i < left; i++ {
		proportion[i] = average + 1
	}
	for i := left; i < N; i++ {
		proportion[i] = average
	}
	return proportion
}
func (m *ShardMaster) splitWork(N int) []int {
	if N != 0 {
		return splitWork(m.ShardNum, N)
	}else {
		return nil
	}
}


/*
Join : join serveral new groups to shardmaster, and re-balance
*/
func (m *ShardMaster) Join(ctx context.Context, req *JoinRequest) (*Empty, error) {
	fmt.Printf("receive a Join RPC, %v\n", req.Mapping)

	/* get the lock before make modification, TODO: may change this to optimistic lock */
	m.confLock.Lock()
	defer m.confLock.Unlock()
	latestConf := m.Confs[m.latest]
	if latestConf.Version != m.latest {
		panic("no way")
	}

	/* make some changes to latest conf */
	/* determine how many shards each group will have after */
	oldGroupNum := len(latestConf.Groups)
	//oldSplit := m.splitWork(oldGroupNum)
	newGroupNum := oldGroupNum + len(req.Mapping)
	newSplit := m.splitWork(newGroupNum)
	//fmt.Printf("	newsplit %v\n", newSplit)

	/* shards taken from some */
	shardsTaken := make([]int, 0, m.ShardNum)
	index := 0
	if oldGroupNum == 0 { /* start from empty*/
		for i := 0; i < m.ShardNum; i++ {
			shardsTaken = append(shardsTaken, i)
		}
	} else { /* collect from old groups */
		for gid, group := range latestConf.Groups {
			oldLen := len(group.Shards)
			if newSplit[index] < oldLen { // move first oldLen-newSplit[index] to new groups
				shardsTaken = append(shardsTaken, group.Shards[0:oldLen-newSplit[index]]...)
				groupConf := latestConf.Groups[gid]
				groupConf.Shards = group.Shards[oldLen-newSplit[index]:]
				latestConf.Groups[gid] = groupConf
			}
			index++
		}
	}
	//fmt.Printf("	shardsTaken %v\n", shardsTaken)

	newConf,_ := latestConf.DeepCopy() // maybe merge latest conf with new conf
	begin := 0
	/*  re-balance to others  */
	for gid, serverConfs := range req.Mapping {
		group := NewGroup(int(gid), serverConfs)
		group.Shards = shardsTaken[begin : begin+newSplit[index]]
		begin += newSplit[index]
		index++
		/* add new group to Groups if gid not collide.*/
		if _, exist := newConf.Groups[group.Gid]; exist {
			return nil, errors.New("gid already exist:" + strconv.Itoa(int(gid)) + " \n")
		}
		newConf.Groups[group.Gid] = *group
		for _, shard := range group.Shards { // assign those shards to new group.
			newConf.Assignment[shard] = group.Gid
		}

	}
	/* ask corresponding groups to shift shards. */

	/* update the change back to ShardMaster*/
	m.latest++
	newConf.Version = m.latest
	m.Confs = append(m.Confs, *newConf)

	/*unlock(done by defer)*/

	return &Empty{}, nil
}

/*
Leave : leave several old groups from shardmaster, and re-balance
*/
func (m *ShardMaster) Leave(ctx context.Context, req *LeaveRequest) (*Empty, error) {
	fmt.Printf("receive a Leave RPC, %v\n", req.GidList)

	/* get the lock before make modification, TODO: may change this to optimistic lock */
	m.confLock.Lock()
	defer m.confLock.Unlock()
	latestConf := m.Confs[m.latest]
	if latestConf.Version != m.latest {
		panic("no way")
	}

	/* make some changes to lastest conf */
	/* determine how many shards each group will have after */
	oldGroupNum := len(latestConf.Groups)
	//oldSplit := m.splitWork(oldGroupNum)
	newGroupNum := oldGroupNum - len(req.GidList)
	if newGroupNum == 0 {
		return nil, errors.New("leave will remove all the groups from shardmaster, denying request")
	}
	newSplit := m.splitWork(newGroupNum)

	//fmt.Printf("	newSplit %v\n", newSplit)
	/* shards taken from some and  re-balance to others */
	shardsTaken := make([]int, 0, m.ShardNum)
	for _, gid := range req.GidList {
		group, ok := latestConf.Groups[int(gid)]
		if ok {
			for _, shardID := range group.Shards {
				shardsTaken = append(shardsTaken, shardID)
			}
			delete(latestConf.Groups, int(gid))
		} else {
			return nil, errors.New("the gid " + strconv.Itoa(int(gid)) + " in leave list does not exist")
		}
	}
	//fmt.Printf("	shardsTaken %v\n", shardsTaken)
	newConf,_ := latestConf.DeepCopy() // maybe merge latest conf with new conf
	begin := 0
	index := 0
	for gid, group := range latestConf.Groups {
		oldLen := len(group.Shards)
		if newSplit[index] > oldLen {
			oldGroup := newConf.Groups[gid]
			oldGroup.Shards = append(oldGroup.Shards, shardsTaken[begin:begin+newSplit[index]-oldLen]...)
			newConf.Groups[gid] = oldGroup
		}
		for _, shardID := range shardsTaken[begin : begin+newSplit[index]-oldLen] {
			newConf.Assignment[shardID] = gid
		}
		begin += newSplit[index] - oldLen
		index++
	}
	/* ask corresponding groups to shift shards. */

	/* update the change back to ShardMaster*/
	m.latest++
	newConf.Version = m.latest
	m.Confs = append(m.Confs, *newConf)

	/*unlock(done by defer)*/

	return &Empty{}, nil
}

/*
Query : Query for configuration giving a version
*/
func (m *ShardMaster) Query(ctx context.Context, req *QueryRequest) (*Conf, error) {
	//fmt.Printf("receive a Query RPC, %v\n", req.ConfVersion)

	if req.ConfVersion == -1 { /*asking for latest configuration*/
		return m.Confs[m.latest].ToConf()
	} else if req.ConfVersion >= 0 && int(req.ConfVersion) <= m.latest {
		return m.Confs[req.ConfVersion].ToConf()
	} else {
		return nil, errors.New("conf version not valid")
	}
}
