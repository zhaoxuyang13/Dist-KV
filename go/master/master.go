package master

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)


type ShardMaster struct {
	latest        int        // latest version number

	ip       string
	port     int
	shardNum int
	hostname string

	shardConfLock sync.RWMutex // big lock for configuration modification.
	ShardConfs    []*Configuration
}
type ServerConf struct {
	IP       string
	Port     int
	ShardNum int
	Hostname string
}
func NewShardMaster(conf ServerConf) *ShardMaster {
	sm := ShardMaster{
		ip :           conf.IP,
		port :         conf.Port,
		shardNum:      conf.ShardNum,
		hostname:      conf.Hostname,
		latest:        0,
		shardConfLock: sync.RWMutex{},
		ShardConfs:    make([]*Configuration, 1),
	}

	firstConf := Configuration{
		Version:    0,
		ShardNum:   conf.ShardNum,
		Id2Groups:  make(map[int]Group),
		Assignment: make(map[int]int),
	}
	/* initial configuration map all shards to group 0, which is not valid */
	for i := 0; i < conf.ShardNum; i++ {
		firstConf.Assignment[i] = 0
	}
	sm.ShardConfs[0] = &firstConf
	return &sm
}
func (m *ShardMaster)ServerString() string{
	return m.ip + ":" + strconv.Itoa(m.port);
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
		return splitWork(m.shardNum, N)
	}else {
		return nil
	}
}


/*
Join : join several new groups to shardMaster, and re-balance
*/
func (m *ShardMaster) Join(newGroups map[int][]string) error {

	/* get the lock before make modification, TODO: may change this to optimistic lock */
	m.shardConfLock.Lock()
	defer m.shardConfLock.Unlock()

	lastConf := m.ShardConfs[m.latest] // maybe merge latest conf with new conf
	newConf,_ := lastConf.DeepCopy()
	if newConf.Version != m.latest {
		panic("no way")
	}

	/* determine how many shards each group will have after */
	oldGroupNum := len(lastConf.Id2Groups)
	newGroupNum := oldGroupNum + len(newGroups)
	newSplit := m.splitWork(newGroupNum)
	fmt.Printf("	new split %v\n", newSplit)

	/* shards taken from some */
	shardsTaken := make([]int, 0)
	index := 0
	if oldGroupNum == 0 { /* start from empty, then all shards are available */
		for i := 0; i < m.shardNum; i++ {
			shardsTaken = append(shardsTaken, i)
		}
	} else { /* collect from old groups, if they are allocated less shards, just take first some from them */
		for gid, group := range lastConf.Id2Groups {
			oldLen := len(group.Shards)
			if newSplit[index] < oldLen {
				// take first oldLen-newSplit[index]
				// assign newConf just
				shardsTaken = append(shardsTaken, group.Shards[0:oldLen-newSplit[index]]...)

				group.Shards = group.Shards[oldLen-newSplit[index]:]
				newConf.Id2Groups[gid] = group
			}
			index++
		}
	}


	begin := 0
	/*  re-balance to others  */
	for gid, serverConfs := range newGroups {
		group := NewGroup(gid, serverConfs)
		group.Shards = append(group.Shards, shardsTaken[begin : begin+newSplit[index]]...)
		begin += newSplit[index]
		index++
		/* add new group to Id2Groups if gid not collide.*/
		if _, exist := newConf.Id2Groups[group.Gid]; exist {
			return errors.New("gid already exist:" + strconv.Itoa(int(gid)) + " \n")
		}
		newConf.Id2Groups[group.Gid] = *group
		for _, shard := range group.Shards { // assign those shards to new group.
			newConf.Assignment[shard] = group.Gid
		}
	}
	/* update the change back to ShardMaster*/
	m.latest ++
	newConf.Version = m.latest
	m.ShardConfs = append(m.ShardConfs, newConf)

	return nil
}

/*
Leave : leave several old groups from shardMaster, and re-balance
*/
func (m *ShardMaster) Leave(groupIDs []int) error {

	/* get the lock before make modification, TODO: may change this to optimistic lock */
	m.shardConfLock.Lock()
	defer m.shardConfLock.Unlock()

	lastConf := m.ShardConfs[m.latest] // maybe merge latest conf with new conf
	newConf,_ := lastConf.DeepCopy()
	if newConf.Version != m.latest {
		panic("no way")
	}

	/* determine how many shards each group will have after */
	oldGroupNum := len(lastConf.Id2Groups)
	newGroupNum := oldGroupNum - len(groupIDs)
	if newGroupNum == 0 {
		return errors.New("leave will remove all the groups from shardmaster, denying request")
	}
	newSplit := m.splitWork(newGroupNum)
	//fmt.Printf("	newSplit %v\n", newSplit)

	/* shards taken from some and  re-balance to others */
	shardsTaken := make([]int, 0)
	for _, gid := range groupIDs {
		if group, ok := newConf.Id2Groups[gid]; ok{
			shardsTaken = append(shardsTaken,group.Shards...)
			delete(newConf.Id2Groups, gid)
		} else {
			return errors.New("the gid " + strconv.Itoa(int(gid)) + " in leave list does not exist")
		}
	}
	begin := 0
	index := 0
	for gid, group := range newConf.Id2Groups { /* only groups left wil be iterated */
		oldLen := len(group.Shards)
		delta := newSplit[index] - oldLen
		if delta > 0 {
			group.Shards =
				append(group.Shards, shardsTaken[begin:begin + delta]...)
			newConf.Id2Groups[gid] = group

			for _, shardID := range shardsTaken[begin : begin + delta] {
				newConf.Assignment[shardID] = gid
			}
		}
		begin += delta
		index++
	}
	/* ask corresponding groups to shift shards. */

	/* update the change back to ShardMaster*/
	m.latest++
	newConf.Version = m.latest
	m.ShardConfs = append(m.ShardConfs, newConf)

	/*unlock(done by defer)*/

	return nil
}

/*
Query : Query for configuration of version (-1 means latest)
*/
func (m *ShardMaster) Query(version int) (*Configuration, error) {
	//fmt.Printf("receive a Query RPC, %v\n", req.ConfVersion)
	m.shardConfLock.RLock()
	defer m.shardConfLock.RUnlock()

	if version == -1 { /*asking for latest configuration*/
		return m.ShardConfs[m.latest],nil
	} else if version >= 0 && version <= m.latest {
		return m.ShardConfs[version],nil
	} else {
		return nil, errors.New("conf version not valid")
	}
}

