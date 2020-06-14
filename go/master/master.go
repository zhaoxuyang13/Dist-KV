package master

import (
	"context"
	"ds/go/common/zk_client"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"sync"
)

/*
ShardMaster : RPC Interface of a ShardMaster
*/
type ShardMaster struct {
	confLock sync.RWMutex // big lock for configuration modification.
	latest   int        // latest version number
	ShardNum int        // total number of shards to be split across slave groups
	Confs    []*Configuration
}

/*
NewShardMaster : creat the ShardMaster with numbers of shard
*/
func NewShardMaster(shardNum int) *ShardMaster {
	sm := ShardMaster{
		ShardNum: shardNum,
		latest:   0,
		confLock: sync.RWMutex{},
		Confs:    make([]*Configuration, 1),
	}

	firstConf := Configuration{
		Version:    0,
		ShardNum: shardNum,
		Groups:     make(map[int]Group),
		Assignment: make(map[int]int),
	}
	/* init with first configuration map all shards to GID 0, which is not valid */
	for i := 0; i < shardNum; i++ {
		firstConf.Assignment[i] = 0
	}
	sm.Confs[0] = &firstConf
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
Join : join several new groups to shardMaster, and re-balance
*/
func (m *ShardMaster) Join(ctx context.Context, req *JoinRequest) (*Empty, error) {
	fmt.Printf("receive a Join RPC, %v\n", req.Mapping)

	/* get the lock before make modification, TODO: may change this to optimistic lock */
	m.confLock.Lock()
	defer m.confLock.Unlock()

	lastConf := m.Confs[m.latest] // maybe merge latest conf with new conf
	newConf,_ := lastConf.DeepCopy()
	if newConf.Version != m.latest {
		panic("no way")
	}

	/* determine how many shards each group will have after */
	oldGroupNum := len(lastConf.Groups)
	newGroupNum := oldGroupNum + len(req.Mapping)
	newSplit := m.splitWork(newGroupNum)
	fmt.Printf("	new split %v\n", newSplit)

	/* shards taken from some */
	shardsTaken := make([]int, 0)
	index := 0
	if oldGroupNum == 0 { /* start from empty, then all shards are available */
		for i := 0; i < m.ShardNum; i++ {
			shardsTaken = append(shardsTaken, i)
		}
	} else { /* collect from old groups, if they are allocated less shards, just take first some from them */
		for gid, group := range lastConf.Groups {
			oldLen := len(group.Shards)
			if newSplit[index] < oldLen {
				// take first oldLen-newSplit[index]
				// assign newConf just
				shardsTaken = append(shardsTaken, group.Shards[0:oldLen-newSplit[index]]...)

				group.Shards = group.Shards[oldLen-newSplit[index]:]
				newConf.Groups[gid] = group
			}
			index++
		}
	}


	begin := 0
	/*  re-balance to others  */
	for gid, serverConfs := range req.Mapping {
		group := NewGroup(int(gid), serverConfs)
		group.Shards = append(group.Shards, shardsTaken[begin : begin+newSplit[index]]...)
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
	/* update the change back to ShardMaster*/
	m.latest ++
	newConf.Version = m.latest
	m.Confs = append(m.Confs, newConf)

	return &Empty{}, nil
}

/*
Leave : leave several old groups from shardMaster, and re-balance
*/
func (m *ShardMaster) Leave(ctx context.Context, req *LeaveRequest) (*Empty, error) {
	fmt.Printf("receive a Leave RPC, %v\n", req.GidList)

	/* get the lock before make modification, TODO: may change this to optimistic lock */
	m.confLock.Lock()
	defer m.confLock.Unlock()

	lastConf := m.Confs[m.latest] // maybe merge latest conf with new conf
	newConf,_ := lastConf.DeepCopy()
	if newConf.Version != m.latest {
		panic("no way")
	}

	/* determine how many shards each group will have after */
	oldGroupNum := len(lastConf.Groups)
	newGroupNum := oldGroupNum - len(req.GidList)
	if newGroupNum == 0 {
		return nil, errors.New("leave will remove all the groups from shardmaster, denying request")
	}
	newSplit := m.splitWork(newGroupNum)
	//fmt.Printf("	newSplit %v\n", newSplit)

	/* shards taken from some and  re-balance to others */
	shardsTaken := make([]int, 0)
	for _, gid := range req.GidList {
		if group, ok := newConf.Groups[int(gid)]; ok{
			shardsTaken = append(shardsTaken,group.Shards...)
			delete(newConf.Groups, int(gid))
		} else {
			return nil, errors.New("the gid " + strconv.Itoa(int(gid)) + " in leave list does not exist")
		}
	}
	begin := 0
	index := 0
	for gid, group := range newConf.Groups { /* only groups left wil be iterated */
		oldLen := len(group.Shards)
		delta := newSplit[index] - oldLen
		if delta > 0 {
			group.Shards =
				append(group.Shards, shardsTaken[begin:begin + delta]...)
			newConf.Groups[gid] = group

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
	m.Confs = append(m.Confs, newConf)

	/*unlock(done by defer)*/

	return &Empty{}, nil
}

/*
Query : Query for configuration giving a version
*/
func (m *ShardMaster) Query(ctx context.Context, req *QueryRequest) (*Conf, error) {
	//fmt.Printf("receive a Query RPC, %v\n", req.ConfVersion)
	m.confLock.RLock()
	defer m.confLock.RUnlock()

	if req.ConfVersion == -1 { /*asking for latest configuration*/
		return m.Confs[m.latest].ToConf()
	} else if req.ConfVersion >= 0 && int(req.ConfVersion) <= m.latest {
		return m.Confs[req.ConfVersion].ToConf()
	} else {
		return nil, errors.New("conf version not valid")
	}
}

/* return RPC client of master */
func GetMasterRPCClient (sdClient *zk_client.SdClient) (ShardingServiceClient,*grpc.ClientConn,error){
	/* connect to master */
	if masterNode,err := sdClient.Get1Node("master"); err != nil {
		return nil,nil,err
	}else {
		serverString := masterNode.Host + ":" + strconv.Itoa(masterNode.Port)
		fmt.Println("master server String : " + serverString)
		conn, err := grpc.Dial(serverString, grpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
			return nil,nil,err
		}
		return NewShardingServiceClient(conn),conn,nil
	}
}