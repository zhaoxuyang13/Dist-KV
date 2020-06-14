package master

import (
	"hash/crc32"
)

func (c *Configuration) GetShard(key string) int { // should I convert it to int ?
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	return hash % c.ShardNum
}

//type Server struct {
//	Ip   string
//	Port int
//	Name string
//}
/* a replica group is consist of a primary server and multiple backup servers. */
type Group struct {
	Gid     int
	Servers []string
	Shards  []int // the shard list
}

/* current only support 2 confs*/
func NewGroup(gid int, servers *JoinRequest_ServerConfs) *Group {
	group := Group{
		Gid:     gid,
		Servers: servers.Names,
		Shards:  make([]int, 0),
	}
	return &group
}


/*


、

Query
	Put/Get/Del => command
	Key => Value
Join
	Ip:Port => ReSharding
Leave
	Ip:Port => ReSharding
// Move



*/
type Configuration struct {
	Version  int
	ShardNum int
	// map from a gid -> Group (this map contains all groups in his replica
	Groups map[int]Group
	//map from a shardId -> gid (valid assignments for all 0 ~ ShardNum-1
	Assignment map[int]int //
}

/*
DeepCopy :
	map will be copied by reference by default, need explicit deep copy
*/
func (c *Configuration)DeepCopy() (*Configuration,error){
	groups := make(map[int]Group)
	for gid, group := range c.Groups{
		servers := make([]string,len(group.Servers))
		shards := make([]int,len(group.Shards))
		copy(servers,group.Servers)
		copy(shards,group.Shards)
		groups[gid] = Group{
			Gid:     group.Gid,
			Servers: servers,
			Shards:  shards,
		}
	}
	assignment := make(map[int]int)
	for k,v := range c.Assignment {
		assignment[k] = v
	}
	return &Configuration{
		Version:    c.Version,
		ShardNum:   c.ShardNum,
		Groups:     groups,
		Assignment: assignment,
	},nil
}

/*
ToConf helper method, convert Configuration to Conf
*/
func (c *Configuration) ToConf() (*Conf, error) {
	mapping := make(map[int32]*Conf_Group)
	convIntSlice := func(v []int) []int32 {
		vm := make([]int32, len(v))
		for i, val := range v {
			vm[i] = int32(val)
		}
		return vm
	}
	for gid, group := range c.Groups {
		confGroup := Conf_Group{
			Gid:     int32(gid),
			Servers: group.Servers,
			Shards:  convIntSlice(group.Shards),
		}
		mapping[confGroup.Gid] = &confGroup
	}
	assignment := make(map[int32]int32)
	for sid, gid := range c.Assignment {
		assignment[int32(sid)] = int32(gid)
	}
	return &Conf{
		Version:    int32(c.Version),
		ShardNum:   int32(c.ShardNum),
		Mapping:    mapping,
		Assignment: assignment,
	}, nil
}

/*
NewConf helper function , convert Conf to Configuration
*/
func NewConf(conf *Conf) *Configuration {
	mapping := make(map[int]Group)
	toIntSlice := func(v []int32) []int {
		vm := make([]int, len(v))
		for i, val := range v {
			vm[i] = int(val)
		}
		return vm
	}
	for gid, group := range conf.Mapping {
		servers := make([]string, len(group.Servers))
		copy(servers,group.Servers)

		mapping[int(gid)] = Group{
			Gid:     int(gid),
			Servers: servers,
			Shards:  toIntSlice(group.Shards),
		}
	}
	assignment := make(map[int]int)
	for sid, gid := range conf.Assignment {
		assignment[int(sid)] = int(gid)
	}
	return &Configuration{
		Version:    int(conf.Version),
		ShardNum:   int(conf.ShardNum),
		Groups:     mapping,
		Assignment: assignment,
	}
}
