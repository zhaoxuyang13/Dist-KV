##  Distributed KV Store

### TODOs

- [x] Add Yml for configuration of ZK, https://github.com/go-yaml/yaml (though it may be redundant with zk-cluster.yml)
- [x] change yml to json, for consistency
- [x] Slave - service Registering.
- [x] Slave - Put,Del,Get RPC APIs
- [X] Slave - RPC registering
- [x] Client - A REPL interface
- [x] Master - Join/Leave/Query interface
- [x] Client - Automatic primary/back selection in starting.
- [x] Master - register logic
- [x] Client - Put,Del,Get, CMDLINE-Interface, RPC call (after master done)
- [x] Client - Del,Get Value not exist, Gracefully handle other failures, Update configuration when key-not-belong 
- [x] Master/Slave/Client - On configuration change, how to shift shards.
- [ ] Copy test from 6.824 / or write on my own
- [ ] Master/Slave/Client - handling network error, filter out duplicate RPCs
- [x] Lock library (for slaves data access, not necessary)
- [x] Slave backup & primary failed re-election logic
- [x] Slave backup & primary normal logic
- [x] Starting script for slave-master cluster 


**Features**
- [x] multiple backup nodes
- [ ] backup for master

### Build & Run

**zookeeper** 

```shell
$ docker-compose -f zk-cluster up
```

- [x] deploy zkper using own script

**Slave**

```bash
$ go run go/slave_server.go [ip] [port] [hostname] [groupID] 
# ip and port expose as RPC service
# hostname is used to distinguish
# groupID  is for grouping.
```

**Master**

```
$ go run go/master_server.go [ip] [port] [hostname]
```

**Client** 

```
$ go run go/client.go
```

**Admin**

```
$ go run go/admin.go [command] [groupIDs ...]
# command = "join-group" / "leave-group"
# groupIDs is an array of groupid to be configured
```


---
- Strange cases 

    - when primary failed and become backup again. client's request should fail. (fixed)