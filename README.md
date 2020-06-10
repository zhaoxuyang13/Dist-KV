##  Distributed KV Store

### TODOs

- [x] Add Yml for configuration of ZK, https://github.com/go-yaml/yaml (though it may be redudant with zk-cluster.yml)
- [x] change yml to json, for consistency
- [x] Slave - service Registering.
- [x] Slave - Put,Del,Get RPC APIs
- [X] Slave - RPC registering
- [x] Client - A REPL interface
- [x] Master - Join/Leave/Query interface
- [x] Client - Automatic primary/back selection in starting.
- [x] Master - register logic
- [ ] Master - filter out duplicate RPCs
- [ ] Lock library (for slaves data access)
- [ ] Client - Put,Del,Get, CMDLINE-Interface, RPC call (after master done)
- [ ] Slave backup & primary failed re-election logic
- [ ] Slave backup & primary normal logic

**Features**
- [ ] multiple backup nodes
- [ ] backup for master

### Build & Run

**zookeeper** 

```shell
$ docker-compose -f zk-cluster up
```

- [ ] deploy zkper using own script

**Slave**

```bash
$ go run go/slave_server.go [ip] [port] [hostname] [groupID]
# ip and port expose as RPC service
# hostname is used to distinguish
# groupID  is for grouping.
```

**Master**

```
$ go run go/master_server.go
```

**Client** 

```
$ go run go/client.go
```

