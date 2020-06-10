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
- [ ] Client - Automatic primary/backup selection when failing
- [ ] Client - Put,Del,Get, CMDLINE-Interface, RPC call (after master done)
- [ ] Lock library (for slaves data access)
- [ ] Slave
- [ ] Slave backup (maybe together with Slave)
- [ ] Master
- [ ] Client

**Features**
- [ ] multiple backup nodes
- [ ] backup for master

### Build & Run

**zookeeper** 

```shell
$ docker-compose -f zk-cluster up
```

**Slave**

```bash
$ go run go/slave.go [ip] [port]
# ip and port expose as RPC service
```

**Master**

```
$ go run go/master.go
```

**Client** 

```
$ go run go/client.go
```

