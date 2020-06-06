##  Distributed KV Store

### TODOs

- [x] Add Yml for configuration of ZK, https://github.com/go-yaml/yaml (though it may be redudant with zk-cluster.yml)
- [x] change yml to json, for consistency
- [x] Slave - service Registering.
- [x] Slave - Put,Del,Get RPC APIs
- [X] Slave - RPC registering
- [ ] Client - A REPL interface
- [ ] Client - Put,Del,Get, CMDLINE-Interface, RPC call
- [ ] Lock library (for slaves data access)
- [ ] Slave
- [ ] Slave backup (maybe together with Slave)
- [ ] Master
- [ ] Client

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

```

**Client** 

```

```

