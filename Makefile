
ip ?= 127.0.0.1
master_port ?= 4100
master_name ?= master
shard_num ?= 20
master: 
	go run go/master_server.go ${ip} ${port} ${master_name} ${shard_num}

slave_port ?= 4010
slave_name ?= slave
group ?= 1
slave: 
	go run go/slave_server.go ${ip} ${port} ${slave_name} ${group}