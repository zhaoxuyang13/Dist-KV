#!/usr/bin/env bash

# may change to multiple master later
go run go/master_server.go 127.0.0.1 4100 master > logs/master.log &
# wait for master to start
sleep 5

# in real world deployment slave should be started on different machine, and listen on corresponding ip:port
# start 4 groups with 2, 2, 2, 1 servers
go run go/slave_server.go  127.0.0.1 4010 slave1-1 1 > logs/slave-1-1.log &
go run go/slave_server.go  127.0.0.1 4011 slave1-2 1 > logs/slave-1-2.log &
go run go/slave_server.go  127.0.0.1 4020 slave2-1 2 > logs/slave-2-1.log &
go run go/slave_server.go  127.0.0.1 4021 slave2-2 2 > logs/slave-2-2.log &
go run go/slave_server.go  127.0.0.1 4030 slave3-1 3 > logs/slave-3-1.log &
go run go/slave_server.go  127.0.0.1 4031 slave3-2 3 > logs/slave-3-2.log &

go run go/slave_server.go  127.0.0.1 4040 slave4-1 4 > logs/slave-4-1.log &
