#!/bin/zsh

zkServer --config "/Users/zhaoxuyang/Courses/Distributed System/Dist-KV/zk-cluster/zoo1" start
zkServer --config "/Users/zhaoxuyang/Courses/Distributed System/Dist-KV/zk-cluster/zoo2" start
zkServer --config "/Users/zhaoxuyang/Courses/Distributed System/Dist-KV/zk-cluster/zoo3" start

# change the path to your own project path.