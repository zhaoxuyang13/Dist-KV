package main

import (
	"context"
	. "ds/go/slave"
	"testing"
)

func TestSlave_KV(t *testing.T) {
	slave := Slave{}
	var ctx context.Context
	args := Request{
		VnodeNum : 0,
		Key : "test Key",
		Value : "test Value",
	}

	res, err := slave.Put(ctx, &args)
	if err != nil {
		t.Errorf("Put operation failed \n" + err.Error())
	}
	print("put K-V: " + args.GetKey() + "-" + res.GetValue())

	res, err = slave.Get(ctx, &args)
	if err != nil {
		t.Errorf("Get operation failed \n" + err.Error())
	}
	print("get Value " + res.GetValue() + "for key " + args.GetKey())

	res, err = slave.Del(ctx, &args)
	if err != nil {
		t.Errorf("delete operation failed \n" + err.Error())
	}

	res, err = slave.Get(ctx, &args)
	if err == nil && res != nil{
		t.Errorf("delete operation failed")
	}
	print("key deleted \n" + err.Error())

	/* immulate RPC call by directly calling */

}
