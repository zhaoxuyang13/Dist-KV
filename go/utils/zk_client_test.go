package utils
 
import (
	"testing"
)

func Test_ZkClient(t *testing.T) {
	// 服务器地址列表
	servers := []string{"0.0.0.0:2181", "0.0.0.0:2182", "0.0.0.0:2183"}
	client, err := NewClient(servers, "/api", 10)
	if err != nil {
		t.Errorf("new client failed");
	}
	defer client.Close()
	node1 := &ServiceNode{"user", "127.0.0.1", 4000}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002}
	if err := client.Register(node1); err != nil {
		t.Errorf("register failed");
	}
	if err := client.Register(node2); err != nil {
		t.Errorf("register failed");
	}
	if err := client.Register(node3); err != nil {
		t.Errorf("register failed");
	}
	nodes, err := client.GetNodes("user")
	if err != nil {
		t.Errorf("get node failed");
	}
	if nodes[0].Port < 4000  {
		t.Errorf("node info incorrect %v",nodes[0].Port);
	}
}

