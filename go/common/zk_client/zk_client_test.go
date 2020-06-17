package zk_client
 
import (
	"fmt"
	"testing"
	"sync"
)

func Test_ZkClient(t *testing.T) {
	// 服务器地址列表
	servers := []string{"0.0.0.0:2181", "0.0.0.0:2182", "0.0.0.0:2183"}
	client, err := NewClient(servers, "/api", 10)
	if err != nil {
		t.Errorf("new client failed");
	}
	defer client.Close()
	node1 := &ServiceNode{"user", "127.0.0.1", 4000,"slave1"}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001,"slave2"}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002,"slave3"}
	if _,err := client.Register(node1); err != nil {
		t.Errorf("register failed")
	}
	if _,err := client.Register(node2); err != nil {
		t.Errorf("register failed")
	}
	if _,err := client.Register(node3); err != nil {
		t.Errorf("register failed")
	}
	nodes, err := client.GetNodes("user")
	if err != nil {
		t.Errorf("get node failed")
	}
	if nodes[0].Port < 4000  {
		t.Errorf("node info incorrect %v",nodes[0].Port)
	}
}

/* only one of applicant will get the primary */
func Test_Racing(t *testing.T){
	// 服务器地址列表
	servers := []string{"0.0.0.0:2181", "0.0.0.0:2182", "0.0.0.0:2183"}
	client, err := NewClient(servers, "/api", 10)
	if err != nil {
		t.Errorf("new client failed");
	}
	defer client.Close()
	node1 := &ServiceNode{"user", "127.0.0.1", 4000,"slave1"}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001,"slave2"}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002,"slave3"}
	if err := client.TryPrimary(node1); err != nil {
		t.Errorf("register failed")
	}
	if err := client.TryPrimary(node2); err == nil {
		t.Errorf("the second should fail")
	}else {
		fmt.Println(err.Error())
	}

	if err := client.TryPrimary(node3); err == nil {
		t.Errorf("the third should fail")
	}else {
		fmt.Println(err.Error())
	}

	nodes, err := client.GetNodes("user")
	if err != nil {
		t.Errorf("get node failed")
	}
	if len(nodes) != 1 || nodes[0].Port != 4000  {
		t.Errorf("nodes info incorrect %v",nodes)
	}
}

/* only one of applicant will get the primary */
func Test_RacingConcurrent(t *testing.T){
	// 服务器地址列表
	servers := []string{"0.0.0.0:2181", "0.0.0.0:2182", "0.0.0.0:2183"}
	client, err := NewClient(servers, "/api", 10)
	if err != nil {
		t.Errorf("new client failed");
	}
	defer client.Close()
	node1 := &ServiceNode{"user", "127.0.0.1", 4000,"slave1"}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001,"slave2"}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002,"slave3"}
	wg := sync.WaitGroup{}
	wg.Add(3)
	errorCounter := 0
	go func() {
		if err := client.TryPrimary(node1); err != nil {
			errorCounter ++
		}
		wg.Done()
	}()
	go func() {
		if err := client.TryPrimary(node2); err != nil {
			errorCounter ++
		}
		wg.Done()
	}()
	go func() {
		if err := client.TryPrimary(node3); err != nil {
			errorCounter ++
		}
		wg.Done()
	}()
	wg.Wait()
	if errorCounter != 2{
		t.Error(errorCounter)
	}
	nodes, err := client.GetNodes("user")
	if err != nil {
		t.Errorf("get node failed")
	}
	if len(nodes) != 1  {
		fmt.Printf("nodes num incorrect. %d.\n",len(nodes))
		t.Errorf("incorrect")
	}
}