package consistentHash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var ErrNodeNotFound = errors.New("node not found")

/*

	Ring

*/
// Initializes new distribute network of nodes or a ring.
func NewRing() *Ring{
	return &Ring{Nodes : Nodes{}}
}
// Adds node to the ring.
func (r *Ring) AddNode(id string) {
	r.Lock()
	defer r.Unlock()

	node := NewNode(id)
	r.Nodes = append(r.Nodes, *node)

	sort.Sort(r.Nodes)
}

// Removes node from the ring if it exists, else returns
// ErrNodeNotFound.
func (r *Ring) RemoveNode(id string)  error {
	r.Lock()
	defer r.Unlock()

	i := r.search(id)
	if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
		return ErrNodeNotFound
	}

	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

	return nil
}

// Gets node which is mapped to the key. Return value is identifer
// of the node given in `AddNode`.
func (r *Ring) Get(id string) string {
	i := r.search(id)
	if i >= r.Nodes.Len() {
		i = 0
	}
	return r.Nodes[i].Id
}
func (r *Ring) search(id string) int {
	searchfn := func(i int) bool {
		return r.Nodes[i].HashId >= hashId(id)
	}

	return sort.Search(r.Nodes.Len(), searchfn)
}
// Ring is a network of distributed nodes.
type Ring struct {
	Nodes Nodes
	sync.Mutex
}
/*

	Node

*/
func NewNode(id string) *Node {
	return &Node{
		Id:     id,
		HashId: hashId(id),
	}
}
func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Less(i, j int) bool { return n[i].HashId < n[j].HashId }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

// Nodes is an array of nodes.
type Nodes []Node
// Node is a single entity in a ring.
// Node's Id should be of syntax "ip:port"
type Node struct {
	Id     string
	HashId uint32
}
func (node *Node) GetAddr() (string,int,error){
	res := strings.Split(node.Id, ":")
	ip := res[0]
	port,err := strconv.Atoi(res[1])

	return ip,port,err
}
//----------------------------------------------------------
// Helpers
//----------------------------------------------------------

func hashId(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}