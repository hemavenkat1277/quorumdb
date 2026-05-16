package cluster

import (
	"crypto/sha1"
	"encoding/binary"
	"sort"
	"strconv"
)

const DefaultVirtualNodes = 64

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type Ring struct {
	virtualNodes int
	tokens       []uint32
	owners       map[uint32]Node
	nodes        map[string]Node
}

func NewRing(nodes []Node, virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	r := &Ring{
		virtualNodes: virtualNodes,
		owners:       make(map[uint32]Node),
		nodes:        make(map[string]Node),
	}

	for _, node := range nodes {
		r.Add(node)
	}
	return r
}

func (r *Ring) Add(node Node) {
	if node.ID == "" || node.Address == "" {
		return
	}
	if _, exists := r.nodes[node.ID]; exists {
		return
	}

	r.nodes[node.ID] = node
	for i := 0; i < r.virtualNodes; i++ {
		token := hashKey(node.ID + "#" + strconv.Itoa(i))
		r.tokens = append(r.tokens, token)
		r.owners[token] = node
	}
	sort.Slice(r.tokens, func(i, j int) bool { return r.tokens[i] < r.tokens[j] })
}

func (r *Ring) ReplicasFor(key string, replicationFactor int, alive map[string]bool) []Node {
	if len(r.tokens) == 0 || replicationFactor <= 0 {
		return nil
	}

	startToken := hashKey(key)
	start := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= startToken })
	if start == len(r.tokens) {
		start = 0
	}

	replicas := make([]Node, 0, replicationFactor)
	seen := make(map[string]bool)
	for scanned := 0; scanned < len(r.tokens) && len(replicas) < replicationFactor; scanned++ {
		token := r.tokens[(start+scanned)%len(r.tokens)]
		node := r.owners[token]
		if seen[node.ID] {
			continue
		}
		if alive != nil && !alive[node.ID] {
			continue
		}
		seen[node.ID] = true
		replicas = append(replicas, node)
	}

	return replicas
}

func hashKey(value string) uint32 {
	sum := sha1.Sum([]byte(value))
	return binary.BigEndian.Uint32(sum[:4])
}
