package cluster

import "testing"

func TestReplicasForReturnsDistinctAliveNodes(t *testing.T) {
	nodes := []Node{
		{ID: "node1", Address: "127.0.0.1:8001"},
		{ID: "node2", Address: "127.0.0.1:8002"},
		{ID: "node3", Address: "127.0.0.1:8003"},
		{ID: "node4", Address: "127.0.0.1:8004"},
		{ID: "node5", Address: "127.0.0.1:8005"},
	}
	ring := NewRing(nodes, 8)

	alive := map[string]bool{
		"node1": true,
		"node2": true,
		"node3": false,
		"node4": true,
		"node5": true,
	}
	replicas := ring.ReplicasFor("student:42", 3, alive)

	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}
	seen := make(map[string]bool)
	for _, replica := range replicas {
		if !alive[replica.ID] {
			t.Fatalf("replica %s is not alive", replica.ID)
		}
		if seen[replica.ID] {
			t.Fatalf("duplicate replica %s", replica.ID)
		}
		seen[replica.ID] = true
	}
}
