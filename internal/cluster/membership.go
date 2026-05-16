package cluster

import (
	"math/rand"
	"sync"
	"time"
)

type MemberStatus struct {
	Node     Node      `json:"node"`
	Alive    bool      `json:"alive"`
	LastSeen time.Time `json:"last_seen"`
}

type Membership struct {
	mu             sync.RWMutex
	selfID         string
	failureTimeout time.Duration
	members        map[string]MemberStatus
}

func NewMembership(self Node, nodes []Node, failureTimeout time.Duration) *Membership {
	now := time.Now()
	m := &Membership{
		selfID:         self.ID,
		failureTimeout: failureTimeout,
		members:        make(map[string]MemberStatus, len(nodes)),
	}
	for _, node := range nodes {
		m.members[node.ID] = MemberStatus{
			Node:     node,
			Alive:    true,
			LastSeen: now,
		}
	}
	m.members[self.ID] = MemberStatus{
		Node:     self,
		Alive:    true,
		LastSeen: now,
	}
	return m
}

func (m *Membership) MarkAlive(node Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.members[node.ID] = MemberStatus{
		Node:     node,
		Alive:    true,
		LastSeen: time.Now(),
	}
}

func (m *Membership) Merge(remote []MemberStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, incoming := range remote {
		current, exists := m.members[incoming.Node.ID]
		if !exists || incoming.LastSeen.After(current.LastSeen) {
			m.members[incoming.Node.ID] = incoming
		}
	}
}

func (m *Membership) DetectFailures() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, status := range m.members {
		if id == m.selfID {
			status.Alive = true
			status.LastSeen = now
			m.members[id] = status
			continue
		}
		if now.Sub(status.LastSeen) > m.failureTimeout {
			status.Alive = false
			m.members[id] = status
		}
	}
}

func (m *Membership) Snapshot() []MemberStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]MemberStatus, 0, len(m.members))
	for _, status := range m.members {
		out = append(out, status)
	}
	return out
}

func (m *Membership) AliveMap() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]bool, len(m.members))
	for id, status := range m.members {
		out[id] = status.Alive
	}
	return out
}

func (m *Membership) RandomPeer() (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]Node, 0, len(m.members)-1)
	for id, status := range m.members {
		if id == m.selfID || !status.Alive {
			continue
		}
		peers = append(peers, status.Node)
	}
	if len(peers) == 0 {
		return Node{}, false
	}
	return peers[rand.Intn(len(peers))], true
}
