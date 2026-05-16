package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"quorumdb/internal/cluster"
	"quorumdb/internal/store"
)

const (
	ReplicationFactor = 3
	WriteQuorum       = 2
	ReadQuorum        = 2
)

type Server struct {
	self       cluster.Node
	allNodes   []cluster.Node
	ring       *cluster.Ring
	membership *cluster.Membership
	store      *store.Store
	client     *http.Client
	logger     *log.Logger
}

type putRequest struct {
	Value string `json:"value"`
}

type writeRequest struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version int64  `json:"version"`
}

type readResponse struct {
	Found  bool         `json:"found"`
	Record store.Record `json:"record,omitempty"`
}

type quorumResponse struct {
	Key          string        `json:"key"`
	Value        string        `json:"value,omitempty"`
	Version      int64         `json:"version,omitempty"`
	Replicas     []string      `json:"replicas"`
	Acknowledged int           `json:"acknowledged"`
	Latency      time.Duration `json:"latency"`
}

func New(self cluster.Node, allNodes []cluster.Node, logger *log.Logger) *Server {
	return &Server{
		self:       self,
		allNodes:   allNodes,
		ring:       cluster.NewRing(allNodes, cluster.DefaultVirtualNodes),
		membership: cluster.NewMembership(self, allNodes, 2*time.Second),
		store:      store.New(),
		client: &http.Client{
			Timeout: 900 * time.Millisecond,
		},
		logger: logger,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/", s.handlePut)
	mux.HandleFunc("GET /kv/", s.handleGet)
	mux.HandleFunc("POST /internal/write", s.handleInternalWrite)
	mux.HandleFunc("GET /internal/read/", s.handleInternalRead)
	mux.HandleFunc("POST /internal/gossip", s.handleGossip)
	mux.HandleFunc("GET /members", s.handleMembers)
	mux.HandleFunc("GET /health", s.handleHealth)
	return requestLogger(s.logger, mux)
}

func (s *Server) StartGossip(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.membership.DetectFailures()
			if peer, ok := s.membership.RandomPeer(); ok {
				go s.gossipWith(peer)
			}
		}
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	version := time.Now().UnixNano()
	replicas := s.replicasFor(key)
	if len(replicas) < WriteQuorum {
		writeError(w, http.StatusServiceUnavailable, "not enough live replicas for write quorum")
		return
	}

	acks := s.writeToReplicas(r.Context(), replicas, writeRequest{
		Key:     key,
		Value:   req.Value,
		Version: version,
	})
	if acks < WriteQuorum {
		writeError(w, http.StatusServiceUnavailable, "write quorum failed")
		return
	}

	writeJSON(w, http.StatusOK, quorumResponse{
		Key:          key,
		Value:        req.Value,
		Version:      version,
		Replicas:     nodeIDs(replicas),
		Acknowledged: acks,
		Latency:      time.Since(start),
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	replicas := s.replicasFor(key)
	if len(replicas) < ReadQuorum {
		writeError(w, http.StatusServiceUnavailable, "not enough live replicas for read quorum")
		return
	}

	responses := s.readFromReplicas(r.Context(), replicas, key)
	if len(responses) < ReadQuorum {
		writeError(w, http.StatusServiceUnavailable, "read quorum failed")
		return
	}

	latest, found := latestRecord(responses)
	if !found {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	writeJSON(w, http.StatusOK, quorumResponse{
		Key:          latest.Key,
		Value:        latest.Value,
		Version:      latest.Version,
		Replicas:     nodeIDs(replicas),
		Acknowledged: len(responses),
		Latency:      time.Since(start),
	})
}

func (s *Server) handleInternalWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	record := s.store.Put(req.Key, req.Value, req.Version)
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) handleInternalRead(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/read/")
	record, err := s.store.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusOK, readResponse{Found: false})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, readResponse{Found: true, Record: record})
}

func (s *Server) handleGossip(w http.ResponseWriter, r *http.Request) {
	var remote []cluster.MemberStatus
	if err := json.NewDecoder(r.Body).Decode(&remote); err != nil {
		writeError(w, http.StatusBadRequest, "invalid gossip payload")
		return
	}
	s.membership.Merge(remote)
	s.membership.MarkAlive(s.self)
	writeJSON(w, http.StatusOK, s.membership.Snapshot())
}

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.membership.Snapshot())
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      s.self.ID,
		"address": s.self.Address,
		"keys":    s.store.Len(),
		"status":  "ok",
	})
}

func (s *Server) replicasFor(key string) []cluster.Node {
	return s.ring.ReplicasFor(key, ReplicationFactor, s.membership.AliveMap())
}

func (s *Server) writeToReplicas(ctx context.Context, replicas []cluster.Node, req writeRequest) int {
	var wg sync.WaitGroup
	results := make(chan bool, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(node cluster.Node) {
			defer wg.Done()
			if node.ID == s.self.ID {
				s.store.Put(req.Key, req.Value, req.Version)
				results <- true
				return
			}
			results <- s.postJSON(ctx, node, "/internal/write", req, nil) == nil
		}(replica)
	}

	wg.Wait()
	close(results)

	acks := 0
	for ok := range results {
		if ok {
			acks++
		}
	}
	return acks
}

func (s *Server) readFromReplicas(ctx context.Context, replicas []cluster.Node, key string) []readResponse {
	var wg sync.WaitGroup
	results := make(chan readResponse, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(node cluster.Node) {
			defer wg.Done()
			if node.ID == s.self.ID {
				record, err := s.store.Get(key)
				if err == nil {
					results <- readResponse{Found: true, Record: record}
				} else {
					results <- readResponse{Found: false}
				}
				return
			}

			var response readResponse
			if err := s.getJSON(ctx, node, "/internal/read/"+key, &response); err == nil {
				results <- response
			}
		}(replica)
	}

	wg.Wait()
	close(results)

	out := make([]readResponse, 0, len(replicas))
	for response := range results {
		out = append(out, response)
	}
	return out
}

func (s *Server) gossipWith(peer cluster.Node) {
	var response []cluster.MemberStatus
	err := s.postJSON(context.Background(), peer, "/internal/gossip", s.membership.Snapshot(), &response)
	if err != nil {
		return
	}
	s.membership.MarkAlive(peer)
	s.membership.Merge(response)
}

func (s *Server) postJSON(ctx context.Context, node cluster.Node, path string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+node.Address+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("node %s returned %s", node.ID, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (s *Server) getJSON(ctx context.Context, node cluster.Node, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+node.Address+path, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("node %s returned %s", node.ID, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func latestRecord(responses []readResponse) (store.Record, bool) {
	var latest store.Record
	found := false
	for _, response := range responses {
		if !response.Found {
			continue
		}
		if !found || response.Record.Version > latest.Version {
			latest = response.Record
			found = true
		}
	}
	return latest, found
}

func nodeIDs(nodes []cluster.Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func requestLogger(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Microsecond))
	})
}
