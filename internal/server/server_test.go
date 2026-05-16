package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"quorumdb/internal/cluster"
	"quorumdb/internal/server"
)

func TestQuorumWriteAndReadAcrossNodes(t *testing.T) {
	nodes, shutdown := startTestCluster(t)
	defer shutdown()

	body := bytes.NewBufferString(`{"value":"final-year-cse"}`)
	req, err := http.NewRequest(http.MethodPut, "http://"+nodes[0].Address+"/kv/student", body)
	if err != nil {
		t.Fatalf("create put request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put through node1: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("put status %d: %s", resp.StatusCode, payload)
	}

	getResp, err := http.Get("http://" + nodes[3].Address + "/kv/student")
	if err != nil {
		t.Fatalf("get through node4: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(getResp.Body)
		t.Fatalf("get status %d: %s", getResp.StatusCode, payload)
	}

	var payload struct {
		Value        string   `json:"value"`
		Acknowledged int      `json:"acknowledged"`
		Replicas     []string `json:"replicas"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Value != "final-year-cse" {
		t.Fatalf("expected replicated value, got %q", payload.Value)
	}
	if payload.Acknowledged < server.ReadQuorum {
		t.Fatalf("expected read quorum, got %d", payload.Acknowledged)
	}
	if len(payload.Replicas) != server.ReplicationFactor {
		t.Fatalf("expected %d replicas, got %d", server.ReplicationFactor, len(payload.Replicas))
	}
}

func startTestCluster(t *testing.T) ([]cluster.Node, func()) {
	t.Helper()

	listeners := make([]net.Listener, 5)
	nodes := make([]cluster.Node, 5)
	for i := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = ln
		nodes[i] = cluster.Node{
			ID:      "node" + string(rune('1'+i)),
			Address: ln.Addr().String(),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	httpServers := make([]*http.Server, 0, len(nodes))
	for i, node := range nodes {
		qdb := server.New(node, nodes, log.New(io.Discard, "", 0))
		go qdb.StartGossip(ctx)

		httpServer := &http.Server{
			Handler:           qdb.Handler(),
			ReadHeaderTimeout: time.Second,
		}
		httpServers = append(httpServers, httpServer)
		go func(ln net.Listener) {
			_ = httpServer.Serve(ln)
		}(listeners[i])
	}

	shutdown := func() {
		cancel()
		for _, srv := range httpServers {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
			_ = srv.Shutdown(shutdownCtx)
			shutdownCancel()
		}
	}
	return nodes, shutdown
}
