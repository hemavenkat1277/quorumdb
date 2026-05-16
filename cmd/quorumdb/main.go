package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quorumdb/internal/cluster"
	"quorumdb/internal/server"
)

func main() {
	nodeID := flag.String("node", "", "node id to run, for example node1")
	configPath := flag.String("config", "configs/cluster.json", "cluster config path")
	flag.Parse()

	nodes, err := loadNodes(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	self, err := findNode(nodes, *nodeID)
	if err != nil {
		log.Fatal(err)
	}

	logger := log.New(os.Stdout, "["+self.ID+"] ", log.LstdFlags|log.Lmicroseconds)
	qdb := server.New(self, nodes, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go qdb.StartGossip(ctx)

	httpServer := &http.Server{
		Addr:              self.Address,
		Handler:           qdb.Handler(),
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	logger.Printf("QuorumDB node listening on http://%s", self.Address)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("server error: %v", err)
	}
}

func loadNodes(path string) ([]cluster.Node, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var nodes []cluster.Node
	if err := json.NewDecoder(file).Decode(&nodes); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.New("cluster config has no nodes")
	}
	return nodes, nil
}

func findNode(nodes []cluster.Node, id string) (cluster.Node, error) {
	for _, node := range nodes {
		if node.ID == id {
			return node, nil
		}
	}
	return cluster.Node{}, fmt.Errorf("node id %q not found in config", id)
}
