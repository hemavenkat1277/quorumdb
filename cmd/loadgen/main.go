package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:8001", "QuorumDB node URL")
	requests := flag.Int("requests", 1000, "total number of requests")
	concurrency := flag.Int("concurrency", 50, "number of concurrent workers")
	readPercent := flag.Int("read-percent", 50, "percentage of GET requests")
	flag.Parse()

	if *requests <= 0 || *concurrency <= 0 {
		fmt.Println("requests and concurrency must be greater than zero")
		return
	}

	client := &http.Client{Timeout: 2 * time.Second}
	jobs := make(chan int)
	latencies := make([]time.Duration, 0, *requests)
	var latencyMu sync.Mutex
	var okCount atomic.Int64
	var failCount atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := range jobs {
				key := fmt.Sprintf("key-%d", rand.Intn(500))
				if n%100 >= *readPercent {
					key = fmt.Sprintf("key-%d", n)
				}

				requestStart := time.Now()
				err := runRequest(client, *baseURL, key, n%100 < *readPercent)
				elapsed := time.Since(requestStart)

				latencyMu.Lock()
				latencies = append(latencies, elapsed)
				latencyMu.Unlock()

				if err != nil {
					failCount.Add(1)
					continue
				}
				okCount.Add(1)
			}
		}(worker)
	}

	for i := 0; i < *requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("Requests:    %d\n", *requests)
	fmt.Printf("Successful:  %d\n", okCount.Load())
	fmt.Printf("Failed:      %d\n", failCount.Load())
	fmt.Printf("Duration:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput:  %.2f req/sec\n", float64(*requests)/elapsed.Seconds())
	fmt.Printf("Avg latency: %s\n", average(latencies).Round(time.Microsecond))
	fmt.Printf("P95 latency: %s\n", percentile(latencies, 95).Round(time.Microsecond))
}

func runRequest(client *http.Client, baseURL, key string, read bool) error {
	if read {
		resp, err := client.Get(baseURL + "/kv/" + key)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("GET returned %s", resp.Status)
	}

	payload, _ := json.Marshal(map[string]string{"value": fmt.Sprintf("value-%d", time.Now().UnixNano())})
	req, err := http.NewRequest(http.MethodPut, baseURL+"/kv/"+key, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT returned %s", resp.Status)
	}
	return nil
}

func average(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	var total time.Duration
	for _, value := range values {
		total += value
	}
	return total / time.Duration(len(values))
}

func percentile(values []time.Duration, p int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*p + 99) / 100
	if index <= 0 {
		index = 1
	}
	if index > len(values) {
		index = len(values)
	}
	return values[index-1]
}
