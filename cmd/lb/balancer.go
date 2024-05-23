package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	m              sync.Mutex
	healthyServers = make([]bool, len(serversPool))
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func hash(path string) uint32 {
	h := sha256.New()
	h.Write([]byte(path))
	return binary.BigEndian.Uint32(h.Sum(nil))
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func checkServerAvailability(serverIndex uint32) uint32 {
	m.Lock()
	defer m.Unlock()

	if !healthyServers[serverIndex] {
		allDown := true
		for i := uint32(1); i < uint32(len(healthyServers)); i++ {
			nextIndex := (serverIndex + i) % uint32(len(healthyServers))
			if healthyServers[nextIndex] {
				serverIndex = nextIndex
				allDown = false
				break
			}
		}
		if allDown {
			serverIndex = uint32(len(serversPool) + 1)
		}
	}
	return serverIndex
}

func updateHealthServersList() {
	for i, server := range serversPool {
		server := server
		i := i
		go func() {
			for range time.Tick(10 * time.Second) {
				m.Lock()
				healthyServers[i] = health(server)
				m.Unlock()
			}
		}()
	}
}

func main() {
	flag.Parse()
	updateHealthServersList()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		urlHash := hash(r.URL.Path)
		serverIndex := urlHash % uint32(len(serversPool))
		serverIndex = checkServerAvailability(serverIndex)

		if serverIndex > uint32(len(serversPool)) {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		//fmt.Println(serverIndex, urlHash)

		err := forward(serversPool[serverIndex], rw, r)
		if err != nil {
			log.Printf("Failed to forward request: %s", err)
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
