package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func createServer(endPoint string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(endPoint))
		}
	}))
}

func TestLoadBalancer(t *testing.T) {
	server1 := createServer("server1")
	defer server1.Close()

	server2 := createServer("server2")
	defer server2.Close()

	serversPool = []string{server1.Listener.Addr().String(), server2.Listener.Addr().String()}
	healthyServers = []bool{true, true}

	lb := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		urlHash := hash(r.URL.Path)
		serverIndex := urlHash % uint32(len(serversPool))
		serverIndex = checkServerAvailability(serverIndex)

		if serverIndex >= uint32(len(serversPool)) {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		err := forward(serversPool[serverIndex], rw, r)
		if err != nil {
			t.Errorf("Failed to forward request: %s", err)
		}
	}))
	defer lb.Close()

	resp, err := http.Get(lb.URL + "/test")
	if err != nil {
		t.Fatalf("Failed to perform request to load balancer: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, but got %v", resp.StatusCode)
	}
}

func TestScheme(t *testing.T) {
	tests := []struct {
		https       bool
		expectedScheme string
	}{
		{https: false, expectedScheme: "http"},
		{https: true, expectedScheme: "https"},
	}

	for _, test := range tests {
		*https = test.https
		if result := scheme(); result != test.expectedScheme {
			t.Errorf("Expected scheme will be %v, but it is %v", test.expectedScheme, result,)
		}
	}
}

func TestHash(t *testing.T) {
	tests := []struct {
		path string
		expected uint32
	}{
		{"test", 2676412545},
		{"another-test", 3365400561},
	}

	for _, test := range tests {
		if result := hash(test.path); result != test.expected {
			t.Errorf("Expected hash(%q) will be %v, but it is %v", test.path, test.expected, result)
		}
	}
}