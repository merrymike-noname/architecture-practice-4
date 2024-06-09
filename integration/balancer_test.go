package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

const name = "lasthope"

type ResponseBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	testCases := []struct {
		endpoint string
		expected int
	}{
		{"/api/v1/some-data", 0},
		{"/api/v1/some-data2", 1},
		{"/api/v1/some-data", 2},
	}

	servers := make([]string, len(testCases))

	for i, tc := range testCases {
		url := fmt.Sprintf("%s%s", baseAddress, tc.endpoint)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Request to %s failed: %v", url, err)
		}
		defer resp.Body.Close()

		server := resp.Header.Get("lb-from")
		if server == "" {
			t.Fatalf("Missing 'lb-from' header in response for request %d", i)
		}
		servers[i] = server
	}

	if servers[0] != servers[2] {
		t.Errorf("Requests to the same endpoint were routed to different servers: got %s and %s", servers[0], servers[2])
	}

	db, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, name))
	if err != nil {
		t.Error(err)
	}
	defer db.Body.Close()

	var body ResponseBody
	err = json.NewDecoder(db.Body).Decode(&body)
	if err != nil {
		t.Error(err)
	}

	if body.Key != name {
		t.Errorf("Expected key to be '%s', got '%s'", name, body.Key)
	}
	if body.Value == "" {
		t.Error("Expected non-empty value")
	}
}

func BenchmarkBalancer(b *testing.B) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		b.Skip("Integration test is not enabled")
	}

	url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("Request to %s failed: %v", url, err)
		}
		defer resp.Body.Close()
	}
}
