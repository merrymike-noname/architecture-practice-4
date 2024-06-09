package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/merrymike-noname/architecture-practice-4/httptools"
	"github.com/merrymike-noname/architecture-practice-4/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const url = "http://db:8083/db"

type ResponseBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RequestBody struct {
	Value string `json:"value"`
}

func main() {
	h := new(http.ServeMux)

	client := http.DefaultClient

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		resp, err := client.Get(fmt.Sprintf("%s/%s", url, key))
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		statusOk := resp.StatusCode >= 200 && resp.StatusCode < 300

		if !statusOk {
			rw.WriteHeader(resp.StatusCode)
			return
		}

		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		var responseBody ResponseBody
		json.NewDecoder(resp.Body).Decode(&responseBody)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode(responseBody)

		defer resp.Body.Close()
	})

	h.HandleFunc("/api/v1/some-data1", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"1", "2",
		})
	})

	h.HandleFunc("/api/v1/some-data2", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"2", "3",
		})
	})

	h.HandleFunc("/api/v1/some-data3", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"3", "4",
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()

	buff := new(bytes.Buffer)
	body := RequestBody{Value: time.Now().Format(time.RFC3339)}
	json.NewEncoder(buff).Encode(body)

	res, _ := client.Post(fmt.Sprintf("%s/lasthope", url), "application/json", buff)
	defer res.Body.Close()

	signal.WaitForTerminationSignal()
}
