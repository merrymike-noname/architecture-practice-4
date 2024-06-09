package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"github.com/merrymike-noname/architecture-practice-4/datastore"
	"github.com/merrymike-noname/architecture-practice-4/httptools"
	"github.com/merrymike-noname/architecture-practice-4/signal"
)

var port = flag.Int("port", 8083, "server port")

type ResponseBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RequestBody struct {
	Value string `json:"value"`
}

func getReq (Db *datastore.Db, rw http.ResponseWriter, key string) {
	value, err := Db.Get(key)

	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	rw.WriteHeader(http.StatusOK)
	rw.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(rw).Encode(ResponseBody{
    Key: key, Value: value,
  }); err != nil {
    log.Println("Error encoding response: ", err)
  }
}

func postReq(Db *datastore.Db, rw http.ResponseWriter, req *http.Request, key string) {
	var body RequestBody

	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	err = Db.Put(key, body.Value)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusCreated)
}

func main() {
	flag.Parse()

	h := new(http.ServeMux)
	dir, err := ioutil.TempDir("", "temp-dir")

	if err != nil {
		log.Fatal(err)
	}

	Db, err := datastore.NewDb(dir, 250)

	if err != nil {
		log.Fatal(err)
	}

	defer Db.Close()

	h.HandleFunc("/db/", func(rw http.ResponseWriter, req *http.Request) {
		url := req.URL.String()
		key := url[4:]

		switch req.Method {
		case "GET":
			getReq(Db, rw, key)
		case "POST":
			postReq(Db, rw, req, key)
		default:
			rw.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}