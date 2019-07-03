package main

//go:generate msgp

import (
	"bytes"
	"encoding/json"
	"event"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/tinylib/msgp/msgp" // TODO: consider using flatbuffers
)

var e event.Event

func init() {
	event.OpenDB("event.data")
}

func main() {
	http.HandleFunc("/", HandleEvent)
	// fs := http.FileServer(http.Dir("static/"))
	// http.Handle("/static/", http.StripPrefix("/static/", fs))
	fmt.Println("RS X Event Store listening on :9090")
	// TODO: NextNumber
	http.ListenAndServe(":9090", nil)
}

// HandleEvent will either update or recall a particular quote or policy number
func HandleEvent(w http.ResponseWriter, r *http.Request) {
	u, err := r.URL.Parse("") // e.g. Path e.g. /quote/12
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	elem := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(elem) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	qp, n := elem[0], elem[1]
	if _, err = strconv.Atoi(n); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var b string
	switch qp {
	case "quote":
		b = fmt.Sprintf("Q%s", n)
	case "policy":
		b = fmt.Sprintf("P%s", n)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch r.Method {
	case "GET":
		HandleRecall(w, r, b)
	case "POST", "PUT":
		HandleUpdate(w, r, b)
	default:
		http.Error(w, "unsupported method", http.StatusBadRequest)
	}
}

// HandleUpdate saves body data to the event store
func HandleUpdate(w http.ResponseWriter, r *http.Request, bucket string) {
	w.Header().Set("Server", "An Event Store Server")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var v event.Map
	switch r.Header.Get("Content-Type") {
	case "application/json":
		err = json.Unmarshal(body, &v)
	case "application/msgpack":
		dc := msgp.NewReader(bytes.NewBuffer(body))
		err = v.DecodeMsg(dc)
	default:
		err = fmt.Errorf("Content-Type %s not supported", r.Header.Get("Content-Type"))
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	e := &event.Event{Type: event.JSON, Value: v}
	err = e.Update(bucket, nil)
	if err != nil {
		log.Printf("Update error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(200)
}

// HandleRecall handles Current State recall requests
func HandleRecall(w http.ResponseWriter, r *http.Request, bucket string) {
	state, err := event.CurrentState(bucket, false)
	if err != nil {
		if strings.HasPrefix(err.Error(), "not found") {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			w.Write([]byte(fmt.Sprintf("fail: CurrentState %v", err)))
			return
		}
	}
	var buf []byte
	if strings.Contains(r.Header.Get("Accept"), "msgpack") {
		w.Header().Set("Content-Type", "application/msgpack")
		en := msgp.NewWriter(bytes.NewBuffer(buf))
		err := event.Map(state).EncodeMsg(en) // generated type 'Event' encoder
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		en.Flush()
	} else {
		w.Header().Set("Content-Type", "application/json")
		buf, err = json.Marshal(state)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	w.Write(buf)
}

