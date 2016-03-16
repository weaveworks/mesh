package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/bernerdschaefer/eventsource"
	"github.com/gorilla/mux"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

type store interface {
	get(key string) (string, error)
	watch(key string, results chan<- string) (cancel chan<- struct{}, err error)
	set(key, value string) error
}

func handle(logger *log.Logger, router *mesh.Router, peer *meshconn.Peer, s store) http.Handler {
	r := mux.NewRouter()
	r.Methods("GET").Path(`/status`).HandlerFunc(handleGetStatus(router, logger))
	r.Methods("GET").Path(`/get/{key}`).HandlerFunc(handleGetKey(s, logger))
	r.Methods("GET").Path(`/watch/{key:.+}`).HandlerFunc(handleWatchKey(s, logger))
	r.Methods("POST").Path("/set/{key:.+}/{val}").HandlerFunc(handleSetKey(s, logger))
	r.Methods("POST").Path("/raw").HandlerFunc(handlePostRaw(peer, logger))
	return r
}

func handleGetStatus(router *mesh.Router, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var n int
		for _, description := range router.Peers.Descriptions() {
			fmt.Fprintf(
				w,
				"%s (%s) Self=%v NumConnections=%d\n",
				description.Name.String(),
				description.NickName,
				description.Self,
				description.NumConnections,
			)
			n++
		}
		logger.Printf("%s %s %s: %d peer(s)", r.RemoteAddr, r.Method, r.RequestURI, n)
	}
}

func handlePostRaw(peer *meshconn.Peer, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dstStr := r.FormValue("dst")
		if dstStr == "" {
			http.Error(w, "no dst specified", http.StatusBadRequest)
			return
		}
		dst, err := mesh.PeerNameFromUserInput(dstStr)
		if err != nil {
			http.Error(w, "dst: "+err.Error(), http.StatusBadRequest)
			return
		}
		msg := r.FormValue("msg")
		if _, err := peer.WriteTo([]byte(msg), meshconn.MeshAddr{PeerName: dst}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Printf("%s %s %s: %d to %s OK (%s)", r.RemoteAddr, r.Method, r.RequestURI, len(msg), dst, msg)
		fmt.Fprintf(w, "%s: %q (%d) OK\n", dst, msg, len(msg))
	}
}

func handleWatchKey(s store, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		eventsource.Handler(func(lastID string, enc *eventsource.Encoder, stop <-chan bool) {
			key := mux.Vars(r)["key"]
			logger.Printf("handler: %s watch %q connected", r.RemoteAddr, key)
			defer logger.Printf("handler: %s watch %q disconnected", r.RemoteAddr, key)

			results := make(chan string)
			cancel, err := s.watch(key, results)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer close(cancel)

			var id int

			val, err := s.get(key)
			if err == nil {
				enc.Encode(eventsource.Event{
					Type: "initial",
					ID:   strconv.Itoa(id),
					Data: []byte(val),
				})
			} else {
				enc.Encode(eventsource.Event{
					Type: "initial",
					ID:   strconv.Itoa(id),
					Data: []byte(err.Error()),
				})

			}
			id++

			for {
				select {
				case val := <-results:
					enc.Encode(eventsource.Event{
						Type: "update",
						ID:   strconv.Itoa(id),
						Data: []byte(val),
					})
					id++

				case <-stop:
					return
				}
			}
		}).ServeHTTP(w, r)
	}
}

func handleGetKey(s store, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		val, err := s.get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "%q = %q\n", key, val)
	}
}

func handleSetKey(s store, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, val := mux.Vars(r)["key"], mux.Vars(r)["val"]
		if err := s.set(key, val); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%q = %q proposed\n", key, val)
	}
}
