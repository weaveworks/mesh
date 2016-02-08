package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

type store interface {
	get(key string) (string, error)
	watch(key string, results chan<- string) (cancel chan<- struct{}, err error)
	post(key, value string) error
}

func handle(logger *log.Logger, router *mesh.Router, peer *meshconn.Peer, s store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch key := r.URL.Path[1:]; len(key) {
		case 0:
			switch r.Method {
			case "GET":
				handleGetStatus(router, logger)(w, r)
			case "POST":
				handlePostRaw(peer, logger)(w, r)
			default:
				http.Error(w, "Unsupported method", http.StatusBadRequest)
			}
		default:
			switch r.Method {
			case "GET":
				handleGetKey(s, logger)(w, r)
			case "POST":
				handlePostKey(s, logger)(w, r)
			default:
				http.Error(w, "Unsupported method", http.StatusBadRequest)
			}
		}

	}
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

func handleGetKey(s store, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[1:]
		if key == "" {
			http.Error(w, "no key specified", http.StatusBadRequest)
			return
		}
		value, err := s.get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "%q = %q\n", key, value)
	}
}

func handlePostKey(s store, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[1:]
		if key == "" {
			http.Error(w, "no key specified", http.StatusBadRequest)
			return
		}
		value := r.FormValue("value")
		if value == "" {
			http.Error(w, "no value specified", http.StatusBadRequest)
			return
		}
		if err := s.post(key, value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%q = %q proposed\n", key, value)
	}
}
