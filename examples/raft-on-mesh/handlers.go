package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

func handle(logger *log.Logger, router *mesh.Router, peer *meshconn.Peer, c *controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			for _, description := range router.Peers.Descriptions() {
				fmt.Fprintf(
					w,
					"%s (%s) Self=%v NumConnections=%d\n",
					description.Name.String(),
					description.NickName,
					description.Self,
					description.NumConnections,
				)
			}

		case "POST":
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
			logger.Printf("sent %d to %s: %s", len(msg), dst, msg)
			fmt.Fprintf(w, "%s: %q (%d) OK\n", dst, msg, len(msg))
		}
	}
}
