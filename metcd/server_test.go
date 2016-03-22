package metcd

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/meshconn"
)

func ExampleUsage() {
	var (
		apiListen    = ":8080"
		meshListen   = ":6379"
		peerName     = "00:00:00:00:00:01"
		nickname     = "node_a"
		password     = ""
		channel      = "my-demo-metcd"
		minPeerCount = 3
		initialPeers = []string{"host2:6379", "host3:6379", "host4:6379", "host5:6379"}
	)

	logger := log.New(os.Stderr, nickname+"> ", log.LstdFlags)

	host, portStr, err := net.SplitHostPort(meshListen)
	if err != nil {
		logger.Fatalf("mesh address: %s: %v", meshListen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatalf("mesh address: %s: %v", meshListen, err)
	}

	name, err := mesh.PeerNameFromString(peerName)
	if err != nil {
		logger.Fatalf("%s: %v", peerName, err)
	}

	ln, err := net.Listen("tcp", apiListen)
	if err != nil {
		logger.Fatalf("binding gRPC listener: %v", err)
		return
	}

	logger.Printf("hello!")
	defer logger.Printf("goodbye!")

	log.SetOutput(ioutil.Discard) // squelch mesh logging
	router := mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		Password:           []byte(password),
		ConnLimit:          64,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, nickname, mesh.NullOverlay{})

	// Create a meshconn.Peer and connect it to a channel.
	peer := meshconn.NewPeer(name, router.Ourself.UID, logger)
	gossip := router.NewGossip(channel, peer)
	peer.Register(gossip)

	// Start the router and join the mesh.
	logger.Printf("mesh router starting (%s)", meshListen)
	router.Start()
	defer router.Stop()

	// Initiate connections and start the server.
	router.ConnectionMaker.InitiateConnections(initialPeers, true)
	err = NewServer(router, peer, minPeerCount, logger).Serve(ln)
	logger.Print(err)
}
