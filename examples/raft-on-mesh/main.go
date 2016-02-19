package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

func main() {
	peers := &stringset{}
	var (
		httpListen = flag.String("http", ":8080", "HTTP listen address")
		meshListen = flag.String("mesh", net.JoinHostPort("0.0.0.0", strconv.Itoa(mesh.Port)), "mesh listen address")
		hwaddr     = flag.String("hwaddr", mustHardwareAddr(), "MAC address, i.e. mesh peer ID")
		nickname   = flag.String("nickname", mustHostname(), "peer nickname")
		password   = flag.String("password", "", "password (optional)")
		channel    = flag.String("channel", "default", "gossip channel name")
	)
	flag.Var(peers, "peer", "initial peer (may be repeated)")
	flag.Parse()

	logger := log.New(os.Stderr, *nickname+"> ", log.LstdFlags)

	host, portStr, err := net.SplitHostPort(*meshListen)
	if err != nil {
		logger.Fatalf("mesh address: %s: %v", *meshListen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatalf("mesh address: %s: %v", *meshListen, err)
	}

	name, err := mesh.PeerNameFromString(*hwaddr)
	if err != nil {
		logger.Fatalf("%s: %v", *hwaddr, err)
	}

	// Create, but do not start, a router.
	router := mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		Password:           []byte(*password),
		ConnLimit:          64,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, *nickname, mesh.NullOverlay{})

	// Create a meshconn.Peer.
	peer := meshconn.NewPeer(name, logger)
	defer func() {
		logger.Printf("peer closing")
		peer.Close()
	}()
	gossip := router.NewGossip(*channel, peer)
	peer.Register(gossip)

	// Start the router and join the mesh.
	func() {
		logger.Printf("mesh router starting (%s)", *meshListen)
		router.Start()
	}()
	defer func() {
		logger.Printf("mesh router stopping")
		router.Stop()
	}()

	router.ConnectionMaker.InitiateConnections(peers.slice(), true)

	// Wait until we have at least 3 peers.
	// TODO(pb): joining existing cluster will require plumbing changes
	var (
		self   = meshconn.MeshAddr{PeerName: name}
		others = []net.Addr{}
	)
	for {
		others = others[:0]
		for _, desc := range router.Peers.Descriptions() {
			others = append(others, meshconn.MeshAddr{PeerName: desc.Name})
		}
		if len(others) >= 3 {
			logger.Printf("got %d peers", len(others))
			break
		}
		logger.Printf("have %d peer(s), waiting...", len(others))
		time.Sleep(time.Second)
	}

	incomingc := make(chan raftpb.Message)  // from meshconn to controller
	outgoingc := make(chan raftpb.Message)  // from controller to meshconn
	snapshotc := make(chan raftpb.Snapshot) // from controller to state machine
	entryc := make(chan raftpb.Entry)       // from controller to state machine
	proposalc := make(chan []byte)          // from state machine to controller

	// Create a packet transport.
	transport := newPacketTransport(peer, incomingc, outgoingc, logger)
	defer transport.stop()

	// Create the controller, which drives the Raft node internally.
	controller := newCtrl(self, others, incomingc, outgoingc, snapshotc, entryc, proposalc, logger)
	defer controller.stop()

	// Create the state machine, which also serves our K/V API.
	stateMachine := newStateMachine(snapshotc, entryc, proposalc, logger)

	errs := make(chan error, 2)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT)
		errs <- fmt.Errorf("%s", <-c)
	}()
	go func() {
		logger.Printf("HTTP server starting (%s)", *httpListen)
		http.HandleFunc("/", handle(logger, router, peer, stateMachine))
		errs <- http.ListenAndServe(*httpListen, nil)
	}()
	logger.Print(<-errs)
}

type stringset map[string]struct{}

func (ss stringset) Set(value string) error {
	ss[value] = struct{}{}
	return nil
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}

func mustHardwareAddr() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		if s := iface.HardwareAddr.String(); s != "" {
			return s
		}
	}
	panic("no valid network interfaces")
}

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}
