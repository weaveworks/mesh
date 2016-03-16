package main

import (
	"flag"
	"fmt"
	"io/ioutil"
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
		hwaddr     = flag.String("hwaddr", mustHardwareAddr(), "MAC address, i.e. mesh peer name")
		nickname   = flag.String("nickname", mustHostname(), "peer nickname")
		password   = flag.String("password", "", "password (optional)")
		channel    = flag.String("channel", "default", "gossip channel name")
		n          = flag.Int("n", 3, "number of peers expected (lower bound)")
		quicktest  = flag.Int("quicktest", 0, "set to integer 1-9 to enable quick test setup of node")
	)
	flag.Var(peers, "peer", "initial peer (may be repeated)")
	flag.Parse()

	logger := log.New(os.Stderr, *nickname+"> ", log.LstdFlags)

	if *quicktest > 0 {
		*hwaddr = fmt.Sprintf("00:00:00:00:00:0%d", *quicktest)
		*meshListen = fmt.Sprintf("0.0.0.0:600%d", *quicktest)
		*httpListen = fmt.Sprintf("0.0.0.0:800%d", *quicktest)
		for i := 1; i <= 9; i++ {
			peers.Set(fmt.Sprintf("127.0.0.1:600%d", i))
		}
	}

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

	logger.Printf("hello!")
	defer logger.Printf("goodbye!")

	// Create, but do not start, a router.
	log.SetOutput(ioutil.Discard) // no log from mesh.Router please
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
	peer := meshconn.NewPeer(name, router.Ourself.UID, logger)
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

	var (
		self   = meshconn.MeshAddr{PeerName: name, PeerUID: router.Ourself.UID}
		others = []net.Addr{}
	)
	// Wait until we have at least n peers.
	for {
		others = others[:0]
		for _, desc := range router.Peers.Descriptions() {
			others = append(others, meshconn.MeshAddr{PeerName: desc.Name, PeerUID: desc.UID})
		}
		if len(others) == *n {
			logger.Printf("detected %d peers; creating", len(others))
			break
		} else if len(others) > *n {
			logger.Printf("detected %d peers; joining", len(others))
			others = others[:0] // empty others slice means join
			break
		}
		logger.Printf("detected %d peers; waiting...", len(others))
		time.Sleep(time.Second)
	}

	var (
		incomingc    = make(chan raftpb.Message)    // from meshconn to controller
		outgoingc    = make(chan raftpb.Message)    // from controller to meshconn
		unreachablec = make(chan uint64, 10000)     // from meshconn to controller
		confchangec  = make(chan raftpb.ConfChange) // from meshconn to controller
		snapshotc    = make(chan raftpb.Snapshot)   // from controller to state machine
		entryc       = make(chan raftpb.Entry)      // from controller to demuxer
		confentryc   = make(chan raftpb.Entry)      // from demuxer to configurator
		normalentryc = make(chan raftpb.Entry)      // from demuxer to state
		proposalc    = make(chan []byte)            // from state machine to controller
		removedc     = make(chan struct{})          // from ctrl to us
		shrunkc      = make(chan struct{})          // from membership to us
	)

	// Create the thing that watches the cluster membership via the router.
	// It signals conf changes, and closes shrunkc when the cluster is too small.
	addc, remc := make(chan uint64), make(chan uint64)
	m := newMembership(router, membershipSet(router), *n, addc, remc, shrunkc, logger)
	defer m.stop()

	// Create the thing that converts mesh membership changes to Raft ConfChange proposals.
	c := newConfigurator(addc, remc, confchangec, confentryc, logger)
	defer c.stop()

	// Create a packet transport.
	transport := newPacketTransport(peer, translateVia(router), incomingc, outgoingc, unreachablec, logger)
	defer transport.stop()

	// Create the controller, which drives the Raft node internally.
	ctrl := newCtrl(self, others, *n, incomingc, outgoingc, unreachablec, confchangec, snapshotc, entryc, proposalc, removedc, logger)
	defer ctrl.stop()

	// Create the demuxer, splitting all committed entries to ConfChange and Normal entries.
	go demux(entryc, confentryc, normalentryc)

	// Create the state machine, which also serves our K/V API.
	stateMachine := newStateMachine(snapshotc, normalentryc, proposalc, logger)

	errs := make(chan error, 3)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errs <- fmt.Errorf("%s", <-c)
	}()
	go func() {
		logger.Printf("HTTP server starting (%s)", *httpListen)
		http.Handle("/", handle(logger, router, peer, stateMachine))
		errs <- http.ListenAndServe(*httpListen, nil)
	}()
	go func() {
		<-removedc
		errs <- fmt.Errorf("Raft node removed from cluster")
	}()
	go func() {
		<-shrunkc
		errs <- fmt.Errorf("Raft cluster got too small")
	}()
	log.Print(<-errs)
}

func translateVia(router *mesh.Router) peerTranslator {
	return func(uid mesh.PeerUID) (mesh.PeerName, error) {
		for _, d := range router.Peers.Descriptions() {
			if d.UID == uid {
				return d.Name, nil
			}
		}
		return 0, fmt.Errorf("peer UID %x not known", uid)
	}
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

func demux(in <-chan raftpb.Entry, confchangec, normalc chan<- raftpb.Entry) {
	for e := range in {
		switch e.Type {
		case raftpb.EntryConfChange:
			confchangec <- e
		default:
			normalc <- e
		}
	}
}
