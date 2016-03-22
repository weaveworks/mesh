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
	"github.com/weaveworks/mesh/meshconn"
)

func main() {
	peers := &stringset{}
	var (
		apiListen  = flag.String("api", ":8080", "API listen address")
		meshListen = flag.String("mesh", net.JoinHostPort("0.0.0.0", strconv.Itoa(mesh.Port)), "mesh listen address")
		hwaddr     = flag.String("hwaddr", mustHardwareAddr(), "MAC address, i.e. mesh peer name")
		nickname   = flag.String("nickname", mustHostname(), "peer nickname")
		password   = flag.String("password", "", "password (optional)")
		channel    = flag.String("channel", "default", "gossip channel name")
		storage    = flag.String("storage", "simple", "storage engine: simple, etcd")
		quicktest  = flag.Int("quicktest", 0, "set to integer 1-9 to enable quick test setup of node")
		n          = flag.Int("n", 3, "number of peers expected (lower bound)")
	)
	flag.Var(peers, "peer", "initial peer (may be repeated)")
	flag.Parse()

	if *quicktest > 0 {
		*hwaddr = fmt.Sprintf("00:00:00:00:00:0%d", *quicktest)
		*meshListen = fmt.Sprintf("0.0.0.0:600%d", *quicktest)
		*apiListen = fmt.Sprintf("0.0.0.0:800%d", *quicktest)
		*nickname = fmt.Sprintf("%d", *quicktest)
		for i := 1; i <= 9; i++ {
			peers.Set(fmt.Sprintf("127.0.0.1:600%d", i))
		}
	}

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
		incomingc    = make(chan raftpb.Message)    // from meshconn to ctrl
		outgoingc    = make(chan raftpb.Message)    // from ctrl to meshconn
		unreachablec = make(chan uint64, 10000)     // from meshconn to ctrl
		confchangec  = make(chan raftpb.ConfChange) // from meshconn to ctrl
		snapshotc    = make(chan raftpb.Snapshot)   // from ctrl to state machine
		entryc       = make(chan raftpb.Entry)      // from ctrl to state
		confentryc   = make(chan raftpb.Entry)      // from state to configurator
		proposalc    = make(chan []byte)            // from state machine to ctrl
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

	// Create the API server. store.stop must go on the defer stack before
	// ctrl.stop so that the ctrl stops first. Otherwise, ctrl can deadlock
	// processing the last tick.
	var listenAndServe func() error
	switch *storage {
	case "simple":
		logger.Printf("HTTP simple store starting (%s)", *apiListen)
		store := newSimpleStore(proposalc, snapshotc, entryc, confentryc, logger)
		http.Handle("/", makeHandler(router, peer, store, logger))
		listenAndServe = func() error { return http.ListenAndServe(*apiListen, nil) }
	case "etcd":
		logger.Printf("etcd V3 (gRPC) engine starting (%s)", *apiListen)
		store := newEtcdStore(proposalc, snapshotc, entryc, confentryc, logger)
		defer store.stop()
		listenAndServe = func() error { return grpcListenAndServe(*apiListen, grpcServer(store)) }
	default:
		listenAndServe = func() error { return fmt.Errorf("invalid -storage %q", *storage) }
	}

	// Create the controller, which drives the Raft node internally.
	ctrl := newCtrl(self, others, *n, incomingc, outgoingc, unreachablec, confchangec, snapshotc, entryc, proposalc, removedc, logger)
	defer ctrl.stop()

	errc := make(chan error)
	go func() {
		errc <- listenAndServe()
	}()
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()
	go func() {
		<-removedc
		errc <- fmt.Errorf("Raft node removed from cluster")
	}()
	go func() {
		<-shrunkc
		errc <- fmt.Errorf("Raft cluster got too small")
	}()
	logger.Print(<-errc)
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
