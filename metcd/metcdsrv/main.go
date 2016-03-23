package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/meshconn"
	"github.com/weaveworks/mesh/metcd"
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
		quicktest  = flag.Int("quicktest", 0, "set to integer 1-9 to enable quick test setup of node")
		n          = flag.Int("n", 3, "number of peers expected (lower bound)")
	)
	flag.Var(peers, "peer", "initial peer (may be repeated)")
	flag.Parse()

	if *quicktest >= 1 && *quicktest <= 9 {
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

	ln, err := net.Listen("tcp", *apiListen)
	if err != nil {
		logger.Fatal(err)
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

	errc := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()
	go func() {
		logger.Printf("gRPC listening at %s", *apiListen)
		errc <- metcd.NewServer(router, peer, *n, logger).Serve(ln)
	}()
	logger.Print(<-errc)
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
