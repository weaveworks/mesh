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

	"github.com/weaveworks/mesh"
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

	log.SetPrefix(*nickname + "> ")

	host, portStr, err := net.SplitHostPort(*meshListen)
	if err != nil {
		log.Fatalf("mesh address: %s: %v", *meshListen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("mesh address: %s: %v", *meshListen, err)
	}

	name, err := mesh.PeerNameFromString(*hwaddr)
	if err != nil {
		log.Fatalf("%s: %v", *hwaddr, err)
	}

	router := mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		Password:           []byte(*password),
		ConnLimit:          64,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, *nickname, mesh.NullOverlay{})

	peer := newPeer(log.New(os.Stderr, *nickname+"> ", log.LstdFlags))
	gossip := router.NewGossip(*channel, peer)
	peer.register(gossip)

	func() {
		log.Printf("mesh router starting (%s)", *meshListen)
		router.Start()
	}()
	defer func() {
		log.Printf("mesh router stopping")
		router.Stop()
	}()

	router.ConnectionMaker.InitiateConnections(peers.slice(), true)

	errs := make(chan error, 2)

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT)
		errs <- fmt.Errorf("%s", <-c)
	}()

	go func() {
		log.Printf("HTTP server starting (%s)", *httpListen)
		http.HandleFunc("/", handle(peer))
		errs <- http.ListenAndServe(*httpListen, nil)
	}()

	log.Print(<-errs)
}

type kv interface {
	get(key string) (result int, ok bool)
	set(key string, value int) (result int)
}

func handle(kv kv) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			key := r.FormValue("key")
			if key == "" {
				http.Error(w, "no key specified", http.StatusBadRequest)
				return
			}
			result, ok := kv.get(key)
			if !ok {
				http.Error(w, fmt.Sprintf("%s not found", key), http.StatusNotFound)
				return
			}
			fmt.Fprintf(w, "get(%s) => %d\n", key, result)

		case "POST":
			key := r.FormValue("key")
			if key == "" {
				http.Error(w, "no key specified", http.StatusBadRequest)
				return
			}
			valueStr := r.FormValue("value")
			if valueStr == "" {
				http.Error(w, "no value specified", http.StatusBadRequest)
				return
			}
			value, err := strconv.Atoi(valueStr)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			result := kv.set(key, value)
			fmt.Fprintf(w, "set(%s, %d) => %d\n", key, value, result)
		}
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
