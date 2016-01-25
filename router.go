package mesh

import (
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

const (
	// Port is the port used for all mesh communication.
	// TODO(pb): this should either not be exported, or made var
	Port = 6783

	// ChannelSize is the buffer size used by so-called actor goroutines
	// throughout mesh.
	// TODO(pb): this should either not be exported, or made var
	ChannelSize = 16

	// TCPHeartbeat is how often a mesh connection will heartbeat.
	// TODO(pb): this should either not be exported, or made var
	TCPHeartbeat = 30 * time.Second

	// GossipInterval is how often peers will broadcast their accumulated
	// gossip.
	// TODO(pb): this should either not be exported, or made var
	GossipInterval = 30 * time.Second

	// MaxDuration is a stand-in for forever.
	// TODO(pb): this should not be exported
	MaxDuration = time.Duration(math.MaxInt64)

	// Capacity of token bucket for rate limiting accepts.
	acceptMaxTokens = 100

	// Control rate at which new tokens are added to the bucket.
	acceptTokenDelay = 100 * time.Millisecond // [2]
)

// Config defines dimensions of configuration for the router.
// TODO(pb): provide usable defaults in NewRouter
type Config struct {
	Port               int
	ProtocolMinVersion byte
	Password           []byte
	ConnLimit          int
	PeerDiscovery      bool
	TrustedSubnets     []*net.IPNet
}

// Router manages communication between this peer and the rest of the mesh.
// Router implements Gossiper.
type Router struct {
	Config
	Overlay         Overlay
	Ourself         *localPeer
	Peers           *Peers
	Routes          *Routes
	ConnectionMaker *connectionMaker
	gossipLock      sync.RWMutex
	gossipChannels  gossipChannels
	TopologyGossip  Gossip
	acceptLimiter   *tokenBucket
}

// NewRouter returns a new router. It must be started.
func NewRouter(config Config, name PeerName, nickName string, overlay Overlay) *Router {
	router := &Router{Config: config, gossipChannels: make(gossipChannels)}

	if overlay == nil {
		overlay = NullOverlay{}
	}

	router.Overlay = overlay
	router.Ourself = newLocalPeer(name, nickName, router)
	router.Peers = newPeers(router.Ourself)
	router.Peers.OnGC(func(peer *Peer) {
		log.Println("Removed unreachable peer", peer)
	})
	router.Routes = newRoutes(router.Ourself, router.Peers)
	router.ConnectionMaker = newConnectionMaker(router.Ourself, router.Peers, router.Port, router.PeerDiscovery)
	router.TopologyGossip = router.NewGossip("topology", router)
	router.acceptLimiter = newTokenBucket(acceptMaxTokens, acceptTokenDelay)

	return router
}

// Start listening for TCP connections. This is separate from NewRouter so
// that gossipers can register before we start forming connections.
func (router *Router) Start() {
	router.listenTCP(router.Port)
}

// Stop shuts down the router. In theory.
func (router *Router) Stop() error {
	// TODO: perform graceful shutdown...
	return nil
}

// UsingPassword returns true if a password is set.
// Passwords are used to establish encrypted connections.
func (router *Router) UsingPassword() bool {
	return router.Password != nil
}

func (router *Router) listenTCP(localPort int) {
	localAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprint(":", localPort))
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.ListenTCP("tcp4", localAddr)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer ln.Close()
		for {
			tcpConn, err := ln.AcceptTCP()
			if err != nil {
				log.Println(err)
				continue
			}
			router.acceptTCP(tcpConn)
			router.acceptLimiter.Wait()
		}
	}()
}

func (router *Router) acceptTCP(tcpConn *net.TCPConn) {
	remoteAddrStr := tcpConn.RemoteAddr().String()
	log.Printf("->[%s] connection accepted", remoteAddrStr)
	connRemote := newRemoteConnection(router.Ourself.Peer, nil, remoteAddrStr, false, false)
	startLocalConnection(connRemote, tcpConn, router, true)
}

// Gossiper methods - the Router is the topology Gossiper

// TopologyGossipData is the set of peers in the mesh network.
// It is gossiped just like anything else.
type topologyGossipData struct {
	peers  *Peers
	update peerNameSet
}

// Merge implements GossipData.
func (d *topologyGossipData) Merge(other GossipData) GossipData {
	names := make(peerNameSet)
	for name := range d.update {
		names[name] = struct{}{}
	}
	for name := range other.(*topologyGossipData).update {
		names[name] = struct{}{}
	}
	return &topologyGossipData{peers: d.peers, update: names}
}

// Encode implements GossipData.
func (d *topologyGossipData) Encode() [][]byte {
	return [][]byte{d.peers.EncodePeers(d.update)}
}

// BroadcastTopologyUpdate is invoked whenever there is a change to the mesh
// topology, and broadcasts the new set of peers to the mesh.
func (router *Router) BroadcastTopologyUpdate(update []*Peer) {
	names := make(peerNameSet)
	for _, p := range update {
		names[p.Name] = struct{}{}
	}
	router.TopologyGossip.GossipBroadcast(&topologyGossipData{peers: router.Peers, update: names})
}

// OnGossipUnicast implements Gossiper, but always returns an error, as a
// router should only receive gossip broadcasts of TopologyGossipData.
func (router *Router) OnGossipUnicast(sender PeerName, msg []byte) error {
	return fmt.Errorf("unexpected topology gossip unicast: %v", msg)
}

// OnGossipBroadcast receives broadcasts of TopologyGossipData.
// It returns the received update unchanged.
func (router *Router) OnGossipBroadcast(_ PeerName, update []byte) (GossipData, error) {
	origUpdate, _, err := router.applyTopologyUpdate(update)
	if err != nil || len(origUpdate) == 0 {
		return nil, err
	}
	return &topologyGossipData{peers: router.Peers, update: origUpdate}, nil
}

// Gossip yields the current topology as GossipData.
func (router *Router) Gossip() GossipData {
	return &topologyGossipData{peers: router.Peers, update: router.Peers.Names()}
}

// OnGossip receives broadcasts of TopologyGossipData.
// It returns an "improved" version of the received update.
// See peers.ApplyUpdate.
func (router *Router) OnGossip(update []byte) (GossipData, error) {
	_, newUpdate, err := router.applyTopologyUpdate(update)
	if err != nil || len(newUpdate) == 0 {
		return nil, err
	}
	return &topologyGossipData{peers: router.Peers, update: newUpdate}, nil
}

func (router *Router) applyTopologyUpdate(update []byte) (peerNameSet, peerNameSet, error) {
	origUpdate, newUpdate, err := router.Peers.ApplyUpdate(update)
	if err != nil {
		return nil, nil, err
	}
	if len(newUpdate) > 0 {
		router.ConnectionMaker.Refresh()
		router.Routes.Recalculate()
	}
	return origUpdate, newUpdate, nil
}

// Trusts returns true if the remote connection is in a trusted subnet.
func (router *Router) Trusts(remote *remoteConnection) bool {
	if tcpAddr, err := net.ResolveTCPAddr("tcp4", remote.remoteTCPAddr); err == nil {
		for _, trustedSubnet := range router.TrustedSubnets {
			if trustedSubnet.Contains(tcpAddr.IP) {
				return true
			}
		}
	} else {
		// Should not happen as remoteTCPAddr was obtained from TCPConn
		log.Printf("Unable to parse remote TCP addr: %s", err)
	}
	return false
}
