package metcd

import (
	"fmt"
	"log"
	"net"

	wackygrpc "github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	"github.com/coreos/etcd/raft/raftpb"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/meshconn"
)

// NewServer returns a gRPC server that implements the etcd V3 API.
// It uses the passed mesh components to create and manage the Raft transport.
func NewServer(
	router *mesh.Router,
	peer *meshconn.Peer,
	otherAddrs func(*mesh.Router) []net.Addr,
	minPeerCount int,
	logger *log.Logger,
) *wackygrpc.Server {
	c := make(chan *wackygrpc.Server)
	go createAndManage(router, peer, otherAddrs, minPeerCount, logger, c)
	return <-c
}

func createAndManage(
	router *mesh.Router,
	peer *meshconn.Peer,
	otherAddrs func(*mesh.Router) []net.Addr,
	minPeerCount int,
	logger *log.Logger,
	out chan<- *wackygrpc.Server,
) {
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

	// Create the thing that watches the cluster membership via the router. It
	// signals conf changes, and closes shrunkc when the cluster is too small.
	var (
		addc = make(chan uint64)
		remc = make(chan uint64)
	)
	m := newMembership(router, membershipSet(router), minPeerCount, addc, remc, shrunkc, logger)
	defer m.stop()

	// Create the thing that converts mesh membership changes to Raft ConfChange
	// proposals.
	c := newConfigurator(addc, remc, confchangec, confentryc, logger)
	defer c.stop()

	// Create a packet transport, wrapping the meshconn.Peer.
	transport := newPacketTransport(peer, translateVia(router), incomingc, outgoingc, unreachablec, logger)
	defer transport.stop()

	// Create the API server. store.stop must go on the defer stack before
	// ctrl.stop so that the ctrl stops first. Otherwise, ctrl can deadlock
	// processing the last tick.
	store := newEtcdStore(proposalc, snapshotc, entryc, confentryc, logger)
	defer store.stop()

	// Create the controller, which drives the Raft node internally.
	var (
		self   = meshconn.MeshAddr{PeerName: router.Ourself.Peer.Name, PeerUID: router.Ourself.UID}
		others = otherAddrs(router)
	)
	ctrl := newCtrl(self, others, minPeerCount, incomingc, outgoingc, unreachablec, confchangec, snapshotc, entryc, proposalc, removedc, logger)
	defer ctrl.stop()

	// Create the gRPC server, wrapping the store. This is what gets returned to
	// the user. But, we can shut it down in certain circumstances.
	server := grpcServer(store)
	defer server.Stop()
	out <- server

	errc := make(chan error)
	go func() {
		<-removedc
		errc <- fmt.Errorf("the Raft peer was removed from the cluster")
	}()
	go func() {
		<-shrunkc
		errc <- fmt.Errorf("the Raft cluster got too small")
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
