package mesh

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// LocalPeer is the only "active" peer in the mesh. It extends Peer with
// additional behaviors, mostly to retrieve and manage connection state.
type localPeer struct {
	sync.RWMutex
	*Peer
	router     *Router
	actionChan chan<- localPeerAction
}

// LocalPeerAction is the actor closure used by LocalPeer.
// TODO(pb): does this need to be exported?
type localPeerAction func()

// NewLocalPeer returns a usable LocalPeer.
func newLocalPeer(name PeerName, nickName string, router *Router) *localPeer {
	actionChan := make(chan localPeerAction, ChannelSize)
	peer := &localPeer{
		Peer:       newPeer(name, nickName, randomPeerUID(), 0, randomPeerShortID()),
		router:     router,
		actionChan: actionChan,
	}
	go peer.actorLoop(actionChan)
	return peer
}

// Connections returns all the connections that the local peer is aware of.
func (peer *localPeer) Connections() connectionSet {
	connections := make(connectionSet)
	peer.RLock()
	defer peer.RUnlock()
	for _, conn := range peer.connections {
		connections[conn] = struct{}{}
	}
	return connections
}

// ConnectionTo returns the connection to the named peer, if any.
func (peer *localPeer) ConnectionTo(name PeerName) (Connection, bool) {
	peer.RLock()
	defer peer.RUnlock()
	conn, found := peer.connections[name]
	return conn, found // yes, you really can't inline that. FFS.
}

// ConnectionsTo returns all known connections to the named peers.
func (peer *localPeer) ConnectionsTo(names []PeerName) []Connection {
	if len(names) == 0 {
		return nil
	}
	conns := make([]Connection, 0, len(names))
	peer.RLock()
	defer peer.RUnlock()
	for _, name := range names {
		conn, found := peer.connections[name]
		// Again, !found could just be due to a race.
		if found {
			conns = append(conns, conn)
		}
	}
	return conns
}

// CreateConnection creates a new connection to peerAddr. If acceptNewPeer is
// false, peerAddr must already be a member of the mesh.
func (peer *localPeer) CreateConnection(peerAddr string, acceptNewPeer bool) error {
	if err := peer.checkConnectionLimit(); err != nil {
		return err
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp4", peerAddr)
	if err != nil {
		return err
	}
	tcpConn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err != nil {
		return err
	}
	connRemote := newRemoteConnection(peer.Peer, nil, tcpConn.RemoteAddr().String(), true, false)
	startLocalConnection(connRemote, tcpConn, peer.router, acceptNewPeer)
	return nil
}

// ACTOR client API

// AddConnection adds the connection to the peer. Synchronous.
func (peer *localPeer) AddConnection(conn *LocalConnection) error {
	resultChan := make(chan error)
	peer.actionChan <- func() {
		resultChan <- peer.handleAddConnection(conn)
	}
	return <-resultChan
}

// ConnectionEstablished marks the connection as established within the peer.
// Asynchronous.
func (peer *localPeer) ConnectionEstablished(conn *LocalConnection) {
	peer.actionChan <- func() {
		peer.handleConnectionEstablished(conn)
	}
}

// DeleteConnection removes the connection from the peer. Synchronous.
func (peer *localPeer) DeleteConnection(conn *LocalConnection) {
	resultChan := make(chan interface{})
	peer.actionChan <- func() {
		peer.handleDeleteConnection(conn)
		resultChan <- nil
	}
	<-resultChan
}

// Encode writes the peer to the encoder.
func (peer *localPeer) Encode(enc *gob.Encoder) {
	peer.RLock()
	defer peer.RUnlock()
	peer.Peer.encode(enc)
}

// ACTOR server

func (peer *localPeer) actorLoop(actionChan <-chan localPeerAction) {
	gossipTimer := time.Tick(gossipInterval)
	for {
		select {
		case action := <-actionChan:
			action()
		case <-gossipTimer:
			peer.router.sendAllGossip()
		}
	}
}

func (peer *localPeer) handleAddConnection(conn Connection) error {
	if peer.Peer != conn.getLocal() {
		log.Fatal("Attempt made to add connection to peer where peer is not the source of connection")
	}
	if conn.Remote() == nil {
		log.Fatal("Attempt made to add connection to peer with unknown remote peer")
	}
	toName := conn.Remote().Name
	dupErr := fmt.Errorf("Multiple connections to %s added to %s", conn.Remote(), peer.String())
	// deliberately non symmetrical
	if dupConn, found := peer.connections[toName]; found {
		if dupConn == conn {
			return nil
		}
		switch conn.breakTie(dupConn) {
		case tieBreakWon:
			dupConn.shutdown(dupErr)
			peer.handleDeleteConnection(dupConn)
		case tieBreakLost:
			return dupErr
		case tieBreakTied:
			// oh good grief. Sod it, just kill both of them.
			dupConn.shutdown(dupErr)
			peer.handleDeleteConnection(dupConn)
			return dupErr
		}
	}
	if err := peer.checkConnectionLimit(); err != nil {
		return err
	}
	_, isConnectedPeer := peer.router.Routes.Unicast(toName)
	peer.addConnection(conn)
	if isConnectedPeer {
		conn.log("connection added")
	} else {
		conn.log("connection added (new peer)")
		peer.router.sendAllGossipDown(conn)
	}

	peer.router.Routes.Recalculate()
	peer.broadcastPeerUpdate(conn.Remote())

	return nil
}

func (peer *localPeer) handleConnectionEstablished(conn Connection) {
	if peer.Peer != conn.getLocal() {
		log.Fatal("Peer informed of active connection where peer is not the source of connection")
	}
	if dupConn, found := peer.connections[conn.Remote().Name]; !found || conn != dupConn {
		conn.shutdown(fmt.Errorf("Cannot set unknown connection active"))
		return
	}
	peer.connectionEstablished(conn)
	conn.log("connection fully established")

	peer.router.Routes.Recalculate()
	peer.broadcastPeerUpdate()
}

func (peer *localPeer) handleDeleteConnection(conn Connection) {
	if peer.Peer != conn.getLocal() {
		log.Fatal("Attempt made to delete connection from peer where peer is not the source of connection")
	}
	if conn.Remote() == nil {
		log.Fatal("Attempt made to delete connection to peer with unknown remote peer")
	}
	toName := conn.Remote().Name
	if connFound, found := peer.connections[toName]; !found || connFound != conn {
		return
	}
	peer.deleteConnection(conn)
	conn.log("connection deleted")
	// Must do garbage collection first to ensure we don't send out an
	// update with unreachable peers (can cause looping)
	peer.router.Peers.GarbageCollect()
	peer.router.Routes.Recalculate()
	peer.broadcastPeerUpdate()
}

// helpers

func (peer *localPeer) broadcastPeerUpdate(peers ...*Peer) {
	// Some tests run without a router.  This should be fixed so
	// that the relevant part of Router can be easily run in the
	// context of a test, but that will involve significant
	// reworking of tests.
	if peer.router != nil {
		peer.router.broadcastTopologyUpdate(append(peers, peer.Peer))
	}
}

func (peer *localPeer) checkConnectionLimit() error {
	limit := peer.router.ConnLimit
	if 0 != limit && peer.connectionCount() >= limit {
		return fmt.Errorf("Connection limit reached (%v)", limit)
	}
	return nil
}

func (peer *localPeer) addConnection(conn Connection) {
	peer.Lock()
	defer peer.Unlock()
	peer.connections[conn.Remote().Name] = conn
	peer.Version++
}

func (peer *localPeer) deleteConnection(conn Connection) {
	peer.Lock()
	defer peer.Unlock()
	delete(peer.connections, conn.Remote().Name)
	peer.Version++
}

func (peer *localPeer) connectionEstablished(conn Connection) {
	peer.Lock()
	defer peer.Unlock()
	peer.Version++
}

func (peer *localPeer) connectionCount() int {
	peer.RLock()
	defer peer.RUnlock()
	return len(peer.connections)
}

func (peer *localPeer) setShortID(shortID PeerShortID) {
	peer.Lock()
	defer peer.Unlock()
	peer.ShortID = shortID
	peer.Version++
}

func (peer *localPeer) setVersionBeyond(version uint64) bool {
	peer.Lock()
	defer peer.Unlock()
	if version >= peer.Version {
		peer.Version = version + 1
		return true
	}
	return false
}
