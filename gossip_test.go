package mesh

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO test gossip unicast; atm we only test topology gossip and
// surrogates, neither of which employ unicast.

type mockGossipConnection struct {
	remoteConnection
	dest    *Router
	senders *gossipSenders
	start   chan struct{}
}

var _ gossipConnection = &mockGossipConnection{}

func newTestRouter(t *testing.T, name string) *Router {
	peerName, _ := PeerNameFromString(name)
	router, err := NewRouter(Config{}, peerName, "nick", nil, log.New(ioutil.Discard, "", 0))
	require.NoError(t, err)
	router.Start()
	return router
}

func (conn *mockGossipConnection) breakTie(dupConn ourConnection) connectionTieBreak {
	return tieBreakTied
}

func (conn *mockGossipConnection) shutdown(err error) {
}

func (conn *mockGossipConnection) logf(format string, args ...interface{}) {
	format = "->[" + conn.remoteTCPAddr + "|" + conn.remote.String() + "]: " + format
	if len(format) == 0 || format[len(format)-1] != '\n' {
		format += "\n"
	}
	fmt.Printf(format, args...)
}

func (conn *mockGossipConnection) SendProtocolMsg(pm protocolMsg) error {
	<-conn.start
	return conn.dest.handleGossip(pm.tag, pm.msg)
}

func (conn *mockGossipConnection) gossipSenders() *gossipSenders {
	return conn.senders
}

func (conn *mockGossipConnection) Start() {
	close(conn.start)
}

func sendPendingGossip(routers ...*Router) {
	// Loop until all routers report they didn't send anything
	for sentSomething := true; sentSomething; {
		sentSomething = false
		for _, router := range routers {
			sentSomething = router.sendPendingGossip() || sentSomething
		}
	}
}

func sendPendingTopologyUpdates(routers ...*Router) {
	for _, router := range routers {
		router.Ourself.Lock()
		pendingUpdate := router.Ourself.pendingTopologyUpdate
		router.Ourself.Unlock()
		if pendingUpdate {
			router.Ourself.broadcastPendingTopologyUpdates()
		}
	}
}

func addTestGossipConnection(t require.TestingT, r1, r2 *Router) {
	c1 := r1.newTestGossipConnection(t, r2)
	c2 := r2.newTestGossipConnection(t, r1)
	c1.Start()
	c2.Start()
}

func (router *Router) newTestGossipConnection(t require.TestingT, r *Router) *mockGossipConnection {
	to := r.Ourself.Peer
	toPeer := newPeer(to.Name, to.NickName, to.UID, 0, to.ShortID)
	toPeer = router.Peers.fetchWithDefault(toPeer) // Has side-effect of incrementing refcount

	conn := &mockGossipConnection{
		remoteConnection: *newRemoteConnection(router.Ourself.Peer, toPeer, "", false, true),
		dest:             r,
		start:            make(chan struct{}),
	}
	conn.senders = newGossipSenders(conn, make(chan struct{}))
	require.NoError(t, router.Ourself.handleAddConnection(conn, false))
	router.Ourself.handleConnectionEstablished(conn)
	return conn
}

func (router *Router) DeleteTestGossipConnection(r *Router) {
	toName := r.Ourself.Peer.Name
	conn, _ := router.Ourself.ConnectionTo(toName)
	router.Peers.dereference(conn.Remote())
	router.Ourself.handleDeleteConnection(conn.(ourConnection))
}

// Create a Peer representing the receiver router, with connections to
// the routers supplied as arguments, carrying across all UID and
// version information.
func (router *Router) tp(routers ...*Router) *Peer {
	peer := newPeerFrom(router.Ourself.Peer)
	connections := make(map[PeerName]Connection)
	for _, r := range routers {
		p := newPeerFrom(r.Ourself.Peer)
		connections[r.Ourself.Peer.Name] = newMockConnection(peer, p)
	}
	peer.Version = router.Ourself.Peer.Version
	peer.connections = connections
	return peer
}

// Check that the topology of router matches the peers and all of their connections
func checkTopology(t *testing.T, router *Router, wantedPeers ...*Peer) {
	router.Peers.RLock()
	checkTopologyPeers(t, true, router.Peers.allPeers(), wantedPeers...)
	router.Peers.RUnlock()
}

func flushAndCheckTopology(t *testing.T, routers []*Router, wantedPeers ...*Peer) {
	sendPendingTopologyUpdates(routers...)
	sendPendingGossip(routers...)
	for _, r := range routers {
		checkTopology(t, r, wantedPeers...)
	}
}

func TestGossipTopology(t *testing.T) {
	// Create some peers that will talk to each other
	r1 := newTestRouter(t, "01:00:00:01:00:00")
	r2 := newTestRouter(t, "02:00:00:02:00:00")
	r3 := newTestRouter(t, "03:00:00:03:00:00")
	routers := []*Router{r1, r2, r3}
	// Check state when they have no connections
	checkTopology(t, r1, r1.tp())
	checkTopology(t, r2, r2.tp())

	// Now try adding some connections
	addTestGossipConnection(t, r1, r2)
	sendPendingGossip(r1, r2)
	checkTopology(t, r1, r1.tp(r2), r2.tp(r1))
	checkTopology(t, r2, r1.tp(r2), r2.tp(r1))

	addTestGossipConnection(t, r2, r3)
	flushAndCheckTopology(t, routers, r1.tp(r2), r2.tp(r1, r3), r3.tp(r2))

	addTestGossipConnection(t, r3, r1)
	flushAndCheckTopology(t, routers, r1.tp(r2, r3), r2.tp(r1, r3), r3.tp(r1, r2))

	// Drop the connection from 2 to 3
	r2.DeleteTestGossipConnection(r3)
	flushAndCheckTopology(t, routers, r1.tp(r2, r3), r2.tp(r1), r3.tp(r1, r2))

	// Drop the connection from 1 to 3
	r1.DeleteTestGossipConnection(r3)
	sendPendingTopologyUpdates(routers...)
	sendPendingGossip(r1, r2, r3)
	forcePendingGC(r1, r2, r3)
	checkTopology(t, r1, r1.tp(r2), r2.tp(r1))
	checkTopology(t, r2, r1.tp(r2), r2.tp(r1))
	// r3 still thinks r1 has a connection to it
	checkTopology(t, r3, r1.tp(r2, r3), r2.tp(r1), r3.tp(r1, r2))
}

func TestGossipSurrogate(t *testing.T) {
	// create the topology r1 <-> r2 <-> r3
	r1 := newTestRouter(t, "01:00:00:01:00:00")
	r2 := newTestRouter(t, "02:00:00:02:00:00")
	r3 := newTestRouter(t, "03:00:00:03:00:00")
	routers := []*Router{r1, r2, r3}
	addTestGossipConnection(t, r1, r2)
	addTestGossipConnection(t, r3, r2)
	flushAndCheckTopology(t, routers, r1.tp(r2), r2.tp(r1, r3), r3.tp(r2))

	// create a gossiper at either end, but not the middle
	g1 := newTestGossiper()
	g3 := newTestGossiper()
	s1, err := r1.NewGossip("Test", g1)
	require.NoError(t, err)
	s3, err := r3.NewGossip("Test", g3)
	require.NoError(t, err)

	// broadcast a message from each end, check it reaches the other
	broadcast(s1, 1)
	broadcast(s3, 2)
	sendPendingGossip(r1, r2, r3)
	g1.checkHas(t, 2)
	g3.checkHas(t, 1)

	// check that each end gets their message back through periodic
	// gossip
	r1.sendAllGossip()
	r3.sendAllGossip()
	sendPendingGossip(r1, r2, r3)
	g1.checkHas(t, 1, 2)
	g3.checkHas(t, 1, 2)
}

type testGossiper struct {
	sync.RWMutex
	state map[byte]struct{}
}

func newTestGossiper() *testGossiper {
	return &testGossiper{state: make(map[byte]struct{})}
}

func (g *testGossiper) OnGossipUnicast(sender PeerName, msg []byte) error {
	return nil
}

func (g *testGossiper) OnGossipBroadcast(_ PeerName, update []byte) (GossipData, error) {
	g.Lock()
	defer g.Unlock()
	for _, v := range update {
		g.state[v] = struct{}{}
	}
	return newSurrogateGossipData(update), nil
}

func (g *testGossiper) Gossip() GossipData {
	g.RLock()
	defer g.RUnlock()
	state := make([]byte, len(g.state))
	for v := range g.state {
		state = append(state, v)
	}
	return newSurrogateGossipData(state)
}

func (g *testGossiper) OnGossip(update []byte) (GossipData, error) {
	g.Lock()
	defer g.Unlock()
	var delta []byte
	for _, v := range update {
		if _, found := g.state[v]; !found {
			delta = append(delta, v)
			g.state[v] = struct{}{}
		}
	}
	if len(delta) == 0 {
		return nil, nil
	}
	return newSurrogateGossipData(delta), nil
}

func (g *testGossiper) checkHas(t *testing.T, vs ...byte) {
	g.RLock()
	defer g.RUnlock()
	for _, v := range vs {
		if _, found := g.state[v]; !found {
			require.FailNow(t, fmt.Sprintf("%d is missing", v))
		}
	}
}

func broadcast(s Gossip, v byte) {
	s.GossipBroadcast(newSurrogateGossipData([]byte{v}))
}

func TestRandomNeighbours(t *testing.T) {
	const nTrials = 5000
	ourself := PeerName(0) // aliased with UnknownPeerName, which is ok here
	// Check fairness of selection across different-sized sets
	for _, test := range []struct{ nPeers, nNeighbours int }{{1, 0}, {2, 1}, {3, 2}, {10, 2}, {10, 3}, {10, 9}, {100, 2}, {100, 99}} {
		t.Run(fmt.Sprint(test.nPeers, "_peers_", test.nNeighbours, "_neighbours"), func(t *testing.T) {
			// Create a test fixture with unicastAll set up
			r := routes{
				unicastAll: make(unicastRoutes, test.nPeers),
			}
			// The route to 'ourself' is always via 'unknown'
			r.unicastAll[ourself] = UnknownPeerName
			// Fully-connected: unicast route to X is via X
			for i := 1; i < test.nPeers; i++ {
				r.unicastAll[PeerName(i)] = PeerName(i%test.nNeighbours + 1)
			}
			total := 0
			counts := make([]int, test.nNeighbours+1)
			// Run randomNeighbours() several times, and count the distribution
			for trial := 0; trial < nTrials; trial++ {
				targets := r.randomNeighbours(ourself)
				expected := int(math.Min(math.Log2(float64(test.nPeers)), float64(test.nNeighbours)))
				require.Equal(t, expected, len(targets))
				total += len(targets)
				for _, p := range targets {
					counts[p]++
				}
			}
			require.Equal(t, 0, counts[ourself], "randomNeighbours should not select source peer")
			// Check that each neighbour was picked within 20% of an average count
			for i := 1; i < test.nNeighbours+1; i++ {
				require.InEpsilon(t, float64(total)/float64(test.nNeighbours), counts[i], 0.2, "peer %d picked %d times out of %d; counts %v", i, counts[i], total, counts)
			}
		})
	}
}
