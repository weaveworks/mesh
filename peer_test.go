package mesh

import "testing"

func newPeerFrom(peer *Peer) *Peer {
	return newPeerFromSummary(peer.peerSummary)
}

func TestPeerRoutes(t *testing.T) {
	t.Skip("TODO")
}

func TestPeerForEachConnectedPeer(t *testing.T) {
	t.Skip("TODO")
}

func forcePendingGC(routers ...*Router) {
	for _, router := range routers {
		router.Peers.Lock()
		if router.Peers.pendingGC {
			var pending peersPendingNotifications
			router.Peers.garbageCollect(&pending)
			router.Peers.unlockAndNotify(&pending)
		} else {
			router.Peers.Unlock()
		}
	}
}
