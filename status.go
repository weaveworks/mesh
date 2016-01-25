package mesh

import (
	"fmt"
	"net"
)

// Status is our current state as a peer, as taken from a router.
type Status struct {
	protocol           string
	protocolMinVersion int
	protocolMaxVersion int
	encryption         bool
	peerDiscovery      bool
	Name               string
	nickName           string
	port               int
	peers              []PeerStatus
	unicastRoutes      []unicastRouteStatus
	broadcastRoutes    []broadcastRouteStatus
	connections        []LocalConnectionStatus
	targets            []string
	overlayDiagnostics interface{}
	trustedSubnets     []string
}

// NewStatus returns a Status object, taken as a snapshot from the router.
func NewStatus(router *Router) *Status {
	return &Status{
		Protocol,
		ProtocolMinVersion,
		ProtocolMaxVersion,
		router.usingPassword(),
		router.PeerDiscovery,
		router.Ourself.Name.String(),
		router.Ourself.NickName,
		router.Port,
		newPeerStatusSlice(router.Peers),
		newUnicastRouteStatusSlice(router.Routes),
		newBroadcastRouteStatusSlice(router.Routes),
		newLocalConnectionStatusSlice(router.ConnectionMaker),
		newTargetSlice(router.ConnectionMaker),
		router.Overlay.Diagnostics(),
		newTrustedSubnetsSlice(router.TrustedSubnets),
	}
}

// PeerStatus is the current state of a peer in the mesh.
type PeerStatus struct {
	Name        string
	NickName    string
	UID         PeerUID
	ShortID     PeerShortID
	Version     uint64
	Connections []connectionStatus
}

// NewPeerStatusSlice takes a snapshot of the state of peers.
// TODO(pb): unexport and rename as Make
func newPeerStatusSlice(peers *Peers) []PeerStatus {
	var slice []PeerStatus

	peers.forEach(func(peer *Peer) {
		var connections []connectionStatus
		if peer == peers.ourself.Peer {
			for conn := range peers.ourself.Connections() {
				connections = append(connections, newConnectionStatus(conn))
			}
		} else {
			// Modifying peer.connections requires a write lock on
			// Peers, and since we are holding a read lock (due to the
			// ForEach), access without locking the peer is safe.
			for _, conn := range peer.connections {
				connections = append(connections, newConnectionStatus(conn))
			}
		}
		slice = append(slice, PeerStatus{
			peer.Name.String(),
			peer.NickName,
			peer.UID,
			peer.ShortID,
			peer.Version,
			connections})
	})

	return slice
}

// ConnectionStatus is the current state of a connection to a peer.
type connectionStatus struct {
	Name        string
	NickName    string
	Address     string
	Outbound    bool
	Established bool
}

// TODO(pb): rename as Make
func newConnectionStatus(c Connection) connectionStatus {
	return connectionStatus{
		c.Remote().Name.String(),
		c.Remote().NickName,
		c.remoteTCPAddress(),
		c.isOutbound(),
		c.isEstablished(),
	}
}

// UnicastRouteStatus is the current state of an established unicast route.
type unicastRouteStatus struct {
	Dest, Via string
}

// NewUnicastRouteStatusSlice takes a snapshot of the unicast routes in routes.
// TODO(pb): unexport and rename as Make
func newUnicastRouteStatusSlice(r *routes) []unicastRouteStatus {
	r.RLock()
	defer r.RUnlock()

	var slice []unicastRouteStatus
	for dest, via := range r.unicast {
		slice = append(slice, unicastRouteStatus{dest.String(), via.String()})
	}
	return slice
}

// BroadcastRouteStatus is the current state of an established broadcast route.
type broadcastRouteStatus struct {
	Source string
	Via    []string
}

// NewBroadcastRouteStatusSlice takes a snapshot of the broadcast routes in routes.
// TODO(pb): unexport and rename as Make
func newBroadcastRouteStatusSlice(r *routes) []broadcastRouteStatus {
	r.RLock()
	defer r.RUnlock()

	var slice []broadcastRouteStatus
	for source, via := range r.broadcast {
		var hops []string
		for _, hop := range via {
			hops = append(hops, hop.String())
		}
		slice = append(slice, broadcastRouteStatus{source.String(), hops})
	}
	return slice
}

// LocalConnectionStatus is the current state of a physical connection to a peer.
type LocalConnectionStatus struct {
	Address  string
	Outbound bool
	State    string
	Info     string
}

// NewLocalConnectionStatusSlice takes a snapshot of the active local
// connections in the ConnectionMaker.
// TODO(pb): unexport and rename as Make
func newLocalConnectionStatusSlice(cm *connectionMaker) []LocalConnectionStatus {
	resultChan := make(chan []LocalConnectionStatus, 0)
	cm.actionChan <- func() bool {
		var slice []LocalConnectionStatus
		for conn := range cm.connections {
			state := "pending"
			if conn.isEstablished() {
				state = "established"
			}
			lc, _ := conn.(*LocalConnection)
			info := fmt.Sprintf("%-6v %v", lc.OverlayConn.DisplayName(), conn.Remote())
			if lc.router.usingPassword() {
				if lc.untrusted() {
					info = fmt.Sprintf("%-11v %v", "encrypted", info)
				} else {
					info = fmt.Sprintf("%-11v %v", "unencrypted", info)
				}
			}
			slice = append(slice, LocalConnectionStatus{conn.remoteTCPAddress(), conn.isOutbound(), state, info})
		}
		for address, target := range cm.targets {
			add := func(state, info string) {
				slice = append(slice, LocalConnectionStatus{address, true, state, info})
			}
			switch target.state {
			case targetWaiting:
				until := "never"
				if !target.tryAfter.IsZero() {
					until = target.tryAfter.String()
				}
				if target.lastError == nil { // shouldn't happen
					add("waiting", "until: "+until)
				} else {
					add("failed", target.lastError.Error()+", retry: "+until)
				}
			case targetAttempting:
				if target.lastError == nil {
					add("connecting", "")
				} else {
					add("retrying", target.lastError.Error())
				}
			case targetConnected:
			}
		}
		resultChan <- slice
		return false
	}
	return <-resultChan
}

// NewTargetSlice takes a snapshot of the active targets (direct peers) in the
// ConnectionMaker.
// TODO(pb): unexport and rename as Make
func newTargetSlice(cm *connectionMaker) []string {
	resultChan := make(chan []string, 0)
	cm.actionChan <- func() bool {
		var slice []string
		for peer := range cm.directPeers {
			slice = append(slice, peer)
		}
		resultChan <- slice
		return false
	}
	return <-resultChan
}

// NewTrustedSubnetsSlice makes a human-readable copy of the trustedSubnets.
// TODO(pb): unexport and rename as Make
func newTrustedSubnetsSlice(trustedSubnets []*net.IPNet) []string {
	trustedSubnetStrs := []string{}
	for _, trustedSubnet := range trustedSubnets {
		trustedSubnetStrs = append(trustedSubnetStrs, trustedSubnet.String())
	}
	return trustedSubnetStrs
}
