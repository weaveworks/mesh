package main

import (
	"log"
	"net"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

type transport struct {
	conn    net.PacketConn
	self    raft.Peer
	others  []raft.Peer
	storage *raft.MemoryStorage
	node    raft.Node
	sm      stateMachine
	quit    chan struct{}
}

const heartbeatTick = 250 // ms

// newTransport boots a Raft peer communicating over the conn.
// We bootstrap with the initial peer(s) and take topology changes as they come, later.
func newTransport(conn net.PacketConn, self net.Addr, others []net.Addr, logger *log.Logger) *transport {
	storage := raft.NewMemoryStorage()
	nodeConfig := &raft.Config{
		ID:              makeRaftPeer(self).ID,
		ElectionTick:    10 * heartbeatTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		Applied:         0,    // starting fresh
		MaxSizePerMsg:   10,   // TODO(pb): bytes, message count?
		MaxInflightMsgs: 10,   // TODO(pb): validate this total fabrication
		CheckQuorum:     true, // leader steps down if quorum is not active for an electionTimeout
		Logger:          &raft.DefaultLogger{Logger: logger},
	}
	t := &transport{
		conn:    conn,
		self:    makeRaftPeer(self),
		others:  makeRaftPeers(others),
		storage: storage,
		node:    raft.StartNode(nodeConfig, makeRaftPeers(others)),
		sm:      surrogateStateMachine{},
		quit:    make(chan struct{}),
	}
	go t.loop()
	return t
}

func (t *transport) loop() {
	// Now that we are holding on to a Node, we have a few responsibilities.
	// See https://godoc.org/github.com/coreos/etcd/raft
	ticker := time.NewTicker((heartbeatTick / 10) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.node.Tick()
		case r := <-t.node.Ready():
			t.handleReady(r)
		case <-t.quit:
			return
		}
	}
}

func (t *transport) stop() {
	close(t.quit)
}

func (t *transport) handleReady(r raft.Ready) {
	// These steps may be performed in parallel, except as noted in step 2.
	//
	// 1. Write HardState, Entries, and Snapshot to persistent storage if they are
	// not empty. Note that when writing an Entry with Index i, any
	// previously-persisted entries with Index >= i must be discarded.
	t.readySave(r.Snapshot, r.HardState, r.Entries)

	// 2. Send all Messages to the nodes named in the To field. It is important
	// that no messages be sent until after the latest HardState has been persisted
	// to disk, and all Entries written by any previous Ready batch (Messages may
	// be sent while entries from the same batch are being persisted). If any
	// Message has type MsgSnap, call Node.ReportSnapshot() after it has been sent
	// (these messages may be large). Note: Marshalling messages is not
	// thread-safe; it is important that you make sure that no new entries are
	// persisted while marshalling. The easiest way to achieve this is to serialise
	// the messages directly inside your main raft loop.
	t.readySend(r.Messages)

	// 3. Apply Snapshot (if any) and CommittedEntries to the state machine. If any
	// committed Entry has Type EntryConfChange, call Node.ApplyConfChange() to
	// apply it to the node. The configuration change may be cancelled at this
	// point by setting the NodeID field to zero before calling ApplyConfChange
	// (but ApplyConfChange must be called one way or the other, and the decision
	// to cancel must be based solely on the state machine and not external
	// information such as the observed health of the node).
	t.readyApply(r.Snapshot, r.CommittedEntries)

	// 4. Call Node.Advance() to signal readiness for the next batch of updates.
	// This may be done at any time after step 1, although all updates must be
	// processed in the order they were returned by Ready.
	t.readyAdvance()
}

func (t *transport) readySave(snapshot raftpb.Snapshot, hardState raftpb.HardState, entries []raftpb.Entry) {
	// For the moment, none of these steps persist to disk. That violates some Raft
	// invariants. But we are ephemeral, and will always boot empty, willingly
	// paying the snapshot cost. I trust that that the etcd Raft implementation
	// permits this.
	if !raft.IsEmptySnap(snapshot) {
		if err := t.storage.ApplySnapshot(snapshot); err != nil {
			panic(err)
		}
	}
	if !raft.IsEmptyHardState(hardState) {
		if err := t.storage.SetHardState(hardState); err != nil {
			panic(err)
		}
	}
	if err := t.storage.Append(entries); err != nil {
		panic(err)
	}
}

func (t *transport) readySend(messages []raftpb.Message) {
	for _, msg := range messages {
		err := t.sendMessage(msg)

		if msg.Type == raftpb.MsgSnap {
			status := raft.SnapshotFinish
			if err != nil {
				status = raft.SnapshotFailure
			}
			t.node.ReportSnapshot(msg.To, status)
		}
	}
}

func (t *transport) readyApply(snapshot raftpb.Snapshot, committedEntries []raftpb.Entry) {
	if err := t.sm.applySnapshot(snapshot); err != nil {
		panic(err)
	}
	for _, committedEntry := range committedEntries {
		if err := t.sm.applyCommittedEntry(committedEntry); err != nil {
			panic(err)
		}
		if committedEntry.Type == raftpb.EntryConfChange {
			t.node.ApplyConfChange(entry2cc(committedEntry))
		}
	}
}

// Convert a generic raftpb.Entry to a specific raftpb.ConfChange.
// TODO(pb): placeholder implementation; this is probably not right.
func entry2cc(entry raftpb.Entry) raftpb.ConfChange {
	buf, err := entry.Marshal()
	if err != nil {
		panic(err)
	}
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(buf); err != nil {
		panic(err)
	}
	return cc
}

func (t *transport) readyAdvance() {
	t.node.Advance()
}

func (t *transport) sendMessage(msg raftpb.Message) error {
	buf, err := msg.Marshal()
	if err != nil {
		return err
	}
	_, err = t.conn.WriteTo(buf, makeAddr(msg.To))
	return err
}

// makeRaftPeer converts a net.Addr into a raft.Peer with no context.
// All peers must perform the Addr-to-Peer mapping in the same way.
// We assume peer_name_mac and treat the uint64 as the raft.Peer.ID.
// To break this coupling, we would need to hash the addr.String()
// and maintain two lookup tables: hash-to-PeerName, and ID-to-hash.
func makeRaftPeer(addr net.Addr) raft.Peer {
	return raft.Peer{
		ID:      uint64(addr.(meshconn.MeshAddr).PeerName),
		Context: nil, // ?
	}
}

func makeRaftPeers(addrs []net.Addr) []raft.Peer {
	peers := make([]raft.Peer, len(addrs))
	for i, addr := range addrs {
		peers[i] = makeRaftPeer(addr)
	}
	return peers
}

// makeAddr converts a raft.Peer ID to a net.Addr that can be given as a dst to
// the net.PacketConn. It assumes peer_name_mac. To break this coupling, we
// would need to query an ID-to-hash, then hash-to-PeerName lookup table.
func makeAddr(raftPeerID uint64) net.Addr {
	return meshconn.MeshAddr{
		PeerName: mesh.PeerName(raftPeerID),
	}
}
