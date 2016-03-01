package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	wackycontext "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

//                                      +-ctrl----------------------+
// +-packetTransport---+                |                           |
// |                   |                |   +-raft.Node---------+   |
// |  +-meshconn---+   |                |   |                   |   |               +-stateMachine-+
// |  |    ReadFrom|-->|--incomingc---->|-->|Step        Propose|<--|<--proposalc---|              |
// |  |            |   |                |   |                   |   |               |              |
// |  |  .-mesh-.  |   |                |   |                   |   |               |              |
// |  |  |      |----->|--confchangec-->|-->|ProposeConfChange  |   |               |              |
// |  |  '------'  |   |                |   |                   |   |               |              |
// |  |            |   |                |   +-------------------+   |               |              |
// |  |            |   |                |     |          |    |     |               |              |
// |  |     WriteTo|<--|<---outgoingc---|<----'          |    '---->|---snapshotc-->|              |
// |  +------------+   |                |                '--------->|---entryc----->|              |
// +-------------------+                +---------------------------+               +--------------+

type ctrl struct {
	self        raft.Peer
	others      []raft.Peer
	incomingc   <-chan raftpb.Message    // from the transport
	outgoingc   chan<- raftpb.Message    // to the transport
	confchangec <-chan raftpb.ConfChange // from the mesh
	snapshotc   chan<- raftpb.Snapshot   // to the state machine
	entryc      chan<- raftpb.Entry      // to the state machine
	proposalc   <-chan []byte            // from the state machine
	quitc       chan struct{}
	storage     *raft.MemoryStorage
	node        raft.Node
	logger      *log.Logger
}

const heartbeatTick = 2 // 2 * 100 ms = 200 ms

func newCtrl(
	self net.Addr,
	others []net.Addr,
	incomingc <-chan raftpb.Message,
	outgoingc chan<- raftpb.Message,
	confchangec <-chan raftpb.ConfChange,
	snapshotc chan<- raftpb.Snapshot,
	entryc chan<- raftpb.Entry,
	proposalc <-chan []byte,
	logger *log.Logger,
) *ctrl {
	storage := raft.NewMemoryStorage()
	raftLogger := &raft.DefaultLogger{Logger: logger}
	raftLogger.EnableDebug()
	nodeConfig := &raft.Config{
		ID:              makeRaftPeer(self).ID,
		ElectionTick:    10 * heartbeatTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		Applied:         0,    // starting fresh
		MaxSizePerMsg:   4096, // TODO(pb): looks like bytes; confirm that
		MaxInflightMsgs: 256,  // TODO(pb): copied from docs; confirm that
		CheckQuorum:     true, // leader steps down if quorum is not active for an electionTimeout
		Logger:          raftLogger,
	}
	node := raft.StartNode(nodeConfig, makeRaftPeers(others))
	c := &ctrl{
		self:        makeRaftPeer(self),
		others:      makeRaftPeers(others),
		incomingc:   incomingc,
		outgoingc:   outgoingc,
		confchangec: confchangec,
		snapshotc:   snapshotc,
		entryc:      entryc,
		proposalc:   proposalc,
		quitc:       make(chan struct{}),
		storage:     storage,
		node:        node,
		logger:      logger,
	}
	go c.driveRaft()      // analagous to raftexample serveChannels
	go c.driveProposals() // analagous to raftexample serveChannels anonymous goroutine
	return c
}

func (c *ctrl) stop() {
	close(c.quitc)
}

func (c *ctrl) driveRaft() {
	defer c.logger.Printf("ctrl: driveRaft loop exit")

	// Now that we are holding a raft.Node we have a few responsibilities.
	// https://godoc.org/github.com/coreos/etcd/raft

	ticker := time.NewTicker(100 * time.Millisecond) // TODO(pb): taken from raftexample; need to validate
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.node.Tick()

		case r := <-c.node.Ready():
			if err := c.handleReady(r); err != nil {
				c.logger.Printf("ctrl: handle ready: %v", err)
				return
			}

		case msg := <-c.incomingc:
			c.logger.Printf("ctrl: incoming msg (%s)", msg.String())
			c.node.Step(wackycontext.TODO(), msg)

		case <-c.quitc:
			return
		}
	}
}

func (c *ctrl) driveProposals() {
	defer c.logger.Printf("ctrl: driveProposals loop exit")

	for c.proposalc != nil && c.confchangec != nil {
		select {
		case data, ok := <-c.proposalc:
			if !ok {
				c.logger.Printf("ctrl: got nil proposal; shutting down proposals")
				c.proposalc = nil // TODO(pb): is this safe?
				continue
			}
			c.logger.Printf("ctrl: incoming proposal (%s)", data)
			c.node.Propose(wackycontext.TODO(), data)
			c.logger.Printf("ctrl: incoming proposal (%s) accepted", data)

		case cc, ok := <-c.confchangec:
			if !ok {
				c.logger.Printf("ctrl: got nil conf change; shutting down conf changes")
				c.confchangec = nil // TODO(pb): is this safe?
				continue
			}
			c.logger.Printf("ctrl: incoming confchange ID=%d type=%d nodeID=%d", cc.ID, cc.Type, cc.NodeID)
			c.node.ProposeConfChange(wackycontext.TODO(), cc)
			c.logger.Printf("ctrl: incoming confchange ID=%d type=%d nodeID=%d accepted", cc.ID, cc.Type, cc.NodeID)

		case <-c.quitc:
			return
		}
	}
}

func (c *ctrl) handleReady(r raft.Ready) error {
	// These steps may be performed in parallel, except as noted in step 2.
	//
	// 1. Write HardState, Entries, and Snapshot to persistent storage if they are
	// not empty. Note that when writing an Entry with Index i, any
	// previously-persisted entries with Index >= i must be discarded.
	if err := c.readySave(r.Snapshot, r.HardState, r.Entries); err != nil {
		return fmt.Errorf("save: %v", err)
	}

	// 2. Send all Messages to the nodes named in the To field. It is important
	// that no messages be sent until after the latest HardState has been persisted
	// to disk, and all Entries written by any previous Ready batch (Messages may
	// be sent while entries from the same batch are being persisted). If any
	// Message has type MsgSnap, call Node.ReportSnapshot() after it has been sent
	// (these messages may be large). Note: Marshalling messages is not
	// thread-safe; it is important that you make sure that no new entries are
	// persisted while marshalling. The easiest way to achieve this is to serialise
	// the messages directly inside your main raft loop.
	c.readySend(r.Messages)

	// 3. Apply Snapshot (if any) and CommittedEntries to the state machine. If any
	// committed Entry has Type EntryConfChange, call Node.ApplyConfChange() to
	// apply it to the node. The configuration change may be cancelled at this
	// point by setting the NodeID field to zero before calling ApplyConfChange
	// (but ApplyConfChange must be called one way or the other, and the decision
	// to cancel must be based solely on the state machine and not external
	// information such as the observed health of the node).
	if err := c.readyApply(r.Snapshot, r.CommittedEntries); err != nil {
		return fmt.Errorf("apply: %v", err)
	}

	// 4. Call Node.Advance() to signal readiness for the next batch of updates.
	// This may be done at any time after step 1, although all updates must be
	// processed in the order they were returned by Ready.
	c.readyAdvance()

	return nil
}

func (c *ctrl) readySave(snapshot raftpb.Snapshot, hardState raftpb.HardState, entries []raftpb.Entry) error {
	// For the moment, none of these steps persist to disk. That violates some Raft
	// invariants. But we are ephemeral, and will always boot empty, willingly
	// paying the snapshot cost. I trust that that the etcd Raft implementation
	// permits this.
	if !raft.IsEmptySnap(snapshot) {
		if err := c.storage.ApplySnapshot(snapshot); err != nil {
			return fmt.Errorf("apply snapshot: %v", err)
		}
	}
	if !raft.IsEmptyHardState(hardState) {
		if err := c.storage.SetHardState(hardState); err != nil {
			return fmt.Errorf("set hard state: %v", err)
		}
	}
	if err := c.storage.Append(entries); err != nil {
		return fmt.Errorf("append: %v", err)
	}
	return nil
}

func (c *ctrl) readySend(msgs []raftpb.Message) {
	for _, msg := range msgs {
		c.outgoingc <- msg

		if msg.Type == raftpb.MsgSnap {
			// Assume snapshot sends always succeed.
			// TODO(pb): do we need error reporting?
			c.node.ReportSnapshot(msg.To, raft.SnapshotFinish)
		}
	}
}

func (c *ctrl) readyApply(snapshot raftpb.Snapshot, committedEntries []raftpb.Entry) error {
	c.snapshotc <- snapshot

	for _, committedEntry := range committedEntries {
		c.entryc <- committedEntry

		if committedEntry.Type == raftpb.EntryConfChange {
			// See raftexample raftNode.publishEntries
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(committedEntry.Data); err != nil {
				return fmt.Errorf("unmarshal ConfChange: %v", err)
			}
			c.node.ApplyConfChange(cc)
			if cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == c.self.ID {
				return errors.New("got ConfChange that removed me from the cluster; terminating")
			}
		}
	}

	return nil
}

func (c *ctrl) readyAdvance() {
	c.node.Advance()
}

// makeRaftPeer converts a net.Addr into a raft.Peer with no context.
// All peers must perform the Addr-to-Peer mapping in the same way.
// We assume peer_name_mac and treat the uint64 as the raft.Peer.ID.
// To break this coupling, we would need to hash the addr.String()
// and maintain two lookup tables: hash-to-PeerName, and ID-to-hash.
func makeRaftPeer(addr net.Addr) raft.Peer {
	return raft.Peer{
		ID:      uint64(addr.(meshconn.MeshAddr).PeerName),
		Context: nil, // TODO(pb): ??
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
