package main

import (
	"errors"
	"log"
	"net"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

// The packetTransport owns the net.PacketConn, delivers messages to the
// stepper, and takes messages via the send method. Similarly the controller
// owns the packetTransport, delivers state changes to the state machine, and
// takes proposals via some future propose method.

// controller is an etcd-flavoured Raft controller.
// It takes ownership of a net.PacketConn, used as a transport.
// It constructs and drives a raft.Peer, saving to a raft.MemoryStorage.
// (It doesn't persist to disk, by design: on death, all state is lost.)
// It forwards messages to the stateMachine, which should implement something useful.
type controller struct {
	sender  msgSender // the only part of the transport that we care about
	self    raft.Peer
	others  []raft.Peer
	storage *raft.MemoryStorage
	node    raft.Node
	sm      stateMachine
	logger  *log.Logger
	quit    chan struct{}
}

const heartbeatTick = 250 // ms

func newController(conn net.PacketConn, self net.Addr, others []net.Addr, logger *log.Logger) *controller {
	storage := raft.NewMemoryStorage()
	nodeConfig := &raft.Config{
		ID:              makeRaftPeer(self).ID,
		ElectionTick:    10 * heartbeatTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		Applied:         0,    // starting fresh
		MaxSizePerMsg:   4096, // TODO(pb): looks like bytes; confirm that
		MaxInflightMsgs: 256,  // TODO(pb): copied from docs; confirm that
		CheckQuorum:     true, // leader steps down if quorum is not active for an electionTimeout
		Logger:          &raft.DefaultLogger{Logger: logger},
	}
	node := raft.StartNode(nodeConfig, makeRaftPeers(others))
	c := &controller{
		sender:  newPacketTransport(conn, node, logger),
		self:    makeRaftPeer(self),
		others:  makeRaftPeers(others),
		storage: storage,
		node:    node,
		sm:      surrogateStateMachine{}, // TODO(pb): real state machine!
		logger:  logger,
		quit:    make(chan struct{}),
	}
	go c.driveRaft()
	return c
}

func (c *controller) driveRaft() {
	// Now that we are holding on to a Node, we have a few responsibilities.
	// See https://godoc.org/github.com/coreos/etcd/raft
	ticker := time.NewTicker((heartbeatTick / 10) * time.Millisecond)
	defer ticker.Stop()
	defer c.sender.stop()
	for {
		select {
		case <-ticker.C:
			c.node.Tick()
		case r := <-c.node.Ready():
			if err := c.handleReady(r); err != nil {
				c.logger.Printf("controller: %v", err)
				return
			}
		case <-c.quit:
			return
		}
	}
}

func (c *controller) stop() {
	close(c.quit)
}

func (c *controller) handleReady(r raft.Ready) error {
	// These steps may be performed in parallel, except as noted in step 2.
	//
	// 1. Write HardState, Entries, and Snapshot to persistent storage if they are
	// not empty. Note that when writing an Entry with Index i, any
	// previously-persisted entries with Index >= i must be discarded.
	if err := c.readySave(r.Snapshot, r.HardState, r.Entries); err != nil {
		return err
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
		return err
	}

	// 4. Call Node.Advance() to signal readiness for the next batch of updates.
	// This may be done at any time after step 1, although all updates must be
	// processed in the order they were returned by Ready.
	c.readyAdvance()

	return nil
}

func (c *controller) readySave(snapshot raftpb.Snapshot, hardState raftpb.HardState, entries []raftpb.Entry) error {
	// For the moment, none of these steps persist to disk. That violates some Raft
	// invariants. But we are ephemeral, and will always boot empty, willingly
	// paying the snapshot cost. I trust that that the etcd Raft implementation
	// permits this.
	if !raft.IsEmptySnap(snapshot) {
		if err := c.storage.ApplySnapshot(snapshot); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(hardState) {
		if err := c.storage.SetHardState(hardState); err != nil {
			return err
		}
	}
	if err := c.storage.Append(entries); err != nil {
		return err
	}
	return nil
}

func (c *controller) readySend(msgs []raftpb.Message) {
	for _, msg := range msgs {
		err := c.sender.send(msg)
		if err != nil {
			c.logger.Printf("controller: send: %v", err) // this will happen sometimes
		}

		if msg.Type == raftpb.MsgSnap {
			status := raft.SnapshotFinish
			if err != nil {
				status = raft.SnapshotFailure
			}
			c.node.ReportSnapshot(msg.To, status)
		}
	}
}

func (c *controller) readyApply(snapshot raftpb.Snapshot, committedEntries []raftpb.Entry) error {
	if err := c.sm.applySnapshot(snapshot); err != nil {
		return err
	}
	for _, committedEntry := range committedEntries {
		if err := c.sm.applyCommittedEntry(committedEntry); err != nil {
			return err
		}
		if committedEntry.Type == raftpb.EntryConfChange {
			// See raftexample raftNode.publishEntries
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(committedEntry.Data); err != nil {
				return err
			}
			c.node.ApplyConfChange(cc)
			if cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == c.self.ID {
				return errors.New("got ConfChange that removed me from the cluster; terminating")
			}
		}
	}
	return nil
}

func (c *controller) readyAdvance() {
	c.node.Advance()
}

// stateMachine is the behavior required by the raft.Node.
// It should be implemented by whatever provides the biz logic and API.
type stateMachine interface {
	applySnapshot(raftpb.Snapshot) error
	applyCommittedEntry(raftpb.Entry) error
}

type surrogateStateMachine struct{}

var _ stateMachine = surrogateStateMachine{}

func (surrogateStateMachine) applySnapshot(raftpb.Snapshot) error {
	return errors.New("surrogate state machine")
}

func (surrogateStateMachine) applyCommittedEntry(raftpb.Entry) error {
	return errors.New("surrogate state machine")
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
