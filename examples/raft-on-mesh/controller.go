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

// The packetTransport owns the net.PacketConn, delivers messages to the
// stepper, and takes messages via the send method. Similarly the controller
// owns the packetTransport, delivers state changes to the state machine, and
// takes proposals via some future propose method.
//
// Architecture diagram:
//
// +----------+   +-----------------+   +-------------------------+   +------------------+
// | meshconn |   | packetTransport |   | controler               |   | stateMachine     |
// |          |   |                 |   |                         |   |           +----+ |
// |  ReadFrom@------------->stepper|-->@step-----.   .--->applyer|-->@apply----->|    | |
// |          |   |                 |   |         |   |           |   |           |    | |
// |          |   |                 |   |         v   |           |   |           |    |---> GET /key
// |          |   |                 |   |      [raft.Node]        |   |           |    |---> WATCH /key
// |          |   |                 |   |         |   ^           |   |           |    |<--- POST /key
// |          |   |                 |   |         |   |           |   |           |    | |
// |   WriteTo@<----------------send@<--|sender<--'   '----propose@<--|proposer<--|    | |
// |          |   |                 |   |                         |   |           +----+ |
// +----------+   +-----------------+   +-------------------------+   +------------------+

// controller is an etcd-flavoured Raft controller.
// It takes ownership of a net.PacketConn, used as a transport.
// It constructs and drives a raft.Peer, saving to a raft.MemoryStorage.
// (It doesn't persist to disk, by design: on death, all state is lost.)
// It forwards messages to the stateMachine, which should implement something useful.
type controller struct {
	sender  sender // the only part of the transport that we care about
	self    raft.Peer
	others  []raft.Peer
	storage *raft.MemoryStorage
	node    raft.Node
	applyer applyer // the only part of the state machine we care about
	logger  *log.Logger
	quit    chan struct{}
}

// sender is the downstream behavior required by the raft.Node.
// sender includes stop, because (as an implementation detail)
// the controller takes ownership of the sender. This could be changed.
// sender should be implemented by the transport.
type sender interface {
	send(msg raftpb.Message) error
	stop()
}

// applyer is the upstream behavior required by the raft.Node.
// applyer should be implemented by whatever provides the biz logic and API.
type applyer interface {
	applySnapshot(raftpb.Snapshot) error
	applyCommittedEntry(raftpb.Entry) error
}

const heartbeatTick = 250 // ms

func newController(s sender, self net.Addr, others []net.Addr, a applyer, logger *log.Logger) *controller {
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
		sender:  s,
		self:    makeRaftPeer(self),
		others:  makeRaftPeers(others),
		storage: storage,
		node:    node,
		applyer: a,
		logger:  logger,
		quit:    make(chan struct{}),
	}
	go c.driveRaft()
	return c
}

func (c *controller) Step(ctx wackycontext.Context, msg raftpb.Message) error {
	return c.node.Step(ctx, msg) // TODO(pb): is this safe to do concurrently?
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
				c.logger.Printf("controller: when handling ready message: %v", err)
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

func (c *controller) readySave(snapshot raftpb.Snapshot, hardState raftpb.HardState, entries []raftpb.Entry) error {
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
	if err := c.applyer.applySnapshot(snapshot); err != nil {
		return fmt.Errorf("apply snapshot: %v", err)
	}
	for _, committedEntry := range committedEntries {
		if err := c.applyer.applyCommittedEntry(committedEntry); err != nil {
			return fmt.Errorf("apply committed entry: %v", err)
		}
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

func (c *controller) readyAdvance() {
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
