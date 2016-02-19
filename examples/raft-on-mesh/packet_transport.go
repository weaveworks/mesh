package main

import (
	"fmt"
	"log"
	"net"

	wackycontext "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

// packetTransport takes ownership of the net.PacketConn.
// It must be started with a stepper, to receive messages.
// Incoming messages are unmarshaled and forwarded to the stepper.
// Outgoing messages are marshaled and sent via the net.PacketConn.
type packetTransport struct {
	conn     net.PacketConn
	incoming stepper
	logger   *log.Logger
}

type stepper interface {
	Step(wackycontext.Context, raftpb.Message) error
}

func newPacketTransport(conn net.PacketConn, logger *log.Logger) *packetTransport {
	return &packetTransport{
		conn:   conn,
		logger: logger,
	}
}

func (t *packetTransport) start(s stepper) {
	t.incoming = s
	go t.recv()
}

func (t *packetTransport) stop() {
	t.conn.Close()
}

func (t *packetTransport) recv() {
	defer t.conn.Close()
	const maxRecvLen = 8192
	b := make([]byte, maxRecvLen)
	for {
		n, remote, err := t.conn.ReadFrom(b)
		if err != nil {
			t.logger.Printf("packet transport: recv: %s: %v (aborting)", remote, err)
			return
		} else if n >= cap(b) {
			t.logger.Printf("packet transport: recv: %s: short read (%d) (continuing)", remote, n)
			continue
		}
		var msg raftpb.Message
		if err := msg.Unmarshal(b); err != nil {
			t.logger.Printf("packet transport: recv: %s: %v (continuing)", remote, err)
			continue
		}
		t.incoming.Step(wackycontext.TODO(), msg)
	}
}

func (t packetTransport) send(msg raftpb.Message) error {
	b, err := msg.Marshal()
	if err != nil {
		return err
	}
	dst := meshconn.MeshAddr{PeerName: mesh.PeerName(msg.To)}
	if n, err := t.conn.WriteTo(b, dst); err != nil {
		return err
	} else if n < len(b) {
		return fmt.Errorf("short write (%d)", len(b))
	}
	t.logger.Printf("packet transport: send: %d to %s OK", len(b), dst)
	return nil
}
