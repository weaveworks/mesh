package main

import (
	"log"
	"net"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

// packetTransport takes ownership of the net.PacketConn.
// Incoming messages are unmarshaled from the conn and send to incomingc.
// Outgoing messages are received from outgoingc and marshaled to the conn.
type packetTransport struct {
	conn      net.PacketConn
	incomingc chan<- raftpb.Message // to controller
	outgoingc <-chan raftpb.Message // from controller
	logger    *log.Logger
}

// TODO(pb): conf changes

func newPacketTransport(
	conn net.PacketConn,
	incomingc chan<- raftpb.Message,
	outgoingc <-chan raftpb.Message,
	logger *log.Logger,
) *packetTransport {
	t := &packetTransport{
		conn:      conn,
		incomingc: incomingc,
		outgoingc: outgoingc,
		logger:    logger,
	}
	go t.recvLoop()
	go t.sendLoop()
	return t
}

func (t *packetTransport) stop() {
	t.conn.Close()
}

func (t *packetTransport) recvLoop() {
	defer t.logger.Printf("packet transport: recv loop exit")
	const maxRecvLen = 8192
	b := make([]byte, maxRecvLen)
	for {
		n, remote, err := t.conn.ReadFrom(b)
		if err != nil {
			t.logger.Printf("packet transport: recv from %s: %v (aborting)", remote, err)
			return
		} else if n >= cap(b) {
			t.logger.Printf("packet transport: recv from %s: short read, %d >= %d (continuing)", remote, n, cap(b))
			continue
		}
		var msg raftpb.Message
		if err := msg.Unmarshal(b); err != nil {
			t.logger.Printf("packet transport: recv from %s: %v (continuing)", remote, err)
			continue
		}
		t.logger.Printf("packet transport: recv from %s: msg size %d", remote, msg.Size())
		t.incomingc <- msg
	}
}

func (t *packetTransport) sendLoop() {
	defer t.logger.Printf("packet transport: send loop exit")
	for msg := range t.outgoingc {
		b, err := msg.Marshal()
		if err != nil {
			t.logger.Printf("packet transport: send: %v (continuing)", err)
			continue
		}
		dst := meshconn.MeshAddr{PeerName: mesh.PeerName(msg.To)}
		if n, err := t.conn.WriteTo(b, dst); err != nil {
			t.logger.Printf("packet transport: send to %s: %v (continuing)", dst, err)
			continue
		} else if n < len(b) {
			t.logger.Printf("packet transport: send to %s: short write, %d < %d (continuing)", dst, n, len(b))
			continue
		}
		t.logger.Printf("packet transport: send to %s: msg size %d (%d) OK", dst, msg.Size(), len(b))
	}
}
