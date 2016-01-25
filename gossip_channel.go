package mesh

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
)

// GossipChannel is a logical communication channel within a physical mesh.
// TODO(pb): does this need to be exported?
type gossipChannel struct {
	name     string
	ourself  *localPeer
	routes   *routes
	gossiper Gossiper
}

// NewGossipChannel returns a named, usable channel.
// It delegates receiving duties to the passed Gossiper.
// TODO(pb): does this need to be exported?
func newGossipChannel(channelName string, ourself *localPeer, r *routes, g Gossiper) *gossipChannel {
	return &gossipChannel{
		name:     channelName,
		ourself:  ourself,
		routes:   r,
		gossiper: g,
	}
}

func (router *Router) handleGossip(tag protocolTag, payload []byte) error {
	decoder := gob.NewDecoder(bytes.NewReader(payload))
	var channelName string
	if err := decoder.Decode(&channelName); err != nil {
		return err
	}
	channel := router.gossipChannel(channelName)
	var srcName PeerName
	if err := decoder.Decode(&srcName); err != nil {
		return err
	}
	switch tag {
	case ProtocolGossipUnicast:
		return channel.deliverUnicast(srcName, payload, decoder)
	case ProtocolGossipBroadcast:
		return channel.deliverBroadcast(srcName, payload, decoder)
	case ProtocolGossip:
		return channel.deliver(srcName, payload, decoder)
	}
	return nil
}

func (c *gossipChannel) deliverUnicast(srcName PeerName, origPayload []byte, dec *gob.Decoder) error {
	var destName PeerName
	if err := dec.Decode(&destName); err != nil {
		return err
	}
	if c.ourself.Name == destName {
		var payload []byte
		if err := dec.Decode(&payload); err != nil {
			return err
		}
		return c.gossiper.OnGossipUnicast(srcName, payload)
	}
	if err := c.relayUnicast(destName, origPayload); err != nil {
		c.log(err)
	}
	return nil
}

func (c *gossipChannel) deliverBroadcast(srcName PeerName, _ []byte, dec *gob.Decoder) error {
	var payload []byte
	if err := dec.Decode(&payload); err != nil {
		return err
	}
	data, err := c.gossiper.OnGossipBroadcast(srcName, payload)
	if err != nil || data == nil {
		return err
	}
	c.relayBroadcast(srcName, data)
	return nil
}

func (c *gossipChannel) deliver(srcName PeerName, _ []byte, dec *gob.Decoder) error {
	var payload []byte
	if err := dec.Decode(&payload); err != nil {
		return err
	}
	update, err := c.gossiper.OnGossip(payload)
	if err != nil || update == nil {
		return err
	}
	c.relay(srcName, update)
	return nil
}

// GossipUnicast implements Gossip, relaying msg to dst, which must be a
// member of the channel.
func (c *gossipChannel) GossipUnicast(dstPeerName PeerName, msg []byte) error {
	return c.relayUnicast(dstPeerName, gobEncode(c.name, c.ourself.Name, dstPeerName, msg))
}

// GossipBroadcast implements Gossip, relaying update to all members of the
// channel.
func (c *gossipChannel) GossipBroadcast(update GossipData) {
	c.relayBroadcast(c.ourself.Name, update)
}

// Send relays data into the channel topology via random neighbours.
func (c *gossipChannel) Send(data GossipData) {
	c.relay(c.ourself.Name, data)
}

// SendDown relays data into the channel topology via conn.
func (c *gossipChannel) SendDown(conn Connection, data GossipData) {
	c.senderFor(conn).Send(data)
}

func (c *gossipChannel) relayUnicast(dstPeerName PeerName, buf []byte) (err error) {
	if relayPeerName, found := c.routes.UnicastAll(dstPeerName); !found {
		err = fmt.Errorf("unknown relay destination: %s", dstPeerName)
	} else if conn, found := c.ourself.ConnectionTo(relayPeerName); !found {
		err = fmt.Errorf("unable to find connection to relay peer %s", relayPeerName)
	} else {
		err = conn.(protocolSender).SendProtocolMsg(protocolMsg{ProtocolGossipUnicast, buf})
	}
	return err
}

func (c *gossipChannel) relayBroadcast(srcName PeerName, update GossipData) {
	c.routes.EnsureRecalculated()
	for _, conn := range c.ourself.ConnectionsTo(c.routes.BroadcastAll(srcName)) {
		c.senderFor(conn).Broadcast(srcName, update)
	}
}

func (c *gossipChannel) relay(srcName PeerName, data GossipData) {
	c.routes.EnsureRecalculated()
	for _, conn := range c.ourself.ConnectionsTo(c.routes.RandomNeighbours(srcName)) {
		c.senderFor(conn).Send(data)
	}
}

func (c *gossipChannel) senderFor(conn Connection) *gossipSender {
	return conn.(gossipConnection).gossipSenders().Sender(c.name, c.makeGossipSender)
}

func (c *gossipChannel) makeGossipSender(sender protocolSender, stop <-chan struct{}) *gossipSender {
	return newGossipSender(c.makeMsg, c.makeBroadcastMsg, sender, stop)
}

func (c *gossipChannel) makeMsg(msg []byte) protocolMsg {
	return protocolMsg{ProtocolGossip, gobEncode(c.name, c.ourself.Name, msg)}
}

func (c *gossipChannel) makeBroadcastMsg(srcName PeerName, msg []byte) protocolMsg {
	return protocolMsg{ProtocolGossipBroadcast, gobEncode(c.name, srcName, msg)}
}

func (c *gossipChannel) log(args ...interface{}) {
	log.Println(append(append([]interface{}{}, "[gossip "+c.name+"]:"), args...)...)
}

// GobEncode gob-encodes each item and returns the resulting byte slice.
// TODO(pb): does this need to be exported?
func gobEncode(items ...interface{}) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	for _, i := range items {
		if err := enc.Encode(i); err != nil {
			log.Fatal(err)
		}
	}
	return buf.Bytes()
}
