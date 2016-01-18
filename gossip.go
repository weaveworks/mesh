package mesh

import (
	"fmt"
	"sync"
)

// Gossip is the sending interface.
// TODO(pb): rename to e.g. Sender
type Gossip interface {
	// GossipUnicast emits a single message to a peer in the mesh.
	// TODO(pb): rename to Unicast?
	//
	// Unicast takes []byte instead of GossipData because "to date there has
	// been no compelling reason [in practice] to do merging on unicast."
	// But there may be some motivation to have unicast Mergeable; see
	// https://github.com/weaveworks/weave/issues/1764
	// TODO(pb): for uniformity of interface, rather take GossipData?
	GossipUnicast(dst PeerName, msg []byte) error

	// GossipBroadcast emits a message to all peers in the mesh.
	// TODO(pb): rename to Broadcast?
	GossipBroadcast(update GossipData) error
}

// Gossiper is the receiving interface.
// TODO(pb): rename to e.g. Receiver
type Gossiper interface {
	// OnGossipUnicast merges received data into state.
	// TODO(pb): rename to e.g. OnUnicast
	OnGossipUnicast(src PeerName, msg []byte) error

	// OnGossipBroadcast merges received data into state and returns a
	// representation of the received data, for further propagation.
	// TODO(pb): rename to e.g. OnBroadcast
	OnGossipBroadcast(src PeerName, update []byte) (GossipData, error)

	// Gossip returns the state of everything we know; gets called periodically.
	Gossip() GossipData

	// OnGossip merges received data into state and returns "everything new
	// I've just learnt", or nil if nothing in the received data was new.
	OnGossip(msg []byte) (GossipData, error)
}

// GossipData is a merge-able dataset.
// Think: log-structured data.
type GossipData interface {
	// Encode encodes the data into multiple byte-slices.
	Encode() [][]byte

	// Merge combines another GossipData into this one and returns the result.
	// TODO(pb): does it need to be leave the original unmodified?
	Merge(GossipData) GossipData
}

// GossipSender accumulates GossipData that needs to be sent to one
// destination, and sends it when possible. GossipSender is one-to-one with a
// channel.
type GossipSender struct {
	sync.Mutex
	makeMsg          func(msg []byte) ProtocolMsg
	makeBroadcastMsg func(srcName PeerName, msg []byte) ProtocolMsg
	sender           ProtocolSender
	gossip           GossipData
	broadcasts       map[PeerName]GossipData
	more             chan<- struct{}
	flush            chan<- chan<- bool // for testing
}

// NewGossipSender constructs a usable GossipSender.
func NewGossipSender(
	makeMsg func(msg []byte) ProtocolMsg,
	makeBroadcastMsg func(srcName PeerName, msg []byte) ProtocolMsg,
	sender ProtocolSender,
	stop <-chan struct{},
) *GossipSender {
	more := make(chan struct{}, 1)
	flush := make(chan chan<- bool)
	s := &GossipSender{
		makeMsg:          makeMsg,
		makeBroadcastMsg: makeBroadcastMsg,
		sender:           sender,
		broadcasts:       make(map[PeerName]GossipData),
		more:             more,
		flush:            flush}
	go s.run(stop, more, flush)
	return s
}

// TODO(pb): no need to parameterize more and flush
func (s *GossipSender) run(stop <-chan struct{}, more <-chan struct{}, flush <-chan chan<- bool) {
	sent := false
	for {
		select {
		case <-stop:
			return
		case <-more:
			sentSomething, err := s.deliver(stop)
			if err != nil {
				return
			}
			sent = sent || sentSomething
		case ch := <-flush: // for testing
			// send anything pending, then reply back whether we sent
			// anything since previous flush
			select {
			case <-more:
				sentSomething, err := s.deliver(stop)
				if err != nil {
					return
				}
				sent = sent || sentSomething
			default:
			}
			ch <- sent
			sent = false
		}
	}
}

func (s *GossipSender) deliver(stop <-chan struct{}) (bool, error) {
	sent := false
	// We must not hold our lock when sending, since that would block
	// the callers of Send/Broadcast while we are stuck waiting for
	// network congestion to clear. So we pick and send one piece of
	// data at a time, only holding the lock during the picking.
	for {
		select {
		case <-stop:
			return sent, nil
		default:
		}
		data, makeProtocolMsg := s.pick()
		if data == nil {
			return sent, nil
		}
		for _, msg := range data.Encode() {
			if err := s.sender.SendProtocolMsg(makeProtocolMsg(msg)); err != nil {
				return sent, err
			}
		}
		sent = true
	}
}

func (s *GossipSender) pick() (data GossipData, makeProtocolMsg func(msg []byte) ProtocolMsg) {
	s.Lock()
	defer s.Unlock()
	switch {
	case s.gossip != nil: // usually more important than broadcasts
		data = s.gossip
		makeProtocolMsg = s.makeMsg
		s.gossip = nil
	case len(s.broadcasts) > 0:
		for srcName, d := range s.broadcasts {
			data = d
			makeProtocolMsg = func(msg []byte) ProtocolMsg { return s.makeBroadcastMsg(srcName, msg) }
			delete(s.broadcasts, srcName)
			break
		}
	}
	return
}

// Send accumulates the GossipData and will send it eventually.
// Send and Broadcast accumulate into different buckets.
func (s *GossipSender) Send(data GossipData) {
	s.Lock()
	defer s.Unlock()
	if s.empty() {
		defer s.prod()
	}
	if s.gossip == nil {
		s.gossip = data
	} else {
		s.gossip = s.gossip.Merge(data)
	}
}

// Broadcast accumulates the GossipData under the given srcName and will send
// it eventually. Send and Broadcast accumulate into different buckets.
func (s *GossipSender) Broadcast(srcName PeerName, data GossipData) {
	s.Lock()
	defer s.Unlock()
	if s.empty() {
		defer s.prod()
	}
	d, found := s.broadcasts[srcName]
	if !found {
		s.broadcasts[srcName] = data
	} else {
		s.broadcasts[srcName] = d.Merge(data)
	}
}

func (s *GossipSender) empty() bool { return s.gossip == nil && len(s.broadcasts) == 0 }

func (s *GossipSender) prod() {
	select {
	case s.more <- void:
	default:
	}
}

// Flush sends all pending data, and returns true if anything was sent since
// the previous flush. For testing.
func (s *GossipSender) Flush() bool {
	ch := make(chan bool)
	s.flush <- ch
	return <-ch
}

// GossipSenders wraps a ProtocolSender (e.g. a LocalConnection) and yields
// per-channel GossipSenders.
// TODO(pb): may be able to remove this and use makeGossipSender directly
type GossipSenders struct {
	sync.Mutex
	sender  ProtocolSender
	stop    <-chan struct{}
	senders map[string]*GossipSender
}

// NewGossipSenders returns a usable GossipSenders leveraging the ProtocolSender.
// TODO(pb): is stop chan the best way to do that?
func NewGossipSenders(sender ProtocolSender, stop <-chan struct{}) *GossipSenders {
	return &GossipSenders{sender: sender, stop: stop, senders: make(map[string]*GossipSender)}
}

// Sender yields the GossipSender for the named channel.
// It will use the factory function if no sender yet exists.
func (gs *GossipSenders) Sender(channelName string, makeGossipSender func(sender ProtocolSender, stop <-chan struct{}) *GossipSender) *GossipSender {
	gs.Lock()
	defer gs.Unlock()
	s, found := gs.senders[channelName]
	if !found {
		s = makeGossipSender(gs.sender, gs.stop)
		gs.senders[channelName] = s
	}
	return s
}

// Flush flushes all managed senders. Used for testing.
func (gs *GossipSenders) Flush() bool {
	sent := false
	gs.Lock()
	defer gs.Unlock()
	for _, sender := range gs.senders {
		sent = sender.Flush() || sent
	}
	return sent
}

// GossipChannels is an index of channel name to gossip channel.
// TODO(pb): does this need to be exported?
type GossipChannels map[string]*GossipChannel

// NewGossip constructs and returns a usable GossipChannel from the router.
// TODO(pb): rename?
// TODO(pb): move all of these methods to router.go
func (router *Router) NewGossip(channelName string, g Gossiper) Gossip {
	channel := NewGossipChannel(channelName, router.Ourself, router.Routes, g)
	router.gossipLock.Lock()
	defer router.gossipLock.Unlock()
	if _, found := router.gossipChannels[channelName]; found {
		checkFatal(fmt.Errorf("[gossip] duplicate channel %s", channelName))
	}
	router.gossipChannels[channelName] = channel
	return channel
}

func (router *Router) gossipChannel(channelName string) *GossipChannel {
	router.gossipLock.RLock()
	channel, found := router.gossipChannels[channelName]
	router.gossipLock.RUnlock()
	if found {
		return channel
	}
	router.gossipLock.Lock()
	defer router.gossipLock.Unlock()
	if channel, found = router.gossipChannels[channelName]; found {
		return channel
	}
	channel = NewGossipChannel(channelName, router.Ourself, router.Routes, &surrogateGossiper)
	channel.log("created surrogate channel")
	router.gossipChannels[channelName] = channel
	return channel
}

func (router *Router) gossipChannelSet() map[*GossipChannel]struct{} {
	channels := make(map[*GossipChannel]struct{})
	router.gossipLock.RLock()
	defer router.gossipLock.RUnlock()
	for _, channel := range router.gossipChannels {
		channels[channel] = void
	}
	return channels
}

// SendAllGossip relays all pending gossip data for each channel via random
// neighbours.
func (router *Router) SendAllGossip() {
	for channel := range router.gossipChannelSet() {
		if gossip := channel.gossiper.Gossip(); gossip != nil {
			channel.Send(gossip)
		}
	}
}

// SendAllGossipDown relays all pending gossip data for each channel via conn.
func (router *Router) SendAllGossipDown(conn Connection) {
	for channel := range router.gossipChannelSet() {
		if gossip := channel.gossiper.Gossip(); gossip != nil {
			channel.SendDown(conn, gossip)
		}
	}
}

// for testing
func (router *Router) sendPendingGossip() bool {
	sentSomething := false
	for conn := range router.Ourself.Connections() {
		sentSomething = conn.(GossipConnection).GossipSenders().Flush() || sentSomething
	}
	return sentSomething
}
