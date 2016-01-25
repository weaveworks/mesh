package mesh

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

// Connection is the commonality of LocalConnection and RemoteConnection.
// TODO(pb): does this need to be exported?
type connection interface {
	Local() *Peer
	Remote() *Peer
	RemoteTCPAddr() string
	Outbound() bool
	Established() bool
	BreakTie(connection) connectionTieBreak
	Shutdown(error)
	Log(args ...interface{})
}

// ConnectionTieBreak describes the outcome of a tiebreaking contest between
// two connections.
// TODO(pb): does this need to be exported?
type connectionTieBreak int

const (
	// TieBreakWon indicates the candidate has won the tiebreak.
	// TODO(pb): does this need to be exported?
	tieBreakWon connectionTieBreak = iota

	// TieBreakLost indicates the candidate has lost the tiebreak.
	// TODO(pb): does this need to be exported?
	tieBreakLost

	// TieBreakTied indicates the tiebreaking contest had no winner.
	// TODO(pb): does this need to be exported?
	tieBreakTied
)

// ErrConnectToSelf will be unexported soon.
// TODO(pb): does this need to be exported?
var ErrConnectToSelf = fmt.Errorf("Cannot connect to ourself")

// RemoteConnection is a local representation of the remote side of a
// connection. It has limited capabilities compared to LocalConnection.
// TODO(pb): does this need to be exported?
type remoteConnection struct {
	local         *Peer
	remote        *Peer
	remoteTCPAddr string
	outbound      bool
	established   bool
}

// LocalConnection is the local side of a connection. It implements
// ProtocolSender, and manages per-channel GossipSenders.
// TODO(pb): does this need to be exported?
type LocalConnection struct {
	sync.RWMutex
	remoteConnection
	TCPConn         *net.TCPConn
	TrustRemote     bool // is remote on a trusted subnet?
	TrustedByRemote bool // does remote trust us?
	version         byte
	tcpSender       tCPSender
	SessionKey      *[32]byte
	heartbeatTCP    *time.Ticker
	Router          *Router
	uid             uint64
	actionChan      chan<- connectionAction
	errorChan       chan<- error
	finished        <-chan struct{} // closed to signal that actorLoop has finished
	OverlayConn     OverlayConnection
	gossipSenders   *gossipSenders
}

// GossipConnection describes something that can yield multiple GossipSenders.
// TODO(pb): does this need to be exported?
type gossipConnection interface {
	GossipSenders() *gossipSenders
}

// ConnectionAction is the actor closure used by LocalConnection. If an action
// returns an error, it will terminate the actor loop, which terminates the
// connection in turn.
// TODO(pb): does this need to be exported?
type connectionAction func() error

// NewRemoteConnection returns a usable RemoteConnection.
func newRemoteConnection(from, to *Peer, tcpAddr string, outbound bool, established bool) *remoteConnection {
	return &remoteConnection{
		local:         from,
		remote:        to,
		remoteTCPAddr: tcpAddr,
		outbound:      outbound,
		established:   established,
	}
}

// Local implements Connection.
func (conn *remoteConnection) Local() *Peer { return conn.local }

// Remote implements Connection.
func (conn *remoteConnection) Remote() *Peer { return conn.remote }

// RemoteTCPAddr implements Connection.
func (conn *remoteConnection) RemoteTCPAddr() string { return conn.remoteTCPAddr }

// Outbound implements Connection.
func (conn *remoteConnection) Outbound() bool { return conn.outbound }

// Established implements Connection.
func (conn *remoteConnection) Established() bool { return conn.established }

// BreakTie implements Connection.
func (conn *remoteConnection) BreakTie(connection) connectionTieBreak { return tieBreakTied }

// Shutdown implements Connection.
func (conn *remoteConnection) Shutdown(error) {}

// Log implements Connection.
func (conn *remoteConnection) Log(args ...interface{}) {
	log.Println(append(append([]interface{}{}, fmt.Sprintf("->[%s|%s]:", conn.remoteTCPAddr, conn.remote)), args...)...)
}

// StartLocalConnection does not return anything. If the connection is
// successful, it will end up in the local peer's connections map.
func startLocalConnection(connRemote *remoteConnection, tcpConn *net.TCPConn, router *Router, acceptNewPeer bool) {
	if connRemote.local != router.Ourself.Peer {
		log.Fatal("Attempt to create local connection from a peer which is not ourself")
	}
	actionChan := make(chan connectionAction, ChannelSize)
	errorChan := make(chan error, 1)
	finished := make(chan struct{})
	conn := &LocalConnection{
		remoteConnection: *connRemote, // NB, we're taking a copy of connRemote here.
		Router:           router,
		TCPConn:          tcpConn,
		TrustRemote:      router.Trusts(connRemote),
		uid:              randUint64(),
		actionChan:       actionChan,
		errorChan:        errorChan,
		finished:         finished,
	}
	conn.gossipSenders = newGossipSenders(conn, finished)
	go conn.run(actionChan, errorChan, finished, acceptNewPeer)
}

// BreakTie conducts a tiebreaking contest between two connections.
func (conn *LocalConnection) BreakTie(dupConn connection) connectionTieBreak {
	dupConnLocal := dupConn.(*LocalConnection)
	// conn.uid is used as the tie breaker here, in the knowledge that
	// both sides will make the same decision.
	if conn.uid < dupConnLocal.uid {
		return tieBreakWon
	} else if dupConnLocal.uid < conn.uid {
		return tieBreakLost
	}
	return tieBreakTied
}

// Established returns true if the connection is established.
// TODO(pb): data race?
func (conn *LocalConnection) Established() bool {
	return conn.established
}

// SendProtocolMsg implements ProtocolSender.
func (conn *LocalConnection) SendProtocolMsg(m ProtocolMsg) error {
	if err := conn.sendProtocolMsg(m); err != nil {
		conn.Shutdown(err)
		return err
	}
	return nil
}

// GossipSenders implements GossipConnection.
func (conn *LocalConnection) GossipSenders() *gossipSenders {
	return conn.gossipSenders
}

// ACTOR methods

// NB: The conn.* fields are only written by the connection actor
// process, which is the caller of the ConnectionAction funs. Hence we
// do not need locks for reading, and only need write locks for fields
// read by other processes.

// Shutdown is non-blocking.
// TODO(pb): must be?
func (conn *LocalConnection) Shutdown(err error) {
	// err should always be a real error, even if only io.EOF
	if err == nil {
		panic("nil error")
	}

	select {
	case conn.errorChan <- err:
	default:
	}
}

// Send an actor request to the actorLoop, but don't block if
// actorLoop has exited - see http://blog.golang.org/pipelines for
// pattern
func (conn *LocalConnection) sendAction(action connectionAction) {
	select {
	case conn.actionChan <- action:
	case <-conn.finished:
	}
}

// ACTOR server

func (conn *LocalConnection) run(actionChan <-chan connectionAction, errorChan <-chan error, finished chan<- struct{}, acceptNewPeer bool) {
	var err error // important to use this var and not create another one with 'err :='
	defer func() { conn.shutdown(err) }()
	defer close(finished)

	if err = conn.TCPConn.SetLinger(0); err != nil {
		return
	}

	intro, err := protocolIntroParams{
		MinVersion: conn.Router.ProtocolMinVersion,
		MaxVersion: ProtocolMaxVersion,
		Features:   conn.makeFeatures(),
		Conn:       conn.TCPConn,
		Password:   conn.Router.Password,
		Outbound:   conn.outbound,
	}.DoIntro()
	if err != nil {
		return
	}

	conn.SessionKey = intro.SessionKey
	conn.tcpSender = intro.Sender
	conn.version = intro.Version

	remote, err := conn.parseFeatures(intro.Features)
	if err != nil {
		return
	}

	if err = conn.registerRemote(remote, acceptNewPeer); err != nil {
		return
	}

	conn.Log("connection ready; using protocol version", conn.version)

	// only use negotiated session key for untrusted connections
	var sessionKey *[32]byte
	if conn.Untrusted() {
		sessionKey = conn.SessionKey
	}

	params := OverlayConnectionParams{
		RemotePeer:         conn.remote,
		LocalAddr:          conn.TCPConn.LocalAddr().(*net.TCPAddr),
		RemoteAddr:         conn.TCPConn.RemoteAddr().(*net.TCPAddr),
		Outbound:           conn.outbound,
		ConnUID:            conn.uid,
		SessionKey:         sessionKey,
		SendControlMessage: conn.sendOverlayControlMessage,
		Features:           intro.Features,
	}
	if conn.OverlayConn, err = conn.Router.Overlay.PrepareConnection(params); err != nil {
		return
	}

	// As soon as we do AddConnection, the new connection becomes
	// visible to the packet routing logic.  So AddConnection must
	// come after PrepareConnection
	if err = conn.Router.Ourself.AddConnection(conn); err != nil {
		return
	}
	conn.Router.ConnectionMaker.ConnectionCreated(conn)

	// OverlayConnection confirmation comes after AddConnection,
	// because only after that completes do we know the connection is
	// valid: in particular that it is not a duplicate connection to
	// the same peer. Overlay communication on a duplicate connection
	// can cause problems such as tripping up overlay crypto at the
	// other end due to data being decoded by the other connection. It
	// is also generally wasteful to engage in any interaction with
	// the remote on a connection that turns out to be invalid.
	conn.OverlayConn.Confirm()

	// receiveTCP must follow also AddConnection. In the absence
	// of any indirect connectivity to the remote peer, the first
	// we hear about it (and any peers reachable from it) is
	// through topology gossip it sends us on the connection. We
	// must ensure that the connection has been added to Ourself
	// prior to processing any such gossip, otherwise we risk
	// immediately gc'ing part of that newly received portion of
	// the topology (though not the remote peer itself, since that
	// will have a positive ref count), leaving behind dangling
	// references to peers. Hence we must invoke AddConnection,
	// which is *synchronous*, first.
	conn.heartbeatTCP = time.NewTicker(TCPHeartbeat)
	go conn.receiveTCP(intro.Receiver)

	// AddConnection must precede actorLoop. More precisely, it
	// must precede shutdown, since that invokes DeleteConnection
	// and is invoked on termination of this entire
	// function. Essentially this boils down to a prohibition on
	// running AddConnection in a separate goroutine, at least not
	// without some synchronisation. Which in turn requires the
	// launching of the receiveTCP goroutine to precede actorLoop.
	err = conn.actorLoop(actionChan, errorChan)
}

func (conn *LocalConnection) makeFeatures() map[string]string {
	features := map[string]string{
		"PeerNameFlavour": PeerNameFlavour,
		"Name":            conn.local.Name.String(),
		"NickName":        conn.local.NickName,
		"ShortID":         fmt.Sprint(conn.local.ShortID),
		"UID":             fmt.Sprint(conn.local.UID),
		"ConnID":          fmt.Sprint(conn.uid),
		"Trusted":         fmt.Sprint(conn.TrustRemote),
	}
	conn.Router.Overlay.AddFeaturesTo(features)
	return features
}

type features map[string]string

// TODO(pb): don't export
func (features features) MustHave(keys []string) error {
	for _, key := range keys {
		if _, ok := features[key]; !ok {
			return fmt.Errorf("Field %s is missing", key)
		}
	}
	return nil
}

// TODO(pb): don't export
func (features features) Get(key string) string {
	return features[key]
}

func (conn *LocalConnection) parseFeatures(features features) (*Peer, error) {
	if err := features.MustHave([]string{"PeerNameFlavour", "Name", "NickName", "UID", "ConnID"}); err != nil {
		return nil, err
	}

	remotePeerNameFlavour := features.Get("PeerNameFlavour")
	if remotePeerNameFlavour != PeerNameFlavour {
		return nil, fmt.Errorf("Peer name flavour mismatch (ours: '%s', theirs: '%s')", PeerNameFlavour, remotePeerNameFlavour)
	}

	name, err := PeerNameFromString(features.Get("Name"))
	if err != nil {
		return nil, err
	}

	nickName := features.Get("NickName")

	var shortID uint64
	var hasShortID bool
	if shortIDStr, present := features["ShortID"]; present {
		hasShortID = true
		shortID, err = strconv.ParseUint(shortIDStr, 10, PeerShortIDBits)
		if err != nil {
			return nil, err
		}
	}

	var trusted bool
	if trustedStr, present := features["Trusted"]; present {
		trusted, err = strconv.ParseBool(trustedStr)
		if err != nil {
			return nil, err
		}
	}
	conn.TrustedByRemote = trusted

	uid, err := parsePeerUID(features.Get("UID"))
	if err != nil {
		return nil, err
	}

	remoteConnID, err := strconv.ParseUint(features.Get("ConnID"), 10, 64)
	if err != nil {
		return nil, err
	}

	conn.uid ^= remoteConnID
	peer := newPeer(name, nickName, uid, 0, PeerShortID(shortID))
	peer.HasShortID = hasShortID
	return peer, nil
}

func (conn *LocalConnection) registerRemote(remote *Peer, acceptNewPeer bool) error {
	if acceptNewPeer {
		conn.remote = conn.Router.Peers.FetchWithDefault(remote)
	} else {
		conn.remote = conn.Router.Peers.FetchAndAddRef(remote.Name)
		if conn.remote == nil {
			return fmt.Errorf("Found unknown remote name: %s at %s", remote.Name, conn.remoteTCPAddr)
		}
	}

	if conn.remote == conn.local {
		return ErrConnectToSelf
	}

	return nil
}

func (conn *LocalConnection) actorLoop(actionChan <-chan connectionAction, errorChan <-chan error) (err error) {
	fwdErrorChan := conn.OverlayConn.ErrorChannel()
	fwdEstablishedChan := conn.OverlayConn.EstablishedChannel()

	for err == nil {
		select {
		case err = <-errorChan:
		case err = <-fwdErrorChan:
		default:
			select {
			case action := <-actionChan:
				err = action()
			case <-conn.heartbeatTCP.C:
				err = conn.sendSimpleProtocolMsg(ProtocolHeartbeat)
			case <-fwdEstablishedChan:
				conn.established = true
				fwdEstablishedChan = nil
				conn.Router.Ourself.ConnectionEstablished(conn)
			case err = <-errorChan:
			case err = <-fwdErrorChan:
			}
		}
	}

	return
}

func (conn *LocalConnection) shutdown(err error) {
	if conn.remote == nil {
		log.Printf("->[%s] connection shutting down due to error during handshake: %v", conn.remoteTCPAddr, err)
	} else {
		conn.Log("connection shutting down due to error:", err)
	}

	if conn.TCPConn != nil {
		if err := conn.TCPConn.Close(); err != nil {
			log.Printf("warning: %v", err)
		}
	}

	if conn.remote != nil {
		conn.Router.Peers.Dereference(conn.remote)
		conn.Router.Ourself.DeleteConnection(conn)
	}

	if conn.heartbeatTCP != nil {
		conn.heartbeatTCP.Stop()
	}

	if conn.OverlayConn != nil {
		conn.OverlayConn.Stop()
	}

	conn.Router.ConnectionMaker.ConnectionTerminated(conn, err)
}

func (conn *LocalConnection) sendOverlayControlMessage(tag byte, msg []byte) error {
	return conn.sendProtocolMsg(ProtocolMsg{protocolTag(tag), msg})
}

// Helpers

func (conn *LocalConnection) sendSimpleProtocolMsg(tag protocolTag) error {
	return conn.sendProtocolMsg(ProtocolMsg{tag: tag})
}

func (conn *LocalConnection) sendProtocolMsg(m ProtocolMsg) error {
	return conn.tcpSender.Send(append([]byte{byte(m.tag)}, m.msg...))
}

func (conn *LocalConnection) receiveTCP(receiver tCPReceiver) {
	var err error
	for {
		if err = conn.extendReadDeadline(); err != nil {
			break
		}
		var msg []byte
		if msg, err = receiver.Receive(); err != nil {
			break
		}
		if len(msg) < 1 {
			conn.Log("ignoring blank msg")
			continue
		}
		if err = conn.handleProtocolMsg(protocolTag(msg[0]), msg[1:]); err != nil {
			break
		}
	}
	conn.Shutdown(err)
}

func (conn *LocalConnection) handleProtocolMsg(tag protocolTag, payload []byte) error {
	switch tag {
	case ProtocolHeartbeat:
	case ProtocolReserved1, ProtocolReserved2, ProtocolReserved3, ProtocolOverlayControlMsg:
		conn.OverlayConn.ControlMessage(byte(tag), payload)
	case ProtocolGossipUnicast, ProtocolGossipBroadcast, ProtocolGossip:
		return conn.Router.handleGossip(tag, payload)
	default:
		conn.Log("ignoring unknown protocol tag:", tag)
	}
	return nil
}

func (conn *LocalConnection) extendReadDeadline() error {
	return conn.TCPConn.SetReadDeadline(time.Now().Add(TCPHeartbeat * 2))
}

// Untrusted returns true if either we don't trust our remote, or are not
// trusted by our remote.
// TODO(pb): does this need to be exported?
func (conn *LocalConnection) Untrusted() bool {
	return !conn.TrustRemote || !conn.TrustedByRemote
}
