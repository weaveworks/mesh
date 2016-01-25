// +build peer_name_mac !peer_name_alternative

package mesh

// The !peer_name_alternative effectively makes this the default,
// i.e. to choose an alternative, run
//
//   go build -tags 'peer_name_alternative peer_name_hash'
//
// Let peer names be MACs...
//
// MACs need to be unique across our network, or bad things will
// happen anyway. So they make pretty good candidates for peer
// names. And doing so is pretty efficient both computationally and
// network overhead wise.
//
// Note that we do not mandate *what* MAC should be used as the peer
// name. In particular it doesn't actually have to be the MAC of, say,
// the network interface the peer is sniffing on.

import (
	"net"
)

// PeerName is used as a map key. Since net.HardwareAddr isn't suitable for
// that - it's a slice, and slices can't be map keys - we convert that to/from
// uint64.
type PeerName uint64

const (
	// PeerNameFlavour is the type of peer names we use.
	PeerNameFlavour = "mac"
	// NameSize is the number of bytes in a peer name.
	NameSize = 6
	// UnknownPeerName is used as a sentinel value.
	UnknownPeerName = PeerName(0)
)

// PeerNameFromUserInput parses PeerName from a user-provided string.
// TODO(pb): does this need to be exported?
func PeerNameFromUserInput(userInput string) (PeerName, error) {
	return PeerNameFromString(userInput)
}

// PeerNameFromString parses PeerName from a generic string.
// TODO(pb): does this need to be exported?
func PeerNameFromString(nameStr string) (PeerName, error) {
	mac, err := net.ParseMAC(nameStr)
	if err != nil {
		return UnknownPeerName, err
	}
	return PeerName(macint(mac)), nil
}

// PeerNameFromBin parses PeerName from a byte slice.
// TODO(pb): does this need to be exported?
func PeerNameFromBin(nameByte []byte) PeerName {
	return PeerName(macint(net.HardwareAddr(nameByte)))
}

// Bin encodes PeerName as a byte slice.
func (name PeerName) bytes() []byte {
	return intmac(uint64(name))
}

// String encodes PeerName as a string.
func (name PeerName) String() string {
	return intmac(uint64(name)).String()
}

func macint(mac net.HardwareAddr) (r uint64) {
	for _, b := range mac {
		r <<= 8
		r |= uint64(b)
	}
	return
}

func intmac(key uint64) (r net.HardwareAddr) {
	r = make([]byte, 6)
	for i := 5; i >= 0; i-- {
		r[i] = byte(key)
		key >>= 8
	}
	return
}
