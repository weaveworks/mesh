package mesh

// SurrogateGossiper ignores unicasts and relays broadcasts and gossips.
// TODO(pb): should this be exported?
type SurrogateGossiper struct{}

var _ Gossiper = &SurrogateGossiper{}

// OnGossipUnicast implements Gossiper.
func (*SurrogateGossiper) OnGossipUnicast(sender PeerName, msg []byte) error {
	return nil
}

// OnGossipBroadcast implements Gossiper.
func (*SurrogateGossiper) OnGossipBroadcast(_ PeerName, update []byte) (GossipData, error) {
	return NewSurrogateGossipData(update), nil
}

// Gossip implements Gossiper.
func (*SurrogateGossiper) Gossip() GossipData {
	return nil
}

// OnGossip implements Gossiper.
func (*SurrogateGossiper) OnGossip(update []byte) (GossipData, error) {
	return NewSurrogateGossipData(update), nil
}

// TODO(pb): remove?
var (
	surrogateGossiper SurrogateGossiper
)

// SurrogateGossipData is a simple in-memory GossipData.
// TODO(pb): should this be exported?
type SurrogateGossipData struct {
	messages [][]byte
}

var _ GossipData = &SurrogateGossipData{}

// NewSurrogateGossipData returns a new SurrogateGossipData.
func NewSurrogateGossipData(msg []byte) *SurrogateGossipData {
	return &SurrogateGossipData{messages: [][]byte{msg}}
}

// Encode implements GossipData.
func (d *SurrogateGossipData) Encode() [][]byte {
	return d.messages
}

// Merge implements GossipData.
func (d *SurrogateGossipData) Merge(other GossipData) GossipData {
	o := other.(*SurrogateGossipData)
	messages := make([][]byte, 0, len(d.messages)+len(o.messages))
	messages = append(messages, d.messages...)
	messages = append(messages, o.messages...)
	return &SurrogateGossipData{messages: messages}
}
