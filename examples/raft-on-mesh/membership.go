package main

import (
	"sort"
	"time"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/weaveworks/mesh"
)

func membershipLoop(router *mesh.Router, confchangec chan<- raftpb.ConfChange) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var (
		members = membership(router)
		add     []uint64
		rem     []uint64
	)

	for range ticker.C {
		members, add, rem = diff(members, membership(router))
		for _, nodeID := range add {
			confchangec <- raftpb.ConfChange{
				ID:      uint64(router.Ourself.Peer.Name),
				Type:    raftpb.ConfChangeAddNode,
				NodeID:  nodeID,
				Context: []byte{}, // TODO(pb): is this right?
			}
		}
		for _, nodeID := range rem {
			confchangec <- raftpb.ConfChange{
				ID:      uint64(router.Ourself.Peer.Name),
				Type:    raftpb.ConfChangeRemoveNode,
				NodeID:  nodeID,
				Context: []byte{}, // TODO(pb): is this right?
			}
		}
	}
}

func membership(router *mesh.Router) []uint64 {
	descriptions := router.Peers.Descriptions()
	members := make([]uint64, len(descriptions))
	for i, description := range descriptions {
		members[i] = uint64(description.Name)
	}
	sort.Sort(uint64Slice(members))
	return members
}

func diff(prev, curr []uint64) (add, rem, next []uint64) {
	// TODO(pb)
	return []uint64{}, []uint64{}, curr
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
