package main

import (
	"testing"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/examples/meshconn"
)

func TestMakeRaftPeerDifference(t *testing.T) {
	s1 := "00:00:00:00:00:01"
	a1 := meshconn.MeshAddr{PeerName: mustPeerNameFromUserInput(s1)}
	p1 := makeRaftPeer(a1)

	s2 := "00:00:00:00:00:02"
	a2 := meshconn.MeshAddr{PeerName: mustPeerNameFromUserInput(s2)}
	p2 := makeRaftPeer(a2)

	if p1.ID == p2.ID {
		t.Fatalf("%s (%d) == %s (%d)", s1, p1, s2, p2)
	}
}

func mustPeerNameFromUserInput(input string) mesh.PeerName {
	peerName, err := mesh.PeerNameFromUserInput(input)
	if err != nil {
		panic(err)
	}
	return peerName
}
