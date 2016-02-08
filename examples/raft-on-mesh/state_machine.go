package main

import (
	"errors"

	"github.com/coreos/etcd/raft/raftpb"
)

// stateMachine is the behavior required by the raft.Node
// and should be implemented by e.g. an etcd server.
type stateMachine interface {
	applySnapshot(raftpb.Snapshot) error
	applyCommittedEntry(raftpb.Entry) error
}

type surrogateStateMachine struct{}

var _ stateMachine = surrogateStateMachine{}

func (surrogateStateMachine) applySnapshot(raftpb.Snapshot) error {
	return errors.New("surrogate state machine")
}

func (surrogateStateMachine) applyCommittedEntry(raftpb.Entry) error {
	return errors.New("surrogate state machine")
}
