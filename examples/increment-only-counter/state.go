package main

import (
	"encoding/json"
	"sync"

	"github.com/weaveworks/mesh"
)

// An increment-only counter for an arbitrary keyspace.
type state struct {
	mtx sync.RWMutex
	set map[string]int
}

// state implements GossipData.
var _ mesh.GossipData = &state{}

// Construct an empty state object, ready to receive updates.
// This is suitable to use at program start.
// Other peers will populate us with data.
func newState() *state {
	return &state{
		set: map[string]int{},
	}
}

// Encode serializes our complete state to a slice of byte-slices.
// In this simple example, we use a single JSON-encoded buffer.
func (st *state) Encode() [][]byte {
	st.mtx.RLock()
	defer st.mtx.RUnlock()
	buf, err := json.Marshal(st.set)
	if err != nil {
		panic(err)
	}
	return [][]byte{buf}
}

// Merge merges the other GossipData into this one,
// and returns our resulting, complete state.
func (st *state) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	return st.mergeComplete(other.(*state).copy().set)
}

func (st *state) copy() *state {
	st.mtx.RLock()
	defer st.mtx.RUnlock()
	return &state{
		set: st.set,
	}
}

// Merge the set into our state, abiding increment-only semantics.
// Return a non-nil mesh.GossipData representation of the received set.
func (st *state) mergeReceived(set map[string]int) (received mesh.GossipData) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	for k, v := range set {
		if v <= st.set[k] {
			delete(set, k) // optimization: make the forwarded data smaller
			continue
		}
		st.set[k] = v
	}

	return &state{
		set: set, // all remaining elements were novel to us
	}
}

// Merge the set into our state, abiding increment-only semantics.
// Return any key/values that have been mutated, or nil if nothing changed.
func (st *state) mergeDelta(set map[string]int) (delta mesh.GossipData) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	for k, v := range set {
		if v <= st.set[k] {
			delete(set, k) // requirement: it's not part of a delta
			continue
		}
		st.set[k] = v
	}

	if len(set) <= 0 {
		return nil // per OnGossip requirements
	}
	return &state{
		set: set, // all remaining elements were novel to us
	}
}

// Merge the set into our state, abiding increment-only semantics.
// Return our resulting, complete state.
func (st *state) mergeComplete(set map[string]int) (complete mesh.GossipData) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	for k, v := range set {
		if v > st.set[k] {
			st.set[k] = v
		}
	}

	return &state{
		set: st.set, // n.b. can't .copy() due to lock contention
	}
}
