package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
)

type store struct {
	mtx       sync.RWMutex
	data      map[string]string
	watchers  map[string]map[chan<- string]struct{}
	snapshotc chan raftpb.Snapshot
	entryc    chan raftpb.Entry
	proposalc chan []byte
	actionc   chan func()
	quitc     chan struct{}
	logger    *log.Logger
}

func newStore(
	snapshotc chan raftpb.Snapshot,
	entryc chan raftpb.Entry,
	proposalc chan []byte,
	logger *log.Logger,
) *store {
	sm := &store{
		data:      map[string]string{},
		watchers:  map[string]map[chan<- string]struct{}{},
		snapshotc: snapshotc,
		entryc:    entryc,
		proposalc: proposalc,
		actionc:   make(chan func()),
		quitc:     make(chan struct{}),
		logger:    logger,
	}
	go sm.loop()
	return sm
}

func (s *store) loop() {
	for {
		select {
		case snapshot := <-s.snapshotc:
			if err := s.applySnapshot(snapshot); err != nil {
				s.logger.Printf("state machine: apply snapshot: %v", err)
			}

		case entry := <-s.entryc:
			if err := s.applyCommittedEntry(entry); err != nil {
				s.logger.Printf("state machine: apply committed entry: %v", err)
			}

		case f := <-s.actionc:
			f()

		case <-s.quitc:
			return
		}
	}
}

func (s *store) applySnapshot(snapshot raftpb.Snapshot) error {
	if len(snapshot.Data) == 0 {
		//s.logger.Printf("state machine: apply snapshot with empty snapshot; skipping")
		return nil
	}
	s.logger.Printf("state machine: applying snapshot: size %d", len(snapshot.Data))
	s.logger.Printf("state machine: applying snapshot: metadata %s", snapshot.Metadata.String())
	if err := json.Unmarshal(snapshot.Data, &s.data); err != nil {
		return err
	}
	return nil
}

func (s *store) applyCommittedEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		break
	case raftpb.EntryConfChange:
		s.logger.Printf("state machine: ignoring ConfChange entry")
		return nil
	default:
		s.logger.Printf("state machine: got unknown entry type %s", entry.Type)
		return fmt.Errorf("unknown entry type %d", entry.Type)
	}

	// entry.Size can be nonzero when len(entry.Data) == 0
	if len(entry.Data) <= 0 {
		s.logger.Printf("state machine: got empty committed entry (term %d, index %d, type %s); skipping", entry.Term, entry.Index, entry.Type)
		return nil
	}

	var single map[string]string
	if err := json.Unmarshal(entry.Data, &single); err != nil {
		s.logger.Printf("state machine: unmarshaling entry.Data (%s): %v", entry.Data, err)
		return err
	}
	if n := len(single); n != 1 {
		s.logger.Printf("state machine: got entry with %d keys; strange", n)
	}

	s.logger.Printf("state machine: applying committed entry %v", single)

	// TODO(pb): maybe early return?
	// TODO(pb): do I need to validate the index somehow?

	for k, v := range single {
		s.data[k] = v // set

		if m, ok := s.watchers[k]; ok {
			for c := range m {
				c <- v // notify (blocking)
			}
		}
	}

	return nil
}

func (s *store) set(key, value string) error {
	buf, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return err
	}
	s.proposalc <- buf
	return nil
}

func (s *store) get(key string) (value string, err error) {
	ready := make(chan struct{})
	s.actionc <- func() {
		defer close(ready)
		if v, ok := s.data[key]; ok {
			value = v
		} else {
			err = fmt.Errorf("%q not found", key)
		}
	}
	<-ready
	return value, err
}

func (s *store) watch(key string, results chan<- string) (cancel chan<- struct{}, err error) {
	ready := make(chan struct{})
	s.actionc <- func() {
		defer close(ready)
		if _, ok := s.watchers[key]; !ok {
			s.watchers[key] = map[chan<- string]struct{}{} // first watcher for this key
		}
		s.watchers[key][results] = struct{}{} // register the update chan
		s.logger.Printf("state machine: watch key %q", key)
		c := make(chan struct{})
		go func() {
			<-c                     // when the user cancels the watch,
			s.unwatch(key, results) // unwatch the key,
			close(results)          // and close the results chan
		}()
		cancel = c
	}
	<-ready
	return cancel, err
}

func (s *store) unwatch(key string, c chan<- string) {
	s.actionc <- func() {
		if _, ok := s.watchers[key]; !ok {
			s.logger.Printf("state machine: unwatch key %q had no watchers; strange", key)
			return
		}
		if s.watchers[key] == nil {
			s.logger.Printf("state machine: unwatch key %q revealed nil map; logic error", key)
			return
		}
		if _, ok := s.watchers[key][c]; !ok {
			s.logger.Printf("state machine: unwatch key %q with missing chan; strange", key)
			return
		}
		delete(s.watchers[key], c)
		if len(s.watchers[key]) == 0 {
			delete(s.watchers, key)
		}
		s.logger.Printf("state machine: unwatch key %q", key)
	}
}
