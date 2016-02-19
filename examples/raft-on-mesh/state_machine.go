package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
)

type stateMachine struct {
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

func newStateMachine(
	snapshotc chan raftpb.Snapshot,
	entryc chan raftpb.Entry,
	proposalc chan []byte,
	logger *log.Logger,
) *stateMachine {
	sm := &stateMachine{
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

func (s *stateMachine) loop() {
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

func (s *stateMachine) applySnapshot(snapshot raftpb.Snapshot) error {
	if len(snapshot.Data) == 0 {
		s.logger.Printf("state machine: apply snapshot with empty snapshot; skipping")
		return nil
	}
	s.logger.Printf("state machine: applying snapshot: size %d", len(snapshot.Data))
	s.logger.Printf("state machine: applying snapshot: metadata %s", snapshot.Metadata.String())
	if err := json.Unmarshal(snapshot.Data, &s.data); err != nil {
		return err
	}
	return nil
}

func (s *stateMachine) applyCommittedEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		break
	case raftpb.EntryConfChange:
		s.logger.Printf("state machine: ignoring ConfChange")
		return nil
	default:
		s.logger.Printf("state machine: got unknown entry type %s", entry.Type)
		return fmt.Errorf("unknown entry type %d", entry.Type)
	}

	var single map[string]string
	if err := json.Unmarshal(entry.Data, &single); err != nil {
		return err
	}
	if n := len(single); n != 1 {
		s.logger.Printf("state machine: got entry with %d keys; strange", n)
	}

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

func (s *stateMachine) post(key, value string) error {
	buf, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return err
	}
	s.proposalc <- buf
	return nil
}

func (s *stateMachine) get(key string) (value string, err error) {
	s.actionc <- func() {
		if v, ok := s.data[key]; ok {
			value = v
		} else {
			err = fmt.Errorf("%q not found", key)
		}
	}
	return value, err
}

func (s *stateMachine) watch(key string, results chan<- string) (cancel chan<- struct{}, err error) {
	s.actionc <- func() {
		if _, ok := s.watchers[key]; !ok {
			s.watchers[key] = map[chan<- string]struct{}{} // first watcher for this key
		}
		s.watchers[key][results] = struct{}{} // register the update chan
		c := make(chan struct{})
		go func() {
			<-c                     // when the user cancels the watch,
			s.unwatch(key, results) // unwatch the key,
			close(results)          // and close the results chan
		}()
		cancel = c
	}
	return cancel, err
}

func (s *stateMachine) unwatch(key string, c chan<- string) {
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
	}
}
