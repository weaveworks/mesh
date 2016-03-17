package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	wackycontext "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	wackygrpc "github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/raft/raftpb"
)

func newEtcdStore(proposalc chan<- []byte, snapshotc <-chan raftpb.Snapshot, entryc <-chan raftpb.Entry, logger *log.Logger) *etcdStore {
	s := &etcdStore{
		proposalc: proposalc,
		snapshotc: snapshotc,
		entryc:    entryc,
		actionc:   make(chan func()),
		quitc:     make(chan struct{}),
		logger:    logger,

		revision: 1,
		data:     map[string]string{},
	}
	go s.loop()
	return s
}

// Transport-agnostic reimplementation of coreos/etcd/etcdserver.
//
// Almost all methods need to go through the Raft log, i.e. they should only
// return to the client once they've been committed. So, those methods create
// serializable requests, which are registered and then proposed. Committed
// proposals trigger their registered channel with a response.
type etcdStore struct {
	proposalc chan<- []byte
	snapshotc <-chan raftpb.Snapshot
	entryc    <-chan raftpb.Entry
	actionc   chan func()
	quitc     chan struct{}
	logger    *log.Logger

	revision uint64            // of the store, incremented for each invocation
	data     map[string]string // current state of the store
}

// This type is a serializable union of all methods.
// ID is used to correlate responses to the client.
// Analogous to etcdserverpb.Request.
type invocation struct {
	ID  uint64 `json:"id"` // unique to the requesting peer, used to correlate responses
	Ops []op   `json:"ops"`
}

type op struct {
	Method   string    `json:"method"`
	Key      string    `json:"key,omitempty"`      // for single keys (XOR Range)
	Range    [2]string `json:"range,omitempty"`    // for ranges of keys (XOR Key)
	Value    []byte    `json:"value,omitempty"`    // for Put only
	Revision uint64    `json:"revision,omitempty"` // for Compact only
}

const (
	methodGet     = "get"
	methodPut     = "put"
	methodDelete  = "delete"
	methodCompact = "compact"
)

func (s *etcdStore) loop() {
	for {
		select {
		case snapshot := <-s.snapshotc:
			if err := s.applySnapshot(snapshot); err != nil {
				s.logger.Printf("etcd server: apply snapshot: %v", err)
			}

		case entry := <-s.entryc:
			if err := s.applyCommittedEntry(entry); err != nil {
				s.logger.Printf("etcd server: apply committed entry: %v", err)
			}

		case f := <-s.actionc:
			f()

		case <-s.quitc:
			return
		}
	}
}

func (s *etcdStore) applySnapshot(snapshot raftpb.Snapshot) error {
	if len(snapshot.Data) == 0 {
		//s.logger.Printf("etcd server: apply snapshot with empty snapshot; skipping")
		return nil
	}

	s.logger.Printf("etcd server: applying snapshot: size %d", len(snapshot.Data))
	s.logger.Printf("etcd server: applying snapshot: metadata %s", snapshot.Metadata.String())
	s.logger.Printf("etcd server: applying snapshot: TODO") // TODO(pb)

	return nil
}

func (s *etcdStore) applyCommittedEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		break
	case raftpb.EntryConfChange:
		s.logger.Printf("etcd server: ignoring ConfChange entry")
		return nil

	default:
		s.logger.Printf("etcd server: got unknown entry type %s", entry.Type)
		return fmt.Errorf("unknown entry type %d", entry.Type)
	}

	// entry.Size can be nonzero when len(entry.Data) == 0
	if len(entry.Data) <= 0 {
		s.logger.Printf("etcd server: got empty committed entry (term %d, index %d, type %s); skipping", entry.Term, entry.Index, entry.Type)
		return nil
	}

	var inv invocation
	if err := json.Unmarshal(entry.Data, &inv); err != nil {
		s.logger.Printf("etcd server: unmarshaling entry.Data (%s): %v", entry.Data, err)
		return err
	}
	if err := s.applyInvocation(inv); err != nil {
		s.logger.Printf("etcd server: applying invocation %d: %v", inv.ID, err)
		return err
	}

	return nil
}

func (s *etcdStore) applyInvocation(inv invocation) error {
	// TODO(pb)
	for _, op := range inv.Ops {
		switch op.Method {
		case methodGet:
		case methodPut:
		case methodDelete:
		case methodCompact:
		default:
			return fmt.Errorf("unsupported method %q", op.Method)
		}
	}
	return nil
}

// Range gets the keys in the range from the store.
func (s *etcdStore) Range(ctx wackycontext.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return nil, errors.New("not implemented")
}

// Put puts the given key into the store.
// A put request increases the revision of the store,
// and generates one event in the event history.
func (s *etcdStore) Put(ctx wackycontext.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, errors.New("not implemented")
}

// Delete deletes the given range from the store.
// A delete request increase the revision of the store,
// and generates one event in the event history.
func (s *etcdStore) DeleteRange(ctx wackycontext.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, errors.New("not implemented")
}

// Txn processes all the requests in one transaction.
// A txn request increases the revision of the store,
// and generates events with the same revision in the event history.
// It is not allowed to modify the same key several times within one txn.
func (s *etcdStore) Txn(ctx wackycontext.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return nil, errors.New("not implemented")
}

// Compact compacts the event history in s. User should compact the
// event history periodically, or it will grow infinitely.
func (s *etcdStore) Compact(ctx wackycontext.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return nil, errors.New("not implemented")
}

// Hash returns the hash of local KV state for consistency checking purpose.
// This is designed for testing purpose. Do not use this in production when there
// are ongoing transactions.
func (s *etcdStore) Hash(ctx wackycontext.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return nil, errors.New("not implemented")
}

func grpcServer(s *etcdStore, options ...wackygrpc.ServerOption) *wackygrpc.Server {
	srv := wackygrpc.NewServer(options...)
	etcdserverpb.RegisterKVServer(srv, s)
	//etcdserverpb.RegisterAuthServer(srv, makeAuthServer(s))
	//etcdserverpb.RegisterClusterServer(srv, makeClusterServer(s))
	//etcdserverpb.RegisterKVServer(srv, makeKVServer(s))
	//etcdserverpb.RegisterLeaseServer(srv, makeLeaseServer(s))
	//etcdserverpb.RegisterMaintenanceServer(srv, makeMaintenanceServer(s))
	//etcdserverpb.RegisterWatchServer(srv, makeWatchServer(s))
	return srv
}
