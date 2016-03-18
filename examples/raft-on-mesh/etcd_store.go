package main

import (
	"errors"
	"fmt"
	"log"

	wackyproto "github.com/coreos/etcd/Godeps/_workspace/src/github.com/gogo/protobuf/proto"
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
	pending  map[uint64]chan<- interface{}
}

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

	var irr etcdserverpb.InternalRaftRequest
	if err := irr.Unmarshal(entry.Data); err != nil {
		s.logger.Printf("etcd server: unmarshaling entry data: %v", err)
		return err
	}

	msg, err := s.applyInternalRaftRequest(irr)
	if err != nil {
		s.logger.Printf("etcd server: applying internal Raft request %d: %v", irr.ID, err)
		s.cancelPending(irr.ID)
		return err
	}

	s.signalPending(irr.ID, msg)
	return nil
}

// From public API method, to proposalc.
func (s *etcdStore) proposeInternalRaftRequest(irr etcdserverpb.InternalRaftRequest) error {
	data, err := irr.Marshal()
	if err != nil {
		return err
	}
	// TODO(pb): wire up pending
	s.proposalc <- data
	return nil
}

func (s *etcdStore) cancelInternalRaftRequest(irr etcdserverpb.InternalRaftRequest) error {
	// TODO(pb): manage pending
	return nil
}

// From committed entryc, back to public API method.
// etcdserver/v3demo_server.go applyV3Result
func (s *etcdStore) applyInternalRaftRequest(irr etcdserverpb.InternalRaftRequest) (wackyproto.Message, error) {
	switch {
	case irr.Range != nil:
		return s.applyRange(irr.Range)
	case irr.Put != nil:
		return s.applyPut(irr.Put)
	case irr.DeleteRange != nil:
		return s.applyDeleteRange(irr.DeleteRange)
	case irr.Txn != nil:
		return s.applyTxn(irr.Txn)
	case irr.Compaction != nil:
		return s.applyCompaction(irr.Compaction)
	default:
		return nil, fmt.Errorf("internal Raft request type not implemented")
	}
}

func (s *etcdStore) applyRange(req *etcdserverpb.RangeRequest) (wackyproto.Message, error) {
	// TODO(pb)
	return nil, nil
}

func (s *etcdStore) applyPut(req *etcdserverpb.PutRequest) (wackyproto.Message, error) {
	// TODO(pb)
	return nil, nil
}

func (s *etcdStore) applyDeleteRange(req *etcdserverpb.DeleteRangeRequest) (wackyproto.Message, error) {
	// TODO(pb)
	return nil, nil
}

func (s *etcdStore) applyTxn(req *etcdserverpb.TxnRequest) (wackyproto.Message, error) {
	// TODO(pb)
	return nil, nil
}

func (s *etcdStore) applyCompaction(req *etcdserverpb.CompactionRequest) (wackyproto.Message, error) {
	// TODO(pb)
	return nil, nil
}

func (s *etcdStore) signalPending(id uint64, msg wackyproto.Message) {
	// TODO(pb)
}

func (s *etcdStore) cancelPending(id uint64) {
	// TODO(pb)
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
