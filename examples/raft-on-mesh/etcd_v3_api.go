package main

import (
	"errors"

	wackycontext "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	wackygrpc "github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
)

func newV3Server(s *store, options ...wackygrpc.ServerOption) *wackygrpc.Server {
	srv := wackygrpc.NewServer(options...)
	etcdserverpb.RegisterKVServer(srv, kvServer{s})
	//etcdserverpb.RegisterAuthServer(srv, makeAuthServer(s))
	//etcdserverpb.RegisterClusterServer(srv, makeClusterServer(s))
	//etcdserverpb.RegisterKVServer(srv, makeKVServer(s))
	//etcdserverpb.RegisterLeaseServer(srv, makeLeaseServer(s))
	//etcdserverpb.RegisterMaintenanceServer(srv, makeMaintenanceServer(s))
	//etcdserverpb.RegisterWatchServer(srv, makeWatchServer(s))
	return srv
}

type kvServer struct {
	s *store
}

// Range gets the keys in the range from the store.
func (srv kvServer) Range(ctx wackycontext.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return nil, errors.New("not implemented")
}

// Put puts the given key into the store.
// A put request increases the revision of the store,
// and generates one event in the event history.
func (srv kvServer) Put(ctx wackycontext.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, errors.New("not implemented")
}

// Delete deletes the given range from the store.
// A delete request increase the revision of the store,
// and generates one event in the event history.
func (srv kvServer) DeleteRange(ctx wackycontext.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, errors.New("not implemented")
}

// Txn processes all the requests in one transaction.
// A txn request increases the revision of the store,
// and generates events with the same revision in the event history.
// It is not allowed to modify the same key several times within one txn.
func (srv kvServer) Txn(ctx wackycontext.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return nil, errors.New("not implemented")
}

// Compact compacts the event history in etcd. User should compact the
// event history periodically, or it will grow infinitely.
func (srv kvServer) Compact(ctx wackycontext.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return nil, errors.New("not implemented")
}

// Hash returns the hash of local KV state for consistency checking purpose.
// This is designed for testing purpose. Do not use this in production when there
// are ongoing transactions.
func (srv kvServer) Hash(ctx wackycontext.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return nil, errors.New("not implemented")
}
