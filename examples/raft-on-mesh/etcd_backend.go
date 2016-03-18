package main

import (
	"github.com/coreos/etcd/storage/backend"
)

func newEtcdBackend() backend.Backend {
	return &etcdBackend{}
}

// In-memory implementation of backend.Backend.
// The default is unsuitable because it's tightly coupled to the disk.
type etcdBackend struct{}

func (b *etcdBackend) BatchTx() backend.BatchTx {
	return nil // TODO(pb)
}

func (b *etcdBackend) Snapshot() backend.Snapshot {
	return nil // TODO(pb)
}

func (b *etcdBackend) Hash() (uint32, error) {
	return 0, nil // TODO(pb)
}

func (b *etcdBackend) Size() int64 {
	return 0 // TODO(pb)
}

func (b *etcdBackend) Defrag() error {
	return nil // TODO(pb)
}

func (b *etcdBackend) ForceCommit() {
	return // TODO(pb)
}

func (b *etcdBackend) Close() error {
	return nil // TODO(pb)
}
