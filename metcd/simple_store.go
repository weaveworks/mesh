package metcd

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/bernerdschaefer/eventsource"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gorilla/mux"

	"github.com/weaveworks/mesh"
	"github.com/weaveworks/mesh/meshconn"
)

type simpleStore struct {
	mtx        sync.RWMutex
	data       map[string]string
	watchers   map[string]map[chan<- string]struct{}
	proposalc  chan<- []byte
	snapshotc  <-chan raftpb.Snapshot
	entryc     <-chan raftpb.Entry
	confentryc chan<- raftpb.Entry
	actionc    chan func()
	quitc      chan struct{}
	logger     *log.Logger
}

func newSimpleStore(
	proposalc chan<- []byte,
	snapshotc <-chan raftpb.Snapshot,
	entryc <-chan raftpb.Entry,
	confentryc chan<- raftpb.Entry,
	logger *log.Logger,
) *simpleStore {
	s := &simpleStore{
		data:       map[string]string{},
		watchers:   map[string]map[chan<- string]struct{}{},
		proposalc:  proposalc,
		snapshotc:  snapshotc,
		entryc:     entryc,
		confentryc: confentryc,
		actionc:    make(chan func()),
		quitc:      make(chan struct{}),
		logger:     logger,
	}
	go s.loop()
	return s
}

func (s *simpleStore) loop() {
	for {
		select {
		case snapshot := <-s.snapshotc:
			if err := s.applySnapshot(snapshot); err != nil {
				s.logger.Printf("simple store: apply snapshot: %v", err)
			}

		case entry := <-s.entryc:
			if err := s.applyCommittedEntry(entry); err != nil {
				s.logger.Printf("simple store: apply committed entry: %v", err)
			}

		case f := <-s.actionc:
			f()

		case <-s.quitc:
			return
		}
	}
}

func (s *simpleStore) applySnapshot(snapshot raftpb.Snapshot) error {
	if len(snapshot.Data) == 0 {
		//s.logger.Printf("simple store: apply snapshot with empty snapshot; skipping")
		return nil
	}
	s.logger.Printf("simple store: applying snapshot: size %d", len(snapshot.Data))
	s.logger.Printf("simple store: applying snapshot: metadata %s", snapshot.Metadata.String())
	if err := json.Unmarshal(snapshot.Data, &s.data); err != nil {
		return err
	}
	return nil
}

func (s *simpleStore) applyCommittedEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		break
	case raftpb.EntryConfChange:
		s.logger.Printf("simple store: forwarding ConfChange entry")
		s.confentryc <- entry
		return nil
	default:
		s.logger.Printf("simple store: got unknown entry type %s", entry.Type)
		return fmt.Errorf("unknown entry type %d", entry.Type)
	}

	// entry.Size can be nonzero when len(entry.Data) == 0
	if len(entry.Data) <= 0 {
		s.logger.Printf("simple store: got empty committed entry (term %d, index %d, type %s); skipping", entry.Term, entry.Index, entry.Type)
		return nil
	}

	var single map[string]string
	if err := json.Unmarshal(entry.Data, &single); err != nil {
		s.logger.Printf("simple store: unmarshaling entry.Data (%s): %v", entry.Data, err)
		return err
	}
	if n := len(single); n != 1 {
		s.logger.Printf("simple store: got entry with %d keys; strange", n)
	}

	s.logger.Printf("simple store: applying committed entry %v", single)

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

func (s *simpleStore) set(key, value string) error {
	buf, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return err
	}
	s.proposalc <- buf
	return nil
}

func (s *simpleStore) get(key string) (value string, err error) {
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

func (s *simpleStore) watch(key string, results chan<- string) (cancel chan<- struct{}, err error) {
	ready := make(chan struct{})
	s.actionc <- func() {
		defer close(ready)
		if _, ok := s.watchers[key]; !ok {
			s.watchers[key] = map[chan<- string]struct{}{} // first watcher for this key
		}
		s.watchers[key][results] = struct{}{} // register the update chan
		s.logger.Printf("simple store: watch key %q", key)
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

func (s *simpleStore) unwatch(key string, c chan<- string) {
	s.actionc <- func() {
		if _, ok := s.watchers[key]; !ok {
			s.logger.Printf("simple store: unwatch key %q had no watchers; strange", key)
			return
		}
		if s.watchers[key] == nil {
			s.logger.Printf("simple store: unwatch key %q revealed nil map; logic error", key)
			return
		}
		if _, ok := s.watchers[key][c]; !ok {
			s.logger.Printf("simple store: unwatch key %q with missing chan; strange", key)
			return
		}
		delete(s.watchers[key], c)
		if len(s.watchers[key]) == 0 {
			delete(s.watchers, key)
		}
		s.logger.Printf("simple store: unwatch key %q", key)
	}
}

type simpleAPI interface {
	get(key string) (string, error)
	watch(key string, results chan<- string) (cancel chan<- struct{}, err error)
	set(key, value string) error
}

func makeHandler(router *mesh.Router, peer *meshconn.Peer, s simpleAPI, logger *log.Logger) http.Handler {
	r := mux.NewRouter()
	r.Methods("GET").Path(`/status`).HandlerFunc(handleGetStatus(router, logger))
	r.Methods("GET").Path(`/get/{key}`).HandlerFunc(handleGetKey(s, logger))
	r.Methods("GET").Path(`/watch/{key:.+}`).HandlerFunc(handleWatchKey(s, logger))
	r.Methods("POST").Path("/set/{key:.+}/{val}").HandlerFunc(handleSetKey(s, logger))
	r.Methods("POST").Path("/raw").HandlerFunc(handlePostRaw(peer, logger))
	return r
}

func handleGetStatus(router *mesh.Router, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var n int
		for _, description := range router.Peers.Descriptions() {
			fmt.Fprintf(
				w,
				"%s (%s) Self=%v NumConnections=%d\n",
				description.Name.String(),
				description.NickName,
				description.Self,
				description.NumConnections,
			)
			n++
		}
		logger.Printf("%s %s %s: %d peer(s)", r.RemoteAddr, r.Method, r.RequestURI, n)
	}
}

func handlePostRaw(peer *meshconn.Peer, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dstStr := r.FormValue("dst")
		if dstStr == "" {
			http.Error(w, "no dst specified", http.StatusBadRequest)
			return
		}
		dst, err := mesh.PeerNameFromUserInput(dstStr)
		if err != nil {
			http.Error(w, "dst: "+err.Error(), http.StatusBadRequest)
			return
		}
		msg := r.FormValue("msg")
		if _, err := peer.WriteTo([]byte(msg), meshconn.MeshAddr{PeerName: dst}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Printf("%s %s %s: %d to %s OK (%s)", r.RemoteAddr, r.Method, r.RequestURI, len(msg), dst, msg)
		fmt.Fprintf(w, "%s: %q (%d) OK\n", dst, msg, len(msg))
	}
}

func handleWatchKey(s simpleAPI, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		eventsource.Handler(func(lastID string, enc *eventsource.Encoder, stop <-chan bool) {
			key := mux.Vars(r)["key"]
			logger.Printf("handler: %s watch %q connected", r.RemoteAddr, key)
			defer logger.Printf("handler: %s watch %q disconnected", r.RemoteAddr, key)

			results := make(chan string)
			cancel, err := s.watch(key, results)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer close(cancel)

			var id int

			val, err := s.get(key)
			if err == nil {
				enc.Encode(eventsource.Event{
					Type: "initial",
					ID:   strconv.Itoa(id),
					Data: []byte(val),
				})
			} else {
				enc.Encode(eventsource.Event{
					Type: "initial",
					ID:   strconv.Itoa(id),
					Data: []byte(err.Error()),
				})

			}
			id++

			for {
				select {
				case val := <-results:
					enc.Encode(eventsource.Event{
						Type: "update",
						ID:   strconv.Itoa(id),
						Data: []byte(val),
					})
					id++

				case <-stop:
					return
				}
			}
		}).ServeHTTP(w, r)
	}
}

func handleGetKey(s simpleAPI, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		val, err := s.get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "%q = %q\n", key, val)
	}
}

func handleSetKey(s simpleAPI, logger *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, val := mux.Vars(r)["key"], mux.Vars(r)["val"]
		if err := s.set(key, val); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%q = %q proposed\n", key, val)
	}
}
