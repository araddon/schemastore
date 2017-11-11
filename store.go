// Package schemastore provides a schema sync utility to apply schema
// changes consistently (using Raft).
package schemastore

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var (
	// Ensure Store implements raft.FSM
	_ raft.FSM = (*Store)(nil)
)

// Store is a schema applyer store that allows cross-node schema
// consensus and replication.  Schema is synced via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string
	mu       sync.Mutex
	m        map[string]string // The key-value store for the system.
	raft     *raft.Raft        // raft consensus/coordination
	log      *zap.SugaredLogger
}

// New returns a new Store.
func New() *Store {
	return &Store{
		m: make(map[string]string),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Store) Open(env string, enableSingle bool, localID string) error {

	// Using zap's preset constructors is the simplest way to get a feel for the
	// package, but they don't allow much customization.
	var err error
	var logger *zap.Logger
	switch env {
	case "", "dev", "development", "test":
		logger, err = zap.NewDevelopment()
	case "prod":
		logger, err = zap.NewProduction()
	case "example":
		logger = zap.NewExample()
	}
	if err != nil {
		return err
	}

	// Lets redirect standard logs
	stdLog := zap.NewStdLog(logger)

	s.log = logger.Sugar()
	s.log.Infow("failed to fetch URL",
		"url", "http://example.com",
		"attempt", 3,
		"backoff", time.Second,
	)
	s.log.Debugf("failed to fetch URL: %s", "http://example.com")

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.Logger = stdLog
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, s, logStore, logStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				raft.Server{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

// Close closes the schema sync
func (s *Store) Close() error {
	s.log.Sync()
	return nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	m := &Message{
		Op:  "set",
		Key: key,
		Msg: []byte(value),
	}
	by, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	f := s.raft.Apply(by, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	m := &Message{
		Op:  "delete",
		Key: key,
	}
	by, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	f := s.raft.Apply(by, raftTimeout)
	return f.Error()
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr string) error {
	s.log.Debugf("received join request for remote node as %s", addr)

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.log.Infof("node at %s joined successfully", addr)
	return nil
}

// -- raft.FSM implementation

// Apply applies a Raft log entry to the key-value store.
func (s *Store) Apply(l *raft.Log) interface{} {
	var msg Message
	if err := proto.Unmarshal(l.Data, &msg); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch msg.Op {
	case "set":
		return s.applySet(msg.Key, string(msg.Msg))
	case "delete":
		return s.applyDelete(msg.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", msg.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range s.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (s *Store) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	s.m = o
	return nil
}

func (s *Store) applySet(key, value string) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

func (s *Store) applyDelete(key string) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
