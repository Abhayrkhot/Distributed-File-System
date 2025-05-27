// Package meta is the control plane: a Raft group (hashicorp/raft) whose
// replicated state is storage-node membership. The leader turns heartbeats
// into membership changes, and every committed change bumps the ring epoch
// that the data plane routes by. The leader also runs replica repair.
//
// The data plane never waits on Raft: storage nodes and coordinators cache the
// last ring they saw, so uploads and downloads continue while the control
// plane elects a new leader.
package meta

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/ring"
)

type nodeRec struct {
	ID      string          `json:"id"`
	Addr    string          `json:"addr"`
	State   dfsv1.NodeState `json:"state"`
	Changed int64           `json:"changed"` // unix ms, set by the proposer
}

type fsmState struct {
	Epoch uint64              `json:"epoch"`
	Nodes map[string]*nodeRec `json:"nodes"`
	Noops uint64              `json:"noops"`
}

// Command ops.
const (
	opUpsert = "upsert" // register a node or bring it UP (with its address)
	opState  = "state"  // change a node's state
	opRemove = "remove"
	opNoop   = "noop"
)

type command struct {
	Op    string          `json:"op"`
	ID    string          `json:"id,omitempty"`
	Addr  string          `json:"addr,omitempty"`
	State dfsv1.NodeState `json:"state,omitempty"`
	TS    int64           `json:"ts"`
}

type applyResult struct {
	Epoch   uint64
	Changed bool
}

// FSM applies membership commands. Apply must be deterministic, so all
// timestamps come from the command, never from the local clock.
type FSM struct {
	mu      sync.RWMutex
	st      fsmState
	changed chan struct{} // closed and replaced on every epoch change
	ring    *ring.Ring
}

func NewFSM() *FSM {
	f := &FSM{st: fsmState{Nodes: map[string]*nodeRec{}}, changed: make(chan struct{})}
	f.rebuildLocked()
	return f
}

func (f *FSM) Apply(l *raft.Log) any {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return fmt.Errorf("fsm: bad command at %d: %w", l.Index, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	changed := false
	switch c.Op {
	case opUpsert:
		n, ok := f.st.Nodes[c.ID]
		if !ok {
			f.st.Nodes[c.ID] = &nodeRec{ID: c.ID, Addr: c.Addr, State: dfsv1.NodeState_NODE_STATE_UP, Changed: c.TS}
			changed = true
		} else if n.Addr != c.Addr || n.State != dfsv1.NodeState_NODE_STATE_UP {
			n.Addr, n.State, n.Changed = c.Addr, dfsv1.NodeState_NODE_STATE_UP, c.TS
			changed = true
		}
	case opState:
		if n, ok := f.st.Nodes[c.ID]; ok && n.State != c.State {
			n.State, n.Changed = c.State, c.TS
			changed = true
		}
	case opRemove:
		if _, ok := f.st.Nodes[c.ID]; ok {
			delete(f.st.Nodes, c.ID)
			changed = true
		}
	case opNoop:
		f.st.Noops++
	default:
		return fmt.Errorf("fsm: unknown op %q", c.Op)
	}
	if changed {
		f.st.Epoch++
		f.rebuildLocked()
		close(f.changed)
		f.changed = make(chan struct{})
	}
	return applyResult{Epoch: f.st.Epoch, Changed: changed}
}

func (f *FSM) rebuildLocked() {
	var up []ring.Member
	for _, n := range f.st.Nodes {
		if n.State == dfsv1.NodeState_NODE_STATE_UP {
			up = append(up, ring.Member{ID: n.ID, Addr: n.Addr})
		}
	}
	f.ring = ring.New(f.st.Epoch, up, ring.DefaultVnodes)
}

// Ring returns the ring for the current epoch (UP nodes only).
func (f *FSM) Ring() *ring.Ring {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ring
}

// Epoch returns the current membership epoch.
func (f *FSM) Epoch() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.st.Epoch
}

// Changed returns a channel that is closed at the next epoch change.
func (f *FSM) Changed() <-chan struct{} {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.changed
}

// Node returns a copy of a node record.
func (f *FSM) Node(id string) (nodeRec, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, ok := f.st.Nodes[id]
	if !ok {
		return nodeRec{}, false
	}
	return *n, true
}

// Nodes returns all node records sorted by ID.
func (f *FSM) Nodes() []nodeRec {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]nodeRec, 0, len(f.st.Nodes))
	for _, n := range f.st.Nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// RingProto is the wire form of the current membership.
func (f *FSM) RingProto() *dfsv1.Ring {
	f.mu.RLock()
	defer f.mu.RUnlock()
	r := &dfsv1.Ring{Epoch: f.st.Epoch}
	for _, n := range f.st.Nodes {
		r.Nodes = append(r.Nodes, &dfsv1.StorageNode{Id: n.ID, Addr: n.Addr, State: n.State, ChangedUnixMs: n.Changed})
	}
	sort.Slice(r.Nodes, func(i, j int) bool { return r.Nodes[i].Id < r.Nodes[j].Id })
	return r
}

// RingFromProto builds the routing ring a data-plane participant uses.
func RingFromProto(p *dfsv1.Ring) *ring.Ring {
	var up []ring.Member
	for _, n := range p.GetNodes() {
		if n.GetState() == dfsv1.NodeState_NODE_STATE_UP {
			up = append(up, ring.Member{ID: n.GetId(), Addr: n.GetAddr()})
		}
	}
	return ring.New(p.GetEpoch(), up, ring.DefaultVnodes)
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := json.Marshal(f.st)
	if err != nil {
		return nil, err
	}
	return &snapshot{data: b}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var st fsmState
	if err := json.NewDecoder(rc).Decode(&st); err != nil {
		return err
	}
	if st.Nodes == nil {
		st.Nodes = map[string]*nodeRec{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st = st
	f.rebuildLocked()
	close(f.changed)
	f.changed = make(chan struct{})
	return nil
}

type snapshot struct{ data []byte }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
