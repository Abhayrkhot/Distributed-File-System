package meta

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
)

func apply(t *testing.T, f *FSM, idx uint64, c command) applyResult {
	t.Helper()
	b, _ := json.Marshal(c)
	res := f.Apply(&raft.Log{Index: idx, Data: b})
	r, ok := res.(applyResult)
	if !ok {
		t.Fatalf("apply %+v: %v", c, res)
	}
	return r
}

func TestFSMEpochs(t *testing.T) {
	f := NewFSM()
	ch := f.Changed()
	if r := apply(t, f, 1, command{Op: opUpsert, ID: "a", Addr: "x:1", TS: 1}); !r.Changed || r.Epoch != 1 {
		t.Fatalf("register: %+v", r)
	}
	select {
	case <-ch:
	default:
		t.Fatal("Changed not signalled")
	}
	// Re-registering with the same address is a no-op.
	if r := apply(t, f, 2, command{Op: opUpsert, ID: "a", Addr: "x:1", TS: 2}); r.Changed || r.Epoch != 1 {
		t.Fatalf("idempotent upsert bumped epoch: %+v", r)
	}
	apply(t, f, 3, command{Op: opUpsert, ID: "b", Addr: "x:2", TS: 3})
	if f.Ring().Len() != 2 {
		t.Fatal("ring should have 2 members")
	}
	if r := apply(t, f, 4, command{Op: opState, ID: "a", State: dfsv1.NodeState_NODE_STATE_DOWN, TS: 4}); !r.Changed {
		t.Fatal("down should change epoch")
	}
	if f.Ring().Len() != 1 {
		t.Fatal("DOWN node must leave the ring")
	}
	if r := apply(t, f, 5, command{Op: opNoop, TS: 5}); r.Changed {
		t.Fatal("noop changed epoch")
	}
	// Address change brings the node back UP with the new address.
	apply(t, f, 6, command{Op: opUpsert, ID: "a", Addr: "x:9", TS: 6})
	if m, ok := f.Ring().Member("a"); !ok || m.Addr != "x:9" {
		t.Fatalf("readd: %+v %v", m, ok)
	}
	if bad := f.Apply(&raft.Log{Index: 7, Data: []byte("{")}); bad == nil {
		t.Fatal("garbage command should return an error")
	}
}

type sink struct {
	bytes.Buffer
}

func (s *sink) ID() string    { return "s" }
func (s *sink) Cancel() error { return nil }
func (s *sink) Close() error  { return nil }

func TestFSMSnapshotRestore(t *testing.T) {
	f := NewFSM()
	for i, id := range []string{"n1", "n2", "n3"} {
		apply(t, f, uint64(i+1), command{Op: opUpsert, ID: id, Addr: id + ":1", TS: int64(i)})
	}
	apply(t, f, 4, command{Op: opState, ID: "n2", State: dfsv1.NodeState_NODE_STATE_DOWN, TS: 9})
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var s sink
	if err := snap.Persist(&s); err != nil {
		t.Fatal(err)
	}
	g := NewFSM()
	if err := g.Restore(io.NopCloser(&s)); err != nil {
		t.Fatal(err)
	}
	if g.Epoch() != f.Epoch() || g.Ring().Len() != 2 {
		t.Fatalf("restored epoch %d ring %d", g.Epoch(), g.Ring().Len())
	}
	if n, _ := g.Node("n2"); n.State != dfsv1.NodeState_NODE_STATE_DOWN || n.Changed != 9 {
		t.Fatalf("restored node: %+v", n)
	}
	a, b := f.Ring().Lookup("k", 2), g.Ring().Lookup("k", 2)
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("restored ring routes differently")
		}
	}
}

func TestTimingProfiles(t *testing.T) {
	for _, name := range []string{"", "fast", "default"} {
		if _, err := TimingProfile(name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := TimingProfile("warp"); err == nil {
		t.Fatal("unknown profile accepted")
	}
}
