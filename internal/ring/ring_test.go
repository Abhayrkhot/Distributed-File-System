package ring

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
)

func members(n int) []Member {
	ms := make([]Member, n)
	for i := range ms {
		ms[i] = Member{ID: fmt.Sprintf("node-%02d", i), Addr: fmt.Sprintf("127.0.0.1:%d", 9000+i)}
	}
	return ms
}

func chunkID(i int) string {
	s := sha256.Sum256([]byte(fmt.Sprintf("chunk-%d", i)))
	return hex.EncodeToString(s[:])
}

func TestLookupDistinctAndDeterministic(t *testing.T) {
	ms := members(10)
	a := New(1, ms, 64)
	// Same members in a different order must give the same ring.
	rev := append([]Member(nil), ms...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	b := New(1, rev, 64)
	for i := 0; i < 1000; i++ {
		k := chunkID(i)
		pa, pb := a.Lookup(k, 3), b.Lookup(k, 3)
		if len(pa) != 3 {
			t.Fatalf("want 3 replicas, got %d", len(pa))
		}
		seen := map[string]bool{}
		for j := range pa {
			if pa[j] != pb[j] {
				t.Fatalf("order-dependent ring for %s", k)
			}
			if seen[pa[j].ID] {
				t.Fatalf("duplicate replica %s for %s", pa[j].ID, k)
			}
			seen[pa[j].ID] = true
		}
	}
}

func TestLookupMoreThanMembers(t *testing.T) {
	r := New(1, members(2), 8)
	if got := r.Lookup("x", 5); len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	if got := New(1, nil, 8).Lookup("x", 3); got != nil {
		t.Fatalf("empty ring should return nil")
	}
}

func TestLargeRingUsesMapPath(t *testing.T) {
	r := New(1, members(100), 16)
	got := r.Lookup(chunkID(1), 5)
	seen := map[string]bool{}
	for _, m := range got {
		if seen[m.ID] {
			t.Fatal("duplicate")
		}
		seen[m.ID] = true
	}
	if len(got) != 5 {
		t.Fatalf("want 5, got %d", len(got))
	}
}

// TestBalance checks primary+replica load across 50 nodes stays near the mean.
func TestBalance(t *testing.T) {
	const nodes, keys, rf = 50, 200_000, 3
	r := New(1, members(nodes), DefaultVnodes)
	load := map[string]int{}
	for i := 0; i < keys; i++ {
		for _, m := range r.Lookup(chunkID(i), rf) {
			load[m.ID]++
		}
	}
	mean := float64(keys*rf) / nodes
	var ss, maxDev float64
	for _, c := range load {
		d := float64(c) - mean
		ss += d * d
		maxDev = math.Max(maxDev, math.Abs(d)/mean)
	}
	cv := math.Sqrt(ss/nodes) / mean
	t.Logf("50 nodes, vnodes=%d: coefficient of variation %.3f, max deviation %.1f%%", DefaultVnodes, cv, maxDev*100)
	if cv > 0.10 {
		t.Fatalf("load imbalance too high: cv=%.3f", cv)
	}
}

// TestMinimalMovement: removing one of N members should only change the
// replica sets that included it (~rf/N of keys), and never reshuffle others.
func TestMinimalMovement(t *testing.T) {
	const nodes, keys, rf = 50, 50_000, 3
	ms := members(nodes)
	before := New(1, ms, DefaultVnodes)
	after := New(2, append(append([]Member(nil), ms[:7]...), ms[8:]...), DefaultVnodes)
	removed := ms[7].ID
	changed := 0
	for i := 0; i < keys; i++ {
		k := chunkID(i)
		b, a := before.Lookup(k, rf), after.Lookup(k, rf)
		had := false
		for _, m := range b {
			if m.ID == removed {
				had = true
			}
		}
		same := true
		for j := range a {
			if a[j] != b[j] {
				same = false
			}
		}
		if !same {
			changed++
			if !had {
				t.Fatalf("key %s moved although its replica set did not include the removed node", k)
			}
		}
	}
	frac := float64(changed) / keys
	t.Logf("removing 1 of %d nodes changed %.2f%% of replica sets (expected ~%.2f%%)", nodes, frac*100, 100*float64(rf)/nodes)
	if frac > 2*float64(rf)/nodes {
		t.Fatalf("too much movement: %.3f", frac)
	}
}

func BenchmarkLookup(b *testing.B) {
	r := New(1, members(50), DefaultVnodes)
	k := chunkID(42)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Lookup(k, 3)
	}
}
