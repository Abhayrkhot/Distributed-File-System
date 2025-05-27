// Package ring implements the DHT routing layer: a consistent-hash ring with
// virtual nodes. Every participant (storage nodes, the repair coordinator and
// clients) builds the same ring from the membership list that the Raft control
// plane publishes, so routing a chunk to its replicas is a local, one-hop
// lookup with no directory service on the data path (the Dynamo/Cassandra
// "one-hop DHT" design).
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
)

// DefaultVnodes is the number of virtual nodes per member. With 50+ members
// this keeps per-node load within a few percent of the mean (see
// TestBalance), while a membership change still moves only ~1/N of keys.
const DefaultVnodes = 128

// Member is a storage node that owns ranges of the ring.
type Member struct {
	ID   string
	Addr string
}

type point struct {
	hash  uint64
	owner int // index into Ring.members
}

// Ring is immutable once built; build a new one for each membership epoch.
type Ring struct {
	epoch   uint64
	vnodes  int
	members []Member
	byID    map[string]int
	points  []point
}

// New builds a ring. Members are sorted by ID first, so the result does not
// depend on input order.
func New(epoch uint64, members []Member, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVnodes
	}
	ms := append([]Member(nil), members...)
	sort.Slice(ms, func(i, j int) bool { return ms[i].ID < ms[j].ID })
	r := &Ring{
		epoch:   epoch,
		vnodes:  vnodes,
		members: ms,
		byID:    make(map[string]int, len(ms)),
		points:  make([]point, 0, len(ms)*vnodes),
	}
	for i, m := range ms {
		r.byID[m.ID] = i
		for v := 0; v < vnodes; v++ {
			r.points = append(r.points, point{hash: hashString(m.ID + "#" + strconv.Itoa(v)), owner: i})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash == r.points[j].hash {
			return r.points[i].owner < r.points[j].owner
		}
		return r.points[i].hash < r.points[j].hash
	})
	return r
}

// Epoch is the control-plane membership epoch this ring was built from.
func (r *Ring) Epoch() uint64 { return r.epoch }

// Len is the number of members.
func (r *Ring) Len() int { return len(r.members) }

// Members returns the members sorted by ID.
func (r *Ring) Members() []Member { return append([]Member(nil), r.members...) }

// Member looks up a member by ID.
func (r *Ring) Member(id string) (Member, bool) {
	i, ok := r.byID[id]
	if !ok {
		return Member{}, false
	}
	return r.members[i], true
}

// Lookup returns the preference list for key: the first n distinct members
// clockwise from the key's position. The first entry is the key's primary.
// If n exceeds the member count, every member is returned.
func (r *Ring) Lookup(key string, n int) []Member {
	if len(r.members) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.members) {
		n = len(r.members)
	}
	h := hashString(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	out := make([]Member, 0, n)
	var seen uint64 // bitset fast path for small rings
	var seenMap map[int]struct{}
	if len(r.members) > 64 {
		seenMap = make(map[int]struct{}, n)
	}
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		if seenMap != nil {
			if _, dup := seenMap[p.owner]; dup {
				continue
			}
			seenMap[p.owner] = struct{}{}
		} else {
			if seen&(1<<uint(p.owner)) != 0 {
				continue
			}
			seen |= 1 << uint(p.owner)
		}
		out = append(out, r.members[p.owner])
	}
	return out
}

func hashString(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
