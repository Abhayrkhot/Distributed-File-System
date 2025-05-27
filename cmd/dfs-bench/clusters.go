package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/localcluster"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
)

// benchCluster is what the throughput benchmark needs from a cluster,
// whether it launched the processes itself or is pointed at one that is
// already running (e.g. spread over Slurm jobs on many machines).
type benchCluster interface {
	Nodes() []benchNode
	Alive(name string) bool
	Kill(ctx context.Context, name string) error
	MetaAddrs() []string
	RSSKiB() int64 // 0 when unknown
	Stop()
}

type benchNode struct{ Name, Addr string }

// localBench wraps processes this benchmark launched on this host.
type localBench struct{ c *localcluster.Cluster }

func (l localBench) Nodes() []benchNode {
	var out []benchNode
	for _, p := range l.c.Nodes {
		out = append(out, benchNode{p.Name, p.Addr})
	}
	return out
}

func (l localBench) proc(name string) *localcluster.Proc {
	for _, p := range l.c.Nodes {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (l localBench) Alive(name string) bool {
	p := l.proc(name)
	return p != nil && p.Running()
}

func (l localBench) Kill(_ context.Context, name string) error {
	p := l.proc(name)
	if p == nil {
		return fmt.Errorf("no node %q", name)
	}
	return p.Kill()
}

func (l localBench) MetaAddrs() []string { return l.c.MetaAddrs }

func (l localBench) RSSKiB() int64 {
	var rss int64
	for _, p := range l.c.Nodes {
		rss += p.RSS()
	}
	return rss
}

func (l localBench) Stop() { l.c.Stop() }

// externalBench is a cluster someone else started. Storage nodes are found
// from the Raft ring; a node is crashed by running killCmd with {id} and
// {addr} substituted (for Slurm, e.g. touching a file the node's job watches).
type externalBench struct {
	meta    []string
	nodes   []benchNode
	killCmd string

	mu     sync.Mutex
	killed map[string]bool
}

func newExternalBench(ctx context.Context, metaAddrs []string, killCmd string, want int, wait time.Duration) (*externalBench, error) {
	mc := meta.NewClient(metaAddrs, nil)
	defer mc.Close()
	deadline := time.Now().Add(wait)
	last := -1
	for {
		r, _, err := mc.GetRing(ctx)
		if err == nil {
			if r.Len() != last {
				fmt.Printf("  ring: %d storage nodes up\n", r.Len())
				last = r.Len()
			}
			if r.Len() >= want {
				e := &externalBench{meta: metaAddrs, killCmd: killCmd, killed: map[string]bool{}}
				for _, m := range r.Members() {
					e.nodes = append(e.nodes, benchNode{m.ID, m.Addr})
				}
				return e, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("only %d of %d storage nodes joined within %v (last error: %v)", last, want, wait, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func (e *externalBench) Nodes() []benchNode  { return append([]benchNode(nil), e.nodes...) }
func (e *externalBench) MetaAddrs() []string { return e.meta }
func (e *externalBench) RSSKiB() int64       { return 0 }
func (e *externalBench) Stop()               {}

func (e *externalBench) Alive(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.killed[name]
}

func (e *externalBench) Kill(ctx context.Context, name string) error {
	if e.killCmd == "" {
		return fmt.Errorf("existing cluster: pass --kill-cmd to crash a storage node")
	}
	addr := ""
	for _, n := range e.nodes {
		if n.Name == name {
			addr = n.Addr
		}
	}
	cmd := strings.NewReplacer("{id}", name, "{addr}", addr).Replace(e.killCmd)
	if out, err := exec.CommandContext(ctx, "sh", "-c", cmd).CombinedOutput(); err != nil {
		return fmt.Errorf("kill-cmd %q: %w: %s", cmd, err, out)
	}
	e.mu.Lock()
	e.killed[name] = true
	e.mu.Unlock()
	return nil
}
