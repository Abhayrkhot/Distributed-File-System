// Package localcluster launches a cluster of real dfs-meta and dfs-storage
// OS processes on loopback, for benchmarks and demos. Unlike the in-process
// test harness, failures here are real: Kill sends SIGKILL.
package localcluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
)

type Options struct {
	BinDir  string // directory holding dfs-meta and dfs-storage
	Dir     string // state and logs
	Meta    int
	Storage int
	// Extra flags for each process type.
	MetaArgs    []string
	StorageArgs []string
	// Env is added to every child's environment (backend settings).
	Env []string
}

type Proc struct {
	Name     string
	Addr     string // gRPC address
	RaftAddr string
	bin      string
	args     []string
	env      []string
	logPath  string

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{}
}

type Cluster struct {
	opts      Options
	Metas     []*Proc
	Nodes     []*Proc
	MetaAddrs []string
}

func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// Start launches the control plane, waits for a leader, then launches the
// storage nodes and waits until all of them are in the ring.
func Start(ctx context.Context, opts Options) (*Cluster, error) {
	if err := os.MkdirAll(filepath.Join(opts.Dir, "logs"), 0o755); err != nil {
		return nil, err
	}
	c := &Cluster{opts: opts}
	var peers []string
	for i := 0; i < opts.Meta; i++ {
		raft, err := freePort()
		if err != nil {
			return nil, err
		}
		grpc, err := freePort()
		if err != nil {
			return nil, err
		}
		name := fmt.Sprintf("meta-%d", i)
		c.Metas = append(c.Metas, &Proc{Name: name, Addr: grpc, RaftAddr: raft})
		c.MetaAddrs = append(c.MetaAddrs, grpc)
		peers = append(peers, fmt.Sprintf("%s=%s=%s", name, raft, grpc))
	}
	for _, p := range c.Metas {
		p.bin = filepath.Join(opts.BinDir, "dfs-meta")
		p.args = append([]string{
			"--id", p.Name, "--raft-bind", p.RaftAddr, "--grpc-bind", p.Addr,
			"--peers", strings.Join(peers, ","), "--bootstrap",
			"--data-dir", filepath.Join(opts.Dir, p.Name),
		}, opts.MetaArgs...)
		p.env = opts.Env
		p.logPath = filepath.Join(opts.Dir, "logs", p.Name+".log")
		if err := p.Start(); err != nil {
			c.Stop()
			return nil, err
		}
	}
	mc := meta.NewClient(c.MetaAddrs, nil)
	defer mc.Close()
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err := mc.Propose(lctx)
	cancel()
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("control plane did not elect a leader: %w (logs in %s)", err, filepath.Join(opts.Dir, "logs"))
	}

	for i := 0; i < opts.Storage; i++ {
		addr, err := freePort()
		if err != nil {
			c.Stop()
			return nil, err
		}
		name := fmt.Sprintf("node-%02d", i)
		p := &Proc{Name: name, Addr: addr, bin: filepath.Join(opts.BinDir, "dfs-storage"), env: opts.Env,
			logPath: filepath.Join(opts.Dir, "logs", name+".log")}
		p.args = append([]string{
			"--id", name, "--bind", addr, "--data-dir", filepath.Join(opts.Dir, name),
			"--meta", strings.Join(c.MetaAddrs, ","),
		}, opts.StorageArgs...)
		c.Nodes = append(c.Nodes, p)
		if err := p.Start(); err != nil {
			c.Stop()
			return nil, err
		}
	}
	if opts.Storage > 0 {
		if err := c.WaitRing(ctx, func(up int) bool { return up == opts.Storage }, 2*time.Minute); err != nil {
			c.Stop()
			return nil, err
		}
	}
	return c, nil
}

// WaitRing polls until the leader's ring satisfies pred(number of UP nodes).
func (c *Cluster) WaitRing(ctx context.Context, pred func(up int) bool, timeout time.Duration) error {
	mc := meta.NewClient(c.MetaAddrs, nil)
	defer mc.Close()
	deadline := time.Now().Add(timeout)
	last := -1
	for time.Now().Before(deadline) {
		if r, _, err := mc.GetRing(ctx); err == nil {
			last = r.Len()
			if pred(last) {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("ring wait timed out with %d nodes up (logs in %s)", last, filepath.Join(c.opts.Dir, "logs"))
}

// MetaByID finds a control-plane process.
func (c *Cluster) MetaByID(id string) *Proc {
	for _, p := range c.Metas {
		if p.Name == id {
			return p
		}
	}
	return nil
}

// Stop terminates every process (SIGTERM, then SIGKILL).
func (c *Cluster) Stop() {
	var wg sync.WaitGroup
	for _, p := range append(append([]*Proc(nil), c.Nodes...), c.Metas...) {
		wg.Add(1)
		go func(p *Proc) { defer wg.Done(); p.Terminate(5 * time.Second) }(p)
	}
	wg.Wait()
}

func (p *Proc) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	logf, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(p.bin, p.args...)
	cmd.Env = append(os.Environ(), p.env...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("start %s: %w", p.Name, err)
	}
	done := make(chan struct{})
	go func() { cmd.Wait(); logf.Close(); close(done) }()
	p.cmd, p.done = cmd, done
	return nil
}

// Running reports whether the process is alive.
func (p *Proc) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *Proc) signal(sig syscall.Signal) (chan struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return nil, errors.New("not started")
	}
	select {
	case <-p.done:
		return p.done, nil
	default:
	}
	return p.done, p.cmd.Process.Signal(sig)
}

// Kill sends SIGKILL and waits for the process to exit.
func (p *Proc) Kill() error {
	done, err := p.signal(syscall.SIGKILL)
	if err != nil {
		return err
	}
	<-done
	return nil
}

// Freeze sends SIGSTOP: the process stops responding but its sockets stay
// open, like a hung machine. Kill or Thaw it afterwards.
func (p *Proc) Freeze() error {
	_, err := p.signal(syscall.SIGSTOP)
	return err
}

// Thaw sends SIGCONT.
func (p *Proc) Thaw() error {
	_, err := p.signal(syscall.SIGCONT)
	return err
}

// Terminate sends SIGTERM, waits up to grace, then SIGKILLs.
func (p *Proc) Terminate(grace time.Duration) {
	done, err := p.signal(syscall.SIGTERM)
	if err != nil {
		return
	}
	select {
	case <-done:
	case <-time.After(grace):
		p.Kill()
	}
}

// RSS returns the process's resident set size in KiB (via ps), or 0.
func (p *Proc) RSS() int64 {
	p.mu.Lock()
	cmd := p.cmd
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", fmt.Sprint(cmd.Process.Pid)).Output()
	if err != nil {
		return 0
	}
	var kb int64
	fmt.Sscan(strings.TrimSpace(string(out)), &kb)
	return kb
}
