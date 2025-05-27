package meta

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// streamLayer is a TCP raft.StreamLayer that reports when an inbound Raft
// connection is closed by its peer.
//
// Why: a Raft follower only learns that its leader died when the heartbeat
// timeout expires (randomised between 1x and 2x the timeout). But when the
// leader *process* crashes and its host survives, the kernel closes the
// crashed process's sockets at once, and the follower's read on the leader's
// replication connection returns EOF within a round trip. That EOF is a
// strong hint, not proof (connections also close for benign reasons), so the
// server uses it only to shorten the heartbeat timeout for a short window
// (see Server.onPeerConnLost). The real decision is still Raft's own
// "no contact for HeartbeatTimeout" check, and pre-vote keeps a false alarm
// from disrupting a healthy leader.
type streamLayer struct {
	ln        *net.TCPListener
	advertise net.Addr
	onLost    func()
}

func newStreamLayer(bind string, advertise net.Addr, onLost func()) (*streamLayer, error) {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	s := &streamLayer{ln: l.(*net.TCPListener), advertise: advertise, onLost: onLost}
	if s.advertise == nil {
		a := l.Addr().(*net.TCPAddr)
		if a.IP == nil || a.IP.IsUnspecified() {
			l.Close()
			return nil, errors.New("meta: raft bind address is not advertisable; set RaftAdvertise")
		}
		s.advertise = a
	}
	return s, nil
}

func (s *streamLayer) Accept() (net.Conn, error) {
	c, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return &watchedConn{Conn: c, onLost: s.onLost}, nil
}

func (s *streamLayer) Close() error   { return s.ln.Close() }
func (s *streamLayer) Addr() net.Addr { return s.advertise }

func (s *streamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	c, err := net.DialTimeout("tcp", string(addr), timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return c, nil
}

// hostPortAddr is a net.Addr that may hold a DNS name.
type hostPortAddr string

func (a hostPortAddr) Network() string { return "tcp" }
func (a hostPortAddr) String() string  { return string(a) }

// watchedConn calls onLost once when a read fails (EOF or reset).
type watchedConn struct {
	net.Conn
	once   sync.Once
	onLost func()
}

func (c *watchedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil && c.onLost != nil {
		c.once.Do(c.onLost)
	}
	return n, err
}
