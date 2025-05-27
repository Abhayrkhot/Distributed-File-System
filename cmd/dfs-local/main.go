// dfs-local runs a complete cluster in one process for trying the system
// without Cassandra or MinIO: three Raft members and N storage nodes on
// loopback, an in-memory metastore and a directory-backed S3 tier.
//
//	go run ./cmd/dfs-local --storage 6
//	# in another shell, using the printed address:
//	dfs --meta 127.0.0.1:PORT put ./file.bin demo/file.bin
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/backend"
	"github.com/Abhayrkhot/Distributed-File-System/internal/cluster"
)

func main() {
	storage := flag.Int("storage", 6, "storage nodes")
	dir := flag.String("dir", "", "state directory (default: a new temp dir, removed on exit)")
	deadAfter := flag.Duration("dead-after", 3*time.Second, "mark a storage node DOWN after this long without heartbeats")
	logLevel := flag.String("log-level", "warn", "debug, info, warn, error")
	flag.Parse()

	d := *dir
	if d == "" {
		var err error
		if d, err = os.MkdirTemp("", "dfs-local-*.noindex"); err != nil {
			fail(err)
		}
		defer os.RemoveAll(d)
	}
	c, err := cluster.Start(cluster.Options{
		Dir: d, Storage: *storage, DeadAfter: *deadAfter, RepairInterval: 30 * time.Second,
		Logger: backend.Logger(*logLevel),
	})
	if err != nil {
		fail(err)
	}
	defer c.Close()

	var metas, nodes []string
	for _, p := range c.Peers {
		metas = append(metas, p.GRPCAddr)
	}
	for _, n := range c.Nodes {
		nodes = append(nodes, n.ID()+"="+n.Addr())
	}
	fmt.Printf("dfs-local: 3 Raft members + %d storage nodes (state in %s)\n\n", *storage, d)
	fmt.Printf("  export DFS_META=%s\n\n", strings.Join(metas, ","))
	fmt.Printf("storage nodes:\n  %s\n\n", strings.Join(nodes, "\n  "))
	fmt.Println("try: dfs status | dfs put ./somefile demo/somefile | dfs ls | dfs get demo/somefile -")
	fmt.Println("Ctrl-C to stop.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "dfs-local:", err)
	os.Exit(1)
}
