// dfs-storage runs one storage node: chunk store + ChunkService + FileService.
//
//	dfs-storage --id node-00 --bind 127.0.0.1:9100 --data-dir /var/lib/dfs/node-00 \
//	  --meta 127.0.0.1:8100,127.0.0.1:8101,127.0.0.1:8102 --cassandra 127.0.0.1:9042
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/backend"
	"github.com/Abhayrkhot/Distributed-File-System/internal/coord"
	"github.com/Abhayrkhot/Distributed-File-System/internal/storage"
)

func main() {
	var (
		id        = flag.String("id", os.Getenv("DFS_ID"), "node id (env DFS_ID)")
		bind      = flag.String("bind", "127.0.0.1:9100", "gRPC listen address")
		advertise = flag.String("advertise", os.Getenv("DFS_ADVERTISE"), "address other nodes dial, if different (env DFS_ADVERTISE)")
		dataDir   = flag.String("data-dir", "", "chunk store directory")
		fsync     = flag.Bool("fsync", false, "fsync every chunk before acknowledging it")
		metaAddrs = flag.String("meta", os.Getenv("DFS_META"), "control-plane gRPC addresses, comma separated (env DFS_META)")
		replicas  = flag.Int("replicas", 3, "replicas per chunk (N)")
		quorum    = flag.Int("write-quorum", 2, "acks required per chunk write (W)")
		chunkSize = flag.Int("chunk-size", coord.DefaultChunkSize, "chunk size in bytes")
		parallel  = flag.Int("parallelism", 4, "chunks in flight per upload/download")
		hbEvery   = flag.Duration("heartbeat", 250*time.Millisecond, "heartbeat interval")
		workers   = flag.Int("backup-workers", 4, "concurrent S3 uploads")
		logLevel  = flag.String("log-level", "info", "debug, info, warn, error")
		debugAddr = flag.String("debug-addr", "", "serve net/http/pprof on this address (e.g. 127.0.0.1:6060)")
		be        backend.Flags
	)
	be.Register(flag.CommandLine)
	flag.Parse()
	log := backend.Logger(*logLevel)
	if *debugAddr != "" {
		go func() { log.Warn("pprof server exited", "err", http.ListenAndServe(*debugAddr, nil)) }()
	}
	if *id == "" || *dataDir == "" || *metaAddrs == "" {
		fmt.Fprintln(os.Stderr, "dfs-storage: --id, --data-dir and --meta are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // waits for backends
	ms, err := be.Metastore(ctx, log)
	if err != nil {
		fatal(err)
	}
	bs, err := be.Blob(ctx, log)
	cancel()
	if err != nil {
		fatal(err)
	}
	if ms == nil {
		log.Warn("no metastore configured: FileService disabled, chunk service only")
	}
	n, err := storage.Open(storage.Config{
		ID: *id, Bind: *bind, Advertise: *advertise, DataDir: *dataDir, Fsync: *fsync,
		MetaAddrs: strings.Split(*metaAddrs, ","), Metastore: ms, Blob: bs, BackupWorkers: *workers,
		HeartbeatInterval: *hbEvery, Replicas: *replicas, WriteQuorum: *quorum, ChunkSize: *chunkSize,
		Parallelism: *parallel, Logger: log,
	})
	if err != nil {
		fatal(err)
	}
	n.Start()
	chunks, bytes := n.Store().Stats()
	log.Info("dfs-storage started", "id", *id, "addr", n.Addr(), "chunks", chunks, "bytes", bytes, "fsync", *fsync)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
	n.Shutdown()
	if ms != nil {
		ms.Close()
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "dfs-storage:", err)
	os.Exit(1)
}
