package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/client"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
)

// runSmoke is an end-to-end check against an existing cluster (used by the
// Kubernetes smoke Job): discover nodes from the ring, upload files through
// random coordinators, read them back and compare.
func runSmoke(args []string) error {
	fs := flag.NewFlagSet("k8s-smoke", flag.ExitOnError)
	metaAddrs := fs.String("meta", "", "control-plane addresses")
	files := fs.Int("files", 20, "files to write")
	sizeS := fs.String("size", "8MiB", "file size")
	wait := fs.Duration("wait", 3*time.Minute, "how long to wait for the cluster to come up")
	clients := fs.Int("clients", 1, "concurrent client streams")
	minNodes := fs.Int("min-nodes", 3, "wait until at least this many storage nodes are in the ring")
	fs.Parse(args)
	size, err := parseSize(*sizeS)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait+5*time.Minute)
	defer cancel()
	mc := meta.NewClient(strings.Split(*metaAddrs, ","), nil)
	defer mc.Close()

	deadline := time.Now().Add(*wait)
	var nodes []string
	for {
		st, err := mc.LeaderStatus(ctx)
		if err == nil {
			r := meta.RingFromProto(st.Ring)
			if r.Len() >= *minNodes {
				nodes = nil
				for _, m := range r.Members() {
					nodes = append(nodes, m.Addr)
				}
				fmt.Printf("leader %s, term %d, ring epoch %d with %d storage nodes\n", st.LeaderId, st.Term, r.Epoch(), r.Len())
				break
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster not ready: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	cl := client.New(nodes)
	defer cl.Close()
	start := time.Now()
	jobs := make(chan int, *files)
	for i := 0; i < *files; i++ {
		jobs <- i
	}
	close(jobs)
	stamp := time.Now().Unix()
	var (
		mu       sync.Mutex
		firstErr error
		total    int64
		wg       sync.WaitGroup
	)
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, size)
			for i := range jobs {
				rand.Read(data)
				name := fmt.Sprintf("smoke/%d-%d", stamp, i)
				err := func() error {
					if _, err := cl.UploadBytes(ctx, name, data); err != nil {
						return fmt.Errorf("upload %s: %w", name, err)
					}
					var buf bytes.Buffer
					if _, err := cl.Download(ctx, name, &buf); err != nil {
						return fmt.Errorf("download %s: %w", name, err)
					}
					if !bytes.Equal(buf.Bytes(), data) {
						return fmt.Errorf("%s: content mismatch", name)
					}
					return nil
				}()
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				if err == nil {
					total += size
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	el := time.Since(start)
	fmt.Printf("OK: %d files (%d MiB) written and read back by %d clients in %v (%.1f MB/s round trip, upload+download bytes)\n",
		*files, total>>20, *clients, el.Round(time.Millisecond), float64(2*total)/1e6/el.Seconds())
	return nil
}
