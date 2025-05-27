package integration

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/cluster"
)

// BenchmarkTransfer measures aggregate upload and download throughput of an
// in-process cluster (no Cassandra/MinIO unless the env vars are set).
//
//	go test ./test/integration -run '^$' -bench Transfer -benchtime 1x
func BenchmarkTransfer(b *testing.B) {
	for _, nodes := range []int{10, 50} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			t := &testing.T{}
			_ = t
			dir := b.TempDir()
			c, err := cluster.Start(cluster.Options{Dir: dir, Storage: nodes, ChunkSize: 4 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			const clients, fileSize, files = 32, 16 << 20, 64
			payload := randomBytes(fileSize)
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				var next atomic.Int64
				start := time.Now()
				for w := 0; w < clients; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							k := next.Add(1) - 1
							if k >= files {
								return
							}
							// Vary one byte so files do not dedup.
							data := append([]byte(nil), payload...)
							data[0], data[1] = byte(k), byte(k>>8)
							if _, err := c.Client.UploadBytes(ctx, fmt.Sprintf("b%d/%d", i, k), data); err != nil {
								b.Error(err)
								return
							}
						}
					}()
				}
				wg.Wait()
				up := time.Since(start)
				start = time.Now()
				next.Store(0)
				for w := 0; w < clients; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						var buf bytes.Buffer
						for {
							k := next.Add(1) - 1
							if k >= files {
								return
							}
							buf.Reset()
							if _, err := c.Client.Download(ctx, fmt.Sprintf("b%d/%d", i, k), &buf); err != nil {
								b.Error(err)
								return
							}
						}
					}()
				}
				wg.Wait()
				down := time.Since(start)
				total := float64(files * fileSize)
				b.ReportMetric(total/1e6/up.Seconds(), "upload_MB/s")
				b.ReportMetric(total/1e6/down.Seconds(), "download_MB/s")
			}
		})
	}
}
