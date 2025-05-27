// dfs-bench runs the reproducible experiments behind docs/results.md. Each
// subcommand launches its own cluster of real processes (bin/dfs-meta and
// bin/dfs-storage), drives it, tears it down and writes a JSON result.
//
//	dfs-bench throughput --storage 50 --clients 32 --total 1536MiB
//	dfs-bench failover --trials 30 --timing fast
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: dfs-bench throughput|failover|k8s-smoke [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "throughput":
		err = runThroughput(os.Args[2:])
	case "failover":
		err = runFailover(os.Args[2:])
	case "k8s-smoke":
		err = runSmoke(os.Args[2:])
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "dfs-bench:", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------

type Dist struct {
	N    int     `json:"n"`
	Min  float64 `json:"min"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

func dist(xs []float64) Dist {
	if len(xs) == 0 {
		return Dist{}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	q := func(p float64) float64 {
		// nearest-rank
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	var sum float64
	for _, x := range s {
		sum += x
	}
	return Dist{N: len(s), Min: s[0], P50: q(0.50), P90: q(0.90), P99: q(0.99), Max: s[len(s)-1], Mean: sum / float64(len(s))}
}

func (d Dist) String() string {
	return fmt.Sprintf("n=%d min=%.1f p50=%.1f p90=%.1f p99=%.1f max=%.1f mean=%.1f", d.N, d.Min, d.P50, d.P90, d.P99, d.Max, d.Mean)
}

type Env struct {
	Host      string `json:"host"`
	CPU       string `json:"cpu"`
	NumCPU    int    `json:"num_cpu"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	When      string `json:"when"`
}

func hostEnv() Env {
	cpu := runtime.GOARCH
	if b, err := runCmd("sysctl", "-n", "machdep.cpu.brand_string"); err == nil {
		cpu = strings.TrimSpace(b)
	}
	return Env{Host: "single host, loopback networking", CPU: cpu, NumCPU: runtime.NumCPU(), GoVersion: runtime.Version(),
		OS: runtime.GOOS + "/" + runtime.GOARCH, When: time.Now().UTC().Format(time.RFC3339)}
}

func writeJSON(dir, name string, v any) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	p := filepath.Join(dir, name)
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return p, os.WriteFile(p, append(b, '\n'), 0o644)
}

// parseSize accepts 123, 64KiB, 16MiB, 1.5GiB, 100MB.
func parseSize(s string) (int64, error) {
	units := []struct {
		suf string
		mul float64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}}
	for _, u := range units {
		if strings.HasSuffix(s, u.suf) {
			f, err := strconv.ParseFloat(strings.TrimSuffix(s, u.suf), 64)
			return int64(f * u.mul), err
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
