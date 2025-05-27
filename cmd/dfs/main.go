// dfs is the command-line client.
//
//	dfs --meta 127.0.0.1:8100 put ./photo.jpg photos/photo.jpg
//	dfs --meta 127.0.0.1:8100 get photos/photo.jpg ./out.jpg
//	dfs --meta 127.0.0.1:8100 ls photos/
//	dfs --meta 127.0.0.1:8100 status
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/client"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: dfs [--meta addrs | --nodes addrs] <command> [args]

commands:
  put <local-file|-> <name>   upload
  get <name> <local-file|->   download (latest version)
  stat <name>                 show manifest
  ls [prefix]                 list files
  rm <name>                   delete
  status                      control plane, ring and repair status
  repair                      run a repair pass now and wait for it
  transfer-leader             hand Raft leadership to another member
`)
	os.Exit(2)
}

func main() {
	metaAddrs := flag.String("meta", os.Getenv("DFS_META"), "control-plane addresses (env DFS_META)")
	nodeAddrs := flag.String("nodes", os.Getenv("DFS_NODES"), "storage node addresses; default: discovered from the ring")
	timeout := flag.Duration("timeout", 10*time.Minute, "overall timeout")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var mc *meta.Client
	if *metaAddrs != "" {
		mc = meta.NewClient(strings.Split(*metaAddrs, ","), nil)
		defer mc.Close()
	}
	files := func() *client.Client {
		var nodes []string
		if *nodeAddrs != "" {
			nodes = strings.Split(*nodeAddrs, ",")
		} else if mc != nil {
			r, _, err := mc.GetRing(ctx)
			check(err)
			for _, m := range r.Members() {
				nodes = append(nodes, m.Addr)
			}
		}
		if len(nodes) == 0 {
			fail("no storage nodes: pass --nodes or --meta")
		}
		return client.New(nodes)
	}
	needMeta := func() {
		if mc == nil {
			fail("this command needs --meta")
		}
	}

	args := flag.Args()[1:]
	switch flag.Arg(0) {
	case "put":
		if len(args) != 2 {
			usage()
		}
		var r io.Reader = os.Stdin
		if args[0] != "-" {
			f, err := os.Open(args[0])
			check(err)
			defer f.Close()
			r = f
		}
		c := files()
		defer c.Close()
		start := time.Now()
		fi, err := c.UploadTo(ctx, c.Pick(), args[1], r)
		check(err)
		el := time.Since(start)
		fmt.Printf("stored %s (%d bytes, version %s, sha256 %s) in %v, %.1f MB/s\n",
			fi.Name, fi.Size, fi.Version, fi.Sha256[:16], el.Round(time.Millisecond), float64(fi.Size)/1e6/el.Seconds())
	case "get":
		if len(args) != 2 {
			usage()
		}
		var w io.Writer = os.Stdout
		if args[1] != "-" {
			f, err := os.Create(args[1])
			check(err)
			defer f.Close()
			w = f
		}
		c := files()
		defer c.Close()
		start := time.Now()
		fi, err := c.Download(ctx, args[0], w)
		check(err)
		if args[1] != "-" {
			el := time.Since(start)
			fmt.Printf("fetched %s (%d bytes) in %v, %.1f MB/s\n", fi.Name, fi.Size, el.Round(time.Millisecond), float64(fi.Size)/1e6/el.Seconds())
		}
	case "stat":
		if len(args) != 1 {
			usage()
		}
		c := files()
		defer c.Close()
		fi, err := c.Stat(ctx, args[0])
		check(err)
		fmt.Printf("name:    %s\nversion: %s\nsize:    %d\nsha256:  %s\ncreated: %s\nchunks:  %d\n",
			fi.Name, fi.Version, fi.Size, fi.Sha256, time.UnixMilli(fi.CreatedUnixMs).Format(time.RFC3339), len(fi.Chunks))
		if mc != nil {
			r, _, err := mc.GetRing(ctx)
			check(err)
			for i, ch := range fi.Chunks {
				var ids []string
				for _, m := range r.Lookup(ch.Id, 3) {
					ids = append(ids, m.ID)
				}
				fmt.Printf("  %4d %s %8d  -> %s\n", i, ch.Id[:16], ch.Size, strings.Join(ids, ","))
			}
		}
	case "ls":
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}
		c := files()
		defer c.Close()
		fs, err := c.List(ctx, prefix, 0)
		check(err)
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		for _, f := range fs {
			fmt.Fprintf(tw, "%d\t%s\t%s\n", f.Size, time.UnixMilli(f.CreatedUnixMs).Format(time.DateTime), f.Name)
		}
		tw.Flush()
	case "rm":
		if len(args) != 1 {
			usage()
		}
		c := files()
		defer c.Close()
		check(c.Delete(ctx, args[0]))
	case "status":
		needMeta()
		printStatus(ctx, mc)
	case "repair":
		needMeta()
		st, err := mc.TriggerRepair(ctx, true)
		check(err)
		fmt.Printf("repair pass: %d ms, scanned %d chunks, under-replicated %d, repaired %d replicas (%d from S3), %d S3 backups, %d errors\n",
			st.LastDurationMs, st.ChunksScanned, st.UnderReplicated, st.ReplicasRepaired, st.RestoredFromS3, st.BackupsTriggered, st.Errors)
	case "transfer-leader":
		needMeta()
		resp, err := mc.TransferLeadership(ctx)
		check(err)
		fmt.Println("new leader:", resp.NewLeaderId)
	default:
		usage()
	}
}

func printStatus(ctx context.Context, mc *meta.Client) {
	st, err := mc.LeaderStatus(ctx)
	check(err)
	fmt.Printf("leader: %s (%s)  term %d  commit %d\n", st.LeaderId, st.LeaderGrpcAddr, st.Term, st.CommitIndex)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MEMBER\tRAFT\tGRPC\tROLE")
	for _, p := range st.Peers {
		role := "follower"
		if p.Leader {
			role = "leader"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", p.Id, p.RaftAddr, p.GrpcAddr, role)
	}
	tw.Flush()
	nodes := append([]*dfsv1.StorageNode(nil), st.Ring.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Id < nodes[j].Id })
	up := 0
	for _, n := range nodes {
		if n.State == dfsv1.NodeState_NODE_STATE_UP {
			up++
		}
	}
	fmt.Printf("\nring epoch %d: %d storage nodes, %d up\n", st.Ring.Epoch, len(nodes), up)
	tw = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tADDR\tSTATE\tSINCE")
	for _, n := range nodes {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.Id, n.Addr, strings.TrimPrefix(n.State.String(), "NODE_STATE_"),
			time.UnixMilli(n.ChangedUnixMs).Format(time.TimeOnly))
	}
	tw.Flush()
	r := st.Repair
	fmt.Printf("\nrepair: %d passes; last at epoch %d took %d ms: scanned %d, under-replicated %d, repaired %d (%d from S3), backups %d, errors %d\n",
		r.Runs, r.LastEpoch, r.LastDurationMs, r.ChunksScanned, r.UnderReplicated, r.ReplicasRepaired, r.RestoredFromS3, r.BackupsTriggered, r.Errors)
}

func check(err error) {
	if err != nil {
		fail(err.Error())
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "dfs:", msg)
	os.Exit(1)
}
