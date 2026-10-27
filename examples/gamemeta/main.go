// Command gamemeta runs the game meta services demo: 7 asm nodes as separate processes,
// which form a raft cluster, share the player bitmap, sync their transitions and forward player events
// to the node owning each step.
//
// Without flags it is the launcher: it starts the 7 nodes and prints their output.
//
//	go run ./examples/gamemeta
//	go run ./examples/gamemeta -duration 30s
package main

import (
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	asm "rumue.io/asm/v2"
)

type role struct {
	id      int
	name    string
	gateway bool
	region  string
	// store: the node loads and stores players in the shared player store
	store bool
	// member: the node runs raft and holds the lock table; the others are clients (WithClientMode), they use it
	// through a member and apply no log
	member bool
}

// roles of the 7 nodes. Three service nodes are the raft members, the first one starts the cluster and the others
// join it. The busiest nodes (gateways, progression, arena) are clients.
var roles = []role{
	{id: 1, name: "gateway-eu", gateway: true, region: "eu", store: true},
	{id: 2, name: "gateway-us", gateway: true, region: "us", store: true},
	{id: 3, name: "matchmaking", member: true},
	{id: 4, name: "store", member: true},
	{id: 5, name: "anticheat", member: true},
	{id: 6, name: "progression", store: true},
	// the arena runs the second entity, Match
	{id: 7, name: "arena"},
}

// starter returns the node which starts the cluster: the first raft member.
func starter() role {
	for _, r := range roles {
		if r.member {
			return r
		}
	}
	return roles[0]
}

// clusterOpts are the options of a node to join the cluster: every node but the starter joins it, clients attach.
// With -persist the members keep the raft log in a data directory.
func clusterOpts(r role, s settings) []asm.NodeSetupOption {
	var opts []asm.NodeSetupOption
	if r.id != starter().id {
		opts = append(opts, asm.WithJoinAddr(addr(s.basePort, starter().id)))
	}
	if !r.member {
		opts = append(opts, asm.WithClientMode())
	} else if s.persist {
		opts = append(opts, asm.WithDataDir(raftDir(s.basePort, r)))
	}
	if s.tls {
		// the launcher created the CA and a certificate per node
		dir := tlsDir(s.basePort)
		ca, cert, err := asm.LoadTLSFiles(filepath.Join(dir, "ca.pem"), filepath.Join(dir, r.name+".pem"), filepath.Join(dir, r.name+"-key.pem"))
		if err != nil {
			fmt.Printf("loading the certificate of %s: %v\n", r.name, err)
			os.Exit(1)
		}
		opts = append(opts, asm.WithTLS(ca, cert))
	}
	return opts
}

// tlsDir holds the throwaway CA and the certificates of a run, next to the players database.
func tlsDir(basePort int) string {
	return filepath.Join(storeDir(basePort), "tls")
}

// raftDir is the data directory of a member's raft log, next to the players database.
func raftDir(basePort int, r role) string {
	return filepath.Join(storeDir(basePort), "raft", r.name)
}

// settings are the flags the launcher passes on to the nodes
type settings struct {
	basePort int
	players  int
	pacing   string
	// trace is the number of events every node keeps for the dev tool, 0 turns the trace off
	trace   int
	logMode string
	// persist makes the members keep the raft log in data directories (WithDataDir)
	persist bool
	// tls secures the node ports with certificates of a CA the launcher creates for the run (WithTLS)
	tls bool
}

func addr(basePort, id int) string {
	return fmt.Sprintf("127.0.0.1:%d", basePort+id)
}

func main() {
	var s settings
	nodeId := flag.Int("node", 0, "run a single node with this id (1-7), used by the launcher")
	flag.IntVar(&s.basePort, "base-port", 7100, "node n listens on base-port + n")
	duration := flag.Duration("duration", 0, "stop after this duration, 0 runs until ctrl+c")
	flag.IntVar(&s.players, "players", 1000, "players, half on each gateway")
	flag.StringVar(&s.pacing, "pacing", "online", "online: sessions of 5-20 min with several matches, players think 2-8s; stress: a short session per match, players barely think")
	flag.IntVar(&s.trace, "trace", 2000, "events every node keeps for the dev tool, 0 turns the trace off")
	flag.StringVar(&s.logMode, "log", "auto", "events: log every event, stats: print counts every 5s, auto: events up to 100 players")
	flag.BoolVar(&s.persist, "persist", false, "the raft members write their log to data directories next to the players database (synced writes)")
	flag.BoolVar(&s.tls, "tls", true, "mutual TLS between the nodes with certificates of a CA created for the run")
	flag.Parse()

	if _, ok := pacings[s.pacing]; !ok {
		fmt.Printf("unknown pacing %q, use one of %s\n", s.pacing, strings.Join(slices.Sorted(maps.Keys(pacings)), ", "))
		os.Exit(2)
	}
	if s.logMode == "auto" {
		s.logMode = map[bool]string{true: "events", false: "stats"}[s.players <= 100]
	}
	eventLogs = s.logMode == "events"

	if *nodeId > 0 {
		if r := roles[*nodeId-1]; r.name == "arena" {
			runArena(r, s)
		} else {
			runNode(r, s)
		}
		return
	}
	runLauncher(s, *duration)
}
