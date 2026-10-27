package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	asm "rumue.io/asm/v2"
	"rumue.io/asm/v2/certs"
)

const (
	reset = "\033[0m"
	dim   = "\033[2m"
	bold  = "\033[1m"
)

// colors of the nodes in the output
var nodeColors = []string{"\033[38;5;39m", "\033[38;5;208m", "\033[38;5;170m", "\033[38;5;42m", "\033[38;5;196m", "\033[38;5;220m", "\033[38;5;75m"}

// highlights color lines by what happens in them
var highlights = []struct {
	contains string
	color    string
}{
	{"failed", "\033[31m"},
	{"banned", "\033[31m"},
	{"flagged", "\033[33m"},
	{"is locked by node", "\033[33m"},
	{"waiting", "\033[33m"},
	{"forwarding ", "\033[36m"},
	{"forwarded by another node", "\033[35m"},
	{"promoted", "\033[32m"},
	{"demoted", "\033[33m"},
	{"joined the cluster", "\033[32m"},
	{"is ready", "\033[1;32m"},
	{"became leader", "\033[1;32m"},
	{"committed bitmap", "\033[34m"},
	{"transitions", "\033[34m"},
}

// printer prints the output of all nodes aligned and colored.
type printer struct {
	mu    sync.Mutex
	start time.Time
	ready map[int]chan struct{}
}

func (p *printer) line(r role, text string) {
	color := ""
	for _, h := range highlights {
		if strings.Contains(text, h.contains) {
			color = h.color
			break
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	elapsed := time.Since(p.start).Seconds()
	fmt.Printf("%s%7.2fs%s %s%-12s%s %s%s%s\n",
		dim, elapsed, reset,
		nodeColors[r.id-1], r.name, reset,
		color, text, reset)
}

func (p *printer) system(text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("%s%7.2fs%s %s%-12s%s %s\n", dim, time.Since(p.start).Seconds(), reset, bold, "launcher", reset, text)
}

// read prints the lines a node writes. Node logs are "LEVEL<tab>message<tab>fields",
// raft logs are only shown for leader changes.
func (p *printer) read(r role, out io.Reader) {
	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		text := scanner.Text()

		if strings.HasPrefix(text, "raft") {
			if !strings.Contains(text, "became leader") && !strings.Contains(text, "elected leader") {
				continue
			}
			if i := strings.Index(text, "INFO: "); i >= 0 {
				text = "raft: " + text[i+len("INFO: "):]
			}
		} else if parts := strings.SplitN(text, "\t", 3); len(parts) >= 2 {
			level, msg := parts[0], parts[1]
			if len(parts) == 3 {
				msg += " " + dim + parts[2] + reset
			}
			text = msg
			if level == "WARN" || level == "ERROR" {
				text = level + " " + msg
			}
		}

		if strings.Contains(text, "is ready") {
			if c, ok := p.ready[r.id]; ok {
				select {
				case <-c:
				default:
					close(c)
				}
			}
		}
		p.line(r, text)
	}
}

func runLauncher(s settings, duration time.Duration) {
	basePort := s.basePort
	exe, err := os.Executable()
	if err != nil {
		fmt.Println("failed to find executable:", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	p := &printer{start: time.Now(), ready: make(map[int]chan struct{})}
	for _, r := range roles {
		p.ready[r.id] = make(chan struct{})
	}

	var (
		mu    sync.Mutex
		procs []*exec.Cmd
	)
	// every node gets its share of the CPUs, so the schedulers of 7 processes don't spin on the same cores
	maxProcs := os.Getenv("GOMAXPROCS")
	if maxProcs == "" {
		maxProcs = strconv.Itoa(max(2, (2*runtime.NumCPU()+len(roles)-1)/len(roles)))
	}

	startNode := func(r role) {
		cmd := exec.Command(exe, "-node", strconv.Itoa(r.id), "-base-port", strconv.Itoa(basePort),
			"-players", strconv.Itoa(s.players), "-pacing", s.pacing, "-trace", strconv.Itoa(s.trace), "-log", s.logMode,
			"-persist="+strconv.FormatBool(s.persist), "-tls="+strconv.FormatBool(s.tls))
		cmd.Env = append(os.Environ(), "GOMAXPROCS="+maxProcs)
		// collecting at twice the live heap instead of at once halves the GC work, memory is plenty here
		if os.Getenv("GOGC") == "" {
			cmd.Env = append(cmd.Env, "GOGC=200")
		}
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			p.system(fmt.Sprintf("failed to start %s: %v", r.name, err))
			return
		}
		mu.Lock()
		procs = append(procs, cmd)
		mu.Unlock()

		p.system(fmt.Sprintf("started %s (pid %d) on %s", r.name, cmd.Process.Pid, addr(basePort, r.id)))
		go p.read(r, stdout)
		go p.read(r, stderr)
	}

	// the gateways and progression share the players database, start with an empty one (and with empty raft logs:
	// every run starts a new cluster)
	dir := storeDir(basePort)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.system(fmt.Sprintf("failed to create player store: %v", err))
		os.Exit(1)
	}
	if err := createPlayersDB(playersDB(basePort)); err != nil {
		p.system(fmt.Sprintf("failed to create the players database: %v", err))
		os.Exit(1)
	}
	p.system("players database: " + playersDB(basePort))
	if s.tls {
		devtool, err := createCertificates(tlsDir(basePort))
		if err != nil {
			p.system(fmt.Sprintf("failed to create the certificates: %v", err))
			os.Exit(1)
		}
		p.system(fmt.Sprintf("certificates in %s, dev tool: go run ./devtool/cmd/asmdev -join %s %s", tlsDir(basePort), addr(basePort, starter().id), devtool))
	}
	p.system(fmt.Sprintf("%d players, pacing %s, logging %s, GOMAXPROCS %s per node, raft log %s", s.players, s.pacing, s.logMode, maxProcs,
		map[bool]string{true: "in " + filepath.Join(dir, "raft"), false: "in memory"}[s.persist]))

	first := starter()
	p.system(fmt.Sprintf("starting %d nodes, %s starts the cluster%s", len(roles), first.name, map[bool]string{true: fmt.Sprintf(", stopping after %s", duration), false: ", ctrl+c to stop"}[duration > 0]))
	startNode(first)

	select {
	case <-p.ready[first.id]:
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		p.system(first.name + " did not become ready")
	}

	// the other members join at the same time, the clients attach once they are ready
	var others []role
	for _, r := range roles {
		if r.member && r.id != first.id {
			startNode(r)
			others = append(others, r)
		}
	}
	for _, r := range others {
		select {
		case <-p.ready[r.id]:
		case <-ctx.Done():
		case <-time.After(30 * time.Second):
			p.system(r.name + " did not become ready")
		}
	}
	for _, r := range roles {
		if !r.member {
			startNode(r)
		}
	}

	<-ctx.Done()

	p.system("stopping nodes")
	mu.Lock()
	for _, cmd := range procs {
		_ = cmd.Process.Kill()
	}
	for _, cmd := range procs {
		_ = cmd.Wait()
	}
	mu.Unlock()
	p.system("done")
}

// createCertificates creates a CA for the run and a certificate per node with its role (member or client), and one
// with the observer role for the dev tool. It returns the dev tool's TLS flags.
func createCertificates(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	ca, err := certs.NewCA("gamemeta demo")
	if err != nil {
		return "", err
	}
	caFile, err := ca.WriteCA(dir)
	if err != nil {
		return "", err
	}
	for _, r := range roles {
		role := asm.RoleClient
		if r.member {
			role = asm.RoleMember
		}
		if _, _, err := ca.WriteFiles(dir, r.name, strconv.Itoa(r.id), role); err != nil {
			return "", err
		}
	}
	certFile, keyFile, err := ca.WriteFiles(dir, "asmdev", "asmdev", asm.RoleObserver)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("-ca %s -cert %s -key %s", caFile, certFile, keyFile), nil
}
