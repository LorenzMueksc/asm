package asm

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type (
	// ContainerIdFunc derives the id of the container an event received via the Event rpc is sent to.
	// Returning an empty id rejects the event.
	ContainerIdFunc func(eventName string, payload proto.Message) string
	NodeSetupOption interface {
		apply(*nodeSetupOptions)
	}
	funcNodeSetupOption struct {
		f func(*nodeSetupOptions)
	}
	nodeSetupOptions struct {
		initF             any
		saveF             any
		closeF            any
		errorF            any
		containerIdF      ContainerIdFunc
		logger            *zap.Logger
		deps              Dependencies
		bitmap            *Bitmap
		maxContainerCount int
		maxHandOvers      int // WithMaxPendingHandOvers, 0 = default
		joinAddr          string
		stopC             chan struct{}
		skipRaft          bool
		leaseTimeout      time.Duration
		renewInterval     time.Duration
		snapshotCount     uint64
		snapshotCatchUp   uint64
		snapshotInterval  *time.Duration
		keepAlive         *time.Duration
		traceCapacity     int
		metricsAddr       string
		clientMode        bool
		dataDir           string
		tls               *nodeTLS
		publicAddr        string
		certCheck         func(*x509.Certificate) error
		maxConnAge        *time.Duration
	}
)

func (no *funcNodeSetupOption) apply(o *nodeSetupOptions) {
	no.f(o)
}

func newFuncNodeSetupOption(f func(*nodeSetupOptions)) *funcNodeSetupOption {
	return &funcNodeSetupOption{
		f: f,
	}
}

func WithInitFunc[T proto.Message, K Builder[T]](initF lifecycleFunc[T, K]) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(no *nodeSetupOptions) {
		no.initF = initF
	})
}

func WithSaveFunc[T proto.Message, K Builder[T]](saveF lifecycleFunc[T, K]) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(no *nodeSetupOptions) {
		no.saveF = saveF
	})
}

func WithCloseFunc[T proto.Message, K Builder[T]](closeF lifecycleFunc[T, K]) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(no *nodeSetupOptions) {
		no.closeF = closeF
	})
}

func WithErrorFunc[T proto.Message, K Builder[T]](errorF errorFunc[T, K]) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.errorF = errorF
	})
}

func WithLogger(logger *zap.Logger) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(n *nodeSetupOptions) {
		n.logger = logger
	})
}

func WithDependencies(deps Dependencies) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(n *nodeSetupOptions) {
		n.deps = deps
	})
}

func WithBitmapByPath(bitmapPath string) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(n *nodeSetupOptions) {
		bm, err := LoadBitMapFromDisk(bitmapPath)
		if err != nil {
			panic(err)
		}

		n.bitmap = bm
	})
}

func WithBitmap(bitmap *Bitmap) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(n *nodeSetupOptions) {
		n.bitmap = bitmap
	})
}

func WithMaxContainerCount(maxContainerCount int) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(n *nodeSetupOptions) {
		n.maxContainerCount = maxContainerCount
	})
}

func WithJoinAddr(joinAddr string) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.joinAddr = joinAddr
	})
}

// WithEventTrace keeps the last capacity events of the node for dev tools: events processed by its containers,
// events forwarded to other nodes or received from them, container errors, and containers starting and closing.
// They are served by the WatchEvents rpc. It costs one JSON encoding per payload, so it is off by default.
func WithEventTrace(capacity int) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.traceCapacity = capacity
	})
}

// WithSnapshotInterval sets the minimum time between two snapshots (default 30s), 0 takes one after every
// snapshot count entries. A log grown to 64 MB since the last snapshot is compacted right away anyway.
func WithSnapshotInterval(interval time.Duration) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.snapshotInterval = &interval
	})
}

// defaultContainerKeepAlive covers back-to-back events and a local round trip (a waited forward, a raft commit)
const defaultContainerKeepAlive = 5 * time.Millisecond

// WithContainerKeepAlive sets how long the goroutine of a container waits for more work after it processed
// everything queued, before it ends (default 5ms, 0 ends at once). An idle container has no goroutine; work arriving
// while it waits is processed without starting a new one.
func WithContainerKeepAlive(d time.Duration) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.keepAlive = &d
	})
}

// WithMetricsAddr serves the metrics of the node in the Prometheus text format on addr, at /metrics.
// With WithTLS it serves HTTPS with the node's certificate and requires a client certificate of the CA with the role
// observer or admin; without it plain HTTP. The output is collected at most once per second.
// The Inspect rpc returns them too, MetricsHandler serves them for an own HTTP server.
func WithMetricsAddr(addr string) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.metricsAddr = addr
	})
}

// WithDataDir persists the raft log and snapshots of a raft member in dir, so the node restarts with its state after
// a crash or a shutdown and catches up with the cluster instead of joining again (the join address is ignored then).
// Without it the raft state is in memory only. Clients and nodes without raft don't use it.
// The node must restart with the same id and listen address. After it left or was removed, it stops on restart;
// delete the directory to join again.
func WithDataDir(dir string) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.dataDir = dir
	})
}

// WithTLS secures the node port with mutual TLS: ca verifies the certificates of the other nodes and tools, cert is
// this node's certificate, with the node id as common name and the role member (raft member) or client
// (WithClientMode) as organizational unit. Every rpc checks the caller's roles, and a caller may only act in its own
// name (see node_tls.go). The certificate of a node id removed from the cluster is rejected, so a removed id can't
// join again. All nodes of a cluster use it or none. LoadTLSFiles loads the files, package certs creates
// certificates for tests and development. See also WithCertificateCheck and WithMaxConnectionAge.
func WithTLS(ca *x509.CertPool, cert tls.Certificate) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.tls = &nodeTLS{ca: ca, cert: cert}
	})
}

// WithPublicAddr serves the Event rpc on a separate listener for callers outside the cluster, which don't have a
// node certificate. With WithTLS the listener uses TLS with the node's certificate, without a client certificate.
// The other rpcs are unimplemented there.
func WithPublicAddr(addr string) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.publicAddr = addr
	})
}

func WithStopChannel(stopC chan struct{}) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.stopC = stopC
	})
}

// WithClientMode makes the node a client: it doesn't run raft, it attaches to a raft member (WithJoinAddr) and uses
// the lock table through it. Clients apply no log, so their CPU goes to containers.
func WithClientMode() *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.clientMode = true
	})
}

func WithSkipRaft() *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.skipRaft = true
	})
}

// WithContainerIdFunc sets the func deriving the container id for events received via the Event rpc.
// Without it the node rejects these events.
func WithContainerIdFunc(f ContainerIdFunc) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.containerIdF = f
	})
}

// WithLeaseTimeout sets how long the container locks of this node stay valid without a heartbeat,
// and how often the node renews them. Defaults: 10s lease, 3s renew interval.
// Expiry is judged from the timestamps of the log entries, so the clocks of the nodes must be in sync
// well below the lease.
func WithLeaseTimeout(lease, renewInterval time.Duration) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.leaseTimeout = lease
		options.renewInterval = renewInterval
	})
}

// WithMaxPendingHandOvers limits the hand-overs (events forwarded without WithWaitRemote) the node keeps until their
// receivers ran them, default 10000. With the limit reached, a container handing work over waits until one of them
// ran or failed, bounded only by its context: a container must not hand over more work for its own id than the limit
// while it is open, that work waits for its lock.
func WithMaxPendingHandOvers(max int) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.maxHandOvers = max
	})
}

// WithSnapshotCount compacts the raft log into a snapshot after count applied entries since the last snapshot,
// keeping catchUpEntries entries so slow followers can catch up without a snapshot. Defaults: 10000 and 5000.
func WithSnapshotCount(count, catchUpEntries uint64) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.snapshotCount = count
		options.snapshotCatchUp = catchUpEntries
	})
}

// WithCertificateCheck adds an own check of the peers' certificates to WithTLS, eg. a revocation list: check gets the
// leaf certificate, already verified against the CA. It runs for every rpc to the node port (an error rejects the rpc
// with PermissionDenied; a node rejected so stops like a removed one), for the metrics endpoint, and when the node
// dials another node (an error fails the handshake). It runs often, so it should be fast, eg. a lookup in a set
// that is refreshed elsewhere. Without WithTLS it has no effect.
func WithCertificateCheck(check func(*x509.Certificate) error) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.certCheck = check
	})
}

// WithMaxConnectionAge sets how long a client may open rpcs on a connection to the node port with WithTLS (default
// 1h, 0 = unlimited). Then the node asks it to open a new connection (GOAWAY), whose handshake verifies the
// certificates again; streams on the old connection go on. Independent of it, a connection ends once its client
// certificate expired.
func WithMaxConnectionAge(age time.Duration) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.maxConnAge = &age
	})
}
