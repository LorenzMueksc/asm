package asm

import (
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// Locality: a node may have a region (WithRegion), a path like "eu/de/fra". The nodes learn each other's regions from
// the raft log (NODE_REGION, which every node proposes once it is ready), clients from their member with the
// membership. Forwards cover the matched remote transitions with the nearest owners first (coverTransitions):
// nearness is the number of leading path segments two regions share. Among equally near owners the one owning most
// of the transitions goes first, random ties, as without regions; an unreachable owner is covered again the same way,
// so the next nearest takes over. A node without region, or owners without one, are equally far from everyone.

type regions struct {
	// mu serializes the changes; m is replaced as a whole, so readers on the forward path don't lock
	mu sync.Mutex
	m  atomic.Pointer[map[string]string]
	// changedC is closed on the next change, so announceRegion waits for its entry without polling
	changedC chan struct{}
}

// normalizeRegion trims the slashes around the segments of a region path.
func normalizeRegion(path string) string {
	segments := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	return strings.Join(segments, "/")
}

// regionNearness returns the number of leading segments the regions share, 0 if one of them is unknown.
func regionNearness(a, b string) int {
	if a == "" || b == "" {
		return 0
	}
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	k := 0
	for k < len(as) && k < len(bs) && as[k] == bs[k] {
		k++
	}
	return k
}

// regionOf returns the region of the node, "" if it has none or it is unknown.
func (r *regions) regionOf(node string) string {
	if m := r.m.Load(); m != nil {
		return (*m)[node]
	}
	return ""
}

// all returns a copy of the regions by node id.
func (r *regions) all() map[string]string {
	if m := r.m.Load(); m != nil {
		return maps.Clone(*m)
	}
	return map[string]string{}
}

// set sets the region of the node, "" removes it; it reports whether it changed.
func (r *regions) set(node, path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.regionOf(node) == path {
		return false
	}
	m := r.all()
	if path == "" {
		delete(m, node)
	} else {
		m[node] = path
	}
	r.m.Store(&m)
	r.changedLocked()
	return true
}

// replace sets all regions, from a snapshot or a client's membership update.
func (r *regions) replace(m map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m = maps.Clone(m)
	if m == nil {
		m = map[string]string{}
	}
	r.m.Store(&m)
	r.changedLocked()
}

func (r *regions) changedLocked() {
	if r.changedC != nil {
		close(r.changedC)
		r.changedC = nil
	}
}

// changed returns a channel closed on the next change.
func (r *regions) changed() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.changedC == nil {
		r.changedC = make(chan struct{})
	}
	return r.changedC
}

// nearness returns how near the node is to this one: the leading segments their regions share.
func (n *Node[T, K]) nearness(node string) int {
	return regionNearness(n.region, n.regions.regionOf(node))
}

// announceRegion proposes the region of this node until the log has it. It doesn't hold up the start: forwards work
// without it, the other nodes just don't know yet that this one is near.
func (n *Node[T, K]) announceRegion() {
	if n.region == "" || n.skipRaft {
		return
	}
	l := &service.NodeLog{
		NodeId: n.Id(),
		Type:   service.NodeEventType_NODE_REGION,
		Body:   &service.NodeLog_Region{Region: &service.NodeRegion{Path: n.region}},
	}
	for n.regions.regionOf(n.Id()) != n.region {
		changedC := n.regions.changed()
		l.Ts = timestamppb.Now()
		if err := n.propose(l); err != nil {
			debugf(n.logger, "proposing the region %s failed, retrying: %v", n.region, err)
		}
		select {
		case <-changedC:
		case <-time.After(proposeRetryInterval):
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		}
	}
}

// applyRegionLog sets the region of the node of the entry and tells the clients.
func (n *Node[T, K]) applyRegionLog(l *service.NodeLog) {
	path := normalizeRegion(l.GetRegion().GetPath())
	if !n.regions.set(l.GetNodeId(), path) {
		return
	}
	debugf(n.logger, "node %s is in region %s", l.GetNodeId(), path)
	if !n.client {
		n.notifyClients(false, true)
	}
}

// checkRegion takes the region of the node's certificate if the node has none, and refuses a different one.
func checkRegion(own, cert string) (string, error) {
	switch {
	case cert == "":
		return own, nil
	case own == "":
		return cert, nil
	case own != cert:
		return "", fmt.Errorf("the region %q differs from the region %q of the TLS certificate", own, cert)
	}
	return own, nil
}
