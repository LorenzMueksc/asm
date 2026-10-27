package asm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// A container id may only be open on one node of the cluster. Locks are leases in the raft log:
// acquire, release and cancel entries change the lock table, heartbeat entries of a node renew all its locks.
// A lock expires when the last entry of its holder (any entry, heartbeats are only the ones sent when nothing else is)
// is older than the holder's lease.
// Expiry is judged from the timestamps in the entries, so every node applying the log decides the same.
//
// Waited calls can't deadlock. A waited call carries its wait path: the lock tokens of the containers waiting for it,
// up the chain of waited calls (a container on a lent lock adds nothing, its lender is on the path already).
// A receiver of another entity acquires the lock of its container with the path, so the lock table knows who waits
// for whom: the containers on the path of a queued acquire, or of a call running in the holder's container (calls),
// wait for the holder. An acquire with a path is refused (ErrWouldDeadlock) if the holder waits for a container on
// the path, directly or through other locks: waiting would close a cycle. Every node decides the same from the log.
// Waited calls wait nowhere else: not for a container slot, not for a start of the id on their node, and an open
// container of the id is only joined through the lock table (join). A call ends with LOCK_DONE.
//
// A node keeps the lock of a closed container (retained) until another node queues for it, then it releases it right
// away; opening the container again on the node needs no log entry. A node leaving the cluster releases on close.
//
// Acquires of a held lock are queued: a released or expired lock goes to the first waiter whose node
// is still alive. A holder asking again while others wait queues behind them. A waiter which gives up
// cancels its place; a lock handed to an attempt which already gave up is released right away.

const (
	defaultLeaseTimeout  = 10 * time.Second
	defaultRenewInterval = 3 * time.Second
)

type (
	lockTable struct {
		mu    sync.Mutex
		locks map[string]containerLock
		nodes map[string]nodeLiveness
		// waiters holds a channel per container id somebody waits on. It is closed and removed on the next change
		// of that lock, so a change only wakes the goroutines waiting for that lock.
		waiters map[string]chan struct{}
		// livenessC is closed on the next change of a node's liveness, nil while nobody waits for one
		livenessC chan struct{}
		// expiries holds a channel per node whose lease somebody waits for, closed by the expiry watcher once
		// the node's lease ended, so queued attempts don't need a timer each
		expiries map[string]chan struct{}
		// waitedFor indexes the wait paths: for every lock token on the path of a waiter or a call, the ids of those
		// locks with the number of waiters and calls having it. The deadlock check follows it from token to holder.
		waitedFor map[string]map[string]int
	}
	// containerLock is the lock of a container id. A lock in the table always has a holder.
	// It is replaced as a whole on every change, so copies handed out are never modified.
	containerLock struct {
		holder string
		token  string
		// queue holds the nodes waiting for the lock, in the order they asked
		queue []lockWaiter
		// calls are the waited calls running in the holder's container or waiting for their turn there.
		// They belong to the holder's container: a new holder starts without calls.
		calls []lockCall
	}
	lockWaiter struct {
		node  string
		token string
		// waitPath holds the lock tokens of the containers waiting for the waiter, nil if nobody does
		waitPath []string
	}
	lockCall struct {
		token    string
		waitPath []string
	}
	nodeLiveness struct {
		last  time.Time
		lease time.Duration
	}
)

func newLockTable() lockTable {
	return lockTable{
		locks:     make(map[string]containerLock),
		nodes:     make(map[string]nodeLiveness),
		waiters:   make(map[string]chan struct{}),
		waitedFor: make(map[string]map[string]int),
		expiries:  make(map[string]chan struct{}),
	}
}

// ErrWouldDeadlock is returned when a waited call of another entity would wait for a container which waits for it:
// the lock of the container it calls is held by a container on its wait path, or by one waiting for such a container.
var ErrWouldDeadlock = errors.New("waiting for the lock would deadlock")

// maxWaitPath caps the lock tokens on the wait path of a waited call: a chain of waited calls that long is a bug or
// forged, and the deadlock check walks the path. Every member applies the same cap (applyAcquire), so it is fixed.
const maxWaitPath = 64

// checkWaitPath rejects a wait path longer than maxWaitPath.
func checkWaitPath(path []string) error {
	if len(path) > maxWaitPath {
		return fmt.Errorf("the wait path has %d lock tokens, at most %d are allowed", len(path), maxWaitPath)
	}
	return nil
}

// queued reports whether the node waits for the lock with the token.
func (l containerLock) queued(node, token string) bool {
	return slices.ContainsFunc(l.queue, func(w lockWaiter) bool { return w.node == node && w.token == token })
}

// called reports whether a waited call with the token runs in the holder's container.
func (l containerLock) called(token string) bool {
	return slices.ContainsFunc(l.calls, func(c lockCall) bool { return c.token == token })
}

// waitPaths calls f with every wait path of the waiters and calls of the lock.
func (l containerLock) waitPaths(f func(path []string)) {
	for _, w := range l.queue {
		if len(w.waitPath) > 0 {
			f(w.waitPath)
		}
	}
	for _, c := range l.calls {
		f(c.waitPath)
	}
}

// waitsForLocked reports whether the container holding the lock token waits for a container on the wait path:
// it is one of them, or it is on the wait path of a waiter or a call of a lock whose holder waits for one.
// The caller holds mu.
func (lt *lockTable) waitsForLocked(token string, path []string) bool {
	seen := map[string]bool{token: true}
	next := []string{token}
	for len(next) > 0 {
		t := next[len(next)-1]
		next = next[:len(next)-1]
		if slices.Contains(path, t) {
			return true
		}
		for id := range lt.waitedFor[t] {
			if h := lt.locks[id].token; !seen[h] {
				seen[h] = true
				next = append(next, h)
			}
		}
	}
	return false
}

// indexLocked adds (delta 1) or removes (delta -1) the wait paths of the lock to the index. The caller holds mu.
func (lt *lockTable) indexLocked(id string, l containerLock, delta int) {
	l.waitPaths(func(path []string) {
		for _, t := range path {
			ids := lt.waitedFor[t]
			if ids == nil {
				ids = make(map[string]int)
				lt.waitedFor[t] = ids
			}
			if ids[id] += delta; ids[id] <= 0 {
				delete(ids, id)
				if len(ids) == 0 {
					delete(lt.waitedFor, t)
				}
			}
		}
	})
}

// wake wakes the goroutines waiting for the lock of the id.
func (lt *lockTable) wake(id string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.changedLocked(id)
}

// changedLocked wakes the goroutines waiting for the lock of the id. The caller holds mu.
func (lt *lockTable) changedLocked(id string) {
	if c, ok := lt.waiters[id]; ok {
		close(c)
		delete(lt.waiters, id)
	}
}

// livenessChangedLocked wakes the goroutines waiting for a change of liveness. The caller holds mu.
func (lt *lockTable) livenessChangedLocked() {
	if lt.livenessC != nil {
		close(lt.livenessC)
		lt.livenessC = nil
	}
}

// changedAllLocked wakes all waiters, eg. when a snapshot replaced the table. The caller holds mu.
func (lt *lockTable) changedAllLocked() {
	for id := range lt.waiters {
		lt.changedLocked(id)
	}
	lt.livenessChangedLocked()
}

// peek returns the lock of the id.
func (lt *lockTable) peek(id string) (containerLock, bool) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	l, held := lt.locks[id]
	return l, held
}

// get returns the lock of the id and a channel closed on the next change of that lock.
func (lt *lockTable) get(id string) (containerLock, bool, <-chan struct{}) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	c, ok := lt.waiters[id]
	if !ok {
		c = make(chan struct{})
		lt.waiters[id] = c
	}
	l, held := lt.locks[id]
	return l, held, c
}

// hasLiveness reports whether the node has a liveness entry, and returns a channel closed on the next change
// of a liveness.
func (lt *lockTable) hasLiveness(nodeId string, since time.Time) (bool, <-chan struct{}) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if lt.livenessC == nil {
		lt.livenessC = make(chan struct{})
	}
	l, ok := lt.nodes[nodeId]
	return ok && !l.last.Before(since), lt.livenessC
}

// expiredLocked reports whether the locks of the node are expired at the given time. The caller holds mu.
func (lt *lockTable) expiredLocked(nodeId string, at time.Time) bool {
	l, ok := lt.nodes[nodeId]
	return !ok || l.last.Add(l.lease).Before(at)
}

func (lt *lockTable) touchLocked(nodeId string, at time.Time, lease time.Duration) {
	l := lt.nodes[nodeId]
	if at.After(l.last) {
		l.last = at
	}
	if lease > 0 {
		l.lease = lease
	}
	lt.nodes[nodeId] = l
	lt.livenessChangedLocked()
}

func (lt *lockTable) applyHeartbeat(nodeId string, at time.Time, lease time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.touchLocked(nodeId, at, lease)
}

// grantNextLocked hands the lock to the first waiter whose node is alive at the given time,
// dropping the expired ones before it. A zero time doesn't check liveness.
// Without a live waiter the lock has no holder anymore. The caller holds mu.
func (lt *lockTable) grantNextLocked(l containerLock, at time.Time) containerLock {
	queue := l.queue
	l.holder, l.token, l.calls = "", "", nil
	for len(queue) > 0 {
		w := queue[0]
		queue = queue[1:]
		if !at.IsZero() && lt.expiredLocked(w.node, at) {
			continue
		}
		l.holder, l.token = w.node, w.token
		// a waited call runs in the container it got the lock for
		if len(w.waitPath) > 0 {
			l.calls = []lockCall{{token: w.token, waitPath: w.waitPath}}
		}
		break
	}
	l.queue = slices.Clone(queue)
	return l
}

// expireLocked hands the lock to the next waiter if its holder expired at the given time. The caller holds mu.
func (lt *lockTable) expireLocked(l containerLock, at time.Time) containerLock {
	if l.holder != "" && lt.expiredLocked(l.holder, at) {
		return lt.grantNextLocked(l, at)
	}
	return l
}

// storeLocked stores the lock, or removes it once it has no holder. The caller holds mu.
func (lt *lockTable) storeLocked(id string, l containerLock) {
	lt.putLocked(id, l)
	lt.changedLocked(id)
}

// putLocked stores the lock without waking anybody. The caller holds mu.
func (lt *lockTable) putLocked(id string, l containerLock) {
	lt.indexLocked(id, lt.locks[id], -1)
	if l.holder == "" {
		delete(lt.locks, id)
		return
	}
	lt.locks[id] = l
	lt.indexLocked(id, l, 1)
}

// applyAcquire gives the lock to the node if it is free, or expired without other waiters.
// An acquire nobody waits for (no wait path): the holder gets it again with the new token if nobody waits and no call
// runs, otherwise the node queues, or updates the token of its place in the queue.
// A waited call (wait path) is refused if the holder waits for a container on its path. Otherwise it joins the holder's
// container if that is the one it asked to join (join), or queues. A wait path longer than maxWaitPath is refused.
// It returns the lock before and after, and whether the acquire was refused.
func (lt *lockTable) applyAcquire(nodeId string, at time.Time, id, token string, waitPath []string, join string) (before, after containerLock, refused bool) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.touchLocked(nodeId, at, 0)
	before = lt.locks[id]
	after = lt.expireLocked(before, at)

	switch {
	case len(waitPath) > maxWaitPath:
		// refused before anything else: such an acquire is never stored, so it can't be a proposal again
		refused = true
	case after.holder == "":
		after = containerLock{holder: nodeId, token: token}
		if len(waitPath) > 0 {
			after.calls = []lockCall{{token: token, waitPath: waitPath}}
		}
	case after.holder == nodeId && after.token == token, after.queued(nodeId, token), after.called(token):
		// proposed again, eg. to have an expired holder replaced
	case len(waitPath) > 0 && lt.waitsForLocked(after.token, waitPath):
		refused = true
	case len(waitPath) > 0 && join != "" && after.holder == nodeId && after.token == join:
		after.calls = append(slices.Clone(after.calls), lockCall{token: token, waitPath: waitPath})
	case len(waitPath) == 0 && after.holder == nodeId && len(after.queue) == 0 && len(after.calls) == 0:
		after.token = token
	default:
		queue := slices.Clone(after.queue)
		// an acquire nobody waits for keeps the place of an earlier one of its node
		i := -1
		if len(waitPath) == 0 {
			i = slices.IndexFunc(queue, func(w lockWaiter) bool { return w.node == nodeId && len(w.waitPath) == 0 })
		}
		if i >= 0 {
			queue[i].token = token
		} else {
			queue = append(queue, lockWaiter{node: nodeId, token: token, waitPath: waitPath})
		}
		after.queue = queue
	}

	if refused {
		return before, before, true
	}
	lt.storeLocked(id, after)
	return before, after, false
}

// applyDone ends a waited call in the holder's container. It returns the lock before and after.
func (lt *lockTable) applyDone(nodeId string, at time.Time, id, token string) (before, after containerLock) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.touchLocked(nodeId, at, 0)
	before = lt.locks[id]
	after = lt.expireLocked(before, at)
	if after.called(token) {
		after.calls = slices.DeleteFunc(slices.Clone(after.calls), func(c lockCall) bool { return c.token == token })
	}

	lt.storeLocked(id, after)
	return before, after
}

// applyRelease hands the lock to the next waiter if the node holds it with the token.
// It returns the lock before and after.
func (lt *lockTable) applyRelease(nodeId string, at time.Time, id, token string) (before, after containerLock) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.touchLocked(nodeId, at, 0)
	before = lt.locks[id]
	if before.holder == nodeId && before.token == token {
		after = lt.grantNextLocked(before, at)
	} else {
		after = lt.expireLocked(before, at)
	}

	lt.storeLocked(id, after)
	return before, after
}

// applyCancel removes the node from the queue, or a call which gave up from the calls. If the lock was already
// handed to the attempt, it goes to the next waiter. It returns the lock before and after.
func (lt *lockTable) applyCancel(nodeId string, at time.Time, id, token string) (before, after containerLock) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.touchLocked(nodeId, at, 0)
	before = lt.locks[id]
	if before.holder == nodeId && before.token == token {
		after = lt.grantNextLocked(before, at)
	} else {
		after = before
		after.queue = slices.DeleteFunc(slices.Clone(before.queue), func(w lockWaiter) bool { return w.node == nodeId && w.token == token })
		after.calls = slices.DeleteFunc(slices.Clone(before.calls), func(c lockCall) bool { return c.token == token })
		after = lt.expireLocked(after, at)
	}

	lt.storeLocked(id, after)
	return before, after
}

// removeNode drops a node removed from the cluster from all queues, hands its locks to the next waiters
// and forgets its liveness. It returns the locks before and after for every changed lock.
func (lt *lockTable) removeNode(nodeId string) map[string][2]containerLock {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	changes := make(map[string][2]containerLock)
	for id, before := range lt.locks {
		after := before
		after.queue = slices.DeleteFunc(slices.Clone(before.queue), func(w lockWaiter) bool { return w.node == nodeId })
		if after.holder == nodeId {
			// a removal has no timestamp, so the waiters' liveness isn't checked
			after = lt.grantNextLocked(after, time.Time{})
		}
		if after.holder != before.holder || after.token != before.token || len(after.queue) != len(before.queue) {
			changes[id] = [2]containerLock{before, after}
			lt.storeLocked(id, after)
		}
	}
	delete(lt.nodes, nodeId)
	lt.livenessChangedLocked()
	return changes
}

// expiry returns a channel which the expiry watcher closes once the lease of the node ended.
func (lt *lockTable) expiry(node string) <-chan struct{} {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	c, ok := lt.expiries[node]
	if !ok {
		c = make(chan struct{})
		lt.expiries[node] = c
	}
	return c
}

// closeExpired closes the expiry channels of the nodes whose lease ended at the given time.
func (lt *lockTable) closeExpired(at time.Time) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	for node, c := range lt.expiries {
		if lt.expiredLocked(node, at) {
			close(c)
			delete(lt.expiries, node)
		}
	}
}

func lockLog(nodeId string, t service.NodeEventType, lock *service.ContainerLock) *service.NodeLog {
	return &service.NodeLog{NodeId: nodeId, Type: t, Body: &service.NodeLog_Lock{Lock: lock}, Ts: timestamppb.Now()}
}

// acquireLock acquires the lock of the container id for this node. A waited call passes its wait path and the token
// of the open container of this node it wants to join (see applyAcquire); joined reports that it got a place in
// that container instead of the lock. It returns ErrWouldDeadlock if the lock table refused the call, an error
// without asking if the wait path is too long (the lock table would refuse it).
// While another node holds it, this node waits in its queue until the lock is handed to it, or ctx is done.
// Giving up cancels the place in the queue.
// Without raft no lock is needed and the token is empty.
func (n *Node[T, K]) acquireLock(ctx context.Context, id string, waitPath []string, join string) (token string, joined bool, err error) {
	if n.skipRaft {
		return "", false, nil
	}
	if err := checkWaitPath(waitPath); err != nil {
		return "", false, fmt.Errorf("lock of container %s: %w", id, err)
	}

	token = n.newLockToken()
	l := lockLog(n.Id(), service.NodeEventType_LOCK_ACQUIRE, &service.ContainerLock{ContainerId: id, Token: token, WaitPath: waitPath, Join: join})

	// the token is in use from now until it gave up or its lock is released. A lock handed to a token
	// which isn't in use anymore is released (see lockChanged). The token stays in use on success: the apply
	// goroutine wakes this one before it checks the token, so removing it here would release a lock in use.
	n.startingMu.Lock()
	n.lockTokens[token] = true
	n.startingMu.Unlock()
	acquired := false
	defer func() {
		if !acquired {
			n.dropLockToken(token)
		}
	}()

	var prop proposalState
	waitingLogged := false
	start := time.Now()
	for {
		cur, held, changedC := n.locks.get(id)
		if held && cur.holder == n.Id() && cur.token == token {
			acquired = true
			n.metrics.lockAcquires.Add(1)
			n.metrics.lockWait.observe(time.Since(start))
			return token, false, nil
		}
		if held && cur.holder == n.Id() && cur.token == join && cur.called(token) {
			acquired = true
			n.metrics.lockJoins.Add(1)
			n.metrics.lockWait.observe(time.Since(start))
			return token, true, nil
		}
		if n.lockRefused(token) {
			n.metrics.lockRefused.Add(1)
			return "", false, fmt.Errorf("lock of container %s: %w", id, ErrWouldDeadlock)
		}

		// propose until the attempt is in the queue (see needsPropose), and again once the holder's lease ended,
		// so applying the entry, which has a later timestamp, hands the expired lock on
		queued := held && (cur.queued(n.Id(), token) || cur.called(token))
		expired := held && cur.holder != n.Id() && n.lockExpiredNow(cur.holder)
		if (!queued && n.needsPropose(&prop)) || (expired && time.Since(prop.at) >= proposeRetryInterval) {
			if err := n.proposeTracked(&prop, l); err != nil {
				debugf(n.logger, "proposing lock of %s failed, retrying: %v", id, err)
			}
		}

		// a queued attempt waits until the lock changes or the holder's lease ended, without a timer
		var expiredC <-chan struct{}
		if held && cur.holder != n.Id() {
			if !waitingLogged {
				debugf(n.logger, "container %s is locked by node %s, waiting", id, cur.holder)
				waitingLogged = true
			}
			if queued {
				expiredC = n.locks.expiry(cur.holder)
			}
		}
		var timer *time.Timer
		var timerC <-chan time.Time
		if expiredC == nil {
			timer = time.NewTimer(proposeRetryInterval)
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			stopTimer(timer)
			n.cancelLock(id, token)
			return "", false, ctx.Err()
		case <-n.failedC:
			stopTimer(timer)
			return "", false, n.failure()
		case <-changedC:
		case <-expiredC:
		case <-timerC:
		}
		stopTimer(timer)
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

// watchExpiry closes the expiry channels of the nodes whose lease ended, every retry interval, until the node stops.
func (n *Node[T, K]) watchExpiry() {
	ticker := time.NewTicker(proposeRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.locks.closeExpired(time.Now())
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		}
	}
}

func (n *Node[T, K]) lockExpiredNow(holder string) bool {
	n.locks.mu.Lock()
	defer n.locks.mu.Unlock()

	return n.locks.expiredLocked(holder, time.Now())
}

// endLock ends the lock of a closed container: it keeps the lock if nobody waits for it, otherwise it releases it.
// The lock is kept first and the queue checked afterwards: a waiter queuing meanwhile is seen either here or by
// lockChanged, which releases kept locks somebody queued for.
func (n *Node[T, K]) endLock(id, token string) {
	if token == "" {
		return
	}
	if !n.leaving.Load() {
		n.startingMu.Lock()
		n.retained[id] = token
		n.startingMu.Unlock()

		cur, held := n.locks.peek(id)
		if held && cur.holder == n.Id() && cur.token == token && len(cur.queue) == 0 {
			return
		}
		if !n.unretain(id, token) {
			// lockChanged released it meanwhile
			return
		}
	}
	n.releaseLock(id, token)
}

// unretain stops keeping the lock of the id and reports whether it was kept with the token.
func (n *Node[T, K]) unretain(id, token string) bool {
	n.startingMu.Lock()
	defer n.startingMu.Unlock()

	if n.retained[id] != token {
		return false
	}
	delete(n.retained, id)
	return true
}

// reuseRetained takes the lock the node kept for the id, if it still holds it and nobody waits for it.
func (n *Node[T, K]) reuseRetained(id string) (string, bool) {
	n.startingMu.Lock()
	token, ok := n.retained[id]
	delete(n.retained, id)
	n.startingMu.Unlock()
	if !ok {
		return "", false
	}

	cur, held := n.locks.peek(id)
	switch {
	case held && cur.holder == n.Id() && cur.token == token && len(cur.queue) == 0:
		n.metrics.lockReuses.Add(1)
		return token, true
	case held && cur.holder == n.Id() && cur.token == token:
		// a node queued meanwhile, lockChanged didn't see it kept anymore: it gets the lock first
		n.releaseLock(id, token)
	default:
		// lost meanwhile (expired, node removed)
		n.dropLockToken(token)
	}
	return "", false
}

// retainedCount returns the number of locks the node keeps.
func (n *Node[T, K]) retainedCount() int {
	n.startingMu.Lock()
	defer n.startingMu.Unlock()
	return len(n.retained)
}

// releaseLock releases the lock of the container id in the background, retrying until the release is committed.
func (n *Node[T, K]) releaseLock(id, token string) {
	if token != "" {
		n.metrics.lockReleases.Add(1)
	}
	n.dropLockToken(token)
	n.proposeLockEnd(id, token, service.NodeEventType_LOCK_RELEASE)
}

// lockDone ends a waited call in the container of the id in the background, retrying until it is committed.
// The token of a call which joined a container isn't used anymore, the one of a call which got the lock is released
// with the container.
func (n *Node[T, K]) lockDone(id, token string, joined bool) {
	if joined {
		n.dropLockToken(token)
	}
	n.proposeLockEnd(id, token, service.NodeEventType_LOCK_DONE)
}

// dropLockToken marks a token of this node as not in use anymore.
func (n *Node[T, K]) dropLockToken(token string) {
	n.startingMu.Lock()
	defer n.startingMu.Unlock()
	delete(n.lockTokens, token)
	delete(n.refusedTokens, token)
}

// refuseLockToken marks an acquire of this node as refused, it would deadlock.
func (n *Node[T, K]) refuseLockToken(token string) {
	n.startingMu.Lock()
	defer n.startingMu.Unlock()
	if n.lockTokens[token] {
		n.refusedTokens[token] = true
	}
}

func (n *Node[T, K]) lockRefused(token string) bool {
	n.startingMu.Lock()
	defer n.startingMu.Unlock()
	return n.refusedTokens[token]
}

// cancelLock cancels an attempt which gave up in the background, retrying until it is out of the queue
// and doesn't hold the lock.
func (n *Node[T, K]) cancelLock(id, token string) {
	n.proposeLockEnd(id, token, service.NodeEventType_LOCK_CANCEL)
}

// proposeLockEnd proposes a release, cancel or done entry until the token neither holds the lock, nor waits for it,
// nor runs as a call in its container. A cancel is proposed at least once, because the acquire it cancels may not be
// applied yet.
func (n *Node[T, K]) proposeLockEnd(id, token string, t service.NodeEventType) {
	if n.skipRaft || token == "" {
		return
	}

	l := lockLog(n.Id(), t, &service.ContainerLock{ContainerId: id, Token: token})

	go func() {
		first := true
		var prop proposalState
		for {
			cur, _, changedC := n.locks.get(id)
			active := cur.called(token)
			if t != service.NodeEventType_LOCK_DONE {
				active = active || (cur.holder == n.Id() && cur.token == token) || cur.queued(n.Id(), token)
			}
			if !active && !(first && t == service.NodeEventType_LOCK_CANCEL) {
				return
			}
			first = false

			if n.needsPropose(&prop) {
				if err := n.proposeTracked(&prop, l); err != nil {
					debugf(n.logger, "proposing %s of lock %s failed, retrying: %v", t, id, err)
				}
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
	}()
}

// heartbeat renews all locks of this node every renew interval until the node stops.
func (n *Node[T, K]) heartbeat() {
	hb := &service.NodeLog_Heartbeat{Heartbeat: &service.Heartbeat{Lease: durationpb.New(n.leaseTimeout)}}

	ticker := time.NewTicker(n.renewInterval)
	defer ticker.Stop()

	for {
		err := n.propose(&service.NodeLog{
			NodeId: n.Id(),
			Type:   service.NodeEventType_NODE_HEARTBEAT,
			Body:   hb,
			Ts:     timestamppb.Now(),
		})
		if err != nil {
			debugf(n.logger, "proposing heartbeat failed: %v", err)
		}

		select {
		case <-ticker.C:
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		}
	}
}

// waitForHeartbeat waits until a heartbeat of this node is committed, so its locks have a lease.
func (n *Node[T, K]) waitForHeartbeat(since time.Time) error {
	for {
		ok, changedC := n.locks.hasLiveness(n.Id(), since)
		if ok {
			return nil
		}

		select {
		case <-changedC:
		case <-n.failedC:
			return n.failure()
		case <-n.stopC:
			return ErrNodeStopped
		}
	}
}

// applyLockLog applies a committed lock or heartbeat entry.
func (n *Node[T, K]) applyLockLog(l *service.NodeLog) {
	at := l.GetTs().AsTime()

	if l.GetType() == service.NodeEventType_NODE_HEARTBEAT {
		n.locks.applyHeartbeat(l.GetNodeId(), at, l.GetHeartbeat().GetLease().AsDuration())
		return
	}

	cl := l.GetLock()
	if cl == nil {
		n.logger.Error(fmt.Sprintf("lock entry of node %s without lock", l.GetNodeId()))
		return
	}

	var before, after containerLock
	switch l.GetType() {
	case service.NodeEventType_LOCK_ACQUIRE:
		var refused bool
		before, after, refused = n.locks.applyAcquire(l.GetNodeId(), at, cl.GetContainerId(), cl.GetToken(), cl.GetWaitPath(), cl.GetJoin())
		if refused {
			debugf(n.logger, "refused lock of container %s for node %s: it would deadlock or its wait path is too long (%d)", cl.GetContainerId(), l.GetNodeId(), len(cl.GetWaitPath()))
			if l.GetNodeId() == n.Id() {
				n.refuseLockToken(cl.GetToken())
				n.locks.wake(cl.GetContainerId())
			}
			n.notifyRefused(l.GetNodeId(), cl.GetContainerId(), cl.GetToken())
			return
		}
	case service.NodeEventType_LOCK_RELEASE:
		before, after = n.locks.applyRelease(l.GetNodeId(), at, cl.GetContainerId(), cl.GetToken())
	case service.NodeEventType_LOCK_CANCEL:
		before, after = n.locks.applyCancel(l.GetNodeId(), at, cl.GetContainerId(), cl.GetToken())
	case service.NodeEventType_LOCK_DONE:
		before, after = n.locks.applyDone(l.GetNodeId(), at, cl.GetContainerId(), cl.GetToken())
	default:
		return
	}

	// this node releasing or cancelling isn't losing the lock
	own := l.GetNodeId() == n.Id() && l.GetType() != service.NodeEventType_LOCK_ACQUIRE
	n.lockChanged(cl.GetContainerId(), before, after, own)
	n.notifyLock(cl.GetContainerId(), before, after)
}

// lockChanged reacts to a change of a lock: a lock this node lost to another node closes its container,
// a lock handed to an attempt of this node which already gave up is released.
func (n *Node[T, K]) lockChanged(id string, before, after containerLock, own bool) {
	// an asynchronous acquire of this node got the lock
	if after.holder == n.Id() && (before.holder != n.Id() || before.token != after.token) {
		if p := n.takePending(id, after.token); p != nil {
			go n.openPending(p)
		}
	}

	if before.holder == n.Id() && after.holder != n.Id() && !own {
		if n.unretain(id, before.token) {
			// a kept lock expired or the node was removed
			n.dropLockToken(before.token)
		} else {
			// the lease of this node expired and another node took the container, close it here
			n.logger.Warn(fmt.Sprintf("lost the lock of container %s to node %s, closing it", id, after.holder))
			go n.DeleteContainer(id)
		}
	}

	// another node queued for a lock this node keeps: it gets it right away
	if after.holder == n.Id() && len(after.queue) > 0 && n.unretain(id, after.token) {
		n.releaseLock(id, after.token)
	}

	if after.holder == n.Id() && (before.holder != n.Id() || before.token != after.token) {
		n.startingMu.Lock()
		inUse := n.lockTokens[after.token]
		n.startingMu.Unlock()
		if !inUse {
			debugf(n.logger, "got the lock of container %s for an attempt which gave up, releasing it", id)
			n.releaseLock(id, after.token)
		}
	}
}

// verifyLending checks the nodes lending the lock of the container id: a single lender against the lock table,
// a longer chain by asking its last lender, which got the lock lent itself. A chain with this node is refused,
// this node would wait for itself.
func (n *Node[T, K]) verifyLending(ctx context.Context, id string, chain []string) error {
	switch {
	case len(chain) == 0:
		return fmt.Errorf("no node lends the lock of container %s", id)
	case slices.Contains(chain, n.Id()):
		return fmt.Errorf("the lock of container %s is lent through node %s already, it would wait for itself", id, n.Id())
	case len(chain) == 1:
		return n.verifyHolder(ctx, id, chain[0])
	}

	lender := chain[len(chain)-1]
	nodeId, ok := nodeIdOf(lender)
	if !ok {
		return fmt.Errorf("invalid lender %s", lender)
	}
	client := n.getPeerClient(nodeId)
	if client == nil {
		return fmt.Errorf("lender %s not found in peers", lender)
	}
	resp, err := client.CheckLending(ctx, &service.CheckLendingRequest{ContainerId: id, LendChain: chain[:len(chain)-1]})
	if err != nil {
		return fmt.Errorf("asking lender %s: %w", lender, err)
	}
	if !resp.GetLending() {
		return fmt.Errorf("node %s doesn't run on a lent lock of container %s", lender, id)
	}
	return nil
}

// verifyHolder waits until the lock table shows the node as holder of the container id.
func (n *Node[T, K]) verifyHolder(ctx context.Context, id, holder string) error {
	if n.skipRaft {
		return nil
	}
	// a client only knows the locks concerning it, it asks its member for this one
	if n.client {
		n.watchLock(id)
		defer n.unwatchLock(id)
	}

	timeout := time.After(syncTimeout)
	for {
		cur, held, changedC := n.locks.get(id)
		if held && cur.holder == holder {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("node %s does not hold the lock of container %s", holder, id)
		case <-changedC:
		}
	}
}

type (
	// pendingAcquires are the asynchronous acquires of this node by container id (acquireAsync). fresh holds the
	// ones not proposed yet.
	pendingAcquires struct {
		mu      sync.Mutex
		byId    map[string]*pendingAcquire
		fresh   []*pendingAcquire
		stopped bool
		kickC   chan struct{}
		started sync.Once
	}
	pendingAcquire struct {
		id, token string
		l         *service.NodeLog
		// prop is only used by the goroutine proposing (runPendingAcquires)
		prop      proposalState
		startedAt time.Time
		startingC chan struct{}
		resume    func(error)
	}
)

// newLockToken returns a new token for an acquire of this node.
func (n *Node[T, K]) newLockToken() string {
	return fmt.Sprintf("%d-%d", n.id, n.lockSeq.Add(1))
}

// acquireAsync acquires the lock of the id without a goroutine waiting for it: once it is granted, the container is
// opened and resume runs, with the error if the container didn't open. It holds the start of the id on this node
// meanwhile, like StartContainer. It returns false if a start of the id runs on this node already or the node stopped.
func (n *Node[T, K]) acquireAsync(id string, resume func(error)) bool {
	n.startingMu.Lock()
	if _, ok := n.starting[id]; ok {
		n.startingMu.Unlock()
		return false
	}
	startingC := make(chan struct{})
	n.starting[id] = startingC
	n.startingMu.Unlock()

	if token, ok := n.reuseRetained(id); ok {
		go n.openPending(&pendingAcquire{id: id, token: token, startingC: startingC, resume: resume, startedAt: time.Now()})
		return true
	}

	token := n.newLockToken()
	p := &pendingAcquire{
		id:        id,
		token:     token,
		l:         lockLog(n.Id(), service.NodeEventType_LOCK_ACQUIRE, &service.ContainerLock{ContainerId: id, Token: token}),
		startedAt: time.Now(),
		startingC: startingC,
		resume:    resume,
	}

	n.pending.started.Do(func() { go n.runPendingAcquires() })
	n.pending.mu.Lock()
	if n.pending.stopped {
		n.pending.mu.Unlock()
		n.endStart(id, startingC)
		return false
	}
	n.startingMu.Lock()
	n.lockTokens[token] = true
	n.startingMu.Unlock()
	n.pending.byId[id] = p
	n.pending.fresh = append(n.pending.fresh, p)
	n.pending.mu.Unlock()

	select {
	case n.pending.kickC <- struct{}{}:
	default:
	}
	return true
}

// endStart ends the start of the id on this node, starts waiting for it go on.
func (n *Node[T, K]) endStart(id string, startingC chan struct{}) {
	n.startingMu.Lock()
	defer n.startingMu.Unlock()
	delete(n.starting, id)
	close(startingC)
}

// takePending removes the asynchronous acquire of the id with the token and returns it, nil if there is none.
func (n *Node[T, K]) takePending(id, token string) *pendingAcquire {
	n.pending.mu.Lock()
	defer n.pending.mu.Unlock()

	p, ok := n.pending.byId[id]
	if !ok || p.token != token {
		return nil
	}
	delete(n.pending.byId, id)
	return p
}

// openPending opens the container of a granted asynchronous acquire, ends the start of the id and resumes.
func (n *Node[T, K]) openPending(p *pendingAcquire) {
	if p.l != nil {
		n.metrics.lockAcquires.Add(1)
		n.metrics.lockWait.observe(time.Since(p.startedAt))
	}
	// openContainer releases the lock if it fails (eg. the node leaves)
	_, err := n.openContainer(n.ctx, p.id, p.token, false, false, n.withNodeHooks(nil))
	n.endStart(p.id, p.startingC)
	p.resume(err)
}

// runPendingAcquires proposes the asynchronous acquires of this node: new ones right away, together, and on every
// retry interval the ones needsPropose says so of, or whose holder's lease ended. A grant is noticed by lockChanged;
// the retry pass also catches one it missed. Once the node stopped, the pending acquires are dropped.
func (n *Node[T, K]) runPendingAcquires() {
	ticker := time.NewTicker(proposeRetryInterval)
	defer ticker.Stop()
	defer n.dropPending(ErrNodeStopped, false)

	for {
		var list []*pendingAcquire
		select {
		case <-n.pending.kickC:
			n.pending.mu.Lock()
			list, n.pending.fresh = n.pending.fresh, nil
			n.pending.mu.Unlock()
		case <-ticker.C:
			n.pending.mu.Lock()
			n.pending.fresh = nil
			list = make([]*pendingAcquire, 0, len(n.pending.byId))
			for _, p := range n.pending.byId {
				list = append(list, p)
			}
			n.pending.mu.Unlock()
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		}

		var due []*pendingAcquire
		for _, p := range list {
			cur, held := n.locks.peek(p.id)
			if held && cur.holder == n.Id() && cur.token == p.token {
				if n.takePending(p.id, p.token) != nil {
					go n.openPending(p)
				}
				continue
			}
			queued := held && cur.queued(n.Id(), p.token)
			expired := held && cur.holder != n.Id() && n.lockExpiredNow(cur.holder)
			if (!queued && n.needsPropose(&p.prop)) || (expired && time.Since(p.prop.at) >= proposeRetryInterval) {
				due = append(due, p)
			}
		}
		if len(due) == 0 {
			continue
		}

		logs := make([]*service.NodeLog, len(due))
		for i, p := range due {
			p.l.Ts = timestamppb.Now()
			p.prop.at, p.prop.term = time.Now(), n.term.Load()
			logs[i] = p.l
		}
		err := n.proposeMany(logs)
		for _, p := range due {
			p.prop.err = err
		}
		if err != nil {
			debugf(n.logger, "proposing %d acquires of handed over work failed, retrying: %v", len(due), err)
		}
	}
}

// dropPending drops the asynchronous acquires of a stopped or leaving node: their starts end and they resume with
// the error (the drains give their hand-overs back to the senders). Later ones aren't taken. A leaving node cancels
// its places in the lock queues (cancel), so the locks don't wait for it; a stopped one can't propose anymore.
func (n *Node[T, K]) dropPending(err error, cancel bool) {
	n.pending.mu.Lock()
	n.pending.stopped = true
	list := make([]*pendingAcquire, 0, len(n.pending.byId))
	for _, p := range n.pending.byId {
		list = append(list, p)
	}
	clear(n.pending.byId)
	n.pending.fresh = nil
	n.pending.mu.Unlock()

	for _, p := range list {
		n.dropLockToken(p.token)
		if cancel {
			n.cancelLock(p.id, p.token)
		}
		n.endStart(p.id, p.startingC)
		go p.resume(err)
	}
}

// endPreviousLife ends what the log still holds of this node's life before its restart: the locks it held are
// released, its waiting acquires cancelled and its calls ended. Its containers are gone. Tokens of this life are
// in lockTokens, those of the previous life never are (lockSeq starts at the start time).
func (n *Node[T, K]) endPreviousLife() {
	type end struct {
		id, token string
		t         service.NodeEventType
	}
	var ends []end
	me := n.Id()
	n.startingMu.Lock()
	n.locks.mu.Lock()
	for id, l := range n.locks.locks {
		if l.holder == me && !n.lockTokens[l.token] {
			ends = append(ends, end{id, l.token, service.NodeEventType_LOCK_RELEASE})
		}
		for _, w := range l.queue {
			if w.node == me && !n.lockTokens[w.token] {
				ends = append(ends, end{id, w.token, service.NodeEventType_LOCK_CANCEL})
			}
		}
		for _, c := range l.calls {
			if tokenNode(c.token) == me && !n.lockTokens[c.token] {
				ends = append(ends, end{id, c.token, service.NodeEventType_LOCK_DONE})
			}
		}
	}
	n.locks.mu.Unlock()
	n.startingMu.Unlock()

	if len(ends) > 0 {
		n.logger.Info(fmt.Sprintf("ending %d locks, waits and calls of this node from before its restart", len(ends)))
	}
	for _, e := range ends {
		n.proposeLockEnd(e.id, e.token, e.t)
	}
}
