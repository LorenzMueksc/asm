package asm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
)

// Containers spawn and close containers from transitions like they send deferred events: CloseAfterRound closes the
// container at the end of the round, Spawn queues the start of a container, which the node starts at the end of the
// round, after the closes, in id order. Spawns queued between rounds (by live events) start at the beginning of the
// next round, before it delivers its events.

// spawnRequest is the start of a container a transition queued with Spawn.
type spawnRequest[T proto.Message] struct {
	id, parent string
	entity     T
	opts       []ContainerOption
}

// ErrContainerExists is returned by Node.Round for a spawned container whose id is open on the node already.
var ErrContainerExists = errors.New("container exists")

// Spawn queues the start of a container with the id and the entity, called in a transition with the ctx it got. The
// node starts it at the end of the round (Node.Round), after the containers closing then (CloseAfterRound) closed, so
// it exists when Round returns; it receives the events deferred to it from the next round on. Spawned outside of a
// round (a live event between rounds), it starts at the beginning of the next round, before that round delivers its
// events, so the events deferred to it with it reach it in that round. The containers spawned for the end or the
// beginning of a round start in id order, with the node's container options and opts; a slot of the max container
// count if one is free, else they run without one (Round never waits for a slot). The builder holds the entity (a nil
// entity leaves it empty), the init func runs before the first event as for every container. The new container stays
// open until it is closed (CloseAfterRound, Close, DeleteContainer, Shutdown), calls don't close it.
//
// An id which is open (after the closes), or spawned twice for the same start, makes Round return ErrContainerExists:
// none of the containers spawned for that start starts, they stay queued for the next round (or DropOutboxes), and at
// the beginning of a round nothing is delivered either. Only for containers of a node.
func (c *Container[T, K]) Spawn(ctx context.Context, id string, entity T, opts ...ContainerOption) error {
	if tc, ok := ctx.Value(transitionCtxKey{}).(*transitionCtx); !ok || tc.container != any(c) {
		return ErrNotInTransition
	}
	switch {
	case id == "":
		return errors.New("spawning a container: empty id")
	case c.rw.base != nil:
		return fmt.Errorf("spawning container %s: %w", id, ErrReadOnly)
	case c.round.node == nil:
		return fmt.Errorf("spawning container %s: only containers of a node spawn containers", id)
	}
	debugf(c.logger, "spawning container %s", id)

	cr := &c.round
	cr.mu.Lock()
	cr.spawns = append(cr.spawns, spawnRequest[T]{id: id, parent: c.id, entity: entity, opts: opts})
	cr.mu.Unlock()
	cr.node.markSpawning(c)
	return nil
}

// CloseAfterRound closes the container at the end of the current round, called in a transition with the ctx it got:
// once every container ran the round, the node closes it before Round returns. The deferred events it queued are
// delivered by the next round as usual, the containers it spawned start right after the closes; events deferred to it
// make the next round fail with ErrUnknownTarget, unless a container of its id was spawned meanwhile. Called outside of
// a round, it closes the container at the end of the next one. Only for containers of a node.
func (c *Container[T, K]) CloseAfterRound(ctx context.Context) error {
	if tc, ok := ctx.Value(transitionCtxKey{}).(*transitionCtx); !ok || tc.container != any(c) {
		return ErrNotInTransition
	}
	switch {
	case c.rw.base != nil:
		return fmt.Errorf("closing container %s after the round: %w", c.id, ErrReadOnly)
	case c.round.node == nil:
		return fmt.Errorf("closing container %s after the round: only containers of a node run rounds", c.id)
	}
	if c.round.closing.CompareAndSwap(false, true) {
		r := c.round.node
		r.mu.Lock()
		r.closing = append(r.closing, c)
		r.mu.Unlock()
	}
	return nil
}

// takeSpawning takes the containers with spawns, sorted by id. What they spawn from now on lists them again.
func (n *Node[T, K]) takeSpawning() []*Container[T, K] {
	r := &n.rounds
	r.mu.Lock()
	spawning := r.spawning
	r.spawning = r.spareSpawning
	r.mu.Unlock()
	slices.SortStableFunc(spawning, compareContainerIds[T, K])
	for _, c := range spawning {
		c.round.spawning.Store(false)
	}
	return spawning
}

// collectSpawns copies the spawns queued by the containers (takeSpawning) into the barrier's buffer, sorted by id: two
// spawns of one id keep the order of their parents' ids. They stay queued until startSpawns takes them, so a barrier
// which fails its checks leaves them for the next one (QR4).
func (n *Node[T, K]) collectSpawns(spawning []*Container[T, K]) []spawnRequest[T] {
	spawns := n.rounds.spawns[:0]
	for _, c := range spawning {
		cr := &c.round
		cr.mu.Lock()
		spawns = append(spawns, cr.spawns...)
		cr.spawnsTaken = len(cr.spawns)
		cr.mu.Unlock()
	}
	slices.SortStableFunc(spawns, compareSpawnIds[T])
	n.rounds.spawns = spawns
	return spawns
}

// checkSpawnsLocked checks the collected spawns, sorted by id: every id is free and spawned once. The caller holds
// containerMap.mu.
func (n *Node[T, K]) checkSpawnsLocked(spawns []spawnRequest[T], name string) error {
	for i, s := range spawns {
		if _, open := n.containerMap.m[s.id]; open || (i > 0 && spawns[i-1].id == s.id) {
			return fmt.Errorf("round %s: spawning container %s (from %s): %w", name, s.id, s.parent, ErrContainerExists)
		}
	}
	return nil
}

// keepSpawns leaves the collected spawns queued after a failed check: the containers are listed again for the next
// round (or DropOutboxes).
func (n *Node[T, K]) keepSpawns(spawning []*Container[T, K], spawns []spawnRequest[T]) {
	r := &n.rounds
	clear(spawns)
	r.spawns = spawns[:0]
	r.mu.Lock()
	for _, c := range spawning {
		c.round.spawnsTaken = 0
		if c.round.spawning.CompareAndSwap(false, true) {
			r.spawning = append(r.spawning, c)
		}
	}
	r.mu.Unlock()
	clear(spawning)
	r.spareSpawning = spawning[:0]
}

// startSpawns removes the collected spawns from the queues of their parents and starts them in id order. A start
// failing (the node stops) goes to fail; the others start.
func (n *Node[T, K]) startSpawns(ctx context.Context, spawning []*Container[T, K], spawns []spawnRequest[T], name string, fail func(error)) {
	for _, c := range spawning {
		cr := &c.round
		if cr.spawnsTaken == 0 {
			continue
		}
		// spawns queued meanwhile (a live event's transition) wait for the next barrier
		cr.mu.Lock()
		rest := copy(cr.spawns, cr.spawns[cr.spawnsTaken:])
		clear(cr.spawns[rest:])
		cr.spawns = cr.spawns[:rest]
		cr.mu.Unlock()
		cr.spawnsTaken = 0
	}
	for _, s := range spawns {
		if err := n.spawn(ctx, s); err != nil {
			fail(fmt.Errorf("round %s: spawning container %s (from %s): %w", name, s.id, s.parent, err))
		}
	}
	clear(spawns)
	n.rounds.spawns = spawns[:0]
	clear(spawning)
	n.rounds.spareSpawning = spawning[:0]
}

// startRoundSpawns starts the containers spawned during the round at its end, after the closes (endRound): all of
// them, once their ids checked out (checkSpawnsLocked), or none, which stay queued. It returns the first error.
func (n *Node[T, K]) startRoundSpawns(ctx context.Context, name string) error {
	spawning := n.takeSpawning()
	if len(spawning) == 0 {
		n.rounds.spareSpawning = spawning
		return nil
	}
	spawns := n.collectSpawns(spawning)
	n.containerMap.mu.RLock()
	err := n.checkSpawnsLocked(spawns, name)
	n.containerMap.mu.RUnlock()
	if err != nil {
		n.keepSpawns(spawning, spawns)
		return err
	}
	n.startSpawns(ctx, spawning, spawns, name, func(serr error) {
		if err == nil {
			err = serr
		}
	})
	return err
}

// spawned reports whether the id is among the spawns, sorted by id.
func spawned[T proto.Message](spawns []spawnRequest[T], id string) bool {
	_, ok := slices.BinarySearchFunc(spawns, id, func(s spawnRequest[T], id string) int { return strings.Compare(s.id, id) })
	return ok
}

func compareSpawnIds[T proto.Message](a, b spawnRequest[T]) int {
	return strings.Compare(a.id, b.id)
}

// spawn starts a spawned container. The barrier can't wait for a slot of the max container count, the containers
// holding them wait for it: it takes one if free, else the container runs without one, like waited starts.
func (n *Node[T, K]) spawn(ctx context.Context, s spawnRequest[T]) error {
	if n.hasContainer(s.id) {
		return ErrContainerExists
	}
	// the entity goes into the builder while the container is built, before anyone reaches it (setSpawnEntity)
	opts := append(slices.Clip(s.opts), withSpawnEntity(s.entity))
	c, err := n.openContainer(ctx, s.id, "", false, true, n.withNodeHooks(opts))
	if err != nil {
		return err
	}
	// calls leave it open, like a container StartContainer returned
	c.hold()
	return nil
}

// withSpawnEntity passes the entity of a spawned container to resetContainer.
func withSpawnEntity(entity any) ContainerOption {
	return newFuncContainerOption(func(o *containerOptions) {
		o.spawnEntity = entity
	})
}

// setSpawnEntity puts the entity a container was spawned with into its new builder, called by resetContainer. The
// container isn't in the node's map yet: nobody reaches it, so the write needs no lock, like the builder itself.
func (c *Container[T, K]) setSpawnEntity(entity any) {
	if e, ok := entity.(T); ok && !isNilMessage(e) {
		c.builder.Set(e)
	}
}

// closeRoundContainers closes the containers which asked for it with CloseAfterRound and waits until they closed.
func (n *Node[T, K]) closeRoundContainers() {
	r := &n.rounds
	r.mu.Lock()
	closing := r.closing
	r.closing = nil
	r.mu.Unlock()
	if len(closing) == 0 {
		return
	}
	slices.SortStableFunc(closing, compareContainerIds[T, K])
	for _, c := range closing {
		c.containerProcessing.requestClose()
	}
	for _, c := range closing {
		<-c.containerProcessing.done()
	}
}
