package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"rumue.io/asm/examples/gamemeta/gamepb"
	asm "rumue.io/asm/v2"
)

// The arena keeps the second entity of the cluster, Match. Its containers have match ids, while the events come from
// Player containers (player ids): the arena picks the container of an event by its payload (WithContainerIdFunc).
// Match containers run on their own locks, also when the player's gateway waits.

type (
	arenaNode      = asm.Node[*gamepb.Match, *matchBuilder]
	matchContainer = asm.Container[*gamepb.Match, *matchBuilder]
)

type matchBuilder struct {
	mu sync.Mutex
	m  *gamepb.Match
}

func (b *matchBuilder) Build() *gamepb.Match {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.m
}

func (b *matchBuilder) Set(m *gamepb.Match) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m = m
}

func (b *matchBuilder) IsSet() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.m != nil
}

// matchBitmap is the bitmap config of the arena: the root entity Match.
func matchBitmap() *asm.Bitmap {
	pos := 0
	match := asm.BuildBitmap(&asm.Property{Name: "Match", Properties: []*asm.Property{
		{Name: "status", Values: []any{"open", "full", "finished"}},
		{Name: "platform", Values: []any{"pc", "console", "mobile"}},
		{Name: "size", Values: []any{int64(1), int64(2), int64(3), int64(4)}},
	}}, &pos)
	return &asm.Bitmap{Name: "root", Children: map[string]*asm.Bitmap{"Match": match}}
}

func matchBitmapByType(entity any, root *asm.Bitmap) *asm.Bitmap {
	if root == nil {
		return nil
	}
	switch entity.(type) {
	case *gamepb.Match:
		return root.Children["Match"]
	case *gamepb.MatchResult:
		return root.Children["MatchResult"]
	}
	return nil
}

// matchId picks the Match container of an event of a Player container.
func matchId(_ string, payload proto.Message) string {
	switch p := payload.(type) {
	case *gamepb.JoinMatch:
		return p.GetMatchId()
	case *gamepb.MatchResult:
		return p.GetMatchId()
	}
	return ""
}

// matchStore keeps the matches in memory, only the arena has them.
type matchStore struct {
	matches sync.Map
}

func (s *matchStore) load(_ context.Context, c *matchContainer) error {
	if m, ok := s.matches.Load(c.Id()); ok {
		c.Builder().Set(proto.Clone(m.(*gamepb.Match)).(*gamepb.Match))
		return nil
	}
	c.Builder().Set(&gamepb.Match{Id: c.Id(), Status: "open"})
	return nil
}

func (s *matchStore) save(_ context.Context, c *matchContainer) error {
	if m := c.Builder().Build(); m != nil {
		s.matches.Store(c.Id(), proto.Clone(m))
	}
	return nil
}

func runArena(r role, s settings) {
	logger := newLogger()
	store := &matchStore{}

	opts := []asm.NodeSetupOption{
		asm.WithBitmap(matchBitmap()),
		asm.WithLogger(logger),
		asm.WithMaxContainerCount(s.players + 100),
		asm.WithMetricsAddr(addr(s.basePort+100, r.id)),
		asm.WithContainerIdFunc(matchId),
		asm.WithInitFunc[*gamepb.Match, *matchBuilder](store.load),
		asm.WithCloseFunc[*gamepb.Match, *matchBuilder](store.save),
	}
	if s.trace > 0 {
		opts = append(opts, asm.WithEventTrace(s.trace))
	}
	opts = append(opts, clusterOpts(r, s)...)
	n := asm.NewNode[*gamepb.Match](r.id, matchBitmapByType, func() *matchBuilder { return &matchBuilder{} }, addr(s.basePort, r.id), opts...)
	registerArenaTransitions(n, logger)

	select {
	case <-n.Ready():
	case err := <-n.Err():
		logger.Error(fmt.Sprintf("node failed: %v", err))
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("%s knows %d transitions", r.name, n.Transitions()))
	if !eventLogs {
		go printStats(logger, n, nil)
	}

	select {}
}

func registerArenaTransitions(n *arenaNode, logger *zap.Logger) {
	// emitted by matchmaking for the match it put the player into
	n.AddFuncTransition("add_player", "JOIN_MATCH", func(ctx context.Context, c *matchContainer, payload any) (bool, error) {
		m := c.Builder().Build()
		join := payload.(*gamepb.JoinMatch)
		if !slices.Contains(m.Players, join.GetPlayerId()) {
			m.Players = append(m.Players, join.GetPlayerId())
		}
		m.Platform = join.GetPlatform()
		m.Size = int64(len(m.Players))
		say(logger, "joined match", fmt.Sprintf("%s joined %s (%d/4)", join.GetPlayerId(), m.Id, m.Size))
		return false, nil
	}, asm.WithFuncKey("add_player.v1"), asm.WithPriority(10), asm.WithConditions(n.All(&gamepb.Match{Status: "open"})))

	// a state transition of the Match entity: after add_player the state is evaluated again,
	// so the 4th player makes the match full in the same event. It runs on the arena, the receiver of
	// the Player containers' events, because the senders can't run state transitions of another entity.
	n.AddStateTransition("fill_match", "JOIN_MATCH", &gamepb.Match{Status: "full"},
		asm.WithConditions(n.All(&gamepb.Match{Status: "open", Size: 4})))

	// MATCH_RESULT of a Player container, the gateway waits for it. Once every player of a full match reported,
	// the match is finished and every winner gets a bonus: MATCH_BONUS goes to the Player containers, without waiting,
	// so they get their player locks after the gateways released them.
	n.AddFuncTransition("record_result", "MATCH_RESULT", func(ctx context.Context, c *matchContainer, payload any) (bool, error) {
		m := c.Builder().Build()
		result := payload.(*gamepb.MatchResult)
		m.Reported++
		if result.GetOutcome() == "win" {
			m.Winners = append(m.Winners, result.GetPlayerId())
		}
		if m.Status != "full" || m.Reported < m.Size {
			return false, nil
		}

		m.Status = "finished"
		say(logger, "matches finished", fmt.Sprintf("%s finished, winners %v", m.Id, m.Winners))
		for _, winner := range m.Winners {
			if err := c.Emit(ctx, "MATCH_BONUS", &gamepb.MatchBonus{PlayerId: winner, MatchId: m.Id, Coins: 30}); err != nil {
				return false, err
			}
		}
		return false, nil
	}, asm.WithFuncKey("record_result.v1"), asm.WithConditions(n.None(&gamepb.Match{Status: "finished"})))
}
