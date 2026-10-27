package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
	"rumue.io/asm/examples/gamemeta/gamepb"
	asm "rumue.io/asm/v2"
)

type (
	playerNode      = asm.Node[*gamepb.Player, *playerBuilder]
	playerContainer = asm.Container[*gamepb.Player, *playerBuilder]
)

type playerBuilder struct {
	mu sync.Mutex
	p  *gamepb.Player
}

func (b *playerBuilder) Build() *gamepb.Player {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.p
}

func (b *playerBuilder) Set(p *gamepb.Player) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.p = p
}

func (b *playerBuilder) IsSet() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.p != nil
}

// interestingDebug are the debug messages of asm shown in the demo when it logs every event.
var interestingDebug = []string{
	"forwarding ",
	"forwarded by another node",
	"is locked by node",
	"compacted log",
}

// filterCore drops debug messages except the interesting ones.
type filterCore struct{ zapcore.Core }

func (c filterCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if e.Level < zapcore.InfoLevel && (!eventLogs || !containsAny(e.Message, interestingDebug)) {
		return ce
	}
	return c.Core.Check(e, ce)
}

// Enabled reports debug messages as disabled when only stats are logged, so asm doesn't even format them.
func (c filterCore) Enabled(l zapcore.Level) bool {
	if l < zapcore.InfoLevel && !eventLogs {
		return false
	}
	return c.Core.Enabled(l)
}

func (c filterCore) With(fields []zapcore.Field) zapcore.Core {
	return filterCore{c.Core.With(fields)}
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// newLogger writes "LEVEL<tab>message<tab>fields" lines to stdout, the launcher adds time and node.
func newLogger() *zap.Logger {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		LevelKey:       "level",
		MessageKey:     "msg",
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	})
	core := zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), zapcore.DebugLevel)
	return zap.New(filterCore{core})
}

// playerBitmap returns the bitmap config of a node: the root entities Player and MatchResult.
// The nodes know different values: only anti-cheat knows "banned", the shared bitmap of the cluster merges them.
// party is a nested property with an int64 property, size.
func playerBitmap(r role) *asm.Bitmap {
	statuses := []any{"lobby", "in_match"}
	if r.name == "anticheat" {
		statuses = []any{"in_match", "banned"}
	}

	pos := 0
	player := asm.BuildBitmap(&asm.Property{Name: "Player", Properties: []*asm.Property{
		{Name: "status", Values: statuses},
		{Name: "region", Values: []any{"eu", "us"}},
		{Name: "flagged", Values: []any{true, false}},
		{Name: "tier", Values: []any{"bronze", "silver", "gold", "diamond"}},
		{Name: "subscription", Values: []any{"free", "premium"}},
		{Name: "platform", Values: []any{"pc", "console", "mobile"}},
		{Name: "party", Properties: []*asm.Property{
			{Name: "role", Values: []any{"solo", "leader", "member"}},
			{Name: "size", Values: []any{int64(1), int64(2), int64(3), int64(4)}},
		}},
	}}, &pos)

	// every root entity has its own bits
	pos = 0
	result := asm.BuildBitmap(&asm.Property{Name: "MatchResult", Properties: []*asm.Property{
		{Name: "outcome", Values: []any{"win", "loss"}},
	}}, &pos)

	return &asm.Bitmap{Name: "root", Children: map[string]*asm.Bitmap{"Player": player, "MatchResult": result}}
}

// playerBitmapByType maps players and match results to their bitmaps. The other events have none,
// so they don't change the state.
func playerBitmapByType(entity any, root *asm.Bitmap) *asm.Bitmap {
	if root == nil {
		return nil
	}
	switch entity.(type) {
	case *gamepb.Player:
		return root.Children["Player"]
	case *gamepb.MatchResult:
		return root.Children["MatchResult"]
	}
	return nil
}

// playerId picks the Player container of an event of a Match container.
func playerId(_ string, payload proto.Message) string {
	if bonus, ok := payload.(*gamepb.MatchBonus); ok {
		return bonus.GetPlayerId()
	}
	return ""
}

// loadedPlayer returns the player of the container, or an error if the init func couldn't load it.
func loadedPlayer(c *playerContainer) (*gamepb.Player, error) {
	p := c.Builder().Build()
	if p == nil {
		return nil, fmt.Errorf("player %s isn't loaded", c.Id())
	}
	return p, nil
}

// newPlayer returns a player who never played before. Subscription and platform are derived from the id,
// so they stay the same on every node.
func newPlayer(id, region string) *gamepb.Player {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	sum := h.Sum32()

	subscription := "free"
	if sum%5 == 0 {
		subscription = "premium"
	}
	return &gamepb.Player{
		Id:           id,
		Status:       "lobby",
		Region:       region,
		Coins:        100,
		Tier:         "bronze",
		Subscription: subscription,
		Platform:     []string{"pc", "console", "mobile"}[sum%3],
		Party:        &gamepb.Party{Role: "solo", Size: 1},
	}
}

// storeDir is the directory of the players database of a demo run.
func storeDir(basePort int) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("asm-gamemeta-%d", basePort))
}

func runNode(r role, s settings) {
	logger := newLogger()

	opts := []asm.NodeSetupOption{
		asm.WithBitmap(playerBitmap(r)),
		asm.WithLogger(logger),
		// every session has a player container on its gateway and borrowed ones on the services
		asm.WithMaxContainerCount(2*s.players + 100),
		// Prometheus metrics on base-port + 100 + node id
		asm.WithMetricsAddr(addr(s.basePort+100, r.id)),
	}
	if s.trace > 0 {
		// for the dev tool: go run ./devtool/cmd/asmdev -join 127.0.0.1:7101
		opts = append(opts, asm.WithEventTrace(s.trace))
	}
	opts = append(opts, clusterOpts(r, s)...)
	if r.store {
		store, err := openPlayerStore(playersDB(s.basePort), r.region)
		if err != nil {
			logger.Error(fmt.Sprintf("failed to open the players database: %v", err))
			os.Exit(1)
		}
		opts = append(opts, asm.WithInitFunc[*gamepb.Player, *playerBuilder](store.load))
		if r.gateway {
			// the gateways own the sessions: players are stored when the session ends
			opts = append(opts, asm.WithCloseFunc[*gamepb.Player, *playerBuilder](store.save))
		} else {
			// events of the arena (MATCH_BONUS) carry the player they are for
			opts = append(opts, asm.WithContainerIdFunc(playerId))
			// progression only stores players in transitions saving on change (score when it is handed over,
			// match_bonus: it holds the lock then). It must not store players it got on a lent lock: the gateway
			// owns them, and the borrowed container may close after the gateway stored and released the player.
			opts = append(opts, asm.WithSaveFunc[*gamepb.Player, *playerBuilder](store.save))
		}
	}

	n := asm.NewNode[*gamepb.Player](r.id, playerBitmapByType, func() *playerBuilder { return &playerBuilder{} }, addr(s.basePort, r.id), opts...)
	registerTransitions(n, r, logger, pacings[s.pacing])

	select {
	case <-n.Ready():
	case err := <-n.Err():
		logger.Error(fmt.Sprintf("node failed: %v", err))
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("%s knows %d transitions", r.name, n.Transitions()))

	if r.gateway {
		go simulate(n, r, logger, s.players, pacings[s.pacing])
	}
	if !eventLogs {
		var extra func() string
		if r.gateway {
			extra = func() string { return fmt.Sprintf("%d sessions running", activeSessions.Load()) }
		}
		go printStats(logger, n, extra)
	}

	select {}
}

// registerTransitions registers the transitions a node owns.
//
// Zero values apply in example entities: every Player example also stands for flagged=false, because false
// is a bitmap value. So All and AtLeastOne of a Player example only match unflagged players, and None uses
// an example with flagged=true.
func registerTransitions(n *playerNode, r role, logger *zap.Logger, pacing pacing) {
	switch r.name {
	case "gateway-eu", "gateway-us":
		// a local func transition, both gateways own it: the party a player queues with.
		// None: neither banned nor flagged.
		n.AddFuncTransition("join_party", "PARTY", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			party := payload.(*gamepb.Party)
			p.Party = &gamepb.Party{Role: party.GetRole(), Size: party.GetSize()}
			return false, nil
		}, asm.WithFuncKey("join_party.v1"), asm.WithConditions(n.None(&gamepb.Player{Status: "banned", Flagged: true})))

		// runs after rank_update of progression (priority 10) and sees the tier it changed in the same event
		n.AddFuncTransition("diamond_lounge", "MATCH_END", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			p.Coins += 25
			say(logger, "diamond lounge", fmt.Sprintf("diamond lounge bonus for %s: 25 coins, %d now", p.Id, p.Coins))
			return false, nil
		}, asm.WithFuncKey("diamond_lounge.v1"), asm.WithPriority(5), asm.WithConditions(n.All(&gamepb.Player{Tier: "diamond"})))

		// state transitions: synced to every node and executed locally on the gateways.
		// Zero values apply in targets: an unset flagged sets false, so the targets of flagged players set it to true.

		// back to the lobby after the match. The condition also requires flagged=false (zero value),
		// so resetting flagged only touches unflagged players.
		n.AddStateTransition("finish_match", "MATCH_END", &gamepb.Player{Status: "lobby"},
			asm.WithConditions(n.All(&gamepb.Player{Status: "in_match"})))

		// flagged players go back to the lobby too, still flagged: they are held for review and can't queue
		n.AddStateTransition("hold_for_review", "MATCH_END", &gamepb.Player{Status: "lobby", Flagged: true},
			asm.WithConditions(n.All(&gamepb.Player{Status: "in_match", Flagged: true})))

		// parties break up after the match: a nested target, the party is modified in place
		n.AddStateTransition("leave_party", "MATCH_END", &gamepb.Player{Party: &gamepb.Party{Role: "solo", Size: 1}},
			asm.WithConditions(n.OneOf(
				&gamepb.Player{Party: &gamepb.Party{Role: "leader"}},
				&gamepb.Player{Party: &gamepb.Party{Role: "member"}},
			)))

		// the lowest priority of MATCH_RESULT: the results of anti-cheat, progression and the arena are merged
		// before it, so a player anti-cheat just banned drops to bronze in the same event
		n.AddStateTransition("strip_rank", "MATCH_RESULT", &gamepb.Player{Tier: "bronze", Flagged: true},
			asm.WithPriority(-5), asm.WithConditions(n.All(&gamepb.Player{Status: "banned", Flagged: true})))

	case "matchmaking":
		// up to 4 players of a platform share a match
		var (
			mu      sync.Mutex
			matches int
			open    = make(map[string]*struct {
				id   string
				size int
			})
		)
		// match puts the player into the open match of its platform and emits JOIN_MATCH as part of the transition.
		// The event goes to the Match container of the match id on the arena, another entity with another container id,
		// without waiting: the arena locks the match itself.
		match := func(ctx context.Context, c *playerContainer, p *gamepb.Player) error {
			mu.Lock()
			m := open[p.Platform]
			if m == nil || m.size == 4 {
				matches++
				m = &struct {
					id   string
					size int
				}{id: fmt.Sprintf("m-%s-%d", p.Platform, matches)}
				open[p.Platform] = m
			}
			m.size++
			mu.Unlock()

			p.Status = "in_match"
			p.MatchId = m.id
			return c.Emit(ctx, "JOIN_MATCH", &gamepb.JoinMatch{MatchId: m.id, PlayerId: p.Id, Platform: p.Platform})
		}

		// premium and diamond players skip the queue. It runs first; once it put the player into a match,
		// matchmake doesn't match anymore, because the state is evaluated again after every func transition.
		n.AddFuncTransition("priority_matchmake", "QUEUE", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			time.Sleep(between(100, 300))
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			if err := match(ctx, c, p); err != nil {
				return false, err
			}
			say(logger, "fast-tracked", fmt.Sprintf("fast-tracked %s (%s, %s) into %s", p.Id, p.Subscription, p.Tier, p.MatchId))
			return false, nil
		}, asm.WithFuncKey("priority_matchmake.v1"), asm.WithPriority(10), asm.WithConditions(n.AtLeastOne(
			&gamepb.Player{Status: "lobby", Subscription: "premium"},
			&gamepb.Player{Status: "lobby", Tier: "diamond"},
		)))

		// All(lobby) also requires flagged=false, because the zero value false is a bitmap value
		n.AddFuncTransition("matchmake", "QUEUE", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			// searching for a match takes a while
			time.Sleep(between(400, 1200))
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			if err := match(ctx, c, p); err != nil {
				return false, err
			}
			say(logger, "matched", fmt.Sprintf("matched %s (%s) into %s", p.Id, p.Region, p.MatchId))
			return false, nil
		}, asm.WithFuncKey("matchmake.v1"), asm.WithConditions(n.All(&gamepb.Player{Status: "lobby"})))

	case "anticheat":
		n.AddFuncTransition("review_match", "MATCH_RESULT", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			// analyzing the match takes a while
			time.Sleep(between(300, 800))
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			result := payload.(*gamepb.MatchResult)
			switch {
			case result.GetSuspicion() >= 95:
				p.Status, p.Flagged = "banned", true
				say(logger, "banned", fmt.Sprintf("banned %s after %s, suspicion %d", p.Id, p.MatchId, result.GetSuspicion()))
			case result.GetSuspicion() >= 80:
				p.Flagged = true
				say(logger, "flagged", fmt.Sprintf("flagged %s for review after %s, suspicion %d", p.Id, p.MatchId, result.GetSuspicion()))
			default:
				say(logger, "reviewed", fmt.Sprintf("reviewed %s in %s: clean (suspicion %d)", p.Id, p.MatchId, result.GetSuspicion()))
			}
			return false, nil
		}, asm.WithFuncKey("review_match.v1"), asm.WithConditions(n.All(&gamepb.Player{Status: "in_match"})))

		// flagged and banned players appeal instead of queueing: half of the flags are cleared, a tenth of the bans lifted
		n.AddFuncTransition("review_appeal", "APPEAL", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			time.Sleep(between(100, 300))
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			switch {
			case p.Status == "banned" && rand.IntN(10) == 0:
				p.Status, p.Flagged = "lobby", false
				say(logger, "bans lifted", fmt.Sprintf("lifted the ban of %s", p.Id))
			case p.Status == "banned":
				say(logger, "bans upheld", fmt.Sprintf("upheld the ban of %s", p.Id))
			case rand.IntN(2) == 0:
				p.Flagged = false
				say(logger, "flags cleared", fmt.Sprintf("cleared the flag of %s", p.Id))
			default:
				say(logger, "flags kept", fmt.Sprintf("kept the flag of %s", p.Id))
			}
			return false, nil
		}, asm.WithFuncKey("review_appeal.v1"), asm.WithConditions(n.AtLeastOne(
			&gamepb.Player{Status: "lobby", Flagged: true},
			&gamepb.Player{Status: "banned", Flagged: true},
		)))

	case "store":
		// unflagged players in the lobby or in a match can buy
		n.AddFuncTransition("purchase", "PURCHASE", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			// charging the payment provider takes a while
			time.Sleep(between(100, 300))
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			req := payload.(*gamepb.PurchaseRequest)
			if p.Platform == "mobile" && req.GetItem() == "battle pass" {
				say(logger, "declined (mobile)", fmt.Sprintf("declined %s for %s: not sold on mobile", req.GetItem(), p.Id))
				return true, nil
			}
			if p.Coins < req.GetPrice() {
				say(logger, "declined", fmt.Sprintf("declined %s for %s: %d coins, costs %d", req.GetItem(), p.Id, p.Coins, req.GetPrice()))
				return true, nil
			}
			p.Coins -= req.GetPrice()
			say(logger, "sold", fmt.Sprintf("sold %s to %s for %d coins, %d left", req.GetItem(), p.Id, req.GetPrice(), p.Coins))
			return false, nil
		}, asm.WithFuncKey("purchase.v1"), asm.WithConditions(n.AtLeastOne(
			&gamepb.Player{Status: "lobby"},
			&gamepb.Player{Status: "in_match"},
		)))

		// emitted by rank_update of progression: runs on the lock progression borrowed from the gateway
		n.AddFuncTransition("rank_reward", "RANK_UP", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			rankUp := payload.(*gamepb.RankUp)
			reward := map[string]int64{"silver": 50, "gold": 100, "diamond": 250}[rankUp.GetTier()]
			p.Coins += reward
			say(logger, "rank reward", fmt.Sprintf("rank reward for %s: %d coins for %s, %d now", p.Id, reward, rankUp.GetTier(), p.Coins))
			return false, nil
		}, asm.WithFuncKey("rank_reward.v1"))

	case "progression":
		// event conditions: the outcome of the MatchResult payload is in its own bitmap
		n.AddFuncTransition("count_win", "MATCH_RESULT", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			p.Wins++
			return false, nil
		}, asm.WithFuncKey("count_win.v1"), asm.WithConditions(
			n.All(&gamepb.MatchResult{Outcome: "win"}),
			n.All(&gamepb.Player{Status: "in_match"}),
		))
		n.AddFuncTransition("count_loss", "MATCH_RESULT", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			p.Losses++
			return false, nil
		}, asm.WithFuncKey("count_loss.v1"), asm.WithConditions(
			n.All(&gamepb.MatchResult{Outcome: "loss"}),
			n.All(&gamepb.Player{Status: "in_match"}),
		))

		// OneOf on the nested party role: the player is in a party, as leader or as member
		n.AddFuncTransition("party_xp", "MATCH_RESULT", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			p.Coins += 5
			say(logger, "party bonus", fmt.Sprintf("party bonus for %s (%s of %d): 5 coins", p.Id, p.Party.GetRole(), p.Party.GetSize()))
			return false, nil
		}, asm.WithFuncKey("party_xp.v1"), asm.WithConditions(n.OneOf(
			&gamepb.Player{Party: &gamepb.Party{Role: "leader"}},
			&gamepb.Player{Party: &gamepb.Party{Role: "member"}},
		)))

		// priority 10: runs before diamond_lounge and finish_match of the gateways in MATCH_END.
		// A promotion emits RANK_UP as part of this transition and waits for the reward of the store: progression
		// runs on the lock the gateway lent it and lends it on to the store, the coins come back to the gateway.
		n.AddFuncTransition("rank_update", "MATCH_END", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			tiers := []string{"bronze", "silver", "gold", "diamond"}
			i := slices.Index(tiers, p.Tier)
			switch {
			case p.Wins >= 2 && i < len(tiers)-1:
				p.Tier = tiers[i+1]
				p.Wins, p.Losses = 0, 0
				say(logger, "promoted", fmt.Sprintf("promoted %s to %s", p.Id, p.Tier))
				// the store merges its changes into the player of this container, so p isn't used afterwards
				return false, c.Emit(ctx, "RANK_UP", &gamepb.RankUp{PlayerId: p.Id, Tier: p.Tier}, asm.WithWaitRemote())
			case p.Losses >= 2 && i > 0:
				p.Tier = tiers[i-1]
				say(logger, "demoted", fmt.Sprintf("demoted %s to %s", p.Id, p.Tier))
			default:
				return false, nil
			}
			p.Wins, p.Losses = 0, 0
			return false, nil
		}, asm.WithFuncKey("rank_update.v1"), asm.WithPriority(10), asm.WithConditions(n.All(&gamepb.Player{Status: "in_match"})))

		// emitted by the arena, a Match container, for the Player container of a winner (picked by playerId)
		n.AddFuncTransition("match_bonus", "MATCH_BONUS", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			bonus := payload.(*gamepb.MatchBonus)
			p.Coins += bonus.GetCoins()
			say(logger, "match bonus", fmt.Sprintf("match bonus for %s: %d coins for winning %s, %d now", p.Id, bonus.GetCoins(), bonus.GetMatchId(), p.Coins))
			return false, nil
		}, asm.WithFuncKey("match_bonus.v1"), asm.WithSaveAfter())

		// batching: the sessions don't wait for scores, the gateways batch them before they forward them, with the
		// batching of this transition. Progression runs once per batch.
		// Handed over (stress: 5 events or 2s) it runs after the gateway released the player and stores the player.
		// Forwarded waiting (online: 5 events, the rest when the session closes) it runs on the lent lock right away,
		// a full batch doesn't wait for a timer here; the gateway gets the changes and stores the player at logout,
		// so progression must not store it.
		scoreOpts := []asm.TransitionOption{asm.WithFuncKey("score.v1"), asm.WithBatching(5, 2*time.Second), asm.WithSaveAfter()}
		if pacing.waitScores {
			scoreOpts = []asm.TransitionOption{asm.WithFuncKey("score.v1"), asm.WithBatching(5, 0)}
		}
		n.AddFuncTransition("score", "SCORE", func(ctx context.Context, c *playerContainer, payload any) (bool, error) {
			p, err := loadedPlayer(c)
			if err != nil {
				return true, err
			}
			batch := payload.([]any)
			var points int64
			for _, s := range batch {
				points += s.(*gamepb.Score).GetPoints()
			}
			p.Score += points
			p.Coins += points / 20
			say(logger, "score batches", fmt.Sprintf("added %d points of %d events to %s: %d coins, %d points now", points, len(batch), p.Id, points/20, p.Score))
			return false, nil
		}, scoreOpts...)
	}
}
