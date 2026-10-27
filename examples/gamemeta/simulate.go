package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"rumue.io/asm/examples/gamemeta/gamepb"
	asm "rumue.io/asm/v2"
)

// sharedPlayer plays on both gateways, so their sessions compete for the lock.
const sharedPlayer = "duo-zoe"

// activeSessions counts the sessions running on this gateway
var activeSessions atomic.Int64

// pacing is how players play. A session locks the player (its container stays open), plays matches
// and releases the player.
type pacing struct {
	// think is the time before every action, in ms
	thinkMin, thinkMax int
	// a match lasts matchMin-matchMax ms, the player reports a score every scoreMin-scoreMax ms
	matchMin, matchMax int
	scoreMin, scoreMax int
	// a session lasts sessionMin-sessionMax ms, it plays matches until then. 0: one match per session
	sessionMin, sessionMax int
	// time between two sessions, in ms
	offlineMin, offlineMax int
	// waitScores forwards the score batches waiting, progression adds them on the lent lock right away.
	// Otherwise they are handed over and progression adds them once the session released the player.
	waitScores bool
}

var pacings = map[string]pacing{
	// realistic: players stay online for a while and play several matches
	"online": {
		thinkMin: 2000, thinkMax: 8000,
		matchMin: 30_000, matchMax: 60_000,
		scoreMin: 3000, scoreMax: 8000,
		sessionMin: 5 * 60_000, sessionMax: 20 * 60_000,
		offlineMin: 30_000, offlineMax: 120_000,
		// a session lasts minutes, handed over scores would pile up on progression until the player logs off
		waitScores: true,
	},
	// a stress test: a short session per match, players barely think
	"stress": {
		thinkMin: 200, thinkMax: 600,
		matchMin: 400, matchMax: 1200,
		scoreMin: 80, scoreMax: 200,
		offlineMin: 1000, offlineMax: 3000,
	},
}

// regionPlayers returns the players of a region: half of all players each, the shared player counts once.
func regionPlayers(region string, players int) []string {
	count := players / 2
	if region == "us" {
		count = players - players/2 - 1
	}
	ids := make([]string, 0, count+1)
	for i := range count {
		ids = append(ids, fmt.Sprintf("%s-%03d", region, i+1))
	}
	return append(ids, sharedPlayer)
}

var items = []struct {
	name  string
	price int64
}{
	{"skin", 40}, {"emote", 15}, {"battle pass", 90}, {"loot box", 25},
}

// simulate plays all players of the gateway's region at the same time: every player plays a session,
// is offline for a while and plays the next one. The first sessions start spread out, 2000 per second
// in the cluster, at least over 3s.
func simulate(n *playerNode, r role, logger *zap.Logger, players int, p pacing) {
	// give the other nodes time to join and sync their transitions
	time.Sleep(3 * time.Second)

	rampMs := max(3000, players/2)
	for _, id := range regionPlayers(r.region, players) {
		go func() {
			time.Sleep(between(0, rampMs))
			for {
				activeSessions.Add(1)
				session(n, id, logger, p)
				activeSessions.Add(-1)
				time.Sleep(between(p.offlineMin, p.offlineMax))
			}
		}()
	}
}

// randomParty returns the party of a match: half of the players play solo.
func randomParty() *gamepb.Party {
	if rand.IntN(2) == 0 {
		return &gamepb.Party{Role: "solo", Size: 1}
	}
	role := "member"
	if rand.IntN(3) == 0 {
		role = "leader"
	}
	return &gamepb.Party{Role: role, Size: int64(2 + rand.IntN(3))}
}

// between returns a random duration between min and max milliseconds.
func between(minMs, maxMs int) time.Duration {
	return time.Duration(minMs+rand.IntN(maxMs-minMs+1)) * time.Millisecond
}

// session locks the player, plays matches and releases the player.
func session(n *playerNode, id string, logger *zap.Logger, p pacing) {
	// waits at most 30s for the player, eg. while the other gateway has them open
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	c, _, err := n.StartContainer(ctx, id)
	if errors.Is(err, context.DeadlineExceeded) && id == sharedPlayer {
		say(logger, "lock waits", fmt.Sprintf("session %s: still playing on the other gateway", id))
		return
	}
	if err != nil {
		logger.Error(fmt.Sprintf("session %s: %v", id, err))
		return
	}
	if waited := time.Since(start); waited > 300*time.Millisecond {
		say(logger, "lock waits", fmt.Sprintf("session %s: got the lock after waiting %s", id, waited.Round(time.Millisecond)))
	} else {
		say(logger, "sessions started", fmt.Sprintf("session %s: locked the player", id))
	}

	end := time.Now().Add(between(p.sessionMin, p.sessionMax))
	for {
		match(c, id, logger, p)
		if p.sessionMax == 0 || time.Now().After(end) {
			break
		}
	}

	// the shared player keeps playing a bit longer, so the other gateway has to wait for the lock
	if id == sharedPlayer && p.sessionMax == 0 {
		time.Sleep(1500 * time.Millisecond)
	}

	c.Close()
	say(logger, "sessions ended", fmt.Sprintf("session %s: released the player", id))
}

// match joins a party, queues the player, reports scores and the match result, buys an item and finishes the match.
func match(c *playerContainer, id string, logger *zap.Logger, p pacing) {
	// send waits until the event went through all transitions and returns the player afterwards.
	// The player thinks a moment before every action.
	send := func(event string, payload proto.Message) *gamepb.Player {
		time.Sleep(between(p.thinkMin, p.thinkMax))

		doneC := make(chan struct{}, 1)
		if err := c.Event(event, payload, asm.WithWaitRemote(), asm.WithDoneChannel(doneC)); err != nil {
			logger.Error(fmt.Sprintf("session %s: %s: %v", id, event, err))
			return c.Builder().Build()
		}
		<-doneC
		return proto.Clone(c.Builder().Build()).(*gamepb.Player)
	}

	party := randomParty()
	send("PARTY", party)
	if party.GetRole() != "solo" {
		say(logger, "parties", fmt.Sprintf("session %s: in a party of %d as %s", id, party.GetSize(), party.GetRole()))
	}

	pl := send("QUEUE", &gamepb.QueueRequest{PlayerId: id})
	switch {
	case pl.GetStatus() == "banned":
		pl = send("APPEAL", &gamepb.Appeal{PlayerId: id})
		say(logger, "sessions appealing", fmt.Sprintf("session %s: banned, appealed: %s", id, pl.GetStatus()))
		return
	case pl.GetFlagged():
		pl = send("APPEAL", &gamepb.Appeal{PlayerId: id})
		say(logger, "sessions appealing", fmt.Sprintf("session %s: held for review since %s, appealed: flagged %t", id, pl.GetMatchId(), pl.GetFlagged()))
		return
	case pl.GetStatus() != "in_match":
		say(logger, "not matched", fmt.Sprintf("session %s: not matched (status %s)", id, pl.GetStatus()))
		return
	}
	say(logger, "matches played", fmt.Sprintf("session %s: playing %s (%s, %s)", id, pl.GetMatchId(), pl.GetTier(), pl.GetSubscription()))

	// the session doesn't wait for scores: the gateway batches them and forwards a batch to progression,
	// waiting for it (online) or handing it over (stress, progression adds them once the session released the player)
	var scoreOpts []asm.EventOption
	if p.waitScores {
		scoreOpts = append(scoreOpts, asm.WithWaitRemote())
	}
	matchEnd := time.Now().Add(between(p.matchMin, p.matchMax))
	for time.Now().Before(matchEnd) {
		time.Sleep(between(p.scoreMin, p.scoreMax))
		if err := c.Event("SCORE", &gamepb.Score{PlayerId: id, Points: int64(10 + rand.IntN(91))}, scoreOpts...); err != nil {
			logger.Error(fmt.Sprintf("session %s: SCORE: %v", id, err))
		}
	}

	outcome := "loss"
	if rand.IntN(2) == 0 {
		outcome = "win"
	}
	pl = send("MATCH_RESULT", &gamepb.MatchResult{PlayerId: id, Suspicion: int64(rand.IntN(101)), Outcome: outcome, MatchId: pl.GetMatchId()})

	item := items[rand.IntN(len(items))]
	coins := pl.GetCoins()
	pl = send("PURCHASE", &gamepb.PurchaseRequest{PlayerId: id, Item: item.name, Price: item.price})
	if pl.GetCoins() == coins {
		say(logger, "no purchase", fmt.Sprintf("session %s: no purchase (%d coins, flagged %t)", id, pl.GetCoins(), pl.GetFlagged()))
	}

	tier := pl.GetTier()
	pl = send("MATCH_END", &gamepb.MatchEnd{PlayerId: id})
	if pl.GetTier() != tier {
		say(logger, "rank changes", fmt.Sprintf("session %s: %s, now %s", id, outcome, pl.GetTier()))
	}
	say(logger, "matches ended", fmt.Sprintf("session %s: back to %s with %d coins (%s)", id, pl.GetStatus(), pl.GetCoins(), pl.GetTier()))
}
