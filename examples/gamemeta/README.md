# Game meta services demo

Seven asm v2 nodes run as separate processes and form a raft cluster on localhost. They handle the meta services of an online game: matchmaking, anti-cheat, the in-game store and progression (ranks, scores), for players connecting through two region gateways.

```
go run ./examples/gamemeta                      # runs until ctrl+c
go run ./examples/gamemeta -duration 30s        # stops after 30s
go run ./examples/gamemeta -base-port 7200      # node n listens on 7200+n (default 7100), metrics on 7300+n
go run ./examples/gamemeta -players 50          # 50 players instead of 1000, every event in the output
go run ./examples/gamemeta -pacing stress       # a short session per match, players barely think
go run ./examples/gamemeta -players 50000       # 50,000 players online
go run ./examples/gamemeta -log events          # every event even with 1000 players (floods the console)
go run ./examples/gamemeta -trace 0             # no event trace (the dev tool shows no events)
go run ./examples/gamemeta -persist             # the raft members write their log to disk (WithDataDir)
go run ./examples/gamemeta -tls=false           # no TLS between the nodes
```

The nodes talk mutual TLS by default: the launcher creates a CA and a certificate per node for the run (members get the member role, the others the client role) and prints the dev tool command with a certificate with the observer role.

All players play at the same time (`-players`, default 1000, half on each gateway; `duo-zoe` on both). How they play is the pacing:

| `-pacing` | Session | Between actions | Match | Offline between sessions |
|---|---|---|---|---|
| `online` (default) | 5–20 min, several matches; the player stays locked the whole time | 2–8s | 30–60s, a score every 3–8s | 30–120s |
| `stress` | one match | 0.2–0.6s | 0.4–1.2s, a score every 80–200ms | 1–3s |

The first sessions start spread out, 2000 per second (at least over 3s). With more than 100 players the nodes don't print every event but what happened in the last 5s (`-log stats`, eg. `last 5s: 424 sessions running · lock waits 117 · matches played 115 · ...`), followed by a summary of their metrics (events and forwards per second with their p99, raft entries applied and proposed per second, lock wait p99, containers, goroutines, heap, GC CPU). Every node serves its metrics for Prometheus on base-port + 100 + node id (`https://127.0.0.1:7201/metrics` for node 1, with a client certificate with the observer role such as the dev tool's; `http://` with `-tls=false`). Every node can hold `2·players+100` containers (`WithMaxContainerCount`).

The gateways and progression share the players in a SQLite database file (`players.db` in the temp directory, standing in for a database cluster). Every node commits the saves waiting at that moment in one transaction; a save returns once its transaction is committed, so the node getting the player's lock next reads it.

Three service nodes are the raft members: matchmaking (it starts the cluster), store and anticheat. The gateways, progression and the arena are clients (`WithClientMode`): they apply no log, they use the lock table through a member and propose through the leader, so their CPU goes to containers. The launcher starts matchmaking, waits until it's ready, starts the other members, then the clients. Every node gets its share of the CPUs (`GOMAXPROCS` = 2 × CPUs / 7, at least 2, unless `GOMAXPROCS` is set), so seven Go schedulers don't spin on the same cores. It prints every node's output in one stream, colored per node. The nodes keep an event trace (`-trace`, default 2000 events), so the dev tool shows them: run the asmdev command the launcher prints (with the TLS flags), with `-tls=false` `go run ./devtool/cmd/asmdev -join 127.0.0.1:7101`.

## The nodes

| Node | Role | Owns |
|---|---|---|
| 1 `gateway-eu` | client, simulates EU players | `join_party` (PARTY), `diamond_lounge` (MATCH_END, priority 5); state transitions `finish_match`, `hold_for_review`, `leave_party` (MATCH_END) and `strip_rank` (MATCH_RESULT, priority -5) |
| 2 `gateway-us` | client, simulates US players | the same (same signatures, so both own them) |
| 3 `matchmaking` | raft member, starts the cluster | `priority_matchmake` (QUEUE, priority 10): premium or diamond players skip the queue; `matchmake` (QUEUE): everyone else in the lobby |
| 4 `store` | raft member | `purchase` (PURCHASE): unflagged players buy items with coins, no battle pass on mobile; `rank_reward` (RANK_UP): coins for a promotion |
| 5 `anticheat` | raft member | `review_match` (MATCH_RESULT): flags (≥80) or bans (≥95) suspicious players; `review_appeal` (APPEAL): clears half of the flags and lifts a tenth of the bans |
| 7 `arena` | client, the second entity, `Match` | `add_player` (JOIN_MATCH, priority 10), state transition `fill_match` (JOIN_MATCH), `record_result` (MATCH_RESULT) |
| 6 `progression` | client, loads players like the gateways, stores them only after `score` | `count_win`, `count_loss`, `party_xp` (MATCH_RESULT), `rank_update` (MATCH_END, priority 10), `score` (SCORE, batched), `match_bonus` (MATCH_BONUS) |

## The bitmap

The committed bitmap has two root entities, each with its own bits:

- `Player` (`gamepb/game.proto`): `status` (lobby, in_match, banned), `region` (eu, us), `flagged`, `tier` (bronze, silver, gold, diamond), `subscription` (free, premium), `platform` (pc, console, mobile) and the nested `party`: `party.role` (solo, leader, member) and `party.size` (1–4, int64 values). `coins`, `match_id`, `wins`, `losses` and `score` are plain data.
- `MatchResult`: `outcome` (win, loss). MATCH_RESULT events carry it, so transitions can match on the event.
- `Match`, the second entity, kept by the arena: `status` (open, full, finished), `platform`, `size` (1–4 players). `players`, `reported` and `winners` are plain data. Matches are kept in the arena's memory.

New players start as bronze, free or premium and on a platform derived from their id.

## A session

1. **PARTY:** half of the players join a party (leader or member of 2–4). `join_party` runs locally on the gateway.
2. **QUEUE:** the gateway forwards to matchmaking, which puts up to 4 players of a platform into the same match and emits `JOIN_MATCH`. That event goes to another entity with another container id: the arena picks the Match container by the match id in the payload and locks the match itself (no waiting, nothing lent). Premium and diamond players are fast-tracked; the state is evaluated again after `priority_matchmake`, so `matchmake` doesn't run on them as well.
3. **SCORE:** sent during the match; the session doesn't wait for them. The gateway batches them and forwards a batch to progression:
   - `-pacing stress`: batches of 5 events or 2s, handed over. Progression adds them up once the session released the player, then stores the player.
   - `-pacing online`: batches of 5 events (the rest when the session ends), forwarded waiting, so progression adds them on the lent lock right away and the gateway gets the new score and coins; the gateway stores the player at logout. Handed over, they would pile up on progression until the player logs off, minutes later.
4. **MATCH_RESULT** (win or loss): forwarded to anti-cheat, progression and the arena **in parallel**. The gateway merges the changes of anti-cheat and progression into the player; the arena records the result in the match (its own lock) and returns no entity. Once every player of a full match reported, the arena emits `MATCH_BONUS` for each winner: back to the Player entity, progression picks the player's container by the payload and pays 30 coins after the gateway released the player.
5. **PURCHASE:** forwarded to the store.
6. **MATCH_END:** state transitions on the gateway: `finish_match` moves unflagged players back to the lobby, `hold_for_review` flagged ones (still flagged), and `leave_party` breaks parties up (a nested target).
   Before them, `rank_update` on progression (priority 10) promotes after 2 wins or demotes after 2 losses, then `diamond_lounge` on the gateway (priority 5) already sees the new tier.
   - A promotion emits `RANK_UP` inside `rank_update` (`c.Emit` with `WithWaitRemote()`). The store's `rank_reward` pays 50/100/250 coins for silver/gold/diamond.
   - The gateway lent the lock to progression, and progression lends it on to the store. Every node waits, so the gateway holds the lock until the reward is back, and it ends the session with the coins.

## What to look for in the output

- **Cluster forming:** `node N joined the cluster`, `raft: 1 became leader`. Nodes 2–6 join at the same time; the leader adds them one after another.
- **Shared bitmap:** `committed bitmap Player of node N`. Only anti-cheat's config contains the status `banned`. The committed bitmap appends it, so every node can evaluate it with the same bits. The signatures of transitions don't depend on the bitmap, so `join_party` (whose condition uses `banned`) stays the same on both gateways while the bitmap grows.
- **Transition sync:** `got N transitions of node X`, `pushed N transitions`, `added transitions [...] of node X`. Each node ends up knowing all transitions and their owners.
- **Remote execution:** `forwarding 1 events QUEUE and lending the lock` on a gateway, followed by `processing 1 events QUEUE forwarded by another node` on matchmaking. The gateways evaluate the synced conditions themselves and only forward to the nodes whose transitions match; the receiving node decides which of its transitions run.
- **Hand-over and batching:** `forwarding 5 events SCORE` (no lending) on a gateway, later `added N points of 5 events to ...` on progression, after it got the player's lock.
- **State transitions:** `strip_rank` has the lowest priority of MATCH_RESULT, so the results of anti-cheat, progression and the arena are merged before it, and a player banned in that event drops to bronze right away. `fill_match` runs on the arena after `add_player`: the state is evaluated again after the func, so the 4th player makes the match full in the same event; it runs on the receiver because senders can't run state transitions of another entity. Targets set flagged=false unless they say otherwise (zero values apply), so `hold_for_review` and `strip_rank` set `Flagged: true`, and conditions of Player examples only match unflagged players (flagged players keep their party).
- **Two entities:** `eu-12 joined m-pc-3 (3/4)` and `m-pc-3 finished, winners [...]` on the arena, `match bonus for eu-12: 30 coins for winning m-pc-3` on progression. The dev tool shows the arena's entity Match, its containers with match ids, and RECEIVED JOIN_MATCH/MATCH_RESULT in `m-…` containers.
- **Ranks:** `promoted eu-12 to silver` on progression, `rank reward for eu-12: 50 coins for silver` on the store (lent on by progression), `session eu-12: win, now silver` on the gateway; `diamond lounge bonus` once players reach diamond. In the dev tool, the events show EVENT RANK_UP on progression, a FORWARD to the store and RECEIVED from node 6 on the store.
- **Concurrency:** 1000 players play at the same time: `eu-001`–`eu-500`, `us-001`–`us-499` and `duo-zoe`. Flagged and banned players appeal instead of queueing, so the population doesn't run out of players who can play. The services take time like real ones (matchmaking 0.4–1.2s, fast-track 0.1–0.3s, anti-cheat 0.3–0.8s, store 0.1–0.3s) and players think 0.2–0.6s between actions. Different players are processed in parallel on every node; the events of one player are processed in order.
- **Container locking:** `duo-zoe` plays on both gateways. While one gateway has the player open, the other logs `container duo-zoe is locked by node N, waiting` and later `got the lock after waiting ...`. Progression queues for the lock too, to process handed over scores.
- **Shared data:** players are stored in a SQLite database shared by the gateways and progression (printed at start), standing in for a database cluster. Because of the lock, only one node reads or writes a player at a time, so coins, ranks and bans carry over. The gateways store a player when the session ends; progression only after `score` when it is handed over (stress pacing) and after `match_bonus`, which run on its own lock. It never stores players it got on a lent lock (MATCH_RESULT, MATCH_END, SCORE in online pacing): the gateway owns them, and the borrowed container may close after the gateway already stored and released the player.
- **Conditions:**
  - `All(&Player{Status: "lobby"})` also requires `flagged = false`, because `false` is a bitmap value (zero values apply). That's why flagged players are "held for review" and can't queue or buy.
  - `None(&Player{Status: "banned", Flagged: true})` (join_party) means neither banned nor flagged.
  - `AtLeastOne` (premium or diamond), `OneOf` on the nested party role (leader or member), and event conditions on `MatchResult.outcome`.

## Files

- `main.go`: roles and flags.
- `launcher.go`: starts the node processes and prints their output.
- `node.go`: node setup, bitmap, transitions per role, the shared player store.
- `simulate.go`: player sessions on the gateways.
- `arena.go`: the arena node with the Match entity.
- `gamepb/`: the protobuf messages. Regenerate them with protoc and `protoc-gen-go` v1.36.5:

```
protoc -I examples/gamemeta/gamepb --go_out=examples/gamemeta/gamepb --go_opt=paths=source_relative examples/gamemeta/gamepb/game.proto
```
