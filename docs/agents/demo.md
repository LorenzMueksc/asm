# Demos

## `examples/gamemeta`

User docs: `examples/gamemeta/README.md`. Run: `go run ./examples/gamemeta -duration 30s` (build to the scratchpad, not the repo root).

- 7 nodes as processes (launcher starts them, colored output). Entity Player: gateway-eu/us (1, 2), matchmaking (3, starts the cluster), store (4), anticheat (5), progression (6). Entity Match: arena (7). Members (raft): matchmaking, store, anticheat; the rest are clients (`role.member`, `clusterOpts`).
- Flags: `-players N` (default 1000, half per gateway, `duo-zoe` on both), `-pacing online|stress` (online default: sessions 5-20 min, 2-8s between actions, matches 30-60s, 30-120s offline; stress: short session per match), `-log events|stats|auto` (stats above 100 players: counts + metrics summary every 5s), `-trace N` (default 2000, 0 off), `-tls` (default on), `-persist` (members' raft logs on disk), `-duration`, `-base-port` (7100; metrics on +100).
- TLS: launcher creates a CA and per-node certs (member/client role) plus an observer cert for asmdev in `<temp>/asm-gamemeta-<port>/tls` and prints the asmdev command. `-persist`: data dirs `<temp>/asm-gamemeta-<port>/raft/<name>`, cleared every run.
- Store: shared SQLite (`store.go`, modernc.org/sqlite, WAL, `synchronous(OFF)`, one writer per node batching saves; a save returns once committed). Gateways store at logout (close func); progression stores in transitions saving on change, never on a lent lock.
- Online pacing: gateways forward SCORE batches with `WithWaitRemote()` (progression `score` batches 5 events without period, no save); MATCH_BONUS (arena → progression) stays a hand-over.
- Launcher: GOMAXPROCS per node = 2×CPUs/7 (≥2) unless set, GOGC=200; first sessions start spread (2000/s, ≥3s).
- Transitions per node: `registerTransitions` (node.go), `registerArenaTransitions` (arena.go). Stats: `stats.go` (`say`, in-process `Inspect`).

## econsim

Moved to its own project `../econsim` on 2026-10-26 (module `rumue.io/econsim`, `replace rumue.io/asm => ../asm`). Its docs: `../econsim/AGENTS.md`.
