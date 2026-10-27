# AGENTS.md

Router for AI agents. Read this, then only the docs the task needs (table below). Docs are terse on purpose: keep them that way.

## Rules
- **Ask the user before every design decision** (API, semantics, lifecycle, error handling, proto changes): concrete options + trade-offs. Users often answer with a custom option: follow what they wrote. "Do what you think is best" → decide, then say what you chose. Mechanical fixes with one obvious answer: do, then report.
- **[U]** User away / "keep working without me": don't block on questions. Write each open design decision to `docs/questionnaire.md` (options, recommendation, **Answer:** line), skip the work that depends on it, do everything else, and list what's waiting at the end.
- **[U]** Files: read and write only inside this project folder; ask before reading anything outside it. Temp files, logs, test binaries and backups go to `.scratch/` (gitignored), never to the system temp scratchpad. No agent memory outside the project: durable notes go into these docs. Tool caches are fine (Go build/module cache, `%TEMP%` dirs from `go test`, npm cache).
- Not a git repo: copy affected files to `.scratch/backup-<topic>/` before moving/deleting files or large rewrites.
- Style: functional options (`funcXxxOption`), ginkgo/gomega specs, comments on non-obvious logic.
- After a change: update the topic doc(s), `docs/agents/codemap.md` if files/flows changed, `v2/README.md` if the public API/behavior changed, and add one line to the `plan.md` log. Details and measurements go to `docs/history/`, not into topic docs.
- Doc markers: **[U]** decided by the user, don't change without asking. **[A]** agent's choice, may be revisited with the user.
- Scope: `v2/` (package `asm`), `proto/`, `service/` (generated: never hand-edit), `devtool/`, `examples/`. v1 was deleted. The econsim demo is its own project `../econsim`, using `v2/` through `replace rumue.io/asm => ../asm`.

## Env
- Windows 11, Go 1.27, Git Bash or PowerShell. Searches skip `node_modules`/`dist` via `.ignore`.
- `-race` needs gcc with `CGO_ENABLED=1`: `C:\Users\loren\AppData\Local\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe\mingw64\bin` (on user PATH; old shells may lack it).
- protoc 36.0: `C:\Users\loren\AppData\Local\Microsoft\WinGet\Packages\Google.Protobuf_Microsoft.Winget.Source_8wekyb3d8bbwe\bin\protoc.exe` (well-known types in `..\include`). Plugins in `~/go/bin`: `protoc-gen-go` v1.36.5 (must match go.mod protobuf), `protoc-gen-go-grpc` v1.5.1, `protoc-gen-go-vtproto` v0.6.1-0.20240319094008-0393e58bdf10 (same version as `github.com/planetscale/vtprotobuf` in go.mod; v0.6.0 would downgrade grpc).

## Commands
```
go build ./... && go vet ./v2/   # vet: "struct field unexported has json tag" in bitmaps_test.go is known
go test ./v2/ -ginkgo.label-filter='!cluster'            # fast specs, seconds
go test ./v2/ -ginkgo.focus "<spec text>" -ginkgo.no-color
go test -count=1 ./v2/                                   # all, ~6 min (cluster specs = real raft on localhost)
CGO_ENABLED=1 go test -race -count=1 -timeout 30m ./v2/  # ~6 min
go test ./v2/ -run '^$' -bench . -benchtime 2s           # v2/bench_test.go
cd devtool/web && npm run build                          # before go build of devtool (embeds web/dist)
# protos (repo root):
P=/c/Users/loren/AppData/Local/Microsoft/WinGet/Packages/Google.Protobuf_Microsoft.Winget.Source_8wekyb3d8bbwe
"$P/bin/protoc.exe" -I proto -I "$P/include" --go_out=service --go_opt=paths=source_relative \
  --go-grpc_out=service --go-grpc_opt=paths=source_relative \
  --go-vtproto_out=service --go-vtproto_opt=paths=source_relative,features=marshal+unmarshal+size+clone+equal \
  proto/models.proto proto/services.proto
```
Module `rumue.io/asm`; packages `.../v2` (name `asm`), `.../v2/certs`, `.../service` (generated), `.../devtool`.

## Docs (read on demand)
| Task touches | Read |
|---|---|
| where code lives, main call flows | `docs/agents/codemap.md` |
| Bits, bitmaps, state merge, transitions, signatures, sync, several entities | `docs/agents/state.md` |
| container locks, queue, retention, lending, deadlock check, hand-overs | `docs/agents/locks.md` |
| forwarding/routing, `ForwardEvents` stream, responses, other-entity receivers | `docs/agents/forwarding.md` |
| raft, proposing, sending, snapshots, WAL/restart, startup, membership, client nodes | `docs/agents/cluster.md` |
| container lifecycle, processing, `Emit`, Event rpc, errors, gotchas | `docs/agents/containers.md` |
| TLS, roles, authority, public listener | `docs/agents/security.md` |
| dev tool, event trace, tracing, metrics | `docs/agents/observability.md` |
| specs, test harness, benchmarks, current numbers | `docs/agents/testing.md` |
| demo `examples/gamemeta` | `docs/agents/demo.md` |
| wire schema | `proto/models.proto`, `proto/services.proto` |
| user-facing docs | `v2/README.md`, `devtool/README.md`, `examples/gamemeta/README.md` |
| open work, change log | `plan.md` (history: `docs/history/`) |
| decisions waiting for the user | `docs/questionnaire.md` (add questions there when the user is away) |

## Core model
- `Node[T, K]`: T = proto entity type, K = `Builder[T]` (`Build`/`Set`/`IsSet`). One `Container` per entity id; an id is open on one node only (lock table in the raft log).
- State = `Bits` per root entity bitmap; merged per property; entity bits win over event bits.
- Transitions: state or func, highest priority first. Func transitions owned by other nodes are remote: the event is forwarded and the receiver decides what runs.
- **[U]** Design model: one container (one entity instance, eg. a player) lives on several nodes. Each node contributes the func transitions it owns, run as remote funcs. The lock is lent along waiting calls (lend chain), so the state moves to the node running the next step. Nodes of another entity type use those transitions only remotely, on the target's own lock. New designs (routing, locking, lifecycle) must fit this model.
- Raft log: bitmaps, locks, heartbeats, client add/remove, conf changes; one `NodeLogs` batch per raft entry. Transitions sync by rpc.
- Node modes: raft member (default), client (`WithClientMode`: no raft, locks through a member), standalone (`WithSkipRaft`: no locks).
