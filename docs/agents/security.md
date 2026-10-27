# Security

Code: `node_tls.go`, `certs/certs.go`. Decided [U]: mTLS with id + roles in the certificate, own name only, separate public listener for the Event rpc, trust model below, complete id removal, certificate check hook, max connection age, metrics HTTPS for observer/admin, wait path cap. Details [A].

- Without `WithTLS` nothing is authenticated: any process reaching the port can act as any node (a warning is logged).
- `WithTLS(ca *x509.CertPool, cert tls.Certificate)`: node port requires a client cert of the CA (`RequireAndVerifyClientCert`, TLS 1.3). Identity = common name (node id or tool name); roles = organizational units `member`, `client`, `admin`, `observer` (`RoleMember`, ...). Own cert must name the node and carry `member` (raft) or `client` (`WithClientMode`), else fail at start (`checkOwnCertificate`). `LoadTLSFiles(ca, cert, key)`. Options taken in `setSecurityOptions` (before the metrics endpoint starts).
- `methodRoles` (unary + stream interceptors, `Node.authorizeMethod`; identity put in ctx): `Join`, `Sync`, `SyncStream` = member; `Attach` = client; `Propose`, `Get/AddTransitions`, `ForwardEvent(s)`, `CheckLending` = member|client; `RemovePeer` = member|client|admin; `Inspect`, `WatchEvents` = observer|admin; `Event` = any CA cert. Unknown methods denied. Add new rpcs to `methodRoles`.
- `checkPeerCertificate` (every rpc and stream start, metrics requests): cert expired → `Unauthenticated`; common name = removed node id (`nodeIdOf` + `isRemoved`) → `PermissionDenied`; `WithCertificateCheck` hook error → `PermissionDenied`. Tool names (not numeric) skip the removal check.
- Own name (`ownName`, no-op without TLS): Join's `node_id` metadata, each raft message's `From`, each proposed log's `node_id` and the node of its lock tokens (`token`, `join`; `checkProposal`), client entries only from members except a client's own `CLIENT_REMOVE` (Leave; was rejected before 2026-10-24, client Leave with TLS hung), each added transition's `node_id`, `ForwardEventRequest.sender_node_id` and last `lend_chain` element, Attach's `client_id`. `RemovePeer` of another node needs `admin`. Violations → `PermissionDenied`.
- `stepMessages` rejects local raft message types (always; `MsgHup`, storage responses, ...).
- `syncTransitions` drops pulled transitions whose `node_id` isn't the peer asked (always).
- Dialing (`dialOption(id)`): `InsecureSkipVerify` + `VerifyConnection` (chain to CA, server auth, identity, then the hook): peer must be the expected node id (member or client role); join/attach addresses (`id` 0) must be `member`. Host names not verified. Tools: `ToolCredentials(ca, cert)` accepts any member or client (no hook).
- `WithPublicAddr(addr)`: second gRPC server with only `Event` (`publicServer`, rest `Unimplemented`), server TLS with the node cert if TLS is on, no client cert. Any mode (also `WithSkipRaft`). Stopped with the node.
- Package `v2/certs` (tests, demo, dev): `NewCA`, `Issue(id, roles...)` (ECDSA P-256, server+client auth, SAN localhost/127.0.0.1), `TLSCertificate`, `WriteCA`, `WriteFiles`.

## Removal [U: complete; A: details]
- A removed id is rejected for every rpc except `Join` [U, 2026-10-24: rejoin with an old id stays allowed, also with TLS]. A removed client still gets the membership update over its open `Attach`.
- Open streams: `identityStream.RecvMsg` rejects the next message of a removed caller (`PermissionDenied`, handler ends the stream; not for `Attach`).
- Connections: `trackingCredentials` (wraps the TLS server credentials) records each connection with its client cert's common name (`connTracker`). `applyRemoveNode` / `removeClientNode` → `closeRemovedConns`: after `removedConnGrace` (1s; answers in flight, eg. the leaving node's own `RemovePeer`, still go out) closes the raw TCP connections of that id: gRPC ends all its rpcs and streams. Not after a snapshot restore (rare; rpcs and stream messages are rejected anyway).
- `removeSelf` takes `PermissionDenied` from `RemovePeer` as removed (answer lost).

## Connections [A]
- `WithMaxConnectionAge(d)` (default `defaultMaxConnectionAge` 1h, 0 = off; TLS only): server keepalive `MaxConnectionAge`, GOAWAY → clients open a new connection (new handshake: CA chain, expiry, dial hook on the server cert). No `MaxConnectionAgeGrace`: long-lived streams (SyncStream, ForwardEvents, Attach, WatchEvents) stay on the old connection (cutting them would fail forward calls in flight).
- Expiry: each tracked connection closes at its client cert's `NotAfter` (timer); rpcs check `NotAfter` too.
- `WithCertificateCheck(func(*x509.Certificate) error)`: leaf after CA verification; runs per rpc (keep it cheap), per metrics request, on dial. A node whose cert a peer refuses gets `PermissionDenied` and stops like a removed one.

## Metrics endpoint
- `WithMetricsAddr` with TLS: HTTPS (`ServeTLS`, `serverConfig`: node cert, client cert of the CA required), `authorizeHTTP(handler, RoleObserver, RoleAdmin)`: no verified cert 401, other roles / removed / hook 403, expired 401. Without TLS plain HTTP. `MetricsHandler()` = unauthenticated handler for own servers.

## Wait path cap [U: 64, A: fixed]
- `maxWaitPath` = 64 tokens, a constant: every member must refuse the same (`applyAcquire` refuses longer paths first, like a deadlock refusal). `ForwardEvent` rejects longer `wait_path` with `InvalidArgument` (`checkWaitPath`), `acquireLock` too before proposing. No token filtering.

## Trust model [U: accepted risks]
- A member/client certificate means the node runs trusted code. An authenticated node can: claim a transition name first (legit owner fails as introducing node), read peers' transitions, build wait-path edges from real tokens (false deadlock refusals), hold any lock, hand over events to any id. Raft members are fully trusted.
- No revocation list built in: removal of the node id, or the hook.
- Choices confirmed by the user 2026-10-24 [U]: trust model (a member/client cert runs trusted code: name claiming, reading peers' transitions, wait-path edges from real tokens, holding any lock, handing over to any id are accepted), `MaxConnectionAge` 1h without grace, removed id's connections closed 1s after removal, not after a snapshot restore, hook refusals as PermissionDenied, metrics cache HTTP only, wait-path cap 64 constant.
