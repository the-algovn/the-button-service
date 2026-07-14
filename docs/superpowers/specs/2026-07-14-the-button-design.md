# the-button — Design

**Date:** 2026-07-14
**Status:** Approved (brainstorm dialogue + 4-lens design workflow + adversarial judge panel)
**Depends on:** api-control-plane (live, spec in that repo), iac authnz/postgres/kong conventions
**Target:** 10,000 concurrent users on the existing cluster (worker `algovn-w1`, i9/32GB)

## 1. Product

One button. One goal. Millions of humans.

Page at `https://algovn.com/the-button` (Vite React SPA, path-served like showcase):

- **Meaning at the top** — rotating taglines: *stress-testing a home server · proving
  humans can work together · because the internet needs more joy · every click is a
  tiny rebellion*.
- **One big counter** — the global click total, live via SSE (1s tick).
- **One big contribute button** — anyone can watch; clicking requires Zitadel login
  (SPA + PKCE). Mash freely: the client batches clicks, pays a proof-of-work per
  batch, and submits.
- **Achievements** — personal troll achievements (server-tracked, instant unlock
  toasts) and global milestone banners.

## 2. Decisions (locked in brainstorm)

| Question | Decision |
|---|---|
| Scale target | 10k CCU (most holding SSE; large active fraction clicking). Judged feasible on w1; uplink + uncached assets are the binding constraints. |
| Click model | Unlimited clicking, PoW-gated per batch; difficulty scales with batch size and server load. |
| Durability | Per-batch Postgres transaction for personal truth; global counter in Redis (AOF everysec — ≤1s loss on crash), reconciled from Postgres. |
| Replay / throttle | Redis: `SETNX` burn with small TTL for challenges; `SETNX`-with-expiry per-user min-interval as the hard throttle. |
| Personal storage | Exactly one atomic counter per user in Postgres (`user_clicks`) + unlock rows. No stats columns, no batch log table. |
| SSE | Global counter tick every 1s, independent of who clicked, channel `the-button.counter` (anonymous). Typed payloads. |
| Achievements | Evaluated in the batch transaction (from total + current request only); milestones by the tick leader. |
| Frontend | React + Vite SPA in `web/apps/the-button`, `base: '/the-button/'`, @algovn/ui components (avoid app-shell/command-menu — Next-only). |
| Redis topology | Single instance + AOF everysec + RDB, cluster-mode-ready by construction (single-key ops only). |
| Admin surface | None in v1. Rebellion counters don't reset. |

## 3. Architecture

```
Browser ── Cloudflare (SPA assets edge-cached; API+SSE proxied)
   │
cloudflared ×2 ── Kong ×1 (real-IP fixed; 3 ingresses w/ per-route limits)
   │
api-control-plane ×2 ── the-button-service ×2 (gRPC h2c :9090)
   │  SSE /events/the-button.counter          │            │
   └── RabbitMQ ◀── tick leader (1s) ─────────┤            │
                                              ▼            ▼
                                     Redis (single, AOF)   Postgres (shared CNPG)
                                     pow:* throttle:*      the_button DB:
                                     counter:global        user_clicks
                                     milestone:* pow:L     user_achievements
```

- **Redis = hot control state**: challenge burns, per-user throttle, global counter,
  milestone claims, shared difficulty level. All single-key ops.
- **Postgres = durable personal truth**: one counter row per user + unlocked
  achievements. Global counter is reconciled FROM Postgres (see §8).

## 4. API surface

Proto `algovn.button.v1` (protos repo, tag after merge):

```proto
service ButtonService {
  rpc GetCounter(GetCounterRequest) returns (GetCounterResponse);           // anonymous
  rpc ListAchievements(ListAchievementsRequest) returns (ListAchievementsResponse); // anonymous; personalized when a valid token is present
  rpc IssueChallenge(IssueChallengeRequest) returns (IssueChallengeResponse);       // authenticated
  rpc SubmitClicks(SubmitClicksRequest) returns (SubmitClicksResponse);             // authenticated, deadline 3s
}

message GetCounterRequest {}
message GetCounterResponse { uint64 total = 1; }

message IssueChallengeRequest { uint32 intended_clicks = 1; }
message IssueChallengeResponse {
  string challenge = 1;              // opaque b64url(payload || HMAC-SHA256(payload, K))
  uint64 work_factor = 2;            // W0*L at issuance (per-click expected hashes)
  uint32 min_interval_seconds = 3;
  uint32 max_batch = 4;              // 10000
  google.protobuf.Timestamp expires_at = 5;
}

message SubmitClicksRequest { string challenge = 1; uint64 nonce = 2; uint32 click_count = 3; }
message SubmitClicksResponse {
  uint64 user_total_clicks = 1;
  repeated Achievement unlocked = 2;
  IssueChallengeResponse next_challenge = 3;  // piggyback: client starts solving immediately
}

message ListAchievementsRequest {}
message ListAchievementsResponse {
  repeated Achievement catalog = 1;           // full catalog, unlocked_at set when personalized
  repeated Milestone milestones = 2;          // reached global milestones
}
message Achievement { string id = 1; string title = 2; string description = 3; google.protobuf.Timestamp unlocked_at = 4; }
message Milestone { uint64 threshold = 1; string title = 2; google.protobuf.Timestamp reached_at = 3; }
```

Registration (iac `apps/api-control-plane/registrations/the-button.yaml`):

```yaml
prefix: /the-button
upstream: dns:///the-button-service.the-button.svc.cluster.local:9090
defaultRule: authenticated
routes:
  - { method: algovn.button.v1.ButtonService/GetCounter,        rule: anonymous }
  - { method: algovn.button.v1.ButtonService/ListAchievements,  rule: anonymous }
  - { method: algovn.button.v1.ButtonService/IssueChallenge,    rule: authenticated }
  - { method: algovn.button.v1.ButtonService/SubmitClicks,      rule: authenticated, deadline: 3s }
channels:
  - { name: the-button.counter, rule: anonymous }
```

`ListAchievements` on the anonymous rule still receives the `Authorization` header
when a valid token is present (control-plane semantics: the header is only ever
forwarded after signature verification, on every rule) — the service personalizes
if it can parse a `sub`, else returns the bare catalog. Trust model: the service
does a read-only segment-2 decode per `authnz-conventions.md` and does NOT
re-verify — the gateway is the sole verified ingress, and in-cluster callers are
trusted under the platform's documented tenancy model.

## 5. Proof-of-work protocol

**Challenge token** = `base64url(payload || HMAC-SHA256(payload, K))`, `K` a sealed
secret shared by all service replicas (rotation: dual-key accept window). Payload:
`{id: UUIDv7, sub, iat, exp: iat+300s, w0, l, min_interval_s, max_batch}`. Binding
`sub` kills token farming; embedding the parameters makes them tamper-proof and
replica-consistent. Verification is stateless — any replica verifies any token.

**Work check**: `SHA-256(token_bytes || be32(click_count) || be64(nonce))`
interpreted as a 256-bit big-endian integer must be `< 2^256 / (w0 * click_count * l)`
— the smooth full-target form (no step cliffs, constant per-click cost, so batching
is naturally incentivized under load). `max_batch = 10000`; above → InvalidArgument.

**Difficulty controller** (single shared signal — judges killed per-replica
estimation): the tick leader computes an EWMA of accepted submits/s (from a Redis
stats counter), maps it to `L ∈ {1..16}` with hysteresis (raise above 110% of a
band edge, lower below 70%) and a slew limit (≤1 step per 30s), and stores
`pow:L` + `pow:min_interval` in Redis. All replicas read those when issuing.
`min_interval` ladder 2s → 5s → 10s is the HARD valve; `L` is the cost valve.
`w0` default 2^14 expected hashes/click (≈1.6M hashes for a 100-click batch at
L=1 — ~0.5-1.5s on a mid phone with a WASM solver); calibrated against the real
solver before launch (plan task) — never tuned assuming WebCrypto speeds.

**Client**: WASM SHA-256 solver (hash-wasm) in a Web Worker; flush a batch after
`max(min_interval, solve_time)` or 300 accumulated clicks; the response's
`next_challenge` keeps the pipeline full.

## 6. SubmitClicks flow (any replica)

1. Parse + HMAC-verify token; check `exp` (30s leeway), `sub` matches caller,
   `click_count ≤ max_batch`; verify PoW target. Failures → `INVALID_ARGUMENT`
   (bad work/args) or `FAILED_PRECONDITION("challenge_expired")` (re-issue).
2. Redis, two sequential commands (the second branches on the first, so not one
   pipeline batch):
   `SET pow:<id> 1 NX EX 330` — burn; already-set → `ALREADY_EXISTS` (replay).
   `SET throttle:<sub> 1 NX EX <min_interval>` — rate floor; already-set →
   `DEL pow:<id>` to un-burn, return `RESOURCE_EXHAUSTED` (token stays valid,
   client backs off).
3. Postgres txn:
   `INSERT INTO user_clicks AS u (user_sub, clicks) VALUES ($sub,$n)
    ON CONFLICT (user_sub) DO UPDATE SET clicks = u.clicks + $n
    RETURNING clicks` → evaluate achievement rules in Go (inputs: new total,
   click_count, server time) → if unlocks:
   `INSERT INTO user_achievements ... ON CONFLICT DO NOTHING RETURNING achievement_id`
   → `INSERT INTO counter_outbox (id, clicks) VALUES ($id, $n)` (the PoW
   challenge id — already unique per batch — is the outbox row's own
   idempotency key) → COMMIT.
   On txn failure: best-effort compensation `DEL pow:<id>`, `DEL throttle:<sub>`
   (if DEL fails the client re-solves one challenge — accepted). If the
   commit itself is ambiguous (deadline expiry, connection drop), the burn is
   kept (§13) and, if the commit in fact landed, its outbox row is picked up
   by the sweeper (§8) — closing the counter under-count an ambiguous commit
   would otherwise leave.
4. After commit: apply the outbox row to the public counter with a Lua
   script — `SET applied:<id> 1 NX EX 86400` guarding `INCRBY counter:global
   $n` — then best-effort `DELETE FROM counter_outbox WHERE id = $id`. The
   script is idempotent (safe to re-run for the same id from a retry or the
   sweeper), which is the point: no diff between Postgres and Redis can heal
   this counter, because Redis structurally lags Postgres by the in-flight
   window (commit lands, then the apply) — a diff-based reconcile cannot
   tell a lost apply from one merely in flight (§8). Also `INCR
   stats:accepted_total` (monotonic controller signal; the tick leader
   samples it each tick, rate = delta/dt, EWMA half-life ~30s — one key, no
   buckets, leader failover costs one tick of signal).
5. Respond with new total, unlocks, and a fresh piggybacked challenge.

Redis unavailable → steps 2/4 impossible → `UNAVAILABLE` (clicks fail closed —
surfaces as HTTP 502 via the existing mapping; reads and SSE stay up).
Error-mapping additions to api-control-plane (launch blockers — the SPA must
distinguish these): `ResourceExhausted → 429` with `Retry-After: 2` (back off,
token still valid), `AlreadyExists → 409` (replay), `FailedPrecondition → 400`
(expired challenge — re-issue). The JSON error body's `code` field carries the
gRPC code either way.

## 7. Data model

Postgres (database `the_button`, declarative CNPG onboarding, sealed creds):

```sql
CREATE TABLE user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL);
CREATE TABLE user_achievements (
  user_sub text NOT NULL, achievement_id text NOT NULL,
  unlocked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_sub, achievement_id));
CREATE TABLE counter_outbox (
  id uuid PRIMARY KEY, clicks bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX counter_outbox_created_at_idx ON counter_outbox (created_at);
```

No batch log table (43M rows/day at peak would kill the shared 10Gi PV — durably
applying the batch IS the record). `counter_outbox` is not a batch log: it is the
transactional outbox for the public counter (§6/§8) — one short-lived row per
accepted batch, deleted as soon as its Redis apply lands, so it stays small
under normal operation; the `created_at` index bounds what the sweeper scans.
Achievement catalog lives in Go code. pgxpool: MaxConns 10/replica,
statement_timeout 2s. Budget: ~2.25 busy connections at the 750 txn/s
engineered ceiling (Little's law, ~3ms/txn same-node).

Redis keys: `pow:<uuid>` (EX 330), `throttle:<sub>` (EX 2-10), `counter:global`,
`applied:<uuid>` (EX 86400), `milestone:<threshold>`, `pow:L`, `pow:min_interval`,
`stats:accepted_total`.

## 8. Tick leader, SSE, outbox sweeper

- **Leadership**: `pg_try_advisory_lock` held on ONE dedicated non-pooled
  connection per replica attempt; leadership == that connection's health;
  self-demote (close conn) if the tick loop lags >5s; others poll every ~2s.
- **Every 1s (leader)**: `GET counter:global` (if key missing → seed from
  `SUM(user_clicks)`, then purge outbox rows created at-or-before the SUM's
  own read timestamp — they're already reflected in the seed, so left alone
  the sweeper would apply them again on top of it and over-count), publish
  `{type:"counter","total":N}` to RabbitMQ `the-button.counter` when changed;
  detect milestone crossings → `SETNX milestone:<threshold>` → publish
  `{type:"milestone",...}` only when the SETNX won (exactly-once claim;
  announcement at-most-once — accepted, clients also render milestones from
  `ListAchievements`).
- **Every 30s (leader, its own goroutine — never inline in the 1s tick loop,
  or a slow sweep would trip the >5s self-demote check above)**: sweep the
  outbox — `SELECT id, clicks FROM counter_outbox WHERE created_at < now() -
  interval '30 seconds' ORDER BY created_at LIMIT 500` (rows younger than the
  in-flight window may still be mid-apply, so are left alone); for each, run
  the same idempotent Lua apply as §6 step 4 — a no-op if it already landed —
  then delete the row. This replaces the hourly diff-based reconcile: no diff
  between Postgres and Redis can distinguish a lost apply from one merely in
  flight, because Redis structurally lags Postgres by the in-flight window
  (commit lands, then the apply); at the design's target load the observed
  drift is essentially always non-zero and positive, and "healing" it
  double-counts the in-flight batch's own apply landing moments later. The
  outbox sidesteps the diff entirely — every apply is idempotent and keyed by
  the batch id, so it is safe to retry from a crash, an ambiguous commit, or
  a Redis blip, and safe to leave untouched while genuinely in flight.
- **Every replica**: 1s in-process cached total (`GET`, fallback `SUM`) serving
  `GetCounter` — correct from any pod, even with RabbitMQ or Redis down.
- Controller updates (`pow:L`, `pow:min_interval`) piggyback the 1s leader tick.

## 9. Achievements (draft catalog — tune copy freely)

Personal (id → trigger; all evaluable from new total + current request + clock):

| id | title | trigger |
|---|---|---|
| mvh | Minimum Viable Human | total ≥ 1 |
| ten | Double Digits | total ≥ 10 |
| century | Century of Defiance | total ≥ 100 |
| comma | The Comma Club | total ≥ 1,000 |
| carpal | Carpal Diem | total ≥ 10,000 |
| stretch | Please Stretch | total ≥ 100,000 |
| nice | Nice. | total crosses 69 |
| blaze | Botanical Enthusiast | total crosses 420 |
| bigbatch | Mass Production | single batch ≥ 500 |
| maxbatch | One Batch to Rule Them All | single batch = 10,000 |
| night | 3am Rebellion | batch lands 03:00–03:59 Asia/Ho_Chi_Minh |
| lunch | Lunch Break Rebel | batch lands 12:00–12:59 Asia/Ho_Chi_Minh |

Global milestones (`threshold → title`): 1,000 → "A Thousand Tiny Rebellions";
100,000 → "Six Figures of Defiance"; 1,000,000 → "One Million. Together We Did… This.";
10,000,000 → "Ten Million Clicks Nobody Asked For"; 1,000,000,000 → "The Billion".

"crosses" = old_total < X ≤ new_total (evaluable from RETURNING clicks and $n).

## 10. SPA

`web/apps/the-button` (pnpm workspace, Vite, React 19, @algovn/ui, Tailwind v4;
`base: '/the-button/'`). Pieces:

- **Auth**: `oidc-client-ts`, Zitadel PKCE, redirect `https://algovn.com/the-button/callback`
  (+ `http://localhost:5173/the-button/callback` for dev). Token in memory only.
- **API client**: thin fetch wrapper for the control-plane JSON convention
  (`POST /the-button/algovn.button.v1.ButtonService/<Method>`), 429-aware backoff.
- **Live counter**: EventSource wrapper — full-jitter reconnect (0–5s, exponential),
  honors server `retry:`; falls back to 10s±3s `GetCounter` polling after ≥3
  failures; Page Visibility API disconnects hidden tabs (20-40% connection savings).
- **Clicker**: local optimistic count, Web-Worker hash-wasm solver, flush policy §5.
- **Achievements**: catalog grid (locked = mocking copy), unlock toasts from
  `SubmitClicksResponse`, milestone banner from SSE + `ListAchievements`.
- **Delivery**: nginx static image (immutable hashed assets, `Cache-Control:
  public,max-age=31536000,immutable`; `index.html` no-cache) + **Cloudflare cache
  rule for `/the-button/assets/*`** — mandatory: uncached bundles at 10k arrivals
  = 40-67 Mbps, more than the uplink. Verify `cf-cache-status: HIT` at acceptance.

## 11. Platform prerequisites (deployed before the service)

1. **Redis** platform component: single pod on w1, `appendonly yes` +
   `appendfsync everysec` + RDB, PVC 2Gi local-path, sealed password, 512Mi,
   VMServiceScrape (redis_exporter sidecar).
2. **Kong real-IP fix** (live bug affecting the whole platform, not just this
   product: `limit_by: ip` currently buckets ALL traffic as the cloudflared pod
   IP — an effective global 10 req/s cap): `real_ip_header: CF-Connecting-IP` +
   `trusted_ips` scoped to the pod CIDR, PLUS a NetworkPolicy admitting only
   cloudflared to Kong's proxy port (header forgery guard). Ships as the FIRST
   independent iac change with its own verification and rollback, referenced
   here as a prerequisite.
3. **Ingress split ×3** on api.algovn.com with per-route KongPlugins:
   `/events` → loose per-IP floor sized for NAT cohorts (50/s, 1000/min);
   `/the-button` → clicks floor (20/s, 600/min per IP — PoW+min_interval are the
   real throttles); everything else keeps ~10/s.
4. **api-control-plane**: ResourceExhausted→429+Retry-After; SSE `retry:` field
   (before first flush); global SSE cap 15k → 503; `https://algovn.com` added to
   CORS_ORIGINS; replicas 2, memory 1Gi + GOMEMLIMIT.
5. **cloudflared**: replicas 2, 512Mi, amd64 nodeSelector. **Kong**:
   `KONG_NGINX_EVENTS_WORKER_CONNECTIONS=32768`, `WORKER_RLIMIT_NOFILE=65536`,
   2Gi. **w1 sysctls** (ansible): `net.core.rmem_max/wmem_max` ≥ 8MB (quic-go),
   verify conntrack + nofile.
6. **Zitadel**: `MaxOpenConns` 10→20, memory 2Gi; per-IP rate floor ingress on
   id.algovn.com (login-storm + puppet-account mitigation). Console onboarding:
   project `the-button`, SPA app (PKCE, **Access Token Type: JWT**), redirect URIs.
   No roles in v1 (only `authenticated` rules) — role assertion not needed.

Static replicas everywhere; **no HPA** on any long-lived-connection path.

## 12. Capacity model & verification

Engineered ceilings (judge-verified): 750 batch txn/s sustained (PG shared with
Zitadel/OpenFGA; PG itself ≥5k TPS for this txn class on i9/NVMe); worst-case
aggregate bounded by construction — 5,000 active clickers × hard 10s min-interval
= 500/s. SSE tick at 10k conns ≈ 6-10 Mbps uplink (~48B payload discipline);
whole-chain steady CPU ≈ 2 cores of 16.

Pre-launch evidence (plan tasks):
- `pg_test_fsync` on w1 + 1k txn/s soak against a scratch DB (validates the ~3ms model).
- WASM solver H/s measured on real devices → calibrate `w0`.
- k6 from LAN + external origin: (A) SSE ramp to 10k with tick-latency assertion,
  (B) SubmitClicks soak at ceiling with p95 latency + 429-rate assertions,
  (C) rollout drill at 5k+ conns (deploy acp + service; assert reconnect wave
  drains without 5xx spike).
- Alerts (VictoriaMetrics): uplink TX %, acp RSS + SSE gauge per pod, PG commit
  p95/rate, PV usage 70/85%. One-page degradation runbook: widen tick (1s→2s),
  raise `min_interval`, lower SSE cap — manual knobs, no automation.

## 13. Failure modes

| Failure | Behavior |
|---|---|
| Redis down | Clicks fail closed (`UNAVAILABLE` → 502); counter/SSE serve from per-replica PG SUM cache; milestones pause. |
| RabbitMQ down | SSE 503 → SPA polls GetCounter (10s±3s); clicks unaffected. |
| Postgres down | SubmitClicks + personalized ListAchievements fail; GetCounter/SSE keep serving from Redis; bare catalog + milestones still served (code + Redis). |
| Redis data loss | AOF restore ≈ ≤1s loss; full loss → counter reseeded from PG (§8), milestones re-announced once (accepted). |
| Service pod loss | Stateless; leader lock re-acquired ≤2s; in-flight tokens remain valid (stateless HMAC). |
| acp deploy | `retry:` + jittered SPA reconnect; ~250-450 anonymous reconnects/s — a non-event. |
| Token burned but txn failed + DEL failed | Client re-solves one challenge (rare, bounded). |

## 14. Out of scope (v2+)

Leaderboards · streak achievements (need stored state) · admin/reset ·
authenticated SSE channels (EventSource cannot send headers — documented platform
limit) · per-user analytics · Redis cluster mode / second node · CAPTCHA-class
bot defense beyond PoW+login (accepted: motivated native attackers out-hash
browsers; min_interval bounds them anyway).

## Appendix: repos touched

| Repo | Work |
|---|---|
| protos | `algovn/button/v1` package, next gen/go tag |
| the-button-service (this) | Go service, Dockerfile, CI, this spec |
| web | `apps/the-button` SPA + nginx image + CI |
| iac | Redis platform, Kong real-IP + netpol, ingress split, acp/cloudflared/Kong/Zitadel bumps, the-button namespace/deploy/registration, PG database, sysctls, alerts, runbook |
| api-control-plane | 429 mapping, SSE retry + cap, CORS apex |
| specs | products/the-button.md + portfolio row |
