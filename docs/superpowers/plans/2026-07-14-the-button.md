# the-button Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship The Button — a PoW-gated global click counter at algovn.com/the-button serving 10k CCU — plus the platform hardening it requires (Kong real-IP fix, Redis, edge scaling, rate-limit split).

**Architecture:** Vite SPA → api.algovn.com (Kong → api-control-plane ×2) → the-button-service ×2 (pure gRPC). Redis holds hot control state (PoW replay burns, per-user throttle, global counter with AOF, milestone claims, difficulty level); Postgres holds durable personal truth (one counter row per user + achievement unlocks); a Postgres-advisory-lock tick leader publishes the 1s SSE counter via RabbitMQ and reconciles Redis from PG. Spec: `docs/superpowers/specs/2026-07-14-the-button-design.md` (authoritative for every protocol/number).

**Tech Stack:** Go 1.26 (grpc-go, pgx/v5, go-redis/v9, amqp091), protos via buf, React 19 + Vite + @algovn/ui + oidc-client-ts + hash-wasm, k6, kustomize/Argo CD, SealedSecrets.

## Global Constraints

- Repos: iac `/Users/duclm27/the-algovn/iac`, protos `/Users/duclm27/the-algovn/protos`, api-control-plane `/Users/duclm27/the-algovn/api-control-plane`, web `/Users/duclm27/the-algovn/web`, service (this repo) `/Users/duclm27/the-algovn/the-button-service`, specs `/Users/duclm27/the-algovn/specs`. All push to `main` (user-approved GitOps flow); iac requires `./scripts/validate.sh` before every push.
- The frozen cross-task interface contract (names, env vars, Redis keys, images, ports, task numbering) is `docs/superpowers/plans/2026-07-14-the-button-interfaces.md` — every task's Consumes/Produces references it; deviations are plan bugs.
- Go 1.26.4; testify require; TDD with RED/GREEN evidence; integration tests build-tagged `integration` and run via podman (`export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"; export TESTCONTAINERS_RYUK_DISABLED=true`).
- Images amd64-only on ghcr.io; GHCR packages stay PRIVATE — every new namespace gets sealed `registry-creds` (copy pattern: `iac/apps/api-control-plane/`). kubeseal always with `--context algovn-remote --controller-name sealed-secrets --controller-namespace sealed-secrets`.
- Commits: imperative subject, small and focused, NO Co-Authored-By / Generated-with trailers. Repo-publish and Zitadel-console steps are USER-GATED — the controller asks the user before running them.
- PoW hash preimage everywhere (server verify, SPA solver, load-test generator): `SHA-256(ASCII bytes of the challenge string as issued || be32(click_count) || be64(nonce))` — the challenge string is never decoded for hashing.
- SSE payloads (tick leader publishes, SPA parses): `{"type":"counter","total":<number>}` and `{"type":"milestone","threshold":<number>,"title":<string>}`.
- protojson wire realities: uint64 renders as a JSON string; zero-valued fields are omitted (fresh GetCounter body is `{}`).
- Kong LAN path (node IP :80/:443) remains allowed by the new NetworkPolicy; header forgery is prevented by `trusted_ips` (pod CIDR only), not by blocking LAN.

## Phases and execution order

- **Phase P (platform prerequisites):** T1 Kong real-IP + NetworkPolicy · T2 Redis component · T3 api-control-plane code changes · T4 edge scaling iac · T5 Zitadel bumps + w1 sysctls. T1 ships first and independently.
- **Phase S (service):** T6 protos · T7 scaffold + storage · T8 pow package · T9 SubmitClicks core + achievements · T10 tick leader + RPCs + main · T11 Dockerfile + CI + publish.
- **Phase W (SPA):** T12 Vite scaffold · T13 auth + API client · T14 counter + SSE wrapper · T15 clicker + solver worker · T16 achievements UI · T17 nginx image + web CI.
- **Phase D (deploy/verify):** T18 iac deploy + registration · T19 Zitadel onboarding (USER) + e2e smoke · T20 fsync/soak/solver calibration · T21 k6 suite · T22 alerts + runbook + catalog flip.

Dependencies: P before D; T6 before T7+; T11+T17 images before T18; T19 before full e2e; T20 before T21. Phases S and W can interleave after T6.

---
### Task 1: Kong real-IP fix + NetworkPolicy (platform-wide rate-limit bug)

**Files:**
- Edit: `/Users/duclm27/the-algovn/iac/platform/kong/values.yaml`
- Create: `/Users/duclm27/the-algovn/iac/platform/kong/manifests/networkpolicy.yaml`
- Edit: `/Users/duclm27/the-algovn/iac/platform/kong/manifests/kustomization.yaml`

**Interfaces:**
- Consumes: Kong app `kong` (chart `kong/ingress` 0.24.0, gateway Deployment `kong-gateway`, proxy pod ports proxy=8000/proxy-tls=8443/status=8100/admin-tls=8444), cloudflared pods (ns `cloudflared`, label `app: cloudflared`), cluster pod CIDR `10.42.0.0/16` (k3s default; node podCIDRs are 10.42.0.0/24 + 10.42.1.0/24), KongPlugin `api-rate-limit` (`limit_by: ip`, 10/s) already bound on the `api-control-plane` Ingress.
- Produces: Kong resolves the real client IP from `CF-Connecting-IP` (all `limit_by: ip` plugins become genuinely per-client — prerequisite for T4/T5 plugins `rl-events`, `rl-button`, `rl-idp`); NetworkPolicy `kong-proxy-only-cloudflared` in ns `kong` so only cloudflared pods can reach the proxy port (header-forgery guard).

Preconditions: `k8s-tunnel` running (fish function, see `iac/docs/runbooks/remote-access.md`), so `kubectl --context algovn-remote` works. All iac work happens on `main` in `/Users/duclm27/the-algovn/iac` — run `git pull --ff-only` first (argocd-image-updater pushes to this repo).

- [ ] **Step 1: Capture the bug baseline (client IP seen by Kong = cloudflared pod IP)**

```bash
curl -s -o /dev/null https://api.algovn.com/healthz
kubectl --context algovn-remote -n kong logs deploy/kong-gateway -c proxy --tail=3
```

Expected: an access-log line for `GET /healthz` whose first field is a `10.42.x.y` address (the cloudflared pod) — NOT your public IP. This is the bug: every `limit_by: ip` bucket collapses onto that one IP. Save one log line for the task report.

- [ ] **Step 2: Confirm the pod CIDR (read-only)**

```bash
kubectl --context algovn-remote get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.spec.podCIDR}{"\n"}{end}'
```

Expected output:

```
algovn 10.42.0.0/24
algovn-w1 10.42.1.0/24
```

Both node podCIDRs sit inside `10.42.0.0/16` (k3s default cluster-cidr; `ansible/roles/k3s_server/templates/config.yaml.j2` does not override it). `10.42.0.0/16` is the value for `trusted_ips` below. If the printed CIDRs are NOT under 10.42.0.0/16, stop and use the enclosing /16 actually printed.

- [ ] **Step 3: Add real-IP settings to the Kong gateway env**

Edit `/Users/duclm27/the-algovn/iac/platform/kong/values.yaml` — replace the `env:` block under `gateway:` with:

```yaml
  env:
    database: "off"
    ssl_cert: /etc/secrets/wildcard-algovn-tls/tls.crt
    ssl_cert_key: /etc/secrets/wildcard-algovn-tls/tls.key
    real_ip_header: CF-Connecting-IP
    real_ip_recursive: "on"
    trusted_ips: 10.42.0.0/16
```

(Keys render as `KONG_REAL_IP_HEADER`, `KONG_REAL_IP_RECURSIVE`, `KONG_TRUSTED_IPS` container env — same mechanism as the existing `database: "off"` → `KONG_DATABASE=off`. `trusted_ips` is scoped to the pod CIDR so only in-cluster callers — i.e. cloudflared, enforced exclusively by Step 4 — may assert `CF-Connecting-IP`.)

- [ ] **Step 4: Create the NetworkPolicy (only cloudflared may reach the proxy port)**

Create `/Users/duclm27/the-algovn/iac/platform/kong/manifests/networkpolicy.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kong-proxy-only-cloudflared
  namespace: kong
spec:
  # Selects the gateway pods only (controller pods share app.kubernetes.io/instance).
  podSelector:
    matchLabels: { app.kubernetes.io/name: gateway, app.kubernetes.io/instance: kong }
  policyTypes: [Ingress]
  ingress:
    # proxy ports: cloudflared pods only (CF-Connecting-IP forgery guard)
    - from:
        - namespaceSelector:
            matchLabels: { kubernetes.io/metadata.name: cloudflared }
          podSelector:
            matchLabels: { app: cloudflared }
      ports:
        - { port: 8000, protocol: TCP }
        - { port: 8443, protocol: TCP }
    # LAN direct path (documented in the Kong spec: node IP :80/:443 via svclb).
    # Safe to allow: trusted_ips only lists the pod CIDR, so a LAN client's
    # CF-Connecting-IP header is IGNORED by realip — no forgery risk from here.
    # kube-system covers svclb hairpin pods whose source may be SNAT'd.
    - from:
        - ipBlock: { cidr: 192.168.102.0/24 }
        - namespaceSelector:
            matchLabels: { kubernetes.io/metadata.name: kube-system }
      ports:
        - { port: 8000, protocol: TCP }
        - { port: 8443, protocol: TCP }
    # status (kubelet probes + VMPodScrape metrics) and admin (KIC gatewayDiscovery):
    # unrestricted — a from-less rule allows all sources on these ports only
    - ports:
        - { port: 8100, protocol: TCP }
        - { port: 8444, protocol: TCP }
```

Edit `/Users/duclm27/the-algovn/iac/platform/kong/manifests/kustomization.yaml` to:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - certificate.yaml
  - prometheus-plugin.yaml
  - podscrape.yaml
  - jwt-auth-plugin.yaml
  - zitadel-jwt-3815696693.yaml
  - zitadel-issuer-consumer.yaml
  - networkpolicy.yaml
```

- [ ] **Step 5: Render-check the chart values locally**

```bash
cd /Users/duclm27/the-algovn/iac
helm repo add kong https://charts.konghq.com >/dev/null 2>&1 || true
helm repo update kong >/dev/null
helm template kong kong/ingress --version 0.24.0 -n kong -f platform/kong/values.yaml \
  | grep -E 'KONG_REAL_IP_HEADER|KONG_REAL_IP_RECURSIVE|KONG_TRUSTED_IPS' -A1
```

Expected: three env entries rendered on the gateway Deployment — `KONG_REAL_IP_HEADER` value `CF-Connecting-IP`, `KONG_REAL_IP_RECURSIVE` value `on`, `KONG_TRUSTED_IPS` value `10.42.0.0/16`. If grep prints nothing, the values keys are wrong — stop and fix before committing.

- [ ] **Step 6: Validate and commit (independent, revertable)**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
```

Expected: ends with `PASS`.

```bash
git add platform/kong/values.yaml platform/kong/manifests/networkpolicy.yaml platform/kong/manifests/kustomization.yaml
git diff --cached --stat   # exactly 3 files
git commit -m "Fix Kong client IP via CF-Connecting-IP and gate proxy to cloudflared"
git push origin main
```

- [ ] **Step 7: Wait for Argo sync + gateway rollout**

```bash
kubectl --context algovn-remote -n argocd annotate application root argocd.argoproj.io/refresh=normal --overwrite
kubectl --context algovn-remote -n argocd annotate application kong argocd.argoproj.io/refresh=normal --overwrite
bash -c 'for i in {1..30}; do s=$(kubectl --context algovn-remote -n argocd get application kong -o jsonpath="{.status.sync.status} {.status.health.status}"); echo "$s"; [ "$s" = "Synced Healthy" ] && break; sleep 10; done'
kubectl --context algovn-remote -n kong rollout status deploy/kong-gateway --timeout=180s
```

Expected: `Synced Healthy`, then `deployment "kong-gateway" successfully rolled out`. Note: the gateway env change restarts the single Kong replica — a few seconds of edge downtime is expected and acceptable.

```bash
kubectl --context algovn-remote -n kong get networkpolicy kong-proxy-only-cloudflared
```

Expected: one row, `POD-SELECTOR: app.kubernetes.io/instance=kong,app.kubernetes.io/name=gateway`.

- [ ] **Step 8: VERIFY — Kong now logs the real client IP**

```bash
curl -s https://ifconfig.me; echo
curl -s -o /dev/null https://api.algovn.com/healthz
kubectl --context algovn-remote -n kong logs deploy/kong-gateway -c proxy --tail=3
```

Expected: the `GET /healthz` access-log line now starts with YOUR public IP (the ifconfig.me value), not `10.42.x.y`. This is the acceptance evidence that `limit_by: ip` buckets are per real client.

- [ ] **Step 9: VERIFY — burst from one IP still 429s at >10/s**

```bash
bash -c 'for i in {1..30}; do curl -s -o /dev/null -w "%{http_code}\n" https://api.algovn.com/healthz; done | sort | uniq -c'
```

Expected: BOTH `200` and `429` lines present, e.g. `  14 200` / `  16 429` (the 30 requests span ~1–3s; the `api-rate-limit` plugin allows 10/s per IP). Also confirm the limit headers:

```bash
curl -s -o /dev/null -D - https://api.algovn.com/healthz | grep -i ratelimit
```

Expected: `x-ratelimit-limit-second: 10` (and remaining/reset headers).

Second-origin check (OPTIONAL, only if a second egress is available — phone hotspot or VPN): while the local IP is exhausted (run the burst loop in one terminal), from the second origin run `curl -s -o /dev/null -w '%{http_code}\n' https://api.algovn.com/healthz` — expected `200`, proving buckets are per-IP. If no second egress exists, Step 8's log assertion is the required evidence and this step is skipped.

- [ ] **Step 10: VERIFY — NetworkPolicy blocks non-cloudflared pods from the proxy port**

```bash
kubectl --context algovn-remote run np-test --rm -i --restart=Never --image=curlimages/curl -- \
  curl -m 5 -s -o /dev/null -w '%{http_code}\n' http://kong-gateway-proxy.kong.svc.cluster.local/healthz
```

Expected: `000` and a non-zero pod exit (curl timeout — connection blocked). Then confirm the legitimate path still works: `curl -s -o /dev/null -w '%{http_code}\n' https://api.algovn.com/healthz` → `200`.

**Rollback:** this task is one commit. `cd /Users/duclm27/the-algovn/iac && git revert <sha> && git push origin main` — Argo (prune+selfHeal) restores the old env and deletes the NetworkPolicy within one sync. Known intentional side effect while deployed: direct LAN access to the Kong LoadBalancer (`192.168.102.200/201:80/443`) is cut; all traffic must come via Cloudflare → cloudflared. Nothing in the platform uses the LAN path (remote access uses host tunnels; browsers go via Cloudflare).

---

### Task 2: Redis platform component

**Files:**
- Create: `/Users/duclm27/the-algovn/iac/platform/redis/manifests/kustomization.yaml`
- Create: `/Users/duclm27/the-algovn/iac/platform/redis/manifests/redis-auth-sealed.yaml`
- Create: `/Users/duclm27/the-algovn/iac/platform/redis/manifests/statefulset.yaml`
- Create: `/Users/duclm27/the-algovn/iac/platform/redis/manifests/service.yaml`
- Create: `/Users/duclm27/the-algovn/iac/platform/redis/manifests/vmservicescrape.yaml`
- Create: `/Users/duclm27/the-algovn/iac/clusters/algovn/platform/redis.yaml`

**Interfaces:**
- Consumes: platform/rabbitmq as the structural model (StatefulSet + PVC + sealed secret + VMServiceScrape + Application), sealed-secrets controller, local-path StorageClass.
- Produces (frozen): ns `redis`, Service `redis.redis.svc.cluster.local:6379` (labels `app: redis`), image `docker.io/redis:7.4-alpine`, `--requirepass` from sealed secret `redis-auth` key `password`, `--appendonly yes --appendfsync everysec`, PVC 2Gi local-path, redis_exporter sidecar `:9121`, VMServiceScrape `redis` in ns `monitoring`, password-manager entry `redis-algovn`. Consumed later by T7 (`REDIS_URL`) and T18 (`redis-creds` in ns `the-button`).

- [ ] **Step 1: Pin the redis_exporter version**

```bash
curl -s https://api.github.com/repos/oliver006/redis_exporter/releases/latest | jq -r .tag_name
```

Expected: a tag like `v1.66.0`. Substitute the printed tag everywhere `v1.66.0` appears below (never write `latest` into git).

- [ ] **Step 2: Generate the password and seal it [NEEDS USER]**

```bash
mkdir -p ~/.secrets && chmod 700 ~/.secrets
openssl rand -base64 24 | tr -d '\n' > ~/.secrets/redis-pass
cat ~/.secrets/redis-pass; echo
```

**[NEEDS USER]** Store the printed password in the password manager as entry **`redis-algovn`** (frozen name). Wait for the user to confirm before continuing.

```bash
mkdir -p /Users/duclm27/the-algovn/iac/platform/redis/manifests
kubectl create secret generic redis-auth -n redis --from-file=password="$HOME/.secrets/redis-pass" --dry-run=client -o yaml \
  | kubeseal --context algovn-remote --controller-name sealed-secrets --controller-namespace sealed-secrets --format yaml \
  > /Users/duclm27/the-algovn/iac/platform/redis/manifests/redis-auth-sealed.yaml
rm ~/.secrets/redis-pass
grep -c encryptedData /Users/duclm27/the-algovn/iac/platform/redis/manifests/redis-auth-sealed.yaml
```

Expected: file contains `kind: SealedSecret`, name `redis-auth`, namespace `redis`, one `encryptedData.password` entry; grep prints `1`. (Requires the k8s-tunnel to be up — kubeseal fetches the controller cert through `--context algovn-remote`.)

- [ ] **Step 3: Write the StatefulSet**

Create `/Users/duclm27/the-algovn/iac/platform/redis/manifests/statefulset.yaml`:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
  namespace: redis
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels: { app: redis }
  template:
    metadata:
      labels: { app: redis }
    spec:
      nodeSelector: { kubernetes.io/arch: amd64 }   # spec §11.1: single pod on w1; local-path PVC pins to first node
      containers:
        - name: redis
          image: docker.io/redis:7.4-alpine
          args:
            - --requirepass
            - $(REDIS_PASSWORD)
            - --appendonly
            - "yes"
            - --appendfsync
            - everysec
          ports:
            - { containerPort: 6379, name: redis }
          env:
            - name: REDIS_PASSWORD
              valueFrom: { secretKeyRef: { name: redis-auth, key: password } }
          readinessProbe:
            exec: { command: [sh, -c, 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning ping | grep -q PONG'] }
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 5
          livenessProbe:
            exec: { command: [sh, -c, 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning ping | grep -q PONG'] }
            initialDelaySeconds: 15
            periodSeconds: 30
            timeoutSeconds: 5
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { memory: 512Mi }
          volumeMounts:
            - { name: data, mountPath: /data }
        - name: exporter
          image: docker.io/oliver006/redis_exporter:v1.66.0
          ports:
            - { containerPort: 9121, name: metrics }
          env:
            - { name: REDIS_ADDR, value: "redis://localhost:6379" }
            - name: REDIS_PASSWORD
              valueFrom: { secretKeyRef: { name: redis-auth, key: password } }
          resources:
            requests: { cpu: 25m, memory: 32Mi }
            limits: { memory: 64Mi }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: local-path
        resources: { requests: { storage: 2Gi } }
```

(Notes: `$(REDIS_PASSWORD)` in `args` is kubernetes env interpolation — no shell involved. RDB snapshots stay on redis defaults (`3600 1 300 100 60 10000`) alongside AOF, matching spec §2 "AOF everysec + RDB". The image tag `v1.66.0` must be the tag printed in Step 1.)

- [ ] **Step 4: Write Service, VMServiceScrape, kustomization, Application**

Create `/Users/duclm27/the-algovn/iac/platform/redis/manifests/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: redis
  labels: { app: redis }   # VMServiceScrape selects Services by THEIR labels
spec:
  selector: { app: redis }
  ports:
    - { port: 6379, targetPort: 6379, name: redis }
    - { port: 9121, targetPort: 9121, name: metrics }
```

Create `/Users/duclm27/the-algovn/iac/platform/redis/manifests/vmservicescrape.yaml`:

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMServiceScrape
metadata:
  name: redis
  namespace: monitoring
spec:
  namespaceSelector: { matchNames: [redis] }
  selector:
    matchLabels: { app: redis }
  endpoints:
    - port: metrics
```

Create `/Users/duclm27/the-algovn/iac/platform/redis/manifests/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - redis-auth-sealed.yaml
  - statefulset.yaml
  - service.yaml
  - vmservicescrape.yaml
```

Create `/Users/duclm27/the-algovn/iac/clusters/algovn/platform/redis.yaml`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: redis
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/the-algovn/iac.git
    targetRevision: main
    path: platform/redis/manifests
  destination:
    server: https://kubernetes.default.svc
    namespace: redis
  syncPolicy:
    automated: { prune: true, selfHeal: true }
    syncOptions: [CreateNamespace=true]
    retry:
      limit: 5
      backoff: { duration: 30s, factor: 2, maxDuration: 5m }
```

- [ ] **Step 5: Validate, commit, push**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
```

Expected: `PASS` (the new kustomization builds; SealedSecret/VMServiceScrape validate against the CRD catalog).

```bash
git pull --ff-only
git add platform/redis clusters/algovn/platform/redis.yaml
git diff --cached --stat   # exactly 6 files
git commit -m "Add Redis platform component with AOF persistence and exporter"
git push origin main
```

- [ ] **Step 6: Wait for sync and rollout**

```bash
kubectl --context algovn-remote -n argocd annotate application root argocd.argoproj.io/refresh=normal --overwrite
bash -c 'for i in {1..30}; do s=$(kubectl --context algovn-remote -n argocd get application redis -o jsonpath="{.status.sync.status} {.status.health.status}" 2>/dev/null); echo "${s:-waiting for app}"; [ "$s" = "Synced Healthy" ] && break; sleep 10; done'
kubectl --context algovn-remote -n redis rollout status statefulset/redis --timeout=300s
```

Expected: `Synced Healthy`, then `statefulset rolling update complete 1 pods...`. Confirm placement: `kubectl --context algovn-remote -n redis get pod redis-0 -o wide` → NODE `algovn-w1`.

- [ ] **Step 7: VERIFY — PING with auth, AOF on, exporter up**

```bash
kubectl --context algovn-remote -n redis exec redis-0 -c redis -- sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning ping'
kubectl --context algovn-remote -n redis exec redis-0 -c redis -- sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning config get appendonly'
kubectl --context algovn-remote -n redis exec redis-0 -c redis -- sh -c 'redis-cli ping'
```

Expected: `PONG`; then `appendonly` / `yes`; the third (unauthenticated) prints `NOAUTH Authentication required.` — proving the password is enforced.

```bash
kubectl --context algovn-remote -n redis port-forward svc/redis 9121:9121 >/dev/null 2>&1 &
PF=$!; sleep 2
curl -s localhost:9121/metrics | grep '^redis_up'
kill $PF
```

Expected: `redis_up 1`.

**Rollback:** revert the commit; Argo prunes the app. The PVC survives prune (StatefulSet PVCs are not garbage-collected) — delete manually only if abandoning Redis for good: `kubectl --context algovn-remote -n redis delete pvc data-redis-0`.

---

### Task 3: api-control-plane code changes (429/409/400 mapping, Retry-After, SSE retry + cap, CORS apex)

**Files:**
- Edit: `/Users/duclm27/the-algovn/api-control-plane/internal/transcode/invoke.go`
- Edit: `/Users/duclm27/the-algovn/api-control-plane/internal/transcode/invoke_test.go`
- Edit: `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server.go`
- Edit: `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server_test.go`
- Edit: `/Users/duclm27/the-algovn/api-control-plane/cmd/api-control-plane/main.go`

**Interfaces:**
- Consumes: existing `HTTPStatus` switch, `handleAPI`/`writeError`, `handleSSE`, test fixture in `server_test.go` (testsvc `Fail` RPC returns any gRPC code: code 8 = ResourceExhausted, 6 = AlreadyExists, 9 = FailedPrecondition).
- Produces (frozen): `ResourceExhausted→429`, `AlreadyExists→409`, `FailedPrecondition→400`; `Retry-After: 2` header on 429; SSE stream opens with `retry: 3000\n\n` before the first event; global SSE cap env `SSE_MAX_CONNS` (default `15000`) → 503 when exceeded; CORS default gains `,https://algovn.com`. ADDED interface: exported field `Server.SSEMaxConns int` (0 = unlimited) wired from the env in `main.go`.

All commands run from `/Users/duclm27/the-algovn/api-control-plane`, on `main` (this repo pushes directly to main; CI gates the image build).

- [ ] **Step 1: RED — extend TestHTTPStatus with the three new codes**

In `/Users/duclm27/the-algovn/api-control-plane/internal/transcode/invoke_test.go`, replace the `cases` map in `TestHTTPStatus` with:

```go
	cases := map[codes.Code]int{
		codes.NotFound:           404,
		codes.PermissionDenied:   403,
		codes.InvalidArgument:    400,
		codes.FailedPrecondition: 400,
		codes.AlreadyExists:      409,
		codes.ResourceExhausted:  429,
		codes.Unavailable:        502,
		codes.DeadlineExceeded:   504,
		codes.Unauthenticated:    401,
		codes.Internal:           500,
		codes.Unknown:            500,
	}
```

```bash
go test ./internal/transcode/ -run TestHTTPStatus
```

Expected FAIL — three `Not equal` assertions (e.g. `ResourceExhausted: expected: 429, actual: 500`).

- [ ] **Step 2: GREEN — add the mappings, commit**

In `/Users/duclm27/the-algovn/api-control-plane/internal/transcode/invoke.go`, inside `HTTPStatus`, insert after the `case codes.NotFound:` block:

```go
	case codes.FailedPrecondition:
		return 400
	case codes.AlreadyExists:
		return 409
	case codes.ResourceExhausted:
		return 429
```

```bash
go test ./internal/transcode/ -run TestHTTPStatus
```

Expected: `ok  	github.com/the-algovn/api-control-plane/internal/transcode`.

```bash
git add internal/transcode/invoke.go internal/transcode/invoke_test.go
git commit -m "Map ResourceExhausted, AlreadyExists and FailedPrecondition to HTTP"
```

- [ ] **Step 3: RED — Retry-After header on 429**

Append to `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server_test.go` (after `TestTranscodeRoutes`):

```go
func TestRetryAfterOn429(t *testing.T) {
	f := newFixture(t, true)
	// testsvc Fail returns the given gRPC code: 8 = ResourceExhausted -> 429.
	resp := do(t, "POST", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Fail", f.token(t), `{"code":8,"message":"slow down"}`)
	require.Equal(t, 429, resp.StatusCode)
	require.Equal(t, "2", resp.Header.Get("Retry-After"))
}
```

```bash
go test ./internal/httpserver/ -run TestRetryAfterOn429
```

Expected FAIL: `Not equal: expected: "2", actual: ""` (status is already 429 thanks to Step 2; the header is missing).

- [ ] **Step 4: GREEN — set the header in handleAPI, commit**

In `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server.go`, in `handleAPI`, replace:

```go
		st := status.Convert(err)
		httpCode := transcode.HTTPStatus(st.Code())
		s.count(route, httpCode)
		writeError(w, httpCode, st.Code().String(), st.Message())
		return
```

with:

```go
		st := status.Convert(err)
		httpCode := transcode.HTTPStatus(st.Code())
		s.count(route, httpCode)
		if httpCode == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "2") // PoW token stays valid; client backs off
		}
		writeError(w, httpCode, st.Code().String(), st.Message())
		return
```

```bash
go test ./internal/httpserver/ -run TestRetryAfterOn429
git add internal/httpserver/server.go internal/httpserver/server_test.go
git commit -m "Send Retry-After header on 429 responses"
```

Expected: test `ok` before committing.

- [ ] **Step 5: RED — SSE stream must open with a retry hint**

In `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server_test.go`, in `TestSSE`, insert one line before `waitFor(`data: {"total":1}`)`:

```go
	waitFor("retry: 3000") // reconnect hint precedes any event
	waitFor(`data: {"total":1}`)
```

```bash
go test ./internal/httpserver/ -run 'TestSSE$'
```

Expected FAIL: `SSE line "retry: 3000" not received` (waitFor hits its 5s deadline — the `data:` line for total:1 was consumed while scanning).

- [ ] **Step 6: GREEN — write the retry field before the first flush, commit**

In `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server.go`, in `handleSSE`, replace:

```go
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
```

with:

```go
	w.WriteHeader(http.StatusOK)
	// Base reconnect delay for EventSource; the SPA layers full jitter (0-5s)
	// on top so a rolling deploy doesn't cause a reconnect stampede.
	_, _ = io.WriteString(w, "retry: 3000\n\n")
	flusher.Flush()
```

```bash
go test ./internal/httpserver/ -run 'TestSSE$'
git add internal/httpserver/server.go internal/httpserver/server_test.go
git commit -m "Send SSE retry hint before the first event"
```

Expected: test `ok` before committing.

- [ ] **Step 7: RED — global SSE connection cap**

In `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server_test.go`:

(a) add a `server` field so tests can set the cap — replace the `fixture` struct and the `return` in `newFixture` with:

```go
type fixture struct {
	srv     *httptest.Server
	jwks    *authtest.JWKS
	hub     *push.Hub
	metrics *observability.Metrics
	server  *Server
}
```

and (last line of `newFixture`):

```go
	return &fixture{srv: srv, jwks: jwks, hub: hub, metrics: metrics, server: s}
```

(b) append after `TestSSE_RabbitDown`:

```go
func TestSSECap(t *testing.T) {
	f := newFixture(t, true)
	f.server.SSEMaxConns = 2
	open := func() *http.Response {
		req, err := http.NewRequest("GET", f.srv.URL+"/events/test.events", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	require.Equal(t, 200, open().StatusCode) // held open by t.Cleanup
	require.Equal(t, 200, open().StatusCode)
	require.Equal(t, 503, open().StatusCode) // third connection over the cap
}
```

```bash
go test ./internal/httpserver/ -run TestSSECap
```

Expected FAIL to compile: `f.server.SSEMaxConns undefined (type *Server has no field or method SSEMaxConns)`.

- [ ] **Step 8: GREEN — implement the cap and wire SSE_MAX_CONNS in main, commit**

In `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server.go`:

(a) add `"sync/atomic"` to the imports (stdlib group, after `"strings"`... keep gofmt order: it sorts after `"strconv"`/`"strings"`).

(b) replace the `Server` struct with:

```go
type Server struct {
	Store           *config.Store
	Verifier        *auth.Verifier
	Backends        *transcode.Registry
	Hub             *push.Hub
	RabbitConnected func() bool
	CORSOrigins     []string
	SSEMaxConns     int // global cap on concurrent SSE connections; 0 = unlimited
	Logger          *slog.Logger
	Metrics         *observability.Metrics

	sseConns atomic.Int64
}
```

(c) in `handleSSE`, insert right after the `RabbitConnected` 503 block (before the `flusher` check):

```go
	if s.SSEMaxConns > 0 {
		if n := s.sseConns.Add(1); n > int64(s.SSEMaxConns) {
			s.sseConns.Add(-1)
			writeError(w, 503, "unavailable", "SSE connection limit reached, retry later")
			return
		}
		defer s.sseConns.Add(-1)
	}
```

In `/Users/duclm27/the-algovn/api-control-plane/cmd/api-control-plane/main.go`:

(d) add `"strconv"` to the imports.

(e) after the `corsOrigins := ...` line add:

```go
	sseMaxConns, err := strconv.Atoi(env("SSE_MAX_CONNS", "15000"))
	if err != nil || sseMaxConns < 1 {
		logger.Error("SSE_MAX_CONNS must be a positive integer", "value", env("SSE_MAX_CONNS", "15000"))
		os.Exit(1)
	}
```

(Note: the later `store, err := config.NewStore(regDir)` still compiles — `store` is new, so `:=` is legal.)

(f) add the field to the `httpserver.Server` literal:

```go
	srv := &httpserver.Server{
		Store: store, Verifier: verifier, Backends: backends, Hub: hub,
		RabbitConnected: rabbitConnected, CORSOrigins: corsOrigins,
		SSEMaxConns: sseMaxConns,
		Logger:      logger, Metrics: metrics,
	}
```

```bash
gofmt -l . && go vet ./... && go test ./internal/httpserver/ -run 'TestSSECap|TestSSE$|TestSSE_RabbitDown'
git add internal/httpserver/server.go internal/httpserver/server_test.go cmd/api-control-plane/main.go
git commit -m "Cap concurrent SSE connections via SSE_MAX_CONNS"
```

Expected: gofmt prints nothing, vet clean, all three tests pass before committing.

- [ ] **Step 9: RED — apex origin allowed by CORS**

In `/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/server_test.go`, append to the END of `TestCORS`:

```go
	// apex origin (the-button SPA at https://algovn.com) needs an exact entry:
	// the *.algovn.com wildcard deliberately does not match the apex.
	req3, _ := http.NewRequest("OPTIONS", f.srv.URL+"/test/algovn.testsvc.v1.TestService/Echo", nil)
	req3.Header.Set("Origin", "https://algovn.com")
	req3.Header.Set("Access-Control-Request-Method", "POST")
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	require.Equal(t, 204, resp3.StatusCode)
	require.Equal(t, "https://algovn.com", resp3.Header.Get("Access-Control-Allow-Origin"))
```

```bash
go test ./internal/httpserver/ -run TestCORS
```

Expected FAIL: `Not equal: expected: "https://algovn.com", actual: ""` (fixture allowlist lacks the apex).

- [ ] **Step 10: GREEN — add apex to the fixture and to the main.go default, commit**

(a) in `newFixture` (server_test.go), change:

```go
		CORSOrigins:     []string{"https://*.algovn.com", "https://algovn.com"},
```

(b) in `/Users/duclm27/the-algovn/api-control-plane/cmd/api-control-plane/main.go`, change the default:

```go
	corsOrigins := strings.Split(env("CORS_ORIGINS", "https://*.algovn.com,https://algovn.com"), ",")
```

(c) browsers cannot read `Retry-After` cross-origin unless it is exposed — in
`/Users/duclm27/the-algovn/api-control-plane/internal/httpserver/middleware.go`,
inside `corsMiddleware`'s origin-matched block (next to the other
`Access-Control-*` headers), add:

```go
			w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
```

and extend the Step 9 test with one assertion on the preflight response:

```go
	require.Equal(t, "Retry-After", resp3.Header.Get("Access-Control-Expose-Headers"))
```

```bash
go test ./internal/httpserver/ -run TestCORS
git add internal/httpserver/server_test.go cmd/api-control-plane/main.go internal/httpserver/middleware.go
git commit -m "Allow the algovn.com apex origin and expose Retry-After in CORS"
```

Expected: test `ok` before committing.

- [ ] **Step 11: Full suite, push, watch CI**

```bash
go vet ./... && go test ./... -race
git push origin main
sleep 10
gh run watch --exit-status $(gh run list --branch main --limit 1 --json databaseId -q '.[0].databaseId')
```

Expected: all packages `ok`; `gh run watch` ends with the `build` workflow green (`test` job gates `build`). The pushed image `ghcr.io/the-algovn/api-control-plane:main` gets picked up by argocd-image-updater automatically (digest write-back to the iac repo) — T4 verifies the new behavior end-to-end; if the SSE `retry: 3000` line is not yet visible there, the updater simply hasn't cycled yet.

**Rollback:** `git revert` the offending commit(s) and push; CI rebuilds and the image updater rolls the deployment back to the reverted binary.

---

### Task 4: Edge scaling iac (ingress split ×3, acp ×2, cloudflared ×2, Kong tuning)

**Files:**
- Create: `/Users/duclm27/the-algovn/iac/apps/api-control-plane/rl-events-plugin.yaml`
- Create: `/Users/duclm27/the-algovn/iac/apps/api-control-plane/rl-button-plugin.yaml`
- Create: `/Users/duclm27/the-algovn/iac/apps/api-control-plane/ingress-events.yaml`
- Create: `/Users/duclm27/the-algovn/iac/apps/api-control-plane/ingress-the-button.yaml`
- Edit: `/Users/duclm27/the-algovn/iac/apps/api-control-plane/deployment.yaml`
- Edit: `/Users/duclm27/the-algovn/iac/apps/api-control-plane/kustomization.yaml`
- Edit: `/Users/duclm27/the-algovn/iac/platform/cloudflared/deployment.yaml`
- Edit: `/Users/duclm27/the-algovn/iac/platform/kong/values.yaml`

**Interfaces:**
- Consumes: T1 (real client IPs — makes `limit_by: ip` meaningful), T3 image (SSE retry/cap + CORS apex, arrives via image updater), existing Ingress `api-control-plane` + KongPlugin `api-rate-limit`.
- Produces (frozen): 3 Ingresses on host `api.algovn.com` — `api-events` path `/events` (KongPlugin `rl-events` second:50 minute:1000), `api-the-button` path `/the-button` (KongPlugin `rl-button` second:20 minute:600), existing `api-control-plane` path `/` keeps `api-rate-limit` 10/300; acp replicas 2, 256Mi/1Gi, `GOMEMLIMIT=900MiB`, `SSE_MAX_CONNS=15000`, `CORS_ORIGINS=https://*.algovn.com,https://algovn.com`; cloudflared replicas 2, 512Mi, amd64 nodeSelector; Kong `KONG_NGINX_EVENTS_WORKER_CONNECTIONS=32768`, `KONG_NGINX_MAIN_WORKER_RLIMIT_NOFILE=65536`, memory 2Gi.

- [ ] **Step 1: Pull first (image updater commits digests to this repo)**

```bash
cd /Users/duclm27/the-algovn/iac && git pull --ff-only && git log --oneline -2
```

Expected: fast-forward (or already up to date). Do NOT touch the `images:` digest block in `apps/api-control-plane/kustomization.yaml` — argocd-image-updater owns it.

- [ ] **Step 2: Create the two KongPlugins**

Create `/Users/duclm27/the-algovn/iac/apps/api-control-plane/rl-events-plugin.yaml`:

```yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: rl-events
  namespace: api-control-plane
plugin: rate-limiting
config:
  second: 50
  minute: 1000
  policy: local
  limit_by: ip
```

Create `/Users/duclm27/the-algovn/iac/apps/api-control-plane/rl-button-plugin.yaml`:

```yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: rl-button
  namespace: api-control-plane
plugin: rate-limiting
config:
  second: 20
  minute: 600
  policy: local
  limit_by: ip
```

- [ ] **Step 3: Create the two new Ingresses (existing `ingress.yaml` stays untouched)**

Create `/Users/duclm27/the-algovn/iac/apps/api-control-plane/ingress-events.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api-events
  namespace: api-control-plane
  annotations:
    konghq.com/response-buffering: "false"
    konghq.com/plugins: rl-events
spec:
  ingressClassName: kong
  rules:
    - host: api.algovn.com
      http:
        paths:
          - path: /events
            pathType: Prefix
            backend:
              service:
                name: api-control-plane
                port: { number: 80 }
  tls:
    - hosts: [api.algovn.com]
```

Create `/Users/duclm27/the-algovn/iac/apps/api-control-plane/ingress-the-button.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api-the-button
  namespace: api-control-plane
  annotations:
    konghq.com/response-buffering: "false"
    konghq.com/plugins: rl-button
spec:
  ingressClassName: kong
  rules:
    - host: api.algovn.com
      http:
        paths:
          - path: /the-button
            pathType: Prefix
            backend:
              service:
                name: api-control-plane
                port: { number: 80 }
  tls:
    - hosts: [api.algovn.com]
```

(Kong routes by longest path prefix, so `/events*` and `/the-button*` peel off from `/`; KIC's default `strip_path=false` means acp still sees the full path — same as today. `/the-button` will 404 at acp until the T18 registration lands; the rate-limit tier is what matters now.)

- [ ] **Step 4: Scale the acp Deployment**

Replace `/Users/duclm27/the-algovn/iac/apps/api-control-plane/deployment.yaml` with:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-control-plane
  namespace: api-control-plane
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  selector:
    matchLabels: { app: api-control-plane }
  template:
    metadata:
      labels: { app: api-control-plane }
    spec:
      containers:
        - name: api-control-plane
          image: ghcr.io/the-algovn/api-control-plane:main
          ports:
            - { containerPort: 8080, name: http }
            - { containerPort: 9091, name: metrics }
          env:
            - { name: REGISTRATIONS_DIR, value: /etc/api-registrations }
            - { name: ISSUER, value: "https://id.algovn.com" }
            - { name: CORS_ORIGINS, value: "https://*.algovn.com,https://algovn.com" }
            - { name: GOMEMLIMIT, value: 900MiB }
            - { name: SSE_MAX_CONNS, value: "15000" }
            - name: AMQP_URL
              valueFrom: { secretKeyRef: { name: amqp-creds, key: url } }
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 10
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 20
          resources:
            requests: { cpu: 100m, memory: 256Mi }
            limits: { memory: 1Gi }
          volumeMounts:
            - { name: registrations, mountPath: /etc/api-registrations, readOnly: true }
      volumes:
        - name: registrations
          configMap: { name: api-registrations }
      imagePullSecrets:
        - name: registry-creds
```

Edit `/Users/duclm27/the-algovn/iac/apps/api-control-plane/kustomization.yaml` — replace the `resources:` list only (leave `configMapGenerator`, `generatorOptions` and the updater-owned `images:` block exactly as-is):

```yaml
resources:
  - namespace.yaml
  - amqp-creds-sealed.yaml
  - registry-creds-sealed.yaml
  - deployment.yaml
  - service.yaml
  - ingress.yaml
  - ingress-events.yaml
  - ingress-the-button.yaml
  - vmservicescrape.yaml
  - rate-limit-plugin.yaml
  - rl-events-plugin.yaml
  - rl-button-plugin.yaml
```

- [ ] **Step 5: Scale cloudflared**

Replace `/Users/duclm27/the-algovn/iac/platform/cloudflared/deployment.yaml` with:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cloudflared
  namespace: cloudflared
spec:
  replicas: 2
  selector:
    matchLabels: { app: cloudflared }
  template:
    metadata:
      labels: { app: cloudflared }
    spec:
      nodeSelector: { kubernetes.io/arch: amd64 }
      containers:
        - name: cloudflared
          image: docker.io/cloudflare/cloudflared:2026.7.1
          args: [tunnel, --config, /etc/cloudflared/config/config.yaml, run]
          livenessProbe:
            httpGet: { path: /ready, port: 2000 }
            initialDelaySeconds: 10
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { memory: 512Mi }
          volumeMounts:
            - { name: config, mountPath: /etc/cloudflared/config, readOnly: true }
            - { name: creds, mountPath: /etc/cloudflared/creds, readOnly: true }
      volumes:
        - name: config
          configMap: { name: cloudflared-config }
        - name: creds
          secret: { secretName: tunnel-credentials }
```

(Two replicas of the same named tunnel `algovn-k8s` is Cloudflare-supported HA — each replica registers its own tunnel connections. Both land on w1 via the amd64 nodeSelector, per spec §11.5.)

- [ ] **Step 6: Tune Kong for 10k connections**

Edit `/Users/duclm27/the-algovn/iac/platform/kong/values.yaml` — the `gateway.env` block gains two keys and the gateway memory limit becomes 2Gi. Final `gateway.env` + `gateway.resources`:

```yaml
  env:
    database: "off"
    ssl_cert: /etc/secrets/wildcard-algovn-tls/tls.crt
    ssl_cert_key: /etc/secrets/wildcard-algovn-tls/tls.key
    real_ip_header: CF-Connecting-IP
    real_ip_recursive: "on"
    trusted_ips: 10.42.0.0/16
    nginx_events_worker_connections: "32768"
    nginx_main_worker_rlimit_nofile: "65536"
  resources:
    requests: { cpu: 150m, memory: 256Mi }
    limits: { memory: 2Gi }
```

Render-check:

```bash
cd /Users/duclm27/the-algovn/iac
helm template kong kong/ingress --version 0.24.0 -n kong -f platform/kong/values.yaml \
  | grep -E 'KONG_NGINX_EVENTS_WORKER_CONNECTIONS|KONG_NGINX_MAIN_WORKER_RLIMIT_NOFILE' -A1
```

Expected: both env vars rendered with values `32768` and `65536`.

- [ ] **Step 7: Validate, commit (three focused commits), push**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
```

Expected: `PASS`.

```bash
git add apps/api-control-plane/rl-events-plugin.yaml apps/api-control-plane/rl-button-plugin.yaml \
        apps/api-control-plane/ingress-events.yaml apps/api-control-plane/ingress-the-button.yaml \
        apps/api-control-plane/kustomization.yaml
git commit -m "Split api.algovn.com into per-route rate-limit tiers"

git add apps/api-control-plane/deployment.yaml
git commit -m "Scale api-control-plane to two replicas with SSE headroom"

git add platform/cloudflared/deployment.yaml platform/kong/values.yaml
git commit -m "Scale cloudflared and Kong for 10k concurrent connections"

git push origin main
```

- [ ] **Step 8: Wait for syncs and rollouts**

```bash
for app in api-control-plane cloudflared kong; do
  kubectl --context algovn-remote -n argocd annotate application $app argocd.argoproj.io/refresh=normal --overwrite
done
bash -c 'for i in {1..30}; do
  ok=1
  for app in api-control-plane cloudflared kong; do
    s=$(kubectl --context algovn-remote -n argocd get application $app -o jsonpath="{.status.sync.status} {.status.health.status}")
    echo "$app: $s"; [ "$s" = "Synced Healthy" ] || ok=0
  done
  [ $ok = 1 ] && break; sleep 10
done'
kubectl --context algovn-remote -n api-control-plane rollout status deploy/api-control-plane --timeout=180s
kubectl --context algovn-remote -n cloudflared rollout status deploy/cloudflared --timeout=180s
kubectl --context algovn-remote -n kong rollout status deploy/kong-gateway --timeout=180s
```

Expected: all three `Synced Healthy` and `successfully rolled out`. (Kong restarts once more — brief edge blip, acceptable.)

- [ ] **Step 9: VERIFY — both acp pods serve; env applied**

```bash
kubectl --context algovn-remote -n api-control-plane get pods -l app=api-control-plane -o wide
kubectl --context algovn-remote -n api-control-plane get endpoints api-control-plane
kubectl --context algovn-remote -n api-control-plane get deploy api-control-plane \
  -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}'
bash -c 'for i in {1..6}; do curl -s https://api.algovn.com/demo/algovn.demo.v1.DemoService/Ping -H "content-type: application/json" -d "{\"message\":\"scale\"}"; echo; done'
```

Expected: 2 pods Running; endpoints show TWO `ip:8080` addresses (readiness passing on both = both serve); env list shows `CORS_ORIGINS=https://*.algovn.com,https://algovn.com`, `GOMEMLIMIT=900MiB`, `SSE_MAX_CONNS=15000`; six `{"message":"pong: scale"}` responses (load-balanced across both pods).

- [ ] **Step 10: VERIFY — per-route rate-limit tiers + SSE retry hint end-to-end**

```bash
curl -s -o /dev/null -D - https://api.algovn.com/healthz | grep -i 'x-ratelimit-limit-second'
curl -s -o /dev/null -D - -m 3 https://api.algovn.com/events/demo.ping | grep -i 'x-ratelimit-limit-second' || true
curl -s -o /dev/null -D - -X POST https://api.algovn.com/the-button/x.Y/Z -d '{}' | grep -iE 'x-ratelimit-limit-second|^HTTP'
curl -sN -m 3 https://api.algovn.com/events/demo.ping | head -1 || true
```

Expected, line by line: `x-ratelimit-limit-second: 10` (default tier) · `x-ratelimit-limit-second: 50` (events tier) · `HTTP/2 404` WITH `x-ratelimit-limit-second: 20` (button tier routes + limits; 404 body is fine — no registration yet) · `retry: 3000` (T3 image live; if this last line is missing, the image updater hasn't cycled — check `kubectl --context algovn-remote -n argocd logs deploy/argocd-image-updater --tail 20` and re-run in a few minutes; do not proceed to Task 5 until it prints).

Also verify Kong picked up the nginx tuning:

```bash
kubectl --context algovn-remote -n kong exec deploy/kong-gateway -c proxy -- sh -c \
  'grep -E "worker_connections|worker_rlimit_nofile" /usr/local/kong/nginx.conf /usr/local/kong/nginx-kong.conf 2>/dev/null'
```

Expected: `worker_rlimit_nofile 65536;` and `worker_connections 32768;`.

**Rollback:** each commit reverts independently (`git revert <sha> && git push`); Argo prunes the extra Ingresses/plugins and restores previous replica counts.

---

### Task 5: Zitadel bumps, rl-idp rate limit, w1 sysctls

**Files:**
- Edit: `/Users/duclm27/the-algovn/iac/platform/zitadel/values.yaml`
- Create: `/Users/duclm27/the-algovn/iac/platform/zitadel/manifests/rl-idp-plugin.yaml`
- Edit: `/Users/duclm27/the-algovn/iac/platform/zitadel/manifests/kustomization.yaml`
- Create: `/Users/duclm27/the-algovn/iac/ansible/roles/net_tuning/tasks/main.yml`
- Create: `/Users/duclm27/the-algovn/iac/ansible/roles/net_tuning/handlers/main.yml`
- Create: `/Users/duclm27/the-algovn/iac/ansible/roles/net_tuning/files/99-net-tuning.conf`
- Edit: `/Users/duclm27/the-algovn/iac/ansible/site.yml`

**Interfaces:**
- Consumes: T1 (real client IPs make `rl-idp`'s `limit_by: ip` meaningful), zitadel Applications `zitadel` (chart 9.34.0, values file) + `zitadel-config` (path `platform/zitadel/manifests`), ansible layout (`roles/*`, handler pattern `reload sysctl`, playbook run from the Pi: `cd ~/iac/ansible && ansible-playbook site.yml`).
- Produces: Zitadel `MaxOpenConns: 20`, memory limit 2Gi (ADDED: `GOMEMLIMIT: 1800MiB` to match — leaving 900MiB would defeat the bump); KongPlugin `rl-idp` (second:10, minute:200, per IP) bound to BOTH id.algovn.com ingresses (main `/` and login `/ui/v2/login`); w1 sysctls `net.core.rmem_max=8388608`, `net.core.wmem_max=8388608` (quic-go buffers) via new ansible role `net_tuning`.

- [ ] **Step 1: Bump Zitadel values and annotate its ingresses**

Replace `/Users/duclm27/the-algovn/iac/platform/zitadel/values.yaml` with (current file, four changes: `MaxOpenConns` 10→20, limits 1Gi→2Gi, `GOMEMLIMIT` 900MiB→1800MiB, two `annotations:` lines added):

```yaml
zitadel:
  masterkeySecretName: zitadel-masterkey
  configSecretName: zitadel-config   # merges over configmapConfig; holds DB passwords + FirstInstance
  configmapConfig:
    ExternalDomain: id.algovn.com
    ExternalPort: 443
    ExternalSecure: true
    TLS:
      Enabled: false                 # TLS terminates at CF edge / Kong default cert
    Database:
      Postgres:
        Host: pg-rw.postgres.svc
        Port: 5432
        Database: zitadel
        MaxOpenConns: 20
        MaxIdleConns: 5
        User:
          Username: zitadel
          SSL: { Mode: disable }
        Admin:
          Username: zitadel
          SSL: { Mode: disable }
resources:
  requests: { cpu: 150m, memory: 384Mi }
  limits: { memory: 2Gi }
env:
  - name: GOMEMLIMIT
    value: 1800MiB
ingress:
  enabled: true
  className: kong
  annotations: { konghq.com/plugins: rl-idp }
  hosts:
    - host: id.algovn.com
      paths: [{ path: /, pathType: Prefix }]
  tls: [{ hosts: [id.algovn.com] }]
login:
  enabled: true
  resources:
    requests: { cpu: 100m, memory: 192Mi }
    limits: { memory: 512Mi }
  ingress:
    enabled: true
    className: kong
    annotations: { konghq.com/plugins: rl-idp }
    hosts:
      - host: id.algovn.com
        paths: [{ path: /ui/v2/login, pathType: Prefix }]
    tls: [{ hosts: [id.algovn.com] }]
```

- [ ] **Step 2: Render-check that BOTH ingresses carry the annotation**

```bash
cd /Users/duclm27/the-algovn/iac
helm repo add zitadel https://charts.zitadel.com >/dev/null 2>&1 || true
helm repo update zitadel >/dev/null
helm template zitadel zitadel/zitadel --version 9.34.0 -n zitadel -f platform/zitadel/values.yaml 2>/dev/null \
  | grep -c 'konghq.com/plugins: rl-idp'
```

Expected: `2` (main + login ingress). If it prints `1`, chart 9.34.0's `login.ingress` does not pass annotations through — stop, remove the login annotation from values, and instead note in the commit message that only the main ingress is rate-limited (OIDC endpoints are the storm surface; flag it in the task report).

- [ ] **Step 3: Create the rl-idp KongPlugin**

Create `/Users/duclm27/the-algovn/iac/platform/zitadel/manifests/rl-idp-plugin.yaml`:

```yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: rl-idp
  namespace: zitadel
plugin: rate-limiting
config:
  second: 10
  minute: 200
  policy: local
  limit_by: ip
```

Edit `/Users/duclm27/the-algovn/iac/platform/zitadel/manifests/kustomization.yaml` to:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - vmservicescrape.yaml
  - zitadel-config-sealed.yaml
  - zitadel-masterkey-sealed.yaml
  - rl-idp-plugin.yaml
```

- [ ] **Step 4: Add the net_tuning ansible role (GitOps part of the sysctls)**

Create `/Users/duclm27/the-algovn/iac/ansible/roles/net_tuning/files/99-net-tuning.conf`:

```
net.core.rmem_max = 8388608
net.core.wmem_max = 8388608
```

Create `/Users/duclm27/the-algovn/iac/ansible/roles/net_tuning/tasks/main.yml`:

```yaml
- name: Kernel socket buffer caps for high-connection edge (quic-go wants >=8MB)
  ansible.builtin.copy:
    src: 99-net-tuning.conf
    dest: /etc/sysctl.d/99-net-tuning.conf
    mode: "0644"
  notify: reload sysctl
```

Create `/Users/duclm27/the-algovn/iac/ansible/roles/net_tuning/handlers/main.yml`:

```yaml
- name: reload sysctl
  ansible.builtin.command: sysctl --system
```

Append to `/Users/duclm27/the-algovn/iac/ansible/site.yml` (after the "k3s agents" play, before the cloudflared play):

```yaml
- name: Worker network tuning
  hosts: agents
  become: true
  roles:
    - { role: net_tuning, tags: [net_tuning] }
```

If `ansible-playbook` is installed locally, syntax-check: `cd /Users/duclm27/the-algovn/iac/ansible && ansible-playbook site.yml --syntax-check` → `playbook: site.yml`. Otherwise skip (validate.sh covers YAML hygiene via gitleaks only; the user's run in Step 7 is the real gate).

- [ ] **Step 5: Validate, commit (three focused commits), push**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
```

Expected: `PASS`.

```bash
git pull --ff-only
git add platform/zitadel/manifests/rl-idp-plugin.yaml platform/zitadel/manifests/kustomization.yaml
git commit -m "Rate-limit id.algovn.com per client IP"

git add platform/zitadel/values.yaml
git commit -m "Raise Zitadel connection pool and memory headroom"
# (values commit also binds rl-idp via ingress annotations — noted here because both
#  land in values.yaml; the plugin object itself shipped in the previous commit)

git add ansible/roles/net_tuning ansible/site.yml
git commit -m "Add worker sysctls for quic-go socket buffers"

git push origin main
```

- [ ] **Step 6: Wait for sync + VERIFY the Zitadel side**

```bash
for app in zitadel zitadel-config; do
  kubectl --context algovn-remote -n argocd annotate application $app argocd.argoproj.io/refresh=normal --overwrite
done
bash -c 'for i in {1..30}; do s=$(kubectl --context algovn-remote -n argocd get application zitadel -o jsonpath="{.status.sync.status} {.status.health.status}"); echo "$s"; [ "$s" = "Synced Healthy" ] && break; sleep 10; done'
kubectl --context algovn-remote -n zitadel rollout status deploy/zitadel --timeout=300s
```

Expected: `Synced Healthy`, rollout complete (Zitadel restarts for the config change — logins blip for a few seconds, acceptable). Then:

```bash
kubectl --context algovn-remote -n zitadel get cm -o yaml | grep MaxOpenConns
kubectl --context algovn-remote -n zitadel get ingress -o custom-columns='NAME:.metadata.name,PLUGINS:.metadata.annotations.konghq\.com/plugins'
curl -s -o /dev/null -D - https://id.algovn.com/.well-known/openid-configuration | grep -iE '^HTTP|x-ratelimit-limit-second'
bash -c 'for i in {1..15}; do curl -s -o /dev/null -w "%{http_code}\n" https://id.algovn.com/.well-known/openid-configuration; done | sort | uniq -c'
```

Expected: `MaxOpenConns: 20`; both ingresses list `rl-idp` in PLUGINS; discovery responds `HTTP/2 200` with `x-ratelimit-limit-second: 10`; the 15-request burst shows BOTH `200` and `429` counts (>10/s from one IP throttled). Finally confirm login still loads: `curl -s -o /dev/null -w '%{http_code}\n' https://id.algovn.com/ui/v2/login/loginname` → a non-5xx status (200/302/404 all fine — route reachable, not rate-dead).

- [ ] **Step 7: Apply the sysctls on w1 [NEEDS USER] — ansible runs over SSH from the Pi**

**[NEEDS USER]** This step executes on the nodes; the user runs it (or explicitly approves each command). On the Pi (`ssh pi` from the Mac — ProxyCommand per `docs/runbooks/remote-access.md`):

```bash
cd ~/iac/ansible && git pull --ff-only
ansible-playbook site.yml --tags net_tuning --limit algovn-w1
```

Expected recap: `algovn-w1 : ok=2 changed=2 unreachable=0 failed=0` (copy task + `reload sysctl` handler; a re-run shows `changed=0`).

- [ ] **Step 8: VERIFY sysctls + record conntrack/nofile [NEEDS USER for the ssh variant]**

Preferred (user, from the Mac):

```bash
ssh w1 'sysctl net.core.rmem_max net.core.wmem_max net.netfilter.nf_conntrack_max; ulimit -n'
```

Expected: `net.core.rmem_max = 8388608`, `net.core.wmem_max = 8388608`; RECORD the printed `nf_conntrack_max` (fine if ≥ 131072 — 10k CCU ≈ 20-30k conntrack entries) and the shell `ulimit -n` (informational — Kong's own fd ceiling is the T4 `worker_rlimit_nofile 65536`, not the login shell's). If conntrack is below 131072, flag it in the task report — do not change it ad hoc.

Fallback without SSH (controller-runnable):

```bash
kubectl --context algovn-remote debug node/algovn-w1 --image=busybox:1.36 -it -- \
  sh -c 'sysctl net.core.rmem_max net.core.wmem_max net.netfilter.nf_conntrack_max'
kubectl --context algovn-remote get pods -o name | grep node-debugger | xargs -r kubectl --context algovn-remote delete
```

Expected: same values (the debug pod shares the host network namespace); second command cleans up the debugger pod.

**Rollback:** Zitadel/rl-idp: `git revert` the respective commit(s) and push — Argo restores 10 conns/1Gi and drops the plugin binding. Sysctls: revert the ansible commit, then re-run `ansible-playbook site.yml --tags net_tuning --limit algovn-w1`?  No — reverting removes the role from git but not the file from the node; to actually undo on w1: `ssh w1 'sudo rm /etc/sysctl.d/99-net-tuning.conf && sudo sysctl --system'` (user action). Raising rmem/wmem caps is side-effect-free for existing workloads, so rollback is rarely needed.
### Task 6: protos — `algovn.button.v1` package + `gen/go/v0.2.0` tag

**Repo:** `/Users/duclm27/the-algovn/protos` (all paths below relative to it)
**Spec:** §4 (proto surface, verbatim)

**Files:**
- Create: `algovn/button/v1/button.proto`
- Generated: `gen/go/algovn/button/v1/button.pb.go`, `gen/go/algovn/button/v1/button_grpc.pb.go`

**Interfaces:**
- Produces (frozen): proto package `algovn.button.v1`, go_package `github.com/the-algovn/protos/gen/go/algovn/button/v1;buttonv1`, tag `gen/go/v0.2.0` (module version `v0.2.0` of `github.com/the-algovn/protos/gen/go`). Consumed by Task 10 and the SPA cluster.

- [ ] **Step 1: Write the proto (spec §4 exactly)**

`algovn/button/v1/button.proto`:

```proto
syntax = "proto3";

package algovn.button.v1;

import "google/protobuf/timestamp.proto";

option go_package = "github.com/the-algovn/protos/gen/go/algovn/button/v1;buttonv1";

service ButtonService {
  // GetCounter returns the live global click total (anonymous).
  rpc GetCounter(GetCounterRequest) returns (GetCounterResponse);
  // ListAchievements returns the full catalog plus reached global
  // milestones; unlocked_at is set when a valid token is forwarded
  // (anonymous rule, personalized opportunistically).
  rpc ListAchievements(ListAchievementsRequest) returns (ListAchievementsResponse);
  // IssueChallenge hands out a proof-of-work challenge bound to the caller.
  rpc IssueChallenge(IssueChallengeRequest) returns (IssueChallengeResponse);
  // SubmitClicks redeems a solved challenge for a click batch (deadline 3s).
  rpc SubmitClicks(SubmitClicksRequest) returns (SubmitClicksResponse);
}

message GetCounterRequest {}

message GetCounterResponse {
  uint64 total = 1;
}

message IssueChallengeRequest {
  uint32 intended_clicks = 1;
}

message IssueChallengeResponse {
  // Opaque b64url(payload || HMAC-SHA256(payload, K)).
  string challenge = 1;
  // W0*L at issuance (per-click expected hashes).
  uint64 work_factor = 2;
  uint32 min_interval_seconds = 3;
  // 10000.
  uint32 max_batch = 4;
  google.protobuf.Timestamp expires_at = 5;
}

message SubmitClicksRequest {
  string challenge = 1;
  uint64 nonce = 2;
  uint32 click_count = 3;
}

message SubmitClicksResponse {
  uint64 user_total_clicks = 1;
  repeated Achievement unlocked = 2;
  // Piggyback: client starts solving immediately.
  IssueChallengeResponse next_challenge = 3;
}

message ListAchievementsRequest {}

message ListAchievementsResponse {
  // Full catalog, unlocked_at set when personalized.
  repeated Achievement catalog = 1;
  // Reached global milestones.
  repeated Milestone milestones = 2;
}

message Achievement {
  string id = 1;
  string title = 2;
  string description = 3;
  google.protobuf.Timestamp unlocked_at = 4;
}

message Milestone {
  uint64 threshold = 1;
  string title = 2;
  google.protobuf.Timestamp reached_at = 3;
}
```

- [ ] **Step 2: Lint (buf STANDARD — must be clean, no ignore directives needed)**

```bash
cd /Users/duclm27/the-algovn/protos
buf lint
```
Expected: no output, exit 0. (Every RPC has `<Rpc>Request`/`<Rpc>Response` names, all unique — already compliant.)

- [ ] **Step 3: Generate Go**

```bash
cd /Users/duclm27/the-algovn/protos
buf generate
ls gen/go/algovn/button/v1/
```
Expected: `buf generate` is silent; `ls` shows `button.pb.go  button_grpc.pb.go`. `git status` shows only the new proto + the two new gen files (demo gen files are regenerated byte-identical).

- [ ] **Step 4: Compile the gen module**

```bash
cd /Users/duclm27/the-algovn/protos/gen/go
go build ./...
```
Expected: no output, exit 0 (deps `grpc v1.82.0` / `protobuf v1.36.11` already in `gen/go/go.mod`; timestamppb ships with protobuf).

- [ ] **Step 5: Commit and push**

```bash
cd /Users/duclm27/the-algovn/protos
git add algovn/button/v1/button.proto gen/go/algovn/button/v1/
git commit -m "Add algovn.button.v1 ButtonService"
git push origin main
```
Expected: commit on main, push accepted.

- [ ] **Step 6: Watch CI, then tag the gen module**

```bash
run_id=$(gh run list --repo the-algovn/protos --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch --repo the-algovn/protos --exit-status "$run_id"
```
Expected: job `buf` completes with `success` (lint-only on push).

```bash
cd /Users/duclm27/the-algovn/protos
git tag gen/go/v0.2.0
git push origin gen/go/v0.2.0
git ls-remote origin 'refs/tags/gen/go/*'
```
Expected: `ls-remote` lists both `refs/tags/gen/go/v0.1.0` and `refs/tags/gen/go/v0.2.0`.

---

### Task 7: Service scaffold + storage (config, pgx pool + DDL, go-redis)

**Repo:** `/Users/duclm27/the-algovn/the-button-service` (all paths below relative to it; repo exists with the spec committed on `main`)
**Spec:** §7 (data model), skeleton env freeze

**Files:**
- Create: `go.mod`, `go.sum`, `.gitignore`
- Create: `internal/config/config.go` + `internal/config/config_test.go`
- Create: `internal/store/store.go` + `internal/store/store_integration_test.go` (tag `integration`)
- Create: `internal/testutil/doc.go`, `internal/testutil/containers.go` (tag `integration`)
- Create: `cmd/the-button-service/main.go` (stub — replaced in Task 10)

**Interfaces:**
- Consumes (frozen): env `PG_URL`, `REDIS_URL`, `AMQP_URL`, `POW_SECRET` (std-base64 of 32 raw bytes), `POW_SECRET_PREV` (optional, same encoding), `POW_W0` (default "16384"), `LISTEN_ADDR` (default ":9090"), `METRICS_ADDR` (default ":9091"); schema spec §7; module `github.com/the-algovn/the-button-service`; Go 1.26.4.
- Produces: `config.Config`, `config.Load() (*Config, error)`; `store.Schema` (const), `store.NewPG(ctx, url) (*pgxpool.Pool, error)` (MaxConns 10, statement_timeout 2s, applies idempotent DDL), `store.NewRedis(ctx, url) (*redis.Client, error)` (PING-verified); `testutil.StartPostgres(t) string`, `testutil.StartRedis(t) string` (integration-tagged container helpers).

- [ ] **Step 1: Init module and .gitignore**

```bash
cd /Users/duclm27/the-algovn/the-button-service
go mod init github.com/the-algovn/the-button-service
```
Expected: `go: creating new go.mod: module github.com/the-algovn/the-button-service` and `go.mod` contains `go 1.26.4`.

`.gitignore`:

```
bin/
*.test
.DS_Store
.superpowers/
```

(`.superpowers/` holds plan/skeleton scratch files that must never be committed.)

- [ ] **Step 2: Write the failing config test**

`internal/config/config_test.go`:

```go
package config

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("PG_URL", "postgres://u:p@localhost:5432/the_button")
	t.Setenv("REDIS_URL", "redis://:pw@localhost:6379/0")
	t.Setenv("POW_SECRET", base64.StdEncoding.EncodeToString(make([]byte, 32)))
}

func TestLoad_DefaultsAndDecoding(t *testing.T) {
	setRequired(t)
	c, err := Load()
	require.NoError(t, err)
	require.Equal(t, ":9090", c.ListenAddr)
	require.Equal(t, ":9091", c.MetricsAddr)
	require.Equal(t, uint64(16384), c.PowW0)
	require.Len(t, c.PowSecret, 32)
	require.Nil(t, c.PowSecretPrev)
	require.Empty(t, c.AMQPURL) // optional: events are best-effort
}

func TestLoad_MissingRequired(t *testing.T) {
	for _, missing := range []string{"PG_URL", "REDIS_URL", "POW_SECRET"} {
		t.Run(missing, func(t *testing.T) {
			setRequired(t)
			t.Setenv(missing, "")
			_, err := Load()
			require.ErrorContains(t, err, missing)
		})
	}
}

func TestLoad_PrevKeyAndW0(t *testing.T) {
	setRequired(t)
	t.Setenv("POW_SECRET_PREV", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))
	t.Setenv("POW_W0", "4096")
	c, err := Load()
	require.NoError(t, err)
	require.Len(t, c.PowSecretPrev, 32)
	require.Equal(t, uint64(4096), c.PowW0)
}

func TestLoad_BadBase64Secret(t *testing.T) {
	setRequired(t)
	t.Setenv("POW_SECRET", "not-base64!!!")
	_, err := Load()
	require.ErrorContains(t, err, "POW_SECRET")
}
```

```bash
cd /Users/duclm27/the-algovn/the-button-service
go get github.com/stretchr/testify
go test ./internal/config/
```
Expected: FAIL — `undefined: Load` (build error). RED.

- [ ] **Step 3: Implement `internal/config/config.go`**

```go
// Package config loads service configuration from the environment.
package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	PGURL         string // PG_URL (required)
	RedisURL      string // REDIS_URL (required)
	AMQPURL       string // AMQP_URL (optional; counter events are best-effort)
	PowSecret     []byte // POW_SECRET (required, std-base64 of 32 raw bytes)
	PowSecretPrev []byte // POW_SECRET_PREV (optional, rotation window)
	PowW0         uint64 // POW_W0 (default 16384 = 2^14 expected hashes/click)
	ListenAddr    string // LISTEN_ADDR (default :9090)
	MetricsAddr   string // METRICS_ADDR (default :9091)
}

func Load() (*Config, error) {
	c := &Config{
		PGURL:       os.Getenv("PG_URL"),
		RedisURL:    os.Getenv("REDIS_URL"),
		AMQPURL:     os.Getenv("AMQP_URL"),
		ListenAddr:  env("LISTEN_ADDR", ":9090"),
		MetricsAddr: env("METRICS_ADDR", ":9091"),
	}
	if c.PGURL == "" {
		return nil, fmt.Errorf("PG_URL is required")
	}
	if c.RedisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required")
	}
	secret := os.Getenv("POW_SECRET")
	if secret == "" {
		return nil, fmt.Errorf("POW_SECRET is required")
	}
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, fmt.Errorf("POW_SECRET: %w", err)
	}
	c.PowSecret = key
	if prev := os.Getenv("POW_SECRET_PREV"); prev != "" {
		k, err := base64.StdEncoding.DecodeString(prev)
		if err != nil {
			return nil, fmt.Errorf("POW_SECRET_PREV: %w", err)
		}
		c.PowSecretPrev = k
	}
	w0, err := strconv.ParseUint(env("POW_W0", "16384"), 10, 64)
	if err != nil || w0 == 0 {
		return nil, fmt.Errorf("POW_W0 must be a positive integer")
	}
	c.PowW0 = w0
	return c, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

```bash
go test ./internal/config/
```
Expected: `ok  	github.com/the-algovn/the-button-service/internal/config`. GREEN.

- [ ] **Step 4: Add storage + testcontainers dependencies**

```bash
cd /Users/duclm27/the-algovn/the-button-service
go get github.com/jackc/pgx/v5 github.com/redis/go-redis/v9
go get github.com/testcontainers/testcontainers-go github.com/testcontainers/testcontainers-go/modules/postgres github.com/testcontainers/testcontainers-go/modules/redis
```
Expected: `go: added github.com/jackc/pgx/v5 v5.x.y`, `go: added github.com/redis/go-redis/v9 v9.x.y`, `go: added github.com/testcontainers/testcontainers-go v0.43.x` (exact patch versions as resolved on the day).

- [ ] **Step 5: Container helpers (test infra, not the unit under test)**

`internal/testutil/doc.go` (untagged, so untagged `go build ./...` still sees a valid package):

```go
// Package testutil starts throwaway containers for integration tests.
// All helpers are build-tagged `integration`.
package testutil
```

`internal/testutil/containers.go`:

```go
//go:build integration

// Requires a running podman machine:
//
//	export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
//	export TESTCONTAINERS_RYUK_DISABLED=true
package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// StartPostgres runs postgres:18-alpine and returns a pgx URL.
func StartPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("the_button"),
		tcpostgres.WithUsername("the_button"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return url
}

// StartRedis runs redis:7.4-alpine and returns a redis:// URL.
func StartRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7.4-alpine")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	return url
}
```

- [ ] **Step 6: Write the failing store integration test**

`internal/store/store_integration_test.go`:

```go
//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestNewPG_SchemaIdempotent(t *testing.T) {
	url := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := NewPG(ctx, url)
	require.NoError(t, err)
	defer pool.Close()

	// Second application must be a no-op (CREATE TABLE IF NOT EXISTS).
	_, err = pool.Exec(ctx, Schema)
	require.NoError(t, err)

	// Both tables exist and accept the spec §7 shapes.
	_, err = pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('u1', 5)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO user_achievements (user_sub, achievement_id) VALUES ('u1', 'mvh')`)
	require.NoError(t, err)

	var clicks int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT clicks FROM user_clicks WHERE user_sub = 'u1'`).Scan(&clicks))
	require.EqualValues(t, 5, clicks)
	var unlockedAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT unlocked_at FROM user_achievements WHERE user_sub = 'u1'`).Scan(&unlockedAt))
	require.WithinDuration(t, time.Now(), unlockedAt, time.Minute)
}

func TestNewRedis_Ping(t *testing.T) {
	url := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb, err := NewRedis(ctx, url)
	require.NoError(t, err)
	defer rdb.Close()
	require.NoError(t, rdb.Set(ctx, "k", "v", time.Minute).Err())
	require.Equal(t, "v", rdb.Get(ctx, "k").Val())
}
```

```bash
go vet -tags integration ./internal/store/
```
Expected: FAIL — `undefined: NewPG`, `undefined: Schema`, `undefined: NewRedis`. RED.

- [ ] **Step 7: Implement `internal/store/store.go`**

```go
// Package store wires Postgres (durable personal truth) and Redis (hot
// control state) per the design spec §7.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Schema is the full DDL (spec §7), idempotent so every replica applies it
// at startup. No migration framework by design.
const Schema = `
CREATE TABLE IF NOT EXISTS user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL);
CREATE TABLE IF NOT EXISTS user_achievements (
  user_sub text NOT NULL, achievement_id text NOT NULL,
  unlocked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_sub, achievement_id));
`

// NewPG opens a pgx pool (MaxConns 10, statement_timeout 2s — spec §7) and
// applies the idempotent schema.
func NewPG(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse PG_URL: %w", err)
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, Schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return pool, nil
}

// NewRedis parses REDIS_URL and verifies connectivity with a PING.
func NewRedis(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return rdb, nil
}
```

(Multi-statement `Schema` works: pgx `Exec` without arguments uses the simple query protocol.)

- [ ] **Step 8: Run the integration tests**

```bash
cd /Users/duclm27/the-algovn/the-button-service
podman machine start 2>/dev/null || true
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
go test -tags integration ./internal/store/ -v -timeout 300s
```
Expected: `--- PASS: TestNewPG_SchemaIdempotent`, `--- PASS: TestNewRedis_Ping` (first run pulls postgres:18-alpine and redis:7.4-alpine — allow a few minutes). GREEN.

- [ ] **Step 9: main stub, tidy, unit tests**

`cmd/the-button-service/main.go`:

```go
// the-button-service: PoW-gated global click counter. See docs/superpowers/specs.
package main

import (
	"log/slog"
	"os"

	"github.com/the-algovn/the-button-service/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	// Full wiring lands with the gRPC assembly task.
	logger.Info("scaffold ok", "listen", cfg.ListenAddr)
}
```

```bash
go mod tidy
go build ./...
go vet ./...
go test ./...
```
Expected: build/vet clean; `ok` for `internal/config`, `[no test files]` elsewhere.

- [ ] **Step 10: Commit (two focused commits)**

```bash
cd /Users/duclm27/the-algovn/the-button-service
git add go.mod go.sum .gitignore internal/config/ cmd/the-button-service/main.go
git commit -m "Scaffold module with env config"
git add internal/store/ internal/testutil/ go.mod go.sum
git commit -m "Add Postgres/Redis stores with idempotent startup DDL"
```
Expected: two commits on main; `git status` shows only `.superpowers/` ignored, nothing staged.

---

### Task 8: `internal/pow` — token, work check, difficulty controller

**Repo:** `/Users/duclm27/the-algovn/the-button-service`
**Spec:** §5 (PoW protocol), §6 step 1 (verification checks)

**Files:**
- Create: `internal/pow/token.go` + `internal/pow/token_test.go`
- Create: `internal/pow/work.go` + `internal/pow/work_test.go`
- Create: `internal/pow/controller.go` + `internal/pow/controller_test.go`

**Interfaces:**
- Consumes (frozen): token = `base64url(payloadJSON || HMAC-SHA256(payloadJSON, K))`, payload `{"id","sub","iat","exp","w0","l","min_interval_s","max_batch"}`; hash check `SHA-256(tokenBytes || be32(count) || be64(nonce)) < 2^256/(w0*count*l)`; L 1..16, band [200,400]/s, hysteresis raise>110% of 400 / lower<70% of 200, slew ≤1 step/30s; min_interval ladder 2/5/10s; TTL 300s + 30s leeway (Redis EX 330).
- Produces: `pow.Payload`, `pow.Sign(p, key) (string, error)`, `pow.Parse(token, keys...) (Payload, error)` (dual-key accept), `pow.Verify(p, sub, now) error` (`ErrExpired`/`ErrWrongSub`), `pow.CheckWork(token string, w0 uint64, l, count uint32, nonce uint64) bool`, `pow.Solve(...)` (test helper), `pow.NextL(currentL, ewmaRate, lastChange, now) (uint32, time.Time)`, `pow.MinInterval(l) uint32`, `pow.EWMA(prev, sample, dt) float64`, consts `MaxBatch=10000`, `TokenTTL=300s`, `BurnTTL=330s`, `ExpLeeway=30s`, `MinL=1`, `MaxL=16`.
- **PINNED for the SPA solver (Task 15) and load scripts (Task 20/21):** `tokenBytes` in the hash preimage = the ASCII bytes of the challenge string exactly as issued (the base64url text itself, NOT the decoded payload||mac). MinInterval mapping: L 1–5 → 2s, L 6–11 → 5s, L 12–16 → 10s.

- [ ] **Step 1: Write the failing token tests**

`internal/pow/token_test.go`:

```go
package pow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	keyA = []byte("key-a-0123456789key-a-0123456789")
	keyB = []byte("key-b-0123456789key-b-0123456789")
)

func testPayload(now time.Time) Payload {
	return Payload{
		ID:           "0197-test-id",
		Sub:          "user-1",
		Iat:          now.Unix(),
		Exp:          now.Add(TokenTTL).Unix(),
		W0:           16384,
		L:            1,
		MinIntervalS: 2,
		MaxBatch:     MaxBatch,
	}
}

// flip corrupts the first base64url character without breaking the encoding.
func flip(tok string) string {
	if tok[0] == 'A' {
		return "B" + tok[1:]
	}
	return "A" + tok[1:]
}

func TestSignParse_RoundTrip(t *testing.T) {
	p := testPayload(time.Now())
	tok, err := Sign(p, keyA)
	require.NoError(t, err)

	got, err := Parse(tok, keyA)
	require.NoError(t, err)
	require.Equal(t, p, got)
}

func TestParse_DualKeyRotation(t *testing.T) {
	tok, err := Sign(testPayload(time.Now()), keyB)
	require.NoError(t, err)
	// current=keyA, previous=keyB: a token signed by the old key still parses
	_, err = Parse(tok, keyA, keyB)
	require.NoError(t, err)
	// but not once the old key leaves the accept window
	_, err = Parse(tok, keyA)
	require.ErrorIs(t, err, ErrBadToken)
}

func TestParse_Tampered(t *testing.T) {
	tok, err := Sign(testPayload(time.Now()), keyA)
	require.NoError(t, err)

	for name, bad := range map[string]string{
		"not base64":   "!!!",
		"too short":    "aGk",
		"flipped byte": flip(tok),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(bad, keyA)
			require.ErrorIs(t, err, ErrBadToken)
		})
	}
}

func TestVerify(t *testing.T) {
	now := time.Now()
	p := testPayload(now)

	require.NoError(t, Verify(p, "user-1", now))
	// exp leeway: 29s past exp is fine, 31s is not (spec §6: 30s leeway)
	exp := time.Unix(p.Exp, 0)
	require.NoError(t, Verify(p, "user-1", exp.Add(29*time.Second)))
	require.ErrorIs(t, Verify(p, "user-1", exp.Add(31*time.Second)), ErrExpired)
	// sub binding kills token farming
	require.ErrorIs(t, Verify(p, "user-2", now), ErrWrongSub)
	empty := p
	empty.Sub = ""
	require.ErrorIs(t, Verify(empty, "", now), ErrWrongSub)
}
```

```bash
cd /Users/duclm27/the-algovn/the-button-service
go test ./internal/pow/
```
Expected: FAIL — `undefined: Payload`, `undefined: Sign`, ... (build error). RED.

- [ ] **Step 2: Implement `internal/pow/token.go`**

```go
// Package pow implements the stateless proof-of-work protocol (spec §5):
// HMAC-signed challenge tokens, the SHA-256 work check, and the shared
// difficulty controller's pure math.
package pow

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

const (
	// MaxBatch is the hard batch-size cap (spec §5).
	MaxBatch = 10000
	// TokenTTL is the challenge lifetime at issuance.
	TokenTTL = 300 * time.Second
	// ExpLeeway is the verification-side grace on exp (spec §6 step 1).
	ExpLeeway = 30 * time.Second
	// BurnTTL is the Redis expiry of the burn key: TTL + leeway (spec §7).
	BurnTTL = 330 * time.Second
)

var (
	ErrBadToken = errors.New("malformed or tampered challenge")
	ErrExpired  = errors.New("challenge expired")
	ErrWrongSub = errors.New("challenge bound to another subject")
)

// Payload is the signed challenge payload (spec §5). Verification checks
// the exact signed bytes, so field order only matters at issuance.
type Payload struct {
	ID           string `json:"id"`
	Sub          string `json:"sub"`
	Iat          int64  `json:"iat"`
	Exp          int64  `json:"exp"`
	W0           uint64 `json:"w0"`
	L            uint32 `json:"l"`
	MinIntervalS uint32 `json:"min_interval_s"`
	MaxBatch     uint32 `json:"max_batch"`
}

// Sign serializes p and returns base64url(payloadJSON || HMAC-SHA256(payloadJSON, key)).
func Sign(p Payload, key []byte) (string, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(append(body, mac.Sum(nil)...)), nil
}

// Parse decodes a token and verifies its HMAC against any of keys (dual-key
// rotation window: current, then previous). It does NOT check exp/sub — see
// Verify. Stateless: any replica verifies any token.
func Parse(token string, keys ...[]byte) (Payload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return Payload{}, ErrBadToken
	}
	body, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	ok := false
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, key)
		mac.Write(body)
		if hmac.Equal(sig, mac.Sum(nil)) {
			ok = true
			break
		}
	}
	if !ok {
		return Payload{}, ErrBadToken
	}
	var p Payload
	if err := json.Unmarshal(body, &p); err != nil {
		return Payload{}, ErrBadToken
	}
	return p, nil
}

// Verify applies the semantic checks of spec §6 step 1: expiry (with
// leeway) and subject binding.
func Verify(p Payload, sub string, now time.Time) error {
	if now.After(time.Unix(p.Exp, 0).Add(ExpLeeway)) {
		return ErrExpired
	}
	if p.Sub == "" || p.Sub != sub {
		return ErrWrongSub
	}
	return nil
}
```

```bash
go test ./internal/pow/
```
Expected: PASS. GREEN.

- [ ] **Step 3: Write the failing work-check tests**

`internal/pow/work_test.go`:

```go
package pow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func solveFrom(tok string, w0 uint64, l, count uint32, start uint64) uint64 {
	for n := start; ; n++ {
		if CheckWork(tok, w0, l, count, n) {
			return n
		}
	}
}

func TestCheckWork_SolveRoundTrip(t *testing.T) {
	const tok = "opaque-test-challenge"
	nonce := Solve(tok, 64, 2, 8) // expected ~1024 hashes — instant
	require.True(t, CheckWork(tok, 64, 2, 8, nonce))
}

// A solution must not transfer to a different count, token, or nonce. Any
// single perturbed check can pass by luck (p ≈ 1/1024), so scan solutions
// until each binding is demonstrated — expected ~1 iteration each.
func TestCheckWork_BindsInputs(t *testing.T) {
	const tok = "opaque-test-challenge"
	var boundCount, boundToken, boundNonce bool
	nonce := uint64(0)
	for range 50 {
		nonce = solveFrom(tok, 64, 2, 8, nonce)
		if !CheckWork(tok, 64, 2, 9, nonce) {
			boundCount = true
		}
		if !CheckWork("another-token", 64, 2, 8, nonce) {
			boundToken = true
		}
		if !CheckWork(tok, 64, 2, 8, nonce+1) {
			boundNonce = true
		}
		if boundCount && boundToken && boundNonce {
			break
		}
		nonce++
	}
	require.True(t, boundCount, "count is not bound into the hash")
	require.True(t, boundToken, "token is not bound into the hash")
	require.True(t, boundNonce, "nonce is not bound into the hash")
}

func TestCheckWork_DegenerateDifficulty(t *testing.T) {
	// zero factors mean "reject everything" — never divide by zero
	require.False(t, CheckWork("t", 0, 1, 1, 0))
	require.False(t, CheckWork("t", 1, 0, 1, 0))
	require.False(t, CheckWork("t", 1, 1, 0, 0))
	// w0=l=count=1 → target = 2^256: every digest passes
	require.True(t, CheckWork("t", 1, 1, 1, 0))
}
```

```bash
go test ./internal/pow/
```
Expected: FAIL — `undefined: CheckWork`, `undefined: Solve`. RED.

- [ ] **Step 4: Implement `internal/pow/work.go`**

```go
package pow

import (
	"crypto/sha256"
	"encoding/binary"
	"math/big"
)

// two256 is 2^256, the numerator of the smooth full-target form (spec §5).
var two256 = new(big.Int).Lsh(big.NewInt(1), 256)

// target returns 2^256 / (w0 * count * l); nil means "reject everything".
func target(w0 uint64, l, count uint32) *big.Int {
	d := new(big.Int).SetUint64(w0)
	d.Mul(d, new(big.Int).SetUint64(uint64(count)))
	d.Mul(d, new(big.Int).SetUint64(uint64(l)))
	if d.Sign() == 0 {
		return nil
	}
	return new(big.Int).Div(two256, d)
}

// CheckWork verifies spec §5: SHA-256(tokenBytes || be32(count) || be64(nonce)),
// read as a big-endian 256-bit integer, must be < 2^256/(w0*count*l).
// tokenBytes are the ASCII bytes of the challenge string exactly as issued —
// the SPA solver hashes the same bytes.
func CheckWork(token string, w0 uint64, l, count uint32, nonce uint64) bool {
	tgt := target(w0, l, count)
	if tgt == nil {
		return false
	}
	h := sha256.New()
	h.Write([]byte(token))
	var suffix [12]byte
	binary.BigEndian.PutUint32(suffix[0:4], count)
	binary.BigEndian.PutUint64(suffix[4:12], nonce)
	h.Write(suffix[:])
	return new(big.Int).SetBytes(h.Sum(nil)).Cmp(tgt) < 0
}

// Solve brute-forces the smallest nonce satisfying CheckWork. Test helper —
// production clients solve in a WASM Web Worker.
func Solve(token string, w0 uint64, l, count uint32) uint64 {
	for nonce := uint64(0); ; nonce++ {
		if CheckWork(token, w0, l, count, nonce) {
			return nonce
		}
	}
}
```

```bash
go test ./internal/pow/
```
Expected: PASS. GREEN.

- [ ] **Step 5: Write the failing controller tests**

`internal/pow/controller_test.go`:

```go
package pow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextL(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	after := t0.Add(SlewInterval) // slew window open

	tests := []struct {
		name       string
		l          uint32
		rate       float64
		now        time.Time
		wantL      uint32
		wantMoved  bool
	}{
		{"in band no change", 4, 300, after, 4, false},
		{"raise above 440", 4, 441, after, 5, true},
		{"hysteresis: 400..440 holds", 4, 439, after, 4, false},
		{"lower below 140", 4, 139, after, 3, true},
		{"hysteresis: 140..200 holds", 4, 141, after, 4, false},
		{"clamp at MaxL", 16, 10_000, after, 16, false},
		{"clamp at MinL", 1, 0, after, 1, false},
		{"slew: too soon", 4, 10_000, t0.Add(29 * time.Second), 4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotL, gotChange := NextL(tc.l, tc.rate, t0, tc.now)
			require.Equal(t, tc.wantL, gotL)
			if tc.wantMoved {
				require.Equal(t, tc.now, gotChange)
			} else {
				require.Equal(t, t0, gotChange)
			}
		})
	}
}

func TestNextL_SlewOneStepPer30s(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	l, change := NextL(4, 10_000, t0.Add(-time.Hour), t0)
	require.EqualValues(t, 5, l)
	// 10s later, still storming: no second step yet
	l2, _ := NextL(l, 10_000, change, t0.Add(10*time.Second))
	require.EqualValues(t, 5, l2)
	// 30s after the change the next step is allowed
	l3, _ := NextL(l, 10_000, change, t0.Add(30*time.Second))
	require.EqualValues(t, 6, l3)
}

func TestMinInterval_Ladder(t *testing.T) {
	require.EqualValues(t, 2, MinInterval(1))
	require.EqualValues(t, 2, MinInterval(5))
	require.EqualValues(t, 5, MinInterval(6))
	require.EqualValues(t, 5, MinInterval(11))
	require.EqualValues(t, 10, MinInterval(12))
	require.EqualValues(t, 10, MinInterval(16))
}

func TestEWMA(t *testing.T) {
	// after exactly one half-life the estimate moves halfway to the sample
	require.InDelta(t, 50, EWMA(0, 100, EWMAHalfLife), 0.001)
	// no time elapsed → unchanged
	require.Equal(t, 42.0, EWMA(42, 100, 0))
}
```

```bash
go test ./internal/pow/
```
Expected: FAIL — `undefined: NextL`, `undefined: SlewInterval`, ... RED.

- [ ] **Step 6: Implement `internal/pow/controller.go`**

```go
package pow

import (
	"math"
	"time"
)

// Difficulty controller constants (spec §5): the tick leader keeps the
// accepted-submit rate inside [BandLow, BandHigh] by moving L one step at
// a time; min_interval is the hard valve, L the cost valve.
const (
	MinL     = 1
	MaxL     = 16
	BandLow  = 200.0 // accepted submits/s
	BandHigh = 400.0
	// Hysteresis: raise only above 110% of the high edge, lower only
	// below 70% of the low edge.
	raiseAbove = BandHigh * 1.10 // 440/s
	lowerBelow = BandLow * 0.70  // 140/s
	// SlewInterval limits difficulty movement to one step per 30s.
	SlewInterval = 30 * time.Second
	// EWMAHalfLife smooths the sampled rate (spec §6 step 4: ~30s).
	EWMAHalfLife = 30 * time.Second
)

// NextL applies hysteresis and slew to move currentL toward the band.
// It returns the new L and the lastChange timestamp to carry forward.
func NextL(currentL uint32, ewmaRate float64, lastChange, now time.Time) (uint32, time.Time) {
	if now.Sub(lastChange) < SlewInterval {
		return currentL, lastChange
	}
	switch {
	case ewmaRate > raiseAbove && currentL < MaxL:
		return currentL + 1, now
	case ewmaRate < lowerBelow && currentL > MinL:
		return currentL - 1, now
	}
	return currentL, lastChange
}

// MinInterval maps L to the hard per-user interval ladder 2s → 5s → 10s.
func MinInterval(l uint32) uint32 {
	switch {
	case l <= 5:
		return 2
	case l <= 11:
		return 5
	default:
		return 10
	}
}

// EWMA folds a rate sample observed over dt into prev with half-life
// EWMAHalfLife.
func EWMA(prev, sample float64, dt time.Duration) float64 {
	if dt <= 0 {
		return prev
	}
	alpha := 1 - math.Exp2(-dt.Seconds()/EWMAHalfLife.Seconds())
	return prev + alpha*(sample-prev)
}
```

```bash
go test ./internal/pow/ -v
go vet ./...
```
Expected: all pow tests PASS (`TestSignParse_RoundTrip`, `TestParse_DualKeyRotation`, `TestParse_Tampered`, `TestVerify`, `TestCheckWork_*`, `TestNextL*`, `TestMinInterval_Ladder`, `TestEWMA`); vet clean. GREEN.

- [ ] **Step 7: Commit**

```bash
cd /Users/duclm27/the-algovn/the-button-service
git add internal/pow/
git commit -m "Add proof-of-work token, work check, and difficulty controller"
```

---

### Task 9: `internal/achievements` + `internal/clicks` (SubmitClicks core)

**Repo:** `/Users/duclm27/the-algovn/the-button-service`
**Spec:** §6 (submit flow steps 2–4), §9 (catalog, crosses semantics)

**Files:**
- Create: `internal/achievements/achievements.go` + `internal/achievements/achievements_test.go`
- Create: `internal/clicks/clicks.go` + `internal/clicks/clicks_integration_test.go` (tag `integration`)

**Interfaces:**
- Consumes: `store.NewPG`/`NewRedis`/`Schema` (T7), `pow.Payload`/`BurnTTL`/`TokenTTL`/`MaxBatch` (T8); frozen Redis keys `pow:<uuid>` EX 330, `throttle:<sub>` EX min_interval, `counter:global`, `stats:accepted_total`.
- Produces:
  - `achievements.Achievement{ID,Title,Description}`, `achievements.Catalog` (12 entries, spec §9 ids/titles), `achievements.Milestone{Threshold,Title}`, `achievements.Milestones` (5 entries, ascending), `achievements.Evaluate(total uint64, batch uint32, now time.Time) []Achievement`, `achievements.ByID(id) (Achievement, bool)`. Crosses semantics `old = total - batch`; time rules in Asia/Ho_Chi_Minh (tzdata embedded via `time/tzdata`).
  - `clicks.Rediser` (SetNX/Del/IncrBy/Incr slice of `*redis.Client`), `clicks.Unlock{Achievement, UnlockedAt}`, `clicks.Result{UserTotal, Unlocked}`, `clicks.Submit(ctx, rdb Rediser, pool *pgxpool.Pool, logger *slog.Logger, p pow.Payload, count uint32, now time.Time) (*Result, error)` returning gRPC status errors: `AlreadyExists` (replay), `ResourceExhausted` (throttle, token un-burned), `Unavailable` (Redis/PG down, compensated).

- [ ] **Step 1: Write the failing achievements tests**

`internal/achievements/achievements_test.go`:

```go
package achievements

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ids(as []Achievement) []string {
	out := make([]string, len(as))
	for i, a := range as {
		out[i] = a.ID
	}
	return out
}

// neutral is 22:00 ICT — triggers no time-of-day rule.
var neutral = time.Date(2026, 7, 14, 15, 0, 0, 0, time.UTC)

func TestEvaluate_ThresholdsUseCrosses(t *testing.T) {
	require.Equal(t, []string{"mvh"}, ids(Evaluate(1, 1, neutral)))
	// old=60 → only the 69 crossing fires; no re-award of mvh/ten
	require.Equal(t, []string{"nice"}, ids(Evaluate(70, 10, neutral)))
	// boundary: old=69 does NOT re-cross 69
	require.Empty(t, Evaluate(70, 1, neutral))
	// crossing includes the exact value: old=68, new=69
	require.Equal(t, []string{"nice"}, ids(Evaluate(69, 1, neutral)))
	// old=99,995 crosses only 100,000
	require.Equal(t, []string{"stretch"}, ids(Evaluate(100_000, 5, neutral)))
}

func TestEvaluate_BatchRules(t *testing.T) {
	require.Equal(t, []string{"bigbatch"}, ids(Evaluate(200_500, 500, neutral)))
	require.Equal(t, []string{"bigbatch", "maxbatch"}, ids(Evaluate(210_000, 10_000, neutral)))
	require.Empty(t, Evaluate(200_999, 499, neutral))
}

func TestEvaluate_TimeRulesHoChiMinh(t *testing.T) {
	// 20:30 UTC = 03:30 ICT (+07, no DST)
	night := time.Date(2026, 7, 13, 20, 30, 0, 0, time.UTC)
	require.Equal(t, []string{"night"}, ids(Evaluate(50, 1, night)))
	// 05:15 UTC = 12:15 ICT
	lunch := time.Date(2026, 7, 14, 5, 15, 0, 0, time.UTC)
	require.Equal(t, []string{"lunch"}, ids(Evaluate(50, 1, lunch)))
	// 03:30 UTC = 10:30 ICT — NOT night even though it is 3am UTC
	require.Empty(t, Evaluate(50, 1, time.Date(2026, 7, 14, 3, 30, 0, 0, time.UTC)))
}

func TestEvaluate_FreshWhale(t *testing.T) {
	// first-ever batch of 10,000: every threshold ≤10k, both crossings, both batch rules
	require.Equal(t,
		[]string{"mvh", "ten", "nice", "century", "blaze", "comma", "carpal", "bigbatch", "maxbatch"},
		ids(Evaluate(10_000, 10_000, neutral)))
}

func TestCatalogAndMilestones(t *testing.T) {
	require.Len(t, Catalog, 12)
	for _, a := range Catalog {
		require.NotEmpty(t, a.ID)
		require.NotEmpty(t, a.Title)
		require.NotEmpty(t, a.Description)
	}
	a, ok := ByID("nice")
	require.True(t, ok)
	require.Equal(t, "Nice.", a.Title)

	require.Len(t, Milestones, 5)
	require.Equal(t, uint64(1_000), Milestones[0].Threshold)
	require.Equal(t, uint64(1_000_000_000), Milestones[4].Threshold)
	for i := 1; i < len(Milestones); i++ {
		require.Greater(t, Milestones[i].Threshold, Milestones[i-1].Threshold)
	}
}
```

```bash
cd /Users/duclm27/the-algovn/the-button-service
go test ./internal/achievements/
```
Expected: FAIL — `undefined: Achievement`, `undefined: Evaluate`, ... RED.

- [ ] **Step 2: Implement `internal/achievements/achievements.go`**

```go
// Package achievements holds the catalog (spec §9) and the pure evaluation
// rules. Every rule is evaluable from (new total, batch size, server time).
package achievements

import (
	"time"
	_ "time/tzdata" // time rules are defined in Asia/Ho_Chi_Minh; embed tzdata
)

type Achievement struct {
	ID          string
	Title       string
	Description string
}

type Milestone struct {
	Threshold uint64
	Title     string
}

// Catalog is the full personal catalog (spec §9), in presentation order.
var Catalog = []Achievement{
	{"mvh", "Minimum Viable Human", "You clicked the button once. Truly the least you could do."},
	{"ten", "Double Digits", "Ten clicks. Your dedication is now measurable. Barely."},
	{"century", "Century of Defiance", "One hundred clicks against the void."},
	{"comma", "The Comma Club", "1,000 clicks. You've earned punctuation."},
	{"carpal", "Carpal Diem", "10,000 clicks. Seize the wrist brace."},
	{"stretch", "Please Stretch", "100,000 clicks. This is a wellness intervention."},
	{"nice", "Nice.", "Your total crossed 69. You know what you did."},
	{"blaze", "Botanical Enthusiast", "Your total crossed 420. Purely coincidental, we're sure."},
	{"bigbatch", "Mass Production", "500 clicks in a single batch. Industrial-grade defiance."},
	{"maxbatch", "One Batch to Rule Them All", "A perfect 10,000-click batch. The machines are impressed."},
	{"night", "3am Rebellion", "Clicking at 3am. The button appreciates your insomnia."},
	{"lunch", "Lunch Break Rebel", "Clicked between noon and one. The sandwich can wait."},
}

// Milestones are the global thresholds announced by the tick leader
// (spec §9), ascending.
var Milestones = []Milestone{
	{1_000, "A Thousand Tiny Rebellions"},
	{100_000, "Six Figures of Defiance"},
	{1_000_000, "One Million. Together We Did… This."},
	{10_000_000, "Ten Million Clicks Nobody Asked For"},
	{1_000_000_000, "The Billion"},
}

var hcm = mustLoad("Asia/Ho_Chi_Minh")

func mustLoad(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err) // tzdata is linked in; cannot happen
	}
	return loc
}

var byID = func() map[string]Achievement {
	m := make(map[string]Achievement, len(Catalog))
	for _, a := range Catalog {
		m[a.ID] = a
	}
	return m
}()

// ByID returns the catalog entry for id.
func ByID(id string) (Achievement, bool) {
	a, ok := byID[id]
	return a, ok
}

// thresholds evaluated with crosses semantics, ascending.
var thresholds = []struct {
	x  uint64
	id string
}{
	{1, "mvh"}, {10, "ten"}, {69, "nice"}, {100, "century"},
	{420, "blaze"}, {1_000, "comma"}, {10_000, "carpal"}, {100_000, "stretch"},
}

// crosses reports old_total < x ≤ new_total with old_total = total - batch
// (spec §9).
func crosses(total uint64, batch uint32, x uint64) bool {
	old := uint64(0)
	if total > uint64(batch) {
		old = total - uint64(batch)
	}
	return old < x && x <= total
}

// Evaluate returns every achievement earned by a batch that brought the
// user to total at now. Threshold rules use crosses semantics so already-
// earned rows are never re-proposed; batch/time rules rely on the
// ON CONFLICT DO NOTHING insert to dedupe.
func Evaluate(total uint64, batch uint32, now time.Time) []Achievement {
	var out []Achievement
	add := func(id string) {
		a, _ := ByID(id)
		out = append(out, a)
	}
	for _, th := range thresholds {
		if crosses(total, batch, th.x) {
			add(th.id)
		}
	}
	if batch >= 500 {
		add("bigbatch")
	}
	if batch == 10_000 {
		add("maxbatch")
	}
	switch now.In(hcm).Hour() {
	case 3:
		add("night")
	case 12:
		add("lunch")
	}
	return out
}
```

```bash
go test ./internal/achievements/ -v
```
Expected: all 5 tests PASS. GREEN.

- [ ] **Step 3: Add gRPC dep, write the failing clicks integration test**

```bash
cd /Users/duclm27/the-algovn/the-button-service
go get google.golang.org/grpc
```
Expected: `go: added google.golang.org/grpc v1.82.x` (or newer).

`internal/clicks/clicks_integration_test.go`:

```go
//go:build integration

package clicks

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

func setup(t *testing.T) (*redis.Client, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPG(ctx, testutil.StartPostgres(t))
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	rdb, err := store.NewRedis(ctx, testutil.StartRedis(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, pool
}

func payload(id, sub string, minInterval uint32) pow.Payload {
	now := time.Now()
	return pow.Payload{
		ID: id, Sub: sub, Iat: now.Unix(), Exp: now.Add(pow.TokenTTL).Unix(),
		W0: 16384, L: 1, MinIntervalS: minInterval, MaxBatch: pow.MaxBatch,
	}
}

func unlockedIDs(res *Result) []string {
	out := make([]string, 0, len(res.Unlocked))
	for _, u := range res.Unlocked {
		out = append(out, u.Achievement.ID)
	}
	return out
}

func TestSubmit_HappyPathAndReplay(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	p := payload("tok-1", "user-1", 1)

	res, err := Submit(ctx, rdb, pool, logger, p, 5, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 5, res.UserTotal)
	require.Contains(t, unlockedIDs(res), "mvh")

	// step 4 side effects: hot counter + controller signal
	require.Equal(t, "5", rdb.Get(ctx, "counter:global").Val())
	require.Equal(t, "1", rdb.Get(ctx, "stats:accepted_total").Val())

	// replay: the same challenge id is burned
	_, err = Submit(ctx, rdb, pool, logger, p, 5, time.Now())
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSubmit_ThrottleUnburnsToken(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	_, err := Submit(ctx, rdb, pool, logger, payload("tok-a", "user-2", 1), 1, time.Now())
	require.NoError(t, err)

	// immediately again with a fresh token: throttled AND un-burned
	p2 := payload("tok-b", "user-2", 1)
	_, err = Submit(ctx, rdb, pool, logger, p2, 1, time.Now())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:tok-b").Val())

	// after the interval the SAME token succeeds — client did not re-solve
	time.Sleep(1200 * time.Millisecond)
	res, err := Submit(ctx, rdb, pool, logger, p2, 1, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 2, res.UserTotal)
}

func TestSubmit_TxnFailureCompensates(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	// sabotage the txn
	_, err := pool.Exec(ctx, `ALTER TABLE user_clicks RENAME TO user_clicks_broken`)
	require.NoError(t, err)

	p := payload("tok-c", "user-3", 1)
	_, err = Submit(ctx, rdb, pool, logger, p, 3, time.Now())
	require.Equal(t, codes.Unavailable, status.Code(err))
	// compensation removed both keys — the token is spendable again
	require.Equal(t, int64(0), rdb.Exists(ctx, "pow:tok-c", "throttle:user-3").Val())
	// and no counter bump happened for the failed attempt
	require.Equal(t, int64(0), rdb.Exists(ctx, "counter:global").Val())

	// heal and retry the SAME token: accepted
	_, err = pool.Exec(ctx, `ALTER TABLE user_clicks_broken RENAME TO user_clicks`)
	require.NoError(t, err)
	res, err := Submit(ctx, rdb, pool, logger, p, 3, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 3, res.UserTotal)
	require.Equal(t, "3", rdb.Get(ctx, "counter:global").Val())
}

func TestSubmit_Crosses69(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('user-4', 60)`)
	require.NoError(t, err)

	res, err := Submit(ctx, rdb, pool, logger, payload("tok-d", "user-4", 1), 10, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 70, res.UserTotal)
	got := unlockedIDs(res)
	require.Contains(t, got, "nice")
	require.NotContains(t, got, "mvh") // old=60 already past 1 — no re-award
}

func TestSubmit_BatchAchievementsOnceOnly(t *testing.T) {
	rdb, pool := setup(t)
	ctx := context.Background()

	res, err := Submit(ctx, rdb, pool, logger, payload("tok-e", "user-5", 1), 10_000, time.Now())
	require.NoError(t, err)
	got := unlockedIDs(res)
	for _, want := range []string{"mvh", "ten", "nice", "century", "blaze", "comma", "carpal", "bigbatch", "maxbatch"} {
		require.Contains(t, got, want)
	}
	for _, u := range res.Unlocked {
		require.False(t, u.UnlockedAt.IsZero())
	}

	// second max batch: bigbatch/maxbatch rows already exist → not re-unlocked
	time.Sleep(1200 * time.Millisecond) // clear throttle
	res2, err := Submit(ctx, rdb, pool, logger, payload("tok-f", "user-5", 1), 10_000, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 20_000, res2.UserTotal)
	for _, u := range res2.Unlocked {
		require.NotContains(t, []string{"bigbatch", "maxbatch"}, u.Achievement.ID)
	}
}
```

```bash
go vet -tags integration ./internal/clicks/
```
Expected: FAIL — `undefined: Submit`, `undefined: Result`. RED.

- [ ] **Step 4: Implement `internal/clicks/clicks.go`**

```go
// Package clicks implements the SubmitClicks core flow (spec §6 steps 2-4):
// burn → throttle → durable txn → compensation → counter bump.
package clicks

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// Rediser is the slice of go-redis used by Submit (satisfied by *redis.Client).
type Rediser interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
}

// Unlock is a newly earned achievement with its database timestamp.
type Unlock struct {
	Achievement achievements.Achievement
	UnlockedAt  time.Time
}

// Result is the outcome of an accepted batch.
type Result struct {
	UserTotal uint64
	Unlocked  []Unlock
}

// Submit executes spec §6 steps 2-4 for an already-verified challenge
// payload. Returned errors are gRPC status errors:
//   - AlreadyExists     — challenge replay (burn key present)
//   - ResourceExhausted — per-user min-interval hit (token un-burned, stays valid)
//   - Unavailable       — Redis or Postgres unreachable (clicks fail closed)
func Submit(ctx context.Context, rdb Rediser, pool *pgxpool.Pool, logger *slog.Logger, p pow.Payload, count uint32, now time.Time) (*Result, error) {
	powKey := "pow:" + p.ID
	throttleKey := "throttle:" + p.Sub
	bg := context.WithoutCancel(ctx) // compensation must survive deadline expiry

	// Step 2a: burn the challenge. Two sequential commands — the throttle
	// branches on the burn result, so no pipelining.
	burned, err := rdb.SetNX(ctx, powKey, 1, pow.BurnTTL).Result()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "redis unavailable")
	}
	if !burned {
		return nil, status.Error(codes.AlreadyExists, "challenge already redeemed")
	}

	// Step 2b: hard per-user rate floor.
	ok, err := rdb.SetNX(ctx, throttleKey, 1, time.Duration(p.MinIntervalS)*time.Second).Result()
	if err != nil {
		if derr := rdb.Del(bg, powKey).Err(); derr != nil {
			logger.Warn("un-burn DEL failed", "err", derr)
		}
		return nil, status.Error(codes.Unavailable, "redis unavailable")
	}
	if !ok {
		// un-burn: the token stays valid, the client backs off
		if derr := rdb.Del(bg, powKey).Err(); derr != nil {
			logger.Warn("un-burn DEL failed", "err", derr)
		}
		return nil, status.Error(codes.ResourceExhausted, "min interval not elapsed")
	}

	// Step 3: durable personal truth.
	res, err := applyBatch(ctx, pool, p.Sub, count, now)
	if err != nil {
		logger.Warn("batch txn failed", "sub", p.Sub, "err", err)
		// best-effort compensation; if this DEL fails the client re-solves
		// one challenge — accepted (spec §13)
		if derr := rdb.Del(bg, powKey, throttleKey).Err(); derr != nil {
			logger.Warn("compensation DEL failed", "err", derr)
		}
		return nil, status.Error(codes.Unavailable, "postgres unavailable")
	}

	// Step 4: hot counter + controller signal — drift healed by reconcile.
	if err := rdb.IncrBy(bg, "counter:global", int64(count)).Err(); err != nil {
		logger.Warn("counter INCRBY failed", "err", err)
	}
	if err := rdb.Incr(bg, "stats:accepted_total").Err(); err != nil {
		logger.Warn("stats INCR failed", "err", err)
	}
	return res, nil
}

func applyBatch(ctx context.Context, pool *pgxpool.Pool, sub string, count uint32, now time.Time) (*Result, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after commit

	var total int64
	err = tx.QueryRow(ctx,
		`INSERT INTO user_clicks AS u (user_sub, clicks) VALUES ($1, $2)
		 ON CONFLICT (user_sub) DO UPDATE SET clicks = u.clicks + $2
		 RETURNING clicks`, sub, int64(count)).Scan(&total)
	if err != nil {
		return nil, err
	}

	res := &Result{UserTotal: uint64(total)}
	for _, a := range achievements.Evaluate(uint64(total), count, now) {
		var at time.Time
		err := tx.QueryRow(ctx,
			`INSERT INTO user_achievements (user_sub, achievement_id) VALUES ($1, $2)
			 ON CONFLICT DO NOTHING RETURNING unlocked_at`, sub, a.ID).Scan(&at)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // unlocked in an earlier batch
		}
		if err != nil {
			return nil, err
		}
		res.Unlocked = append(res.Unlocked, Unlock{Achievement: a, UnlockedAt: at})
	}
	return res, tx.Commit(ctx)
}
```

- [ ] **Step 5: Run the integration tests**

```bash
cd /Users/duclm27/the-algovn/the-button-service
podman machine start 2>/dev/null || true
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
go test -tags integration ./internal/clicks/ -v -timeout 600s
go test ./...
go vet ./...
```
Expected: `--- PASS:` for all five `TestSubmit_*` tests; unit suite still green; vet clean. GREEN.

- [ ] **Step 6: Commit (two focused commits)**

```bash
cd /Users/duclm27/the-algovn/the-button-service
git add internal/achievements/
git commit -m "Add achievements catalog and evaluation rules"
git add internal/clicks/ go.mod go.sum
git commit -m "Add SubmitClicks core flow with burn, throttle, and compensation"
```

---

### Task 10: Tick leader + gRPC server + full assembly

**Repo:** `/Users/duclm27/the-algovn/the-button-service`
**Spec:** §4 (API), §6 step 1+5, §8 (leader/SSE/reconcile)

**Files:**
- Create: `internal/ticker/ticker.go` + `internal/ticker/ticker_integration_test.go` (tag `integration`)
- Create: `internal/server/server.go` + `internal/server/server_test.go` + `internal/server/server_integration_test.go` (tag `integration`)
- Create: `internal/testutil/rabbit.go` (tag `integration`)
- Replace: `cmd/the-button-service/main.go`

**Interfaces:**
- Consumes: `buttonv1` from `github.com/the-algovn/protos/gen/go@v0.2.0` (T6); `pow`, `clicks`, `achievements`, `store`, `config` (T7–T9); frozen Redis keys `counter:global`, `milestone:<threshold>`, `pow:L`, `pow:min_interval`, `stats:accepted_total`; AMQP exchange `events` (topic, durable), routing key `the-button.counter`, counter payload `{"type":"counter","total":N}`.
- Produces:
  - `ticker.Ticker{PGURL, Pool, RDB, Publish, Logger}` with `Run(ctx)` (every-replica 1s cached total + leader election) and `Total() (uint64, bool)`; advisory lock key `-7375648620393262386` (fnv1a64 of "the-button.tick"); milestone payload `{"type":"milestone","threshold":N,"title":"…"}` on the same channel; `milestone:<threshold>` Redis value = unix seconds of the claim (read back for `reached_at`).
  - `server.Server` implementing `algovn.button.v1.ButtonService`; IssueChallenge FAILS CLOSED (`Unavailable`) until the leader has written `pow:L`/`pow:min_interval` — no local defaults.
  - `cmd/the-button-service` full wiring: gRPC+health+reflection on `LISTEN_ADDR` (:9090), promhttp on `METRICS_ADDR` (:9091), graceful shutdown.

- [ ] **Step 1: Add dependencies**

```bash
cd /Users/duclm27/the-algovn/the-button-service
GOPROXY=direct go get github.com/the-algovn/protos/gen/go@v0.2.0
go get google.golang.org/protobuf github.com/google/uuid github.com/rabbitmq/amqp091-go github.com/prometheus/client_golang
go get github.com/testcontainers/testcontainers-go/modules/rabbitmq
```
Expected: `go: added github.com/the-algovn/protos/gen/go v0.2.0`, `go: added github.com/google/uuid v1.6.0`, `go: added github.com/rabbitmq/amqp091-go v1.12.x`, `go: added github.com/prometheus/client_golang v1.23.x` (patch versions as resolved).

- [ ] **Step 2: RabbitMQ test helper**

`internal/testutil/rabbit.go`:

```go
//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// StartRabbit runs rabbitmq:4.1-management-alpine and returns the AMQP URL.
func StartRabbit(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcrabbit.Run(ctx, "rabbitmq:4.1-management-alpine")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.AmqpURL(ctx)
	require.NoError(t, err)
	return url
}
```

- [ ] **Step 3: Write the failing ticker integration test (milestone exactly-once)**

`internal/ticker/ticker_integration_test.go`:

```go
//go:build integration

package ticker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

// Two replicas race the advisory lock; only one may claim + announce each
// milestone even while both loops run (spec §8: SETNX exactly-once claim).
func TestMilestone_ExactlyOnceAcrossTwoReplicas(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	amqpURL := testutil.StartRabbit(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// milestone-worthy durable state; counter:global is absent so the
	// leader must seed it from SUM(user_clicks)
	_, err = pool.Exec(ctx, `INSERT INTO user_clicks (user_sub, clicks) VALUES ('seed', 1500)`)
	require.NoError(t, err)

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("events", "topic", true, false, false, false, nil))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, "the-button.counter", "events", false, nil))
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	require.NoError(t, err)

	var mu sync.Mutex // amqp channels are not publish-concurrency-safe
	publish := func(channel string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		_ = ch.PublishWithContext(ctx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
	}

	// two replicas race for leadership on dedicated connections
	for range 2 {
		tk := &Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Publish: publish, Logger: logger}
		go tk.Run(ctx)
	}

	milestones, counters := 0, 0
	timeout := time.After(8 * time.Second)
collect:
	for {
		select {
		case m := <-msgs:
			var ev struct {
				Type      string `json:"type"`
				Threshold uint64 `json:"threshold"`
			}
			require.NoError(t, json.Unmarshal(m.Body, &ev))
			switch {
			case ev.Type == "milestone" && ev.Threshold == 1000:
				milestones++
			case ev.Type == "counter":
				counters++
			}
		case <-timeout:
			break collect
		}
	}
	require.Equal(t, 1, milestones, "milestone 1000 must be announced exactly once")
	require.GreaterOrEqual(t, counters, 1, "leader must publish the seeded total")
	// the claim persists and difficulty keys were initialized
	require.Equal(t, int64(1), rdb.Exists(ctx, "milestone:1000").Val())
	require.Equal(t, "1", rdb.Get(ctx, "pow:L").Val())
	require.Equal(t, "2", rdb.Get(ctx, "pow:min_interval").Val())
}
```

```bash
go vet -tags integration ./internal/ticker/
```
Expected: FAIL — `undefined: Ticker`. RED.

- [ ] **Step 4: Implement `internal/ticker/ticker.go`**

```go
// Package ticker runs the per-replica counter cache and the elected tick
// leader (spec §8): 1s counter publishes, milestone claims, the shared
// difficulty controller, and the hourly reconcile.
package ticker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// leaderLockKey identifies cluster-wide tick leadership:
// fnv1a64("the-button.tick") = 0x99a46ea8595c12ce as a signed bigint.
const leaderLockKey int64 = -7375648620393262386

const (
	tickInterval      = time.Second
	candidateInterval = 2 * time.Second // non-leaders poll the lock (spec §8: ~2s)
	demoteAfter       = 5 * time.Second // self-demote when the loop lags (spec §8)
	reconcileEvery    = time.Hour
	counterChannel    = "the-button.counter"
)

type Ticker struct {
	PGURL   string        // dedicated leader connection — never the pool
	Pool    *pgxpool.Pool // SUM fallback + reconcile
	RDB     *redis.Client
	Publish func(channel string, body []byte) // best-effort; nil disables publishing
	Logger  *slog.Logger

	total     atomic.Uint64
	haveTotal atomic.Bool
}

// Total returns the cached global counter and whether a value has been
// loaded yet. Correct from any pod, even with RabbitMQ or Redis down.
func (t *Ticker) Total() (uint64, bool) {
	return t.total.Load(), t.haveTotal.Load()
}

// Run starts the every-replica cache loop and the leader-election loop and
// blocks until ctx is done.
func (t *Ticker) Run(ctx context.Context) {
	go t.cacheLoop(ctx)
	t.leaderLoop(ctx)
}

func (t *Ticker) cacheLoop(ctx context.Context) {
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		if n, err := t.readTotal(ctx); err == nil {
			t.total.Store(n)
			t.haveTotal.Store(true)
		} else if ctx.Err() == nil {
			t.Logger.Warn("counter cache refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// readTotal prefers Redis and falls back to the durable SUM (spec §8).
func (t *Ticker) readTotal(ctx context.Context) (uint64, error) {
	if v, err := t.RDB.Get(ctx, "counter:global").Uint64(); err == nil {
		return v, nil
	}
	var sum int64
	if err := t.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		return 0, err
	}
	return uint64(sum), nil
}

// leaderLoop holds ONE dedicated non-pooled connection per attempt;
// leadership == that connection's health. Closing it releases the lock.
func (t *Ticker) leaderLoop(ctx context.Context) {
	for {
		conn, err := pgx.Connect(ctx, t.PGURL)
		if err == nil {
			var got bool
			if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&got); err == nil && got {
				t.Logger.Info("tick leadership acquired")
				t.lead(ctx, conn)
				t.Logger.Info("tick leadership released")
			}
			_ = conn.Close(context.WithoutCancel(ctx))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(candidateInterval):
		}
	}
}

func (t *Ticker) lead(ctx context.Context, conn *pgx.Conn) {
	// initialize controller state and make sure the shared difficulty keys
	// exist — IssueChallenge fails closed without them
	l := t.currentL(ctx)
	t.writeDifficulty(ctx, l)
	lastChange := time.Now()
	var ewma float64
	prevStats, _ := t.RDB.Get(ctx, "stats:accepted_total").Int64()
	prevSample := time.Now()

	var lastPublished int64 = -1
	lastTick := time.Now()
	nextReconcile := time.Now().Add(reconcileEvery)

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// self-demote when we could not keep up — closing the conn lets
		// another replica take over within ~2s
		if lag := time.Since(lastTick); lag > demoteAfter {
			t.Logger.Warn("tick loop lagged, demoting", "lag", lag)
			return
		}
		lastTick = time.Now()

		// the lock connection's health IS leadership
		if err := conn.Ping(ctx); err != nil {
			t.Logger.Warn("leader connection lost", "err", err)
			return
		}

		total, err := t.leaderTotal(ctx)
		if err != nil {
			t.Logger.Warn("leader total read failed", "err", err)
			continue
		}
		if int64(total) != lastPublished {
			t.publishJSON(counterChannel, map[string]any{"type": "counter", "total": total})
			t.claimMilestones(ctx, total)
			lastPublished = int64(total)
		}

		// controller: EWMA of accepted submits/s → NextL → shared keys
		if s, err := t.RDB.Get(ctx, "stats:accepted_total").Int64(); err == nil || errors.Is(err, redis.Nil) {
			now := time.Now()
			dt := now.Sub(prevSample)
			ewma = pow.EWMA(ewma, float64(s-prevStats)/dt.Seconds(), dt)
			prevStats, prevSample = s, now
			if next, ts := pow.NextL(l, ewma, lastChange, now); next != l {
				l, lastChange = next, ts
				t.writeDifficulty(ctx, l)
				t.Logger.Info("difficulty changed", "L", l, "ewma_rate", ewma)
			}
		}

		if time.Now().After(nextReconcile) {
			t.reconcile(ctx)
			nextReconcile = time.Now().Add(reconcileEvery)
		}
	}
}

// leaderTotal reads the hot counter, seeding it from the durable SUM when
// missing — with SETNX, never SET, so concurrent INCRBYs survive (spec §8).
func (t *Ticker) leaderTotal(ctx context.Context) (uint64, error) {
	v, err := t.RDB.Get(ctx, "counter:global").Uint64()
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, redis.Nil) {
		return 0, err
	}
	var sum int64
	if err := t.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		return 0, err
	}
	if err := t.RDB.SetNX(ctx, "counter:global", sum, 0).Err(); err != nil {
		return 0, err
	}
	return t.RDB.Get(ctx, "counter:global").Uint64()
}

// claimMilestones SETNX-claims every reached threshold and publishes only a
// won claim: exactly-once claim, at-most-once announcement (spec §8).
func (t *Ticker) claimMilestones(ctx context.Context, total uint64) {
	for _, m := range achievements.Milestones {
		if total < m.Threshold {
			return // Milestones are ascending
		}
		key := fmt.Sprintf("milestone:%d", m.Threshold)
		won, err := t.RDB.SetNX(ctx, key, time.Now().Unix(), 0).Result()
		if err != nil {
			t.Logger.Warn("milestone claim failed", "key", key, "err", err)
			return
		}
		if won {
			t.publishJSON(counterChannel, map[string]any{
				"type": "milestone", "threshold": m.Threshold, "title": m.Title,
			})
		}
	}
}

func (t *Ticker) writeDifficulty(ctx context.Context, l uint32) {
	if err := t.RDB.Set(ctx, "pow:L", l, 0).Err(); err != nil {
		t.Logger.Warn("write pow:L failed", "err", err)
	}
	if err := t.RDB.Set(ctx, "pow:min_interval", pow.MinInterval(l), 0).Err(); err != nil {
		t.Logger.Warn("write pow:min_interval failed", "err", err)
	}
}

// currentL restores the shared level across failovers, defaulting to MinL.
func (t *Ticker) currentL(ctx context.Context) uint32 {
	if v, err := t.RDB.Get(ctx, "pow:L").Int64(); err == nil && v >= pow.MinL && v <= pow.MaxL {
		return uint32(v)
	}
	return pow.MinL
}

// reconcile heals counter drift: INCRBY the delta, never SET — a SET would
// clobber concurrent increments (spec §8).
func (t *Ticker) reconcile(ctx context.Context) {
	var sum int64
	if err := t.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(clicks), 0) FROM user_clicks`).Scan(&sum); err != nil {
		t.Logger.Warn("reconcile SUM failed", "err", err)
		return
	}
	cur, err := t.RDB.Get(ctx, "counter:global").Int64()
	if err != nil {
		t.Logger.Warn("reconcile GET failed", "err", err)
		return
	}
	if drift := sum - cur; drift != 0 {
		t.Logger.Warn("counter drift healed", "drift", drift)
		if err := t.RDB.IncrBy(ctx, "counter:global", drift).Err(); err != nil {
			t.Logger.Warn("reconcile INCRBY failed", "err", err)
		}
	}
}

func (t *Ticker) publishJSON(channel string, v any) {
	if t.Publish == nil {
		return
	}
	body, _ := json.Marshal(v)
	t.Publish(channel, body)
}
```

- [ ] **Step 5: Run the ticker integration test**

```bash
cd /Users/duclm27/the-algovn/the-button-service
podman machine start 2>/dev/null || true
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
go test -tags integration ./internal/ticker/ -v -timeout 600s
```
Expected: `--- PASS: TestMilestone_ExactlyOnceAcrossTwoReplicas` (first run pulls rabbitmq — allow a few minutes). GREEN.

- [ ] **Step 6: Write the failing server tests (unit + end-to-end)**

`internal/server/server_test.go` (untagged — also provides `authCtx` for the integration file):

```go
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// authCtx forges the forwarded (gateway-verified) JWT: only segment 2 is
// read by the trust-model decode. Shared with the integration tests.
func authCtx(sub string) context.Context {
	payload, _ := json.Marshal(map[string]string{"sub": sub})
	tok := "h." + base64.RawURLEncoding.EncodeToString(payload) + ".s"
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+tok))
}

func TestSubFromContext(t *testing.T) {
	sub, err := subFromContext(authCtx("zitadel-user-42"))
	require.NoError(t, err)
	require.Equal(t, "zitadel-user-42", sub)
}

func TestSubFromContext_Rejects(t *testing.T) {
	// no metadata
	_, err := subFromContext(context.Background())
	require.Error(t, err)
	// not a JWT
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer nope"))
	_, err = subFromContext(ctx)
	require.Error(t, err)
	// bad segment-2 base64
	ctx = metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer h.!!!.s"))
	_, err = subFromContext(ctx)
	require.Error(t, err)
	// empty sub claim
	ctx = metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer h."+base64.RawURLEncoding.EncodeToString([]byte(`{}`))+".s"))
	_, err = subFromContext(ctx)
	require.Error(t, err)
}
```

`internal/server/server_integration_test.go`:

```go
//go:build integration

package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
	"github.com/the-algovn/the-button-service/internal/ticker"
)

func TestEndToEnd_SubmitTickPublishCounter(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	amqpURL := testutil.StartRabbit(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// real AMQP publisher + a queue bound to the counter channel
	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("events", "topic", true, false, false, false, nil))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, "the-button.counter", "events", false, nil))
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	require.NoError(t, err)
	publish := func(channel string, body []byte) {
		_ = ch.PublishWithContext(ctx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
	}

	tick := &ticker.Ticker{PGURL: pgURL, Pool: pool, RDB: rdb, Publish: publish, Logger: logger}
	go tick.Run(ctx)

	key := []byte("integration-test-key-0123456789a")
	srv := &Server{Pool: pool, RDB: rdb, Tick: tick, Logger: logger, W0: 4, Keys: [][]byte{key}}

	// fails closed until the leader writes pow:L / pow:min_interval
	require.Eventually(t, func() bool {
		_, err := srv.IssueChallenge(authCtx("user-1"), &buttonv1.IssueChallengeRequest{})
		return status.Code(err) != codes.Unavailable
	}, 15*time.Second, 200*time.Millisecond)

	chResp, err := srv.IssueChallenge(authCtx("user-1"), &buttonv1.IssueChallengeRequest{IntendedClicks: 25})
	require.NoError(t, err)
	require.EqualValues(t, pow.MaxBatch, chResp.MaxBatch)
	require.EqualValues(t, 4, chResp.WorkFactor)          // W0=4 × L=1
	require.EqualValues(t, 2, chResp.MinIntervalSeconds)  // ladder at L=1

	// anonymous callers cannot get challenges
	_, err = srv.IssueChallenge(context.Background(), &buttonv1.IssueChallengeRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	nonce := pow.Solve(chResp.Challenge, 4, 1, 25)
	sub, err := srv.SubmitClicks(authCtx("user-1"), &buttonv1.SubmitClicksRequest{
		Challenge: chResp.Challenge, Nonce: nonce, ClickCount: 25,
	})
	require.NoError(t, err)
	require.EqualValues(t, 25, sub.UserTotalClicks)
	require.NotNil(t, sub.NextChallenge, "next challenge must piggyback")
	require.NotEmpty(t, sub.Unlocked) // mvh + ten at least

	// bad work is rejected before touching state
	_, err = srv.SubmitClicks(authCtx("user-1"), &buttonv1.SubmitClicksRequest{
		Challenge: sub.NextChallenge.Challenge, Nonce: 0, ClickCount: 10_001,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// another sub cannot spend user-1's token
	_, err = srv.SubmitClicks(authCtx("user-2"), &buttonv1.SubmitClicksRequest{
		Challenge: sub.NextChallenge.Challenge, Nonce: 0, ClickCount: 1,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// the tick leader publishes the new total on the counter channel
	waitFor := func(wantTotal uint64) {
		t.Helper()
		deadline := time.After(15 * time.Second)
		for {
			select {
			case m := <-msgs:
				var ev struct {
					Type  string `json:"type"`
					Total uint64 `json:"total"`
				}
				require.NoError(t, json.Unmarshal(m.Body, &ev))
				if ev.Type == "counter" && ev.Total == wantTotal {
					return
				}
			case <-deadline:
				t.Fatalf("no counter publish for total=%d observed", wantTotal)
			}
		}
	}
	waitFor(25)

	// GetCounter serves the per-replica cached total
	require.Eventually(t, func() bool {
		resp, err := srv.GetCounter(context.Background(), &buttonv1.GetCounterRequest{})
		return err == nil && resp.Total == 25
	}, 5*time.Second, 200*time.Millisecond)

	// ListAchievements: personalized for user-1, bare for anonymous
	la, err := srv.ListAchievements(authCtx("user-1"), &buttonv1.ListAchievementsRequest{})
	require.NoError(t, err)
	require.Len(t, la.Catalog, 12)
	var mvhUnlocked bool
	for _, a := range la.Catalog {
		if a.Id == "mvh" {
			mvhUnlocked = a.UnlockedAt != nil
		}
	}
	require.True(t, mvhUnlocked)

	anon, err := srv.ListAchievements(context.Background(), &buttonv1.ListAchievementsRequest{})
	require.NoError(t, err)
	require.Len(t, anon.Catalog, 12)
	for _, a := range anon.Catalog {
		require.Nil(t, a.UnlockedAt)
	}
	require.Empty(t, anon.Milestones) // total 25 — nothing reached
}
```

```bash
go vet -tags integration ./internal/server/
```
Expected: FAIL — `undefined: Server`, `undefined: subFromContext`. RED.

- [ ] **Step 7: Implement `internal/server/server.go`**

```go
// Package server implements algovn.button.v1.ButtonService (spec §4, §6).
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/clicks"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// Totaler is the per-replica cached counter (ticker.Ticker).
type Totaler interface {
	Total() (uint64, bool)
}

type Server struct {
	buttonv1.UnimplementedButtonServiceServer
	Pool   *pgxpool.Pool
	RDB    *redis.Client
	Tick   Totaler
	Logger *slog.Logger
	W0     uint64
	Keys   [][]byte // [current] or [current, previous] — rotation window
}

// subFromContext does the read-only segment-2 decode of the forwarded JWT
// per authnz-conventions.md — the gateway already verified the signature
// and is the sole verified ingress.
func subFromContext(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", errors.New("no authorization metadata")
	}
	parts := strings.Split(strings.TrimPrefix(vals[0], "Bearer "), ".")
	if len(parts) != 3 {
		return "", errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("bad JWT payload")
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Sub == "" {
		return "", errors.New("bad claims")
	}
	return claims.Sub, nil
}

func (s *Server) GetCounter(context.Context, *buttonv1.GetCounterRequest) (*buttonv1.GetCounterResponse, error) {
	total, ok := s.Tick.Total()
	if !ok {
		return nil, status.Error(codes.Unavailable, "counter not warmed up")
	}
	return &buttonv1.GetCounterResponse{Total: total}, nil
}

// issue builds a signed challenge for sub from the shared difficulty keys.
// Fails closed: Redis miss/error → Unavailable — no local defaults, the
// leader owns pow:L / pow:min_interval (spec §5).
func (s *Server) issue(ctx context.Context, sub string) (*buttonv1.IssueChallengeResponse, error) {
	l, err := s.RDB.Get(ctx, "pow:L").Uint64()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "difficulty unavailable")
	}
	minInterval, err := s.RDB.Get(ctx, "pow:min_interval").Uint64()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "difficulty unavailable")
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, status.Error(codes.Internal, "uuid")
	}
	now := time.Now()
	p := pow.Payload{
		ID:           id.String(),
		Sub:          sub,
		Iat:          now.Unix(),
		Exp:          now.Add(pow.TokenTTL).Unix(),
		W0:           s.W0,
		L:            uint32(l),
		MinIntervalS: uint32(minInterval),
		MaxBatch:     pow.MaxBatch,
	}
	tok, err := pow.Sign(p, s.Keys[0])
	if err != nil {
		return nil, status.Error(codes.Internal, "sign")
	}
	return &buttonv1.IssueChallengeResponse{
		Challenge:          tok,
		WorkFactor:         s.W0 * l,
		MinIntervalSeconds: uint32(minInterval),
		MaxBatch:           pow.MaxBatch,
		ExpiresAt:          timestamppb.New(time.Unix(p.Exp, 0)),
	}, nil
}

func (s *Server) IssueChallenge(ctx context.Context, _ *buttonv1.IssueChallengeRequest) (*buttonv1.IssueChallengeResponse, error) {
	sub, err := subFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(ctx, sub)
}

func (s *Server) SubmitClicks(ctx context.Context, req *buttonv1.SubmitClicksRequest) (*buttonv1.SubmitClicksResponse, error) {
	sub, err := subFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	p, err := pow.Parse(req.GetChallenge(), s.Keys...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "bad challenge")
	}
	now := time.Now()
	if err := pow.Verify(p, sub, now); err != nil {
		if errors.Is(err, pow.ErrExpired) {
			return nil, status.Error(codes.FailedPrecondition, "challenge_expired")
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	count := req.GetClickCount()
	if count == 0 || count > p.MaxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "click_count must be 1..%d", p.MaxBatch)
	}
	if !pow.CheckWork(req.GetChallenge(), p.W0, p.L, count, req.GetNonce()) {
		return nil, status.Error(codes.InvalidArgument, "bad proof of work")
	}

	res, err := clicks.Submit(ctx, s.RDB, s.Pool, s.Logger, p, count, now)
	if err != nil {
		return nil, err
	}

	resp := &buttonv1.SubmitClicksResponse{UserTotalClicks: res.UserTotal}
	for _, u := range res.Unlocked {
		resp.Unlocked = append(resp.Unlocked, &buttonv1.Achievement{
			Id:          u.Achievement.ID,
			Title:       u.Achievement.Title,
			Description: u.Achievement.Description,
			UnlockedAt:  timestamppb.New(u.UnlockedAt),
		})
	}
	// piggyback the next challenge; issuance failure must not void the
	// accepted batch (spec §6 step 5)
	if next, err := s.issue(ctx, sub); err == nil {
		resp.NextChallenge = next
	} else {
		s.Logger.Warn("piggyback issue failed", "err", err)
	}
	return resp, nil
}

func (s *Server) ListAchievements(ctx context.Context, _ *buttonv1.ListAchievementsRequest) (*buttonv1.ListAchievementsResponse, error) {
	resp := &buttonv1.ListAchievementsResponse{}

	// personalization is opportunistic: only when a forwarded token parses
	// (anonymous rule — the header arrives verified when present, spec §4)
	unlocked := map[string]time.Time{}
	if sub, err := subFromContext(ctx); err == nil {
		rows, err := s.Pool.Query(ctx,
			`SELECT achievement_id, unlocked_at FROM user_achievements WHERE user_sub = $1`, sub)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "postgres unavailable")
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var at time.Time
			if err := rows.Scan(&id, &at); err != nil {
				return nil, status.Error(codes.Internal, "scan")
			}
			unlocked[id] = at
		}
		if rows.Err() != nil {
			return nil, status.Error(codes.Unavailable, "postgres unavailable")
		}
	}

	for _, a := range achievements.Catalog {
		pa := &buttonv1.Achievement{Id: a.ID, Title: a.Title, Description: a.Description}
		if at, ok := unlocked[a.ID]; ok {
			pa.UnlockedAt = timestamppb.New(at)
		}
		resp.Catalog = append(resp.Catalog, pa)
	}

	// reached milestones from Redis; Redis down → bare catalog still served
	keys := make([]string, len(achievements.Milestones))
	for i, m := range achievements.Milestones {
		keys[i] = fmt.Sprintf("milestone:%d", m.Threshold)
	}
	vals, err := s.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		s.Logger.Warn("milestone read failed", "err", err)
		return resp, nil
	}
	for i, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue // not reached
		}
		ts, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			continue
		}
		m := achievements.Milestones[i]
		resp.Milestones = append(resp.Milestones, &buttonv1.Milestone{
			Threshold: m.Threshold,
			Title:     m.Title,
			ReachedAt: timestamppb.New(time.Unix(ts, 0)),
		})
	}
	return resp, nil
}
```

- [ ] **Step 8: Replace `cmd/the-button-service/main.go` with the full wiring**

```go
// the-button-service: PoW-gated global click counter. See docs/superpowers/specs.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/config"
	"github.com/the-algovn/the-button-service/internal/server"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/ticker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.NewPG(ctx, cfg.PGURL)
	if err != nil {
		logger.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := store.NewRedis(ctx, cfg.RedisURL)
	if err != nil {
		logger.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	var publish func(string, []byte)
	if cfg.AMQPURL != "" {
		publish = newPublisher(ctx, cfg.AMQPURL, logger)
	} else {
		logger.Warn("AMQP_URL not set; counter events will not publish")
	}

	tick := &ticker.Ticker{
		PGURL: cfg.PGURL, Pool: pool, RDB: rdb,
		Publish: publish, Logger: logger,
	}
	go tick.Run(ctx)

	keys := [][]byte{cfg.PowSecret}
	if cfg.PowSecretPrev != nil {
		keys = append(keys, cfg.PowSecretPrev)
	}
	srv := &server.Server{
		Pool: pool, RDB: rdb, Tick: tick, Logger: logger,
		W0: cfg.PowW0, Keys: keys,
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("listen failed", "err", err)
		os.Exit(1)
	}
	gs := grpc.NewServer()
	buttonv1.RegisterButtonServiceServer(gs, srv)
	healthpb.RegisterHealthServer(gs, health.NewServer())
	reflection.Register(gs)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = (&http.Server{Addr: cfg.MetricsAddr, Handler: mux}).ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	logger.Info("the-button-service listening", "addr", cfg.ListenAddr)
	if err := gs.Serve(lis); err != nil {
		logger.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

// newPublisher returns a fire-and-forget AMQP publish func; failures are
// logged, never fatal — events are best-effort by design. (Pattern copied
// from api-control-plane's demo-service.)
func newPublisher(ctx context.Context, url string, logger *slog.Logger) func(string, []byte) {
	type conn struct {
		ch *amqp.Channel
		c  *amqp.Connection
	}
	var mu sync.Mutex // ticker + future callers may publish concurrently
	var cur *conn
	dial := func() *conn {
		c, err := amqp.Dial(url)
		if err != nil {
			logger.Warn("amqp dial failed", "err", err)
			return nil
		}
		ch, err := c.Channel()
		if err != nil {
			_ = c.Close()
			return nil
		}
		if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
			_ = c.Close()
			return nil
		}
		return &conn{ch: ch, c: c}
	}
	return func(channel string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		if cur == nil || cur.c.IsClosed() || cur.ch.IsClosed() {
			cur = dial()
			if cur == nil {
				return
			}
		}
		pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		err := cur.ch.PublishWithContext(pubCtx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
		if err != nil {
			logger.Warn("publish failed", "channel", channel, "err", err)
			cur = nil
		}
	}
}
```

- [ ] **Step 9: Run everything**

```bash
cd /Users/duclm27/the-algovn/the-button-service
go build ./...
go vet ./...
go test ./...
podman machine start 2>/dev/null || true
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
go test -tags integration ./internal/server/ -v -timeout 600s
go vet -tags integration ./...
```
Expected: unit suite green (config, pow, achievements, server); `--- PASS: TestEndToEnd_SubmitTickPublishCounter`; vet clean under both tag sets. GREEN.

- [ ] **Step 10: Commit (two focused commits)**

```bash
cd /Users/duclm27/the-algovn/the-button-service
git add internal/ticker/ internal/testutil/rabbit.go go.mod go.sum
git commit -m "Add tick leader with counter cache, milestones, and reconcile"
git add internal/server/ cmd/the-button-service/main.go
git commit -m "Add ButtonService gRPC server and full service wiring"
```

---

### Task 11: Dockerfile + CI + repo publish (USER-GATED) + image

**Repo:** `/Users/duclm27/the-algovn/the-button-service`
**Spec:** skeleton global constraints (amd64-only, ghcr.io, acp CI pattern)

**Files:**
- Create: `Dockerfile`
- Create: `.github/workflows/build.yaml`

**Interfaces:**
- Produces (frozen): image `ghcr.io/the-algovn/the-button-service` with tags `main` + `sha-<short>` (+semver on tags); GitHub repo `the-algovn/the-button-service` (public); GHCR package stays PRIVATE — pull secrets are Task 18's job.

- [ ] **Step 1: Write the Dockerfile (acp pattern, single binary, distroless, amd64)**

`Dockerfile`:

```dockerfile
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-service ./cmd/the-button-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/the-button-service /the-button-service
ENTRYPOINT ["/the-button-service"]
```

- [ ] **Step 2: Verify the build locally**

```bash
cd /Users/duclm27/the-algovn/the-button-service
podman machine start 2>/dev/null || true
podman build -t the-button-service:dev .
```
Expected: `COMMIT the-button-service:dev` — image builds (build-only smoke; the amd64 binary is not executed on the arm64 VM). Also verify the entrypoint behavior natively:

```bash
go run ./cmd/the-button-service
echo "exit=$?"
```
Expected: one JSON log line `{"level":"ERROR","msg":"config","err":"PG_URL is required"}` and `exit=1` (fail-fast config validation).

- [ ] **Step 3: Write the CI workflow (copy of api-control-plane's, test job gates build)**

`.github/workflows/build.yaml`:

```yaml
name: build
on:
  push:
    branches: [main]
    tags: ["v*.*.*"]
permissions:
  contents: read
  packages: write
jobs:
  test:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - run: go vet ./...
      - run: go test ./... -race
  build:
    needs: test
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/metadata-action@v5
        id: meta
        with:
          images: ghcr.io/${{ github.repository }}
          tags: |
            type=semver,pattern={{version}}
            type=sha,prefix=sha-
            type=raw,value=main,enable={{is_default_branch}}
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

(Integration tests are excluded automatically: they carry the `integration` build tag and CI runs plain `go test ./... -race`.)

- [ ] **Step 4: Commit**

```bash
cd /Users/duclm27/the-algovn/the-button-service
git add Dockerfile .github/workflows/build.yaml
git commit -m "Add Dockerfile and CI build workflow"
```

- [ ] **Step 5: USER GATE — publish the repo**

Controller: ask the user — "Ready to publish `the-algovn/the-button-service` as a PUBLIC GitHub repo and push all commits? (GHCR package will stay private.)" Proceed only on explicit yes, then run:

```bash
cd /Users/duclm27/the-algovn/the-button-service
gh repo create the-algovn/the-button-service --public --source . --push
```
Expected: `✓ Created repository the-algovn/the-button-service on GitHub`, remote `origin` added, `main` pushed.

- [ ] **Step 6: Watch CI and verify the image**

```bash
run_id=$(gh run list --repo the-algovn/the-button-service --workflow build --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch --repo the-algovn/the-button-service --exit-status "$run_id"
```
Expected: jobs `test` then `build` both complete with `success`.

```bash
gh api /orgs/the-algovn/packages/container/the-button-service/versions --jq '.[].metadata.container.tags[]'
```
Expected: `main` and `sha-<short-sha>` tags listed. Do NOT change package visibility — it stays private; the `the-button` namespace gets sealed registry-creds in Task 18.
### Task 12: Vite SPA scaffold in the web monorepo (`apps/the-button`)

**Files:**
- Create: `apps/the-button/` via `pnpm create vite` (template `react-ts`), then replace: `apps/the-button/package.json`, `apps/the-button/index.html`, `apps/the-button/vite.config.ts`, `apps/the-button/tsconfig.json`, `apps/the-button/eslint.config.js`, `apps/the-button/.gitignore`, `apps/the-button/src/vite-env.d.ts`, `apps/the-button/src/index.css`, `apps/the-button/src/main.tsx`, `apps/the-button/src/App.tsx`
- Create: `apps/the-button/vitest.config.mts`, `apps/the-button/vitest.setup.ts`, `apps/the-button/src/__tests__/app.test.tsx`
- Delete (template leftovers): `apps/the-button/tsconfig.app.json`, `apps/the-button/tsconfig.node.json`, `apps/the-button/README.md`, `apps/the-button/public/`, `apps/the-button/src/App.css`, `apps/the-button/src/assets/`
- Edit: `turbo.json` (repo root: `dist/**` output + `VITE_*` build env)

**Interfaces:**
- Consumes: web monorepo workspace (`apps/*` in `pnpm-workspace.yaml`), `@algovn/ui` exports map (`./globals.css`, `./theme-provider`, subpath components), `@algovn/config` tsconfig/eslint, Tailwind v4.
- Produces: workspace app named `the-button` (frozen), Vite + React 19 + TS, `base: '/the-button/'` (frozen), dev server on `:5173` (Zitadel dev redirect URI depends on this port), scripts `dev|build|preview|lint|typecheck|test`, vitest harness that later tasks (T13–T16) extend. React/react-dom pinned `^19.2.7` per web README.

This is the FIRST Vite app in a repo whose README "Adding an app" flow is Next-specific. The honest adaptation: same workspace wiring, same `@algovn/ui` globals import + `@source` for the ui package, ThemeProvider + Geist fonts like showcase — but fonts come from `@fontsource-variable` packages (no `next/font` outside Next), Tailwind runs through `@tailwindcss/vite` (no PostCSS config), and eslint uses `@algovn/config/eslint/react` (no `eslint-config-next`).

- [ ] **Step 1: Scaffold with create-vite and delete the artifacts that would shadow the workspace**

```bash
cd /Users/duclm27/the-algovn/web
printf 'n\n' | pnpm create vite apps/the-button --template react-ts
# clean anything the scaffolder may drop that would shadow the root workspace
# (same warning as the README gives for create-next-app; harmless if absent)
rm -rf apps/the-button/node_modules apps/the-button/pnpm-lock.yaml \
       apps/the-button/.git apps/the-button/pnpm-workspace.yaml \
       apps/the-button/README.md apps/the-button/public
ls apps/the-button
```

Expected: scaffolder prints `Scaffolding project in /Users/duclm27/the-algovn/web/apps/the-button...` then `Done.` (the piped `n` declines any "install and start now?" prompt). `ls` shows `eslint.config.js index.html package.json src tsconfig.app.json tsconfig.json tsconfig.node.json vite.config.ts` (file set may vary slightly by create-vite version — every file listed below gets overwritten or deleted, so drift is fine).

- [ ] **Step 2: Declare the Vite build in root turbo.json**

Replace `/Users/duclm27/the-algovn/web/turbo.json` with:

```json
{
  "$schema": "https://turbo.build/schema.json",
  "tasks": {
    "build": {
      "dependsOn": ["^build"],
      "env": [
        "VITE_OIDC_AUTHORITY",
        "VITE_OIDC_CLIENT_ID",
        "VITE_API_BASE",
        "VITE_EVENTS_URL"
      ],
      "outputs": [".next/**", "!.next/cache/**", "dist/**"]
    },
    "lint": { "dependsOn": ["^lint"] },
    "typecheck": { "dependsOn": ["^typecheck"] },
    "test": { "dependsOn": ["^test"] },
    "dev": { "cache": false, "persistent": true }
  }
}
```

(`dist/**` is the Vite output; the `env` list makes turbo's build cache key on the baked-in `VITE_*` vars and satisfies `turbo/no-undeclared-env-vars` when `import.meta.env.VITE_*` appears in code.)

- [ ] **Step 3: Replace the app's config files**

`apps/the-button/package.json` (versions pinned to what `packages/ui`/showcase already resolve, so pnpm dedupes; react range matches `packages/ui` peers per README):

```json
{
  "name": "the-button",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite --strictPort",
    "build": "vite build",
    "preview": "vite preview",
    "lint": "eslint . --max-warnings 0",
    "typecheck": "tsc --noEmit",
    "test": "vitest run"
  },
  "dependencies": {
    "@algovn/ui": "workspace:*",
    "@fontsource-variable/geist": "^5.2.9",
    "@fontsource-variable/geist-mono": "^5.2.8",
    "react": "^19.2.7",
    "react-dom": "^19.2.7"
  },
  "devDependencies": {
    "@algovn/config": "workspace:*",
    "@tailwindcss/vite": "^4.3.2",
    "@testing-library/jest-dom": "^6.9.1",
    "@testing-library/react": "^16.3.2",
    "@testing-library/user-event": "^14.6.1",
    "@types/node": "^24",
    "@types/react": "^19",
    "@types/react-dom": "^19",
    "@vitejs/plugin-react": "^6.0.3",
    "eslint": "^10.7.0",
    "jsdom": "^29.1.1",
    "tailwindcss": "^4.3.2",
    "typescript": "^6.0.3",
    "vite": "^8.1.4",
    "vitest": "^4.1.10"
  }
}
```

(`--strictPort` guarantees `:5173` — the dev OIDC redirect URI `http://localhost:5173/the-button/callback` is registered in Zitadel by T19 and must not silently drift to 5174.)

`apps/the-button/tsconfig.json` (replaces the template's three tsconfigs — one flat config like `packages/ui`):

```json
{
  "extends": "@algovn/config/tsconfig/react-library",
  "include": ["src", "vite.config.ts", "vitest.config.mts", "vitest.setup.ts"]
}
```

```bash
rm /Users/duclm27/the-algovn/web/apps/the-button/tsconfig.app.json \
   /Users/duclm27/the-algovn/web/apps/the-button/tsconfig.node.json
```

`apps/the-button/eslint.config.js`:

```js
import react from "@algovn/config/eslint/react"

// eslint-plugin-react's "detect" version lookup crashes on ESLint 10
// (same workaround as packages/ui/eslint.config.js) — pin the version.
export default react.map(config => {
  if (config.settings?.react?.version === "detect") {
    return {
      ...config,
      settings: {
        ...config.settings,
        react: { ...config.settings.react, version: "19.0.0" },
      },
    }
  }
  return config
})
```

`apps/the-button/.gitignore`:

```
node_modules
dist
```

`apps/the-button/vite.config.ts`:

```ts
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  base: "/the-button/",
  plugins: [react(), tailwindcss()],
})
```

`apps/the-button/vitest.config.mts` (same shape as `packages/ui/vitest.config.mts`, minus the paths plugin — this app uses plain relative imports):

```ts
import react from "@vitejs/plugin-react"
import { defineConfig } from "vitest/config"

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    setupFiles: ["./vitest.setup.ts"],
  },
})
```

`apps/the-button/vitest.setup.ts`:

```ts
import "@testing-library/jest-dom/vitest"
import { cleanup } from "@testing-library/react"
import { afterEach } from "vitest"

// Explicit cleanup: test.globals is off, so Testing Library's auto-cleanup
// doesn't self-register (same rationale as packages/ui/vitest.setup.ts).
afterEach(() => {
  cleanup()
})

// Restore jsdom's storage implementations shadowed by Node's global accessors
// (same workaround as packages/ui/vitest.setup.ts) — next-themes and
// oidc-client-ts touch localStorage/sessionStorage.
const jsdomWindow = (globalThis as { jsdom?: { window: Window } }).jsdom?.window
if (jsdomWindow) {
  window.localStorage ??= jsdomWindow.localStorage
  window.sessionStorage ??= jsdomWindow.sessionStorage
}

window.matchMedia ??= ((query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: () => {},
  removeListener: () => {},
  addEventListener: () => {},
  removeEventListener: () => {},
  dispatchEvent: () => false,
})) as typeof window.matchMedia

// Tests never hit the network: individual tests stub their own responses
// with vi.stubGlobal("fetch", ...).
globalThis.fetch = () => Promise.reject(new Error("network disabled in tests"))

// jsdom implements neither EventSource nor Worker; units under test receive
// injected fakes — these stubs only keep incidental construction from throwing.
class EventSourceStub {
  onopen: (() => void) | null = null
  onmessage: ((e: MessageEvent) => void) | null = null
  onerror: (() => void) | null = null
  close() {}
}
globalThis.EventSource ??= EventSourceStub as unknown as typeof EventSource

class WorkerStub {
  onmessage: ((e: MessageEvent) => void) | null = null
  postMessage() {}
  terminate() {}
}
globalThis.Worker ??= WorkerStub as unknown as typeof Worker
```

- [ ] **Step 4: Replace the app entry files**

`apps/the-button/index.html` (class `dark` up front = no light-mode flash; `font-sans antialiased` on body mirrors showcase's layout.tsx):

```html
<!doctype html>
<html lang="en" class="dark">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>the button</title>
  </head>
  <body class="font-sans antialiased">
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`apps/the-button/src/vite-env.d.ts`:

```ts
/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_OIDC_AUTHORITY?: string
  readonly VITE_OIDC_CLIENT_ID?: string
  readonly VITE_API_BASE?: string
  readonly VITE_EVENTS_URL?: string
}
```

`apps/the-button/src/index.css` (the README's globals wiring, adapted: css sits at `src/`, so the ui package is three levels up):

```css
@import "@algovn/ui/globals.css";
@source "../../../packages/ui/src";

:root {
  /* @algovn/ui's @theme maps --font-sans/--font-mono to these variables;
     Next apps set them via next/font — here they come from @fontsource. */
  --font-geist-sans: "Geist Variable";
  --font-geist-mono: "Geist Mono Variable";
}
```

`apps/the-button/src/main.tsx`:

```tsx
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { ThemeProvider } from "@algovn/ui/theme-provider"
import App from "./App"
import "@fontsource-variable/geist"
import "@fontsource-variable/geist-mono"
import "./index.css"

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider>
      <App />
    </ThemeProvider>
  </StrictMode>
)
```

- [ ] **Step 5: Install workspace deps**

```bash
cd /Users/duclm27/the-algovn/web
pnpm install
```

Expected: exit 0, lockfile updated with the new importer `apps/the-button`. A warning about `@algovn/ui`'s unmet peer `next` is expected and fine — this app never imports the Next-only components (`app-shell`, `command-menu`); only subpath imports are used.

- [ ] **Step 6: RED — smoke test expects the real page, template still renders Vite boilerplate**

`apps/the-button/src/__tests__/app.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react"
import { expect, it } from "vitest"
import App from "../App"

it("renders the page heading", () => {
  render(<App />)
  expect(screen.getByRole("heading", { name: "the button" })).toBeInTheDocument()
})
```

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button test
```

Expected: FAIL — `Unable to find an accessible element with the role "heading" and name "the button"` (the template App renders "Vite + React").

- [ ] **Step 7: GREEN — replace App.tsx, delete template leftovers**

```bash
rm -rf /Users/duclm27/the-algovn/web/apps/the-button/src/App.css \
       /Users/duclm27/the-algovn/web/apps/the-button/src/assets
```

`apps/the-button/src/App.tsx`:

```tsx
export default function App() {
  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-6 p-6 text-center">
      <h1 className="font-mono text-3xl font-semibold tracking-tight sm:text-4xl">the button</h1>
      <p className="text-muted-foreground text-sm">every click is a tiny rebellion</p>
    </main>
  )
}
```

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button test
```

Expected: PASS (1 test).

- [ ] **Step 8: Verify lint, typecheck, turbo build and the dev server**

```bash
cd /Users/duclm27/the-algovn/web
pnpm turbo lint typecheck test build --filter=the-button
```

Expected: all four tasks succeed; build prints `✓ built in …` with `dist/index.html` and `dist/assets/index-*.js` emitted.

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button dev >/tmp/tb-dev.log 2>&1 &
DEV_PID=$!
sleep 4
curl -s http://localhost:5173/the-button/ | grep -o '<title>the button</title>'
kill $DEV_PID
```

Expected: prints `<title>the button</title>` (Vite serves under the `/the-button/` base on strict port 5173).

Also run the full-repo gate to prove showcase/landing/ui are untouched:

```bash
cd /Users/duclm27/the-algovn/web && pnpm turbo lint typecheck test build
```

Expected: all packages pass.

- [ ] **Step 9: Commit**

```bash
cd /Users/duclm27/the-algovn/web
git add apps/the-button turbo.json pnpm-lock.yaml
git commit -m "feat(the-button): scaffold vite react app wired to @algovn/ui"
```

---

### Task 13: Auth (Zitadel PKCE, in-memory tokens) + control-plane API client

**Files:**
- Create: `apps/the-button/src/lib/env.ts`, `apps/the-button/src/lib/api.ts`, `apps/the-button/src/lib/auth.ts`, `apps/the-button/src/lib/use-auth.ts`, `apps/the-button/src/components/callback.tsx`
- Create: `apps/the-button/src/lib/__tests__/api.test.ts`, `apps/the-button/src/lib/__tests__/auth.test.ts`
- Edit: `apps/the-button/src/App.tsx` (callback route via conditional render + sign-in CTA), `apps/the-button/src/__tests__/app.test.tsx`, `apps/the-button/package.json` (add `oidc-client-ts`)

**Interfaces:**
- Consumes (frozen): `VITE_OIDC_AUTHORITY=https://id.algovn.com`, `VITE_OIDC_CLIENT_ID`, `VITE_API_BASE=https://api.algovn.com/the-button`; acp JSON convention `POST <prefix>/algovn.button.v1.ButtonService/<Method>` with error body `{code,message}`; T3's HTTP mapping `ResourceExhausted→429 (+Retry-After: 2)`, `AlreadyExists→409`, `FailedPrecondition→400`; redirect URIs `https://algovn.com/the-button/callback` + `http://localhost:5173/the-button/callback` (registered by T19); protojson wire shape (camelCase field names, uint64 as decimal strings, RFC3339 timestamps, zero-valued fields omitted).
- Produces: `postRpc(method, body, token?)` + typed wrappers `getCounter/listAchievements/issueChallenge/submitClicks`; `ApiError` with `isRateLimited/isReplay/isExpiredChallenge` discrimination (consumed by T15's batcher and T14's polling); `userManager`/`signIn`/`completeSignIn`/`useAuth` with the user held in memory only.

- [ ] **Step 1: Add oidc-client-ts**

In `apps/the-button/package.json` add to `"dependencies"` (keep alphabetical order):

```json
    "oidc-client-ts": "^3.5.0",
```

so dependencies read: `@algovn/ui`, `@fontsource-variable/geist`, `@fontsource-variable/geist-mono`, `oidc-client-ts`, `react`, `react-dom`. Then:

```bash
cd /Users/duclm27/the-algovn/web && pnpm install
```

Expected: exit 0, `oidc-client-ts 3.5.x` added.

- [ ] **Step 2: RED — API client error-mapping tests**

`apps/the-button/src/lib/__tests__/api.test.ts`:

```ts
import { afterEach, expect, it, vi } from "vitest"
import {
  ApiError,
  isExpiredChallenge,
  isRateLimited,
  isReplay,
  postRpc,
} from "../api"

function mockFetch(status: number, body: unknown, headers: Record<string, string> = {}) {
  const payload = typeof body === "string" ? body : JSON.stringify(body)
  const fetchMock = vi.fn().mockResolvedValue(
    new Response(payload, { status, headers: { "Content-Type": "application/json", ...headers } })
  )
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

afterEach(() => {
  vi.unstubAllGlobals()
})

it("POSTs JSON to the ButtonService method URL with a bearer token", async () => {
  const fetchMock = mockFetch(200, { total: "7" })
  const res = await postRpc<{ total?: string }>("GetCounter", {}, "tok123")
  expect(res.total).toBe("7")
  expect(fetchMock).toHaveBeenCalledWith(
    "https://api.algovn.com/the-button/algovn.button.v1.ButtonService/GetCounter",
    expect.objectContaining({
      method: "POST",
      headers: expect.objectContaining({
        "Content-Type": "application/json",
        Authorization: "Bearer tok123",
      }),
    })
  )
})

it("omits the Authorization header without a token", async () => {
  const fetchMock = mockFetch(200, {})
  await postRpc("GetCounter", {})
  const init = fetchMock.mock.calls[0]![1] as RequestInit
  expect(init.headers).not.toHaveProperty("Authorization")
})

it("maps 429 with Retry-After to a rate-limited ApiError", async () => {
  mockFetch(429, { code: "ResourceExhausted", message: "slow down" }, { "Retry-After": "5" })
  const err = await postRpc("SubmitClicks", {}).catch((e: unknown) => e)
  expect(isRateLimited(err)).toBe(true)
  expect((err as ApiError).retryAfterSeconds).toBe(5)
  expect((err as ApiError).code).toBe("ResourceExhausted")
})

it("defaults Retry-After to 2s when the header is unreadable (CORS)", async () => {
  mockFetch(429, { code: "ResourceExhausted", message: "slow down" })
  const err = await postRpc("SubmitClicks", {}).catch((e: unknown) => e)
  expect((err as ApiError).retryAfterSeconds).toBe(2)
})

it("maps 409 to a replay error, distinct from rate limiting", async () => {
  mockFetch(409, { code: "AlreadyExists", message: "challenge replayed" })
  const err = await postRpc("SubmitClicks", {}).catch((e: unknown) => e)
  expect(isReplay(err)).toBe(true)
  expect(isRateLimited(err)).toBe(false)
  expect(isExpiredChallenge(err)).toBe(false)
})

it("maps 400 FailedPrecondition to an expired-challenge error", async () => {
  mockFetch(400, { code: "FailedPrecondition", message: "challenge_expired" })
  const err = await postRpc("SubmitClicks", {}).catch((e: unknown) => e)
  expect(isExpiredChallenge(err)).toBe(true)
})

it("does not treat other 400s as expired challenges", async () => {
  mockFetch(400, { code: "InvalidArgument", message: "bad work" })
  const err = await postRpc("SubmitClicks", {}).catch((e: unknown) => e)
  expect(isExpiredChallenge(err)).toBe(false)
  expect((err as ApiError).status).toBe(400)
})

it("survives non-JSON error bodies (edge/proxy HTML)", async () => {
  mockFetch(502, "<html>bad gateway</html>")
  const err = await postRpc("GetCounter", {}).catch((e: unknown) => e)
  expect(err).toBeInstanceOf(ApiError)
  expect((err as ApiError).status).toBe(502)
  expect((err as ApiError).code).toBe("unknown")
  expect((err as ApiError).message).toBe("HTTP 502")
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../api'` (or equivalent resolve error).

- [ ] **Step 3: GREEN — env.ts and api.ts**

`apps/the-button/src/lib/env.ts`:

```ts
// Build-time configuration. Production values are baked in as defaults so a
// plain `pnpm build` and the Docker image work without a .env file (the repo
// .gitignore excludes .env*). Any VITE_* var set at build time overrides.
export const env = {
  oidcAuthority: import.meta.env.VITE_OIDC_AUTHORITY ?? "https://id.algovn.com",
  // Set after Zitadel onboarding (T19); empty means sign-in cannot start yet.
  oidcClientId: import.meta.env.VITE_OIDC_CLIENT_ID ?? "",
  apiBase: import.meta.env.VITE_API_BASE ?? "https://api.algovn.com/the-button",
  eventsUrl: import.meta.env.VITE_EVENTS_URL ?? "https://api.algovn.com/events/the-button.counter",
}
```

`apps/the-button/src/lib/api.ts`:

```ts
// Thin client for the control-plane JSON convention:
// POST <VITE_API_BASE>/algovn.button.v1.ButtonService/<Method>.
// Responses are protojson: camelCase fields, uint64 as decimal strings,
// google.protobuf.Timestamp as RFC3339 strings, zero-valued fields omitted.
import { env } from "./env"

export interface Achievement {
  id?: string
  title?: string
  description?: string
  unlockedAt?: string
}

export interface Milestone {
  threshold?: string
  title?: string
  reachedAt?: string
}

export interface GetCounterResponse {
  total?: string
}

export interface IssueChallengeResponse {
  challenge?: string
  workFactor?: string
  minIntervalSeconds?: number
  maxBatch?: number
  expiresAt?: string
}

export interface SubmitClicksRequest {
  challenge: string
  nonce: string
  clickCount: number
}

export interface SubmitClicksResponse {
  userTotalClicks?: string
  unlocked?: Achievement[]
  nextChallenge?: IssueChallengeResponse
}

export interface ListAchievementsResponse {
  catalog?: Achievement[]
  milestones?: Milestone[]
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly retryAfterSeconds?: number
  ) {
    super(message)
    this.name = "ApiError"
  }
}

// Error discrimination per the acp mapping (launch-blocker additions, spec §6):
// ResourceExhausted→429 (+Retry-After), AlreadyExists→409, FailedPrecondition→400.
export const isRateLimited = (err: unknown): err is ApiError =>
  err instanceof ApiError && err.status === 429
export const isReplay = (err: unknown): err is ApiError =>
  err instanceof ApiError && err.status === 409
export const isExpiredChallenge = (err: unknown): err is ApiError =>
  err instanceof ApiError && err.status === 400 && err.code === "FailedPrecondition"

export async function postRpc<T>(method: string, body: unknown, token?: string): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" }
  if (token) headers.Authorization = `Bearer ${token}`
  const res = await fetch(`${env.apiBase}/algovn.button.v1.ButtonService/${method}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  })
  if (res.ok) return (await res.json()) as T

  let code = "unknown"
  let message = `HTTP ${res.status}`
  try {
    const errBody = (await res.json()) as { code?: string; message?: string }
    if (errBody.code) code = errBody.code
    if (errBody.message) message = errBody.message
  } catch {
    // non-JSON error body (edge/proxy HTML) — keep defaults
  }
  let retryAfterSeconds: number | undefined
  if (res.status === 429) {
    // Retry-After is not CORS-safelisted, so the browser may hide it;
    // fall back to the server's fixed 2s.
    const parsed = Number(res.headers.get("Retry-After"))
    retryAfterSeconds = Number.isFinite(parsed) && parsed > 0 ? parsed : 2
  }
  throw new ApiError(res.status, code, message, retryAfterSeconds)
}

export const getCounter = () => postRpc<GetCounterResponse>("GetCounter", {})

export const listAchievements = (token?: string) =>
  postRpc<ListAchievementsResponse>("ListAchievements", {}, token)

export const issueChallenge = (intendedClicks: number, token: string) =>
  postRpc<IssueChallengeResponse>("IssueChallenge", { intendedClicks }, token)

export const submitClicks = (req: SubmitClicksRequest, token: string) =>
  postRpc<SubmitClicksResponse>("SubmitClicks", req, token)
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: PASS (all api tests + the app smoke test).

- [ ] **Step 4: RED — auth settings tests**

`apps/the-button/src/lib/__tests__/auth.test.ts`:

```ts
import { expect, it } from "vitest"
import { User } from "oidc-client-ts"
import { createUserManager } from "../auth"

it("builds PKCE settings from the origin and the Zitadel authority", () => {
  const um = createUserManager("https://algovn.com")
  expect(um.settings.authority).toBe("https://id.algovn.com")
  expect(um.settings.redirect_uri).toBe("https://algovn.com/the-button/callback")
  expect(um.settings.post_logout_redirect_uri).toBe("https://algovn.com/the-button/")
  expect(um.settings.response_type).toBe("code")
  expect(um.settings.scope).toBe("openid profile")
})

it("keeps the signed-in user in memory only — never web storage", async () => {
  const um = createUserManager("http://localhost:5173")
  window.sessionStorage.clear()
  window.localStorage.clear()
  await um.storeUser(
    new User({
      access_token: "tok",
      token_type: "Bearer",
      profile: { sub: "user-1", iss: "https://id.algovn.com", aud: "app", exp: 0, iat: 0 },
    })
  )
  expect(window.sessionStorage.length).toBe(0)
  expect(window.localStorage.length).toBe(0)
  const stored = await um.getUser()
  expect(stored?.access_token).toBe("tok")
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../auth'`.

- [ ] **Step 5: GREEN — auth.ts, use-auth.ts, callback.tsx**

`apps/the-button/src/lib/auth.ts`:

```ts
import {
  InMemoryWebStorage,
  UserManager,
  WebStorageStateStore,
  type User,
} from "oidc-client-ts"
import { env } from "./env"

export function createUserManager(origin: string = window.location.origin): UserManager {
  return new UserManager({
    authority: env.oidcAuthority,
    client_id: env.oidcClientId,
    redirect_uri: `${origin}/the-button/callback`,
    post_logout_redirect_uri: `${origin}/the-button/`,
    response_type: "code",
    scope: "openid profile",
    // Tokens live in memory only (spec §10). The PKCE state/verifier keeps the
    // default sessionStorage stateStore — it must survive the redirect hop.
    userStore: new WebStorageStateStore({ store: new InMemoryWebStorage() }),
  })
}

export const userManager = createUserManager()

export const signIn = (): Promise<void> => userManager.signinRedirect()

export async function completeSignIn(): Promise<User> {
  const user = await userManager.signinCallback()
  if (!user) throw new Error("no user returned from the sign-in callback")
  return user
}
```

`apps/the-button/src/lib/use-auth.ts`:

```ts
import { useEffect, useState } from "react"
import type { User } from "oidc-client-ts"
import { userManager } from "./auth"

export function useAuth(): { user: User | null; token: string | null } {
  const [user, setUser] = useState<User | null>(null)
  useEffect(() => {
    let cancelled = false
    void userManager.getUser().then(u => {
      if (!cancelled) setUser(u && !u.expired ? u : null)
    })
    const onLoaded = (u: User) => setUser(u)
    const onUnloaded = () => setUser(null)
    userManager.events.addUserLoaded(onLoaded)
    userManager.events.addUserUnloaded(onUnloaded)
    return () => {
      cancelled = true
      userManager.events.removeUserLoaded(onLoaded)
      userManager.events.removeUserUnloaded(onUnloaded)
    }
  }, [])
  return { user, token: user?.access_token ?? null }
}
```

`apps/the-button/src/components/callback.tsx`:

```tsx
import { useEffect, useRef, useState } from "react"
import { completeSignIn } from "../lib/auth"

// Finishes the PKCE code exchange. IMPORTANT: onDone must swap the view with
// history.replaceState — a full navigation would reload the page and wipe the
// in-memory user store.
export function Callback({ onDone }: { onDone: () => void }) {
  const [error, setError] = useState<string | null>(null)
  const started = useRef(false)
  useEffect(() => {
    if (started.current) return // StrictMode double-mount: the code is single-use
    started.current = true
    completeSignIn()
      .then(() => onDone())
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
  }, [onDone])

  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-4 p-6 text-center">
      {error ? (
        <>
          <p className="text-destructive text-sm">sign-in failed: {error}</p>
          <a className="text-primary text-sm underline underline-offset-4" href="/the-button/">
            back to the button
          </a>
        </>
      ) : (
        <p className="text-muted-foreground text-sm">signing you in…</p>
      )}
    </main>
  )
}
```

Replace `apps/the-button/src/App.tsx` with:

```tsx
import { useState } from "react"
import { Button } from "@algovn/ui/button"
import { Callback } from "./components/callback"
import { signIn } from "./lib/auth"
import { useAuth } from "./lib/use-auth"

// No router: the app has exactly two views — the page and the OIDC callback.
export default function App() {
  const [isCallback, setIsCallback] = useState(() =>
    window.location.pathname.endsWith("/callback")
  )
  if (isCallback) {
    return (
      <Callback
        onDone={() => {
          window.history.replaceState(null, "", "/the-button/")
          setIsCallback(false)
        }}
      />
    )
  }
  return <Home />
}

function Home() {
  const { user } = useAuth()
  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-6 p-6 text-center">
      <h1 className="font-mono text-3xl font-semibold tracking-tight sm:text-4xl">the button</h1>
      <p className="text-muted-foreground text-sm">every click is a tiny rebellion</p>
      {user ? (
        <p className="text-sm">
          signed in as {user.profile.preferred_username ?? user.profile.sub}
        </p>
      ) : (
        <Button size="lg" onClick={() => void signIn()}>
          sign in to contribute
        </Button>
      )}
    </main>
  )
}
```

Update `apps/the-button/src/__tests__/app.test.tsx` to also cover the signed-out CTA:

```tsx
import { render, screen } from "@testing-library/react"
import { expect, it } from "vitest"
import App from "../App"

it("renders the page heading and the sign-in call to action", async () => {
  render(<App />)
  expect(screen.getByRole("heading", { name: "the button" })).toBeInTheDocument()
  expect(
    await screen.findByRole("button", { name: /sign in to contribute/i })
  ).toBeInTheDocument()
})
```

- [ ] **Step 6: Verify**

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button test
pnpm turbo lint typecheck build --filter=the-button
```

Expected: all tests PASS (app + api + auth); lint/typecheck/build exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/duclm27/the-algovn/web
git add apps/the-button pnpm-lock.yaml
git commit -m "feat(the-button): zitadel pkce auth and control-plane api client"
```

---

### Task 14: Live counter — SSE wrapper, tweened Counter, taglines rotator

**Files:**
- Create: `apps/the-button/src/lib/liveCounter.ts`, `apps/the-button/src/components/counter.tsx`, `apps/the-button/src/components/taglines.tsx`
- Create: `apps/the-button/src/lib/__tests__/liveCounter.test.ts`, `apps/the-button/src/components/__tests__/counter.test.tsx`
- Edit: `apps/the-button/src/App.tsx`

**Interfaces:**
- Consumes (frozen): `VITE_EVENTS_URL=https://api.algovn.com/events/the-button.counter`; SSE frames are unnamed `data:` events whose payload is the JSON published by the tick leader — `{"type":"counter","total":N}` and `{"type":"milestone","threshold":N,"title":"…"}` (numbers, Go `encoding/json`); `getCounter` from T13 for the polling fallback.
- Produces: `LiveCounter` class (start/stop, injectable `createEventSource`/`fetchTotal`/`random` for tests), `LiveEvent`/`CounterEvent`/`MilestoneEvent` types (milestone events consumed by T16), `Counter` and `Taglines` components. Reconnect: full jitter, cap 5s doubling per consecutive failure to 60s max; ≥3 consecutive failures → poll `GetCounter` every 10s±3s until SSE recovers; Page Visibility API disconnects hidden tabs.

- [ ] **Step 1: RED — SSE wrapper state-machine tests**

`apps/the-button/src/lib/__tests__/liveCounter.test.ts`:

```ts
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import { LiveCounter, parseLiveEvent, type LiveCounterOptions, type LiveEvent } from "../liveCounter"

class FakeEventSource {
  static instances: FakeEventSource[] = []
  onopen: (() => void) | null = null
  onmessage: ((e: MessageEvent) => void) | null = null
  onerror: (() => void) | null = null
  closed = false
  constructor(readonly url: string) {
    FakeEventSource.instances.push(this)
  }
  close() {
    this.closed = true
  }
}

function makeCounter(overrides: Partial<LiveCounterOptions> = {}) {
  const events: LiveEvent[] = []
  const fetchTotal = vi.fn(async () => 41)
  const live = new LiveCounter({
    onEvent: e => events.push(e),
    eventsUrl: "https://api.algovn.com/events/the-button.counter",
    createEventSource: url => new FakeEventSource(url) as unknown as EventSource,
    fetchTotal,
    random: () => 0.5,
    ...overrides,
  })
  return { live, events, fetchTotal }
}

beforeEach(() => {
  vi.useFakeTimers()
  FakeEventSource.instances = []
})

afterEach(() => {
  vi.useRealTimers()
})

it("parses typed payloads and rejects junk", () => {
  expect(parseLiveEvent('{"type":"counter","total":42}')).toEqual({ type: "counter", total: 42 })
  expect(
    parseLiveEvent('{"type":"milestone","threshold":1000,"title":"A Thousand Tiny Rebellions"}')
  ).toEqual({ type: "milestone", threshold: 1000, title: "A Thousand Tiny Rebellions" })
  expect(parseLiveEvent("not json")).toBeNull()
  expect(parseLiveEvent('{"type":"counter","total":"nope"}')).toBeNull()
})

it("forwards events from the stream", () => {
  const { live, events } = makeCounter()
  live.start()
  const es = FakeEventSource.instances[0]!
  expect(es.url).toBe("https://api.algovn.com/events/the-button.counter")
  es.onopen?.()
  es.onmessage?.({ data: '{"type":"counter","total":42}' } as MessageEvent)
  es.onmessage?.({ data: ": junk" } as MessageEvent)
  expect(events).toEqual([{ type: "counter", total: 42 }])
  live.stop()
})

it("reconnects with full jitter, cap doubling 5s -> 10s", async () => {
  const { live } = makeCounter()
  live.start()
  FakeEventSource.instances[0]!.onerror?.()
  expect(FakeEventSource.instances[0]!.closed).toBe(true)
  // failure 1: cap 5s, random 0.5 -> 2.5s
  await vi.advanceTimersByTimeAsync(2_499)
  expect(FakeEventSource.instances).toHaveLength(1)
  await vi.advanceTimersByTimeAsync(1)
  expect(FakeEventSource.instances).toHaveLength(2)
  FakeEventSource.instances[1]!.onerror?.()
  // failure 2: cap 10s, random 0.5 -> 5s
  await vi.advanceTimersByTimeAsync(5_000)
  expect(FakeEventSource.instances).toHaveLength(3)
  live.stop()
})

it("falls back to polling after 3 consecutive failures, stops once SSE recovers", async () => {
  const { live, events, fetchTotal } = makeCounter()
  live.start()
  FakeEventSource.instances[0]!.onerror?.()
  await vi.advanceTimersByTimeAsync(2_500) // -> instance 2
  FakeEventSource.instances[1]!.onerror?.()
  await vi.advanceTimersByTimeAsync(5_000) // -> instance 3
  FakeEventSource.instances[2]!.onerror?.() // 3rd failure: polling starts now
  await vi.advanceTimersByTimeAsync(0)
  expect(fetchTotal).toHaveBeenCalledTimes(1)
  expect(events).toContainEqual({ type: "counter", total: 41 })
  // next poll at 10s ± 3s with random 0.5 -> exactly 10s; reconnect (cap 20s,
  // random 0.5 -> 10s) lands at the same instant and creates instance 4
  await vi.advanceTimersByTimeAsync(10_000)
  expect(fetchTotal).toHaveBeenCalledTimes(2)
  expect(FakeEventSource.instances).toHaveLength(4)
  FakeEventSource.instances[3]!.onopen?.() // SSE recovered
  await vi.advanceTimersByTimeAsync(60_000)
  expect(fetchTotal).toHaveBeenCalledTimes(2) // polling stopped
  live.stop()
})

it("disconnects hidden tabs and reconnects when visible again", () => {
  const { live } = makeCounter()
  live.start()
  const es = FakeEventSource.instances[0]!
  es.onopen?.()
  Object.defineProperty(document, "visibilityState", { configurable: true, value: "hidden" })
  document.dispatchEvent(new Event("visibilitychange"))
  expect(es.closed).toBe(true)
  Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" })
  document.dispatchEvent(new Event("visibilitychange"))
  expect(FakeEventSource.instances).toHaveLength(2)
  live.stop()
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../liveCounter'`.

- [ ] **Step 2: GREEN — liveCounter.ts**

`apps/the-button/src/lib/liveCounter.ts`:

```ts
// EventSource wrapper for the anonymous channel the-button.counter (spec §10):
// full-jitter reconnect (uniform in [0,cap], cap 5s doubling per consecutive
// failure, 60s max), polling fallback via GetCounter every 10s±3s after >=3
// consecutive failures (until SSE recovers), and Page Visibility disconnect.
import { getCounter } from "./api"
import { env } from "./env"

export type CounterEvent = { type: "counter"; total: number }
export type MilestoneEvent = { type: "milestone"; threshold: number; title: string }
export type LiveEvent = CounterEvent | MilestoneEvent

export type LiveMode = "connecting" | "live" | "polling"

export interface LiveCounterOptions {
  onEvent: (event: LiveEvent) => void
  onModeChange?: (mode: LiveMode) => void
  eventsUrl?: string
  createEventSource?: (url: string) => EventSource
  fetchTotal?: () => Promise<number>
  random?: () => number
}

const RECONNECT_CAP_START_MS = 5_000
const RECONNECT_CAP_MAX_MS = 60_000
const POLL_BASE_MS = 10_000
const POLL_JITTER_MS = 3_000
const FAILURES_BEFORE_POLLING = 3

export function parseLiveEvent(data: string): LiveEvent | null {
  try {
    const raw = JSON.parse(data) as Record<string, unknown>
    if (raw.type === "counter" && typeof raw.total === "number") {
      return { type: "counter", total: raw.total }
    }
    if (
      raw.type === "milestone" &&
      typeof raw.threshold === "number" &&
      typeof raw.title === "string"
    ) {
      return { type: "milestone", threshold: raw.threshold, title: raw.title }
    }
  } catch {
    // malformed frame — ignore
  }
  return null
}

export class LiveCounter {
  private es: EventSource | null = null
  private failures = 0
  private polling = false
  private stopped = true
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private pollTimer: ReturnType<typeof setTimeout> | null = null

  constructor(private readonly opts: LiveCounterOptions) {}

  start(): void {
    if (!this.stopped) return
    this.stopped = false
    document.addEventListener("visibilitychange", this.onVisibility)
    this.connect()
  }

  stop(): void {
    this.stopped = true
    document.removeEventListener("visibilitychange", this.onVisibility)
    this.disconnect()
  }

  private onVisibility = (): void => {
    if (this.stopped) return
    if (document.visibilityState === "hidden") {
      this.disconnect() // frees a server connection while the tab is hidden
    } else if (!this.es && !this.reconnectTimer) {
      this.failures = 0
      this.connect()
    }
  }

  private disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.stopPolling()
    this.es?.close()
    this.es = null
  }

  private connect(): void {
    const create = this.opts.createEventSource ?? ((url: string) => new EventSource(url))
    const es = create(this.opts.eventsUrl ?? env.eventsUrl)
    this.es = es
    this.opts.onModeChange?.(this.polling ? "polling" : "connecting")
    es.onopen = () => {
      this.failures = 0
      this.stopPolling()
      this.opts.onModeChange?.("live")
    }
    es.onmessage = (e: MessageEvent) => {
      const event = parseLiveEvent(String(e.data))
      if (event) this.opts.onEvent(event)
    }
    es.onerror = () => {
      // We own the backoff: close instead of relying on native EventSource
      // retry (which cannot jitter across 10k clients).
      es.close()
      if (this.es !== es) return
      this.es = null
      if (this.stopped) return
      this.failures += 1
      if (this.failures >= FAILURES_BEFORE_POLLING) this.startPolling()
      this.scheduleReconnect()
    }
  }

  private scheduleReconnect(): void {
    const cap = Math.min(RECONNECT_CAP_START_MS * 2 ** (this.failures - 1), RECONNECT_CAP_MAX_MS)
    const random = this.opts.random ?? Math.random
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, random() * cap)
  }

  private startPolling(): void {
    if (this.polling) return
    this.polling = true
    this.opts.onModeChange?.("polling")
    void this.pollLoop()
  }

  private stopPolling(): void {
    this.polling = false
    if (this.pollTimer) {
      clearTimeout(this.pollTimer)
      this.pollTimer = null
    }
  }

  private async pollLoop(): Promise<void> {
    const random = this.opts.random ?? Math.random
    const fetchTotal =
      this.opts.fetchTotal ?? (async () => Number((await getCounter()).total ?? "0"))
    while (this.polling && !this.stopped) {
      try {
        const total = await fetchTotal()
        if (!this.polling || this.stopped) return
        this.opts.onEvent({ type: "counter", total })
      } catch {
        // polling is best-effort; the next attempt may succeed
      }
      const delay = POLL_BASE_MS + (random() * 2 - 1) * POLL_JITTER_MS // 10s ± 3s
      await new Promise<void>(resolve => {
        this.pollTimer = setTimeout(resolve, delay)
      })
      this.pollTimer = null
    }
  }
}
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: PASS (liveCounter suite + previous suites).

- [ ] **Step 3: RED — Counter tween test**

`apps/the-button/src/components/__tests__/counter.test.tsx`:

```tsx
import { render, screen, waitFor } from "@testing-library/react"
import { expect, it } from "vitest"
import { Counter } from "../counter"

it("shows a placeholder before the first total arrives", () => {
  render(<Counter total={null} />)
  expect(screen.getByTestId("counter")).toHaveTextContent("—")
})

it("tweens to the new total", async () => {
  const { rerender } = render(<Counter total={null} />)
  rerender(<Counter total={1234} />)
  await waitFor(() => expect(screen.getByTestId("counter")).toHaveTextContent("1,234"))
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../counter'`.

- [ ] **Step 4: GREEN — counter.tsx and taglines.tsx**

`apps/the-button/src/components/counter.tsx`:

```tsx
import { useEffect, useRef, useState } from "react"

const TWEEN_MS = 600

// Big number with an ease-out tween on change (SSE ticks at 1s; the tween
// makes each tick feel alive without re-rendering more than rAF allows).
export function Counter({ total }: { total: number | null }) {
  const [shown, setShown] = useState(0)
  const shownRef = useRef(0)

  useEffect(() => {
    if (total === null) return
    const from = shownRef.current
    const to = total
    if (from === to) return
    const started = performance.now()
    let raf = 0
    const step = (now: number) => {
      const t = Math.min((now - started) / TWEEN_MS, 1)
      const eased = 1 - (1 - t) ** 3
      const value = Math.round(from + (to - from) * eased)
      shownRef.current = value
      setShown(value)
      if (t < 1) raf = requestAnimationFrame(step)
    }
    raf = requestAnimationFrame(step)
    return () => cancelAnimationFrame(raf)
  }, [total])

  return (
    <div
      data-testid="counter"
      aria-live="polite"
      className="font-mono text-6xl font-semibold tabular-nums tracking-tight sm:text-8xl"
    >
      {total === null ? "—" : shown.toLocaleString("en-US")}
    </div>
  )
}
```

`apps/the-button/src/components/taglines.tsx` (the four WHYs from spec §1, rotating every 6s):

```tsx
import { useEffect, useState } from "react"

const TAGLINES = [
  "stress-testing a home server",
  "proving humans can work together",
  "because the internet needs more joy",
  "every click is a tiny rebellion",
]

const ROTATE_MS = 6_000

export function Taglines() {
  const [index, setIndex] = useState(0)
  useEffect(() => {
    const timer = setInterval(() => setIndex(i => (i + 1) % TAGLINES.length), ROTATE_MS)
    return () => clearInterval(timer)
  }, [])
  return <p className="text-muted-foreground text-sm sm:text-base">{TAGLINES[index]}</p>
}
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: PASS.

- [ ] **Step 5: Wire the live counter into the page**

Replace `apps/the-button/src/App.tsx` with:

```tsx
import { useEffect, useState } from "react"
import { Button } from "@algovn/ui/button"
import { Callback } from "./components/callback"
import { Counter } from "./components/counter"
import { Taglines } from "./components/taglines"
import { signIn } from "./lib/auth"
import { LiveCounter, type LiveMode } from "./lib/liveCounter"
import { useAuth } from "./lib/use-auth"

// No router: the app has exactly two views — the page and the OIDC callback.
export default function App() {
  const [isCallback, setIsCallback] = useState(() =>
    window.location.pathname.endsWith("/callback")
  )
  if (isCallback) {
    return (
      <Callback
        onDone={() => {
          window.history.replaceState(null, "", "/the-button/")
          setIsCallback(false)
        }}
      />
    )
  }
  return <Home />
}

function Home() {
  const { user } = useAuth()
  const [total, setTotal] = useState<number | null>(null)
  const [mode, setMode] = useState<LiveMode>("connecting")

  useEffect(() => {
    const live = new LiveCounter({
      onEvent: event => {
        if (event.type === "counter") setTotal(event.total)
      },
      onModeChange: setMode,
    })
    live.start()
    return () => live.stop()
  }, [])

  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-8 p-6 text-center">
      <header className="space-y-3">
        <h1 className="font-mono text-3xl font-semibold tracking-tight sm:text-4xl">the button</h1>
        <Taglines />
      </header>
      <Counter total={total} />
      <p className="text-muted-foreground text-xs">
        {mode === "live" ? "live" : mode === "polling" ? "live updates degraded — polling" : "connecting…"}
      </p>
      {user ? (
        <p className="text-sm">
          signed in as {user.profile.preferred_username ?? user.profile.sub}
        </p>
      ) : (
        <Button size="lg" onClick={() => void signIn()}>
          sign in to contribute
        </Button>
      )}
    </main>
  )
}
```

- [ ] **Step 6: Verify and commit**

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button test
pnpm turbo lint typecheck build --filter=the-button
git add apps/the-button
git commit -m "feat(the-button): live counter with sse wrapper and taglines"
```

Expected: all tests PASS (the app smoke test still passes — the stubbed EventSource simply never opens); lint/typecheck/build exit 0.

---

### Task 15: Clicker — PoW solver worker, click batcher, mash button, bench mode

**Files:**
- Create: `apps/the-button/src/worker/solver.ts`, `apps/the-button/src/lib/solverClient.ts`, `apps/the-button/src/lib/batcher.ts`, `apps/the-button/src/lib/bench.ts`, `apps/the-button/src/components/click-button.tsx`
- Create: `apps/the-button/src/worker/__tests__/solver.test.ts`, `apps/the-button/src/lib/__tests__/batcher.test.ts`
- Edit: `apps/the-button/src/App.tsx`, `apps/the-button/package.json` (add `hash-wasm`)

**Interfaces:**
- Consumes (frozen): PoW check `SHA-256(tokenBytes || be32(click_count) || be64(nonce)) < 2^256/(w0*click_count*l)` where `tokenBytes` = the ASCII bytes of the challenge string exactly as issued (matches Task 8's pinned definition — do NOT base64url-decode) and the response's `work_factor = w0*l` — so the client target is `2^256/(workFactor*click_count)`; flush policy `max(min_interval, solve_time)` or 300 accumulated clicks; `next_challenge` piggyback; 429 honors Retry-After (challenge un-burned, nonce stays valid), 409/expired → re-issue; `issueChallenge`/`submitClicks` + error discrimination from T13.
- Produces: typed worker protocol (`SolveRequest`/`BenchRequest` → `SolverProgress`/`SolverResult`/`BenchResult`/`SolverFailure`), `Solver` interface + `createWorkerSolver()` (worker behind an interface so the batcher is testable), `Batcher` class, `ClickButton` component, `?bench` console calibration used by T20 to set `POW_W0`.

- [ ] **Step 1: Add hash-wasm**

In `apps/the-button/package.json` add to `"dependencies"` (alphabetical):

```json
    "hash-wasm": "^4.12.0",
```

so dependencies read: `@algovn/ui`, `@fontsource-variable/geist`, `@fontsource-variable/geist-mono`, `hash-wasm`, `oidc-client-ts`, `react`, `react-dom`. Then:

```bash
cd /Users/duclm27/the-algovn/web && pnpm install
```

Expected: exit 0, `hash-wasm 4.12.x` added (it inlines its WASM — no asset config needed, works in Workers and in Node for tests).

- [ ] **Step 2: RED — solver math tests**

`apps/the-button/src/worker/__tests__/solver.test.ts`:

```ts
import { createSHA256 } from "hash-wasm"
import { expect, it } from "vitest"
import { base64UrlDecode, computeTarget, lessThan, solve } from "../solver"

it("decodes base64url without padding", () => {
  // base64url("hi?") — the '?' exercises the '_' -> '/' mapping
  expect(Array.from(base64UrlDecode("aGk_"))).toEqual([104, 105, 63])
})

it("computes the smooth full target 2^256/(workFactor*count) big-endian", () => {
  // 2^256 / 2^248 = 2^8 = 256 -> bytes ...00 01 00
  const target = computeTarget(1n << 248n, 1)
  expect(Array.from(target.slice(0, 30)).every(b => b === 0)).toBe(true)
  expect(target[30]).toBe(1)
  expect(target[31]).toBe(0)
  // degenerate divisor: everything qualifies
  expect(Array.from(computeTarget(1n, 1)).every(b => b === 0xff)).toBe(true)
})

it("compares digests as 256-bit big-endian integers", () => {
  const target = computeTarget(1n << 248n, 1) // 256
  const below = new Uint8Array(32)
  below[31] = 0xff // 255
  const equal = new Uint8Array(32)
  equal[30] = 1 // 256
  const above = new Uint8Array(32)
  above[29] = 1
  expect(lessThan(below, target)).toBe(true)
  expect(lessThan(equal, target)).toBe(false)
  expect(lessThan(above, target)).toBe(false)
})

it("finds a nonce whose hash beats the target (independently re-verified)", async () => {
  const challenge = "dGVzdC1jaGFsbGVuZ2U" // base64url("test-challenge")
  const clickCount = 3
  const result = await solve({
    type: "solve",
    jobId: 1,
    challenge,
    clickCount,
    workFactor: "64",
  })
  expect(result.type).toBe("result")
  expect(result.hashes).toBeGreaterThan(0)
  // recompute SHA-256(tokenBytes || be32(count) || be64(nonce)) from scratch
  const token = new TextEncoder().encode(challenge)
  const input = new Uint8Array(token.length + 4 + 8)
  input.set(token, 0)
  const view = new DataView(input.buffer)
  view.setUint32(token.length, clickCount)
  view.setBigUint64(token.length + 4, BigInt(result.nonce))
  const hasher = await createSHA256()
  hasher.init()
  hasher.update(input)
  const digest = hasher.digest("binary")
  expect(lessThan(digest, computeTarget(64n, clickCount))).toBe(true)
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../solver'`.

- [ ] **Step 3: GREEN — the worker module**

`apps/the-button/src/worker/solver.ts`:

```ts
// PoW solver — runs inside a Web Worker (spec §5). Work check:
// SHA-256(tokenBytes || be32(clickCount) || be64(nonce)), read as a 256-bit
// big-endian integer, must be < 2^256 / (workFactor * clickCount), where
// workFactor = w0*L from IssueChallengeResponse and tokenBytes is the ASCII
// bytes of the challenge string exactly as issued (same as the server's
// pow.CheckWork — never decode it).
// jsdom tests import the exported pure functions; only a real Worker runs the
// onmessage loop at the bottom.
import { createSHA256 } from "hash-wasm"

export type SolveRequest = {
  type: "solve"
  jobId: number
  challenge: string
  clickCount: number
  workFactor: string // uint64 decimal string (protojson)
}
export type BenchRequest = { type: "bench"; jobId: number; durationMs: number }
export type SolverRequest = SolveRequest | BenchRequest

export type SolverProgress = { type: "progress"; jobId: number; hashes: number }
export type SolverResult = {
  type: "result"
  jobId: number
  nonce: string // uint64 decimal string
  hashes: number
  elapsedMs: number
}
export type BenchResult = { type: "bench-result"; jobId: number; hashesPerSecond: number }
export type SolverFailure = { type: "error"; jobId: number; message: string }
export type SolverResponse = SolverProgress | SolverResult | BenchResult | SolverFailure

const PROGRESS_EVERY = 50_000

export function base64UrlDecode(s: string): Uint8Array {
  const b64 = s.replaceAll("-", "+").replaceAll("_", "/")
  const pad = b64.length % 4 === 0 ? "" : "=".repeat(4 - (b64.length % 4))
  const bin = atob(b64 + pad)
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}

// 2^256 / (workFactor * clickCount) as 32 big-endian bytes.
export function computeTarget(workFactor: bigint, clickCount: number): Uint8Array {
  const divisor = workFactor * BigInt(clickCount)
  const out = new Uint8Array(32)
  if (divisor <= 1n) {
    out.fill(0xff) // target >= 2^256: every hash qualifies
    return out
  }
  let t = (1n << 256n) / divisor
  for (let i = 31; i >= 0; i--) {
    out[i] = Number(t & 0xffn)
    t >>= 8n
  }
  return out
}

export function lessThan(hash: Uint8Array, target: Uint8Array): boolean {
  for (let i = 0; i < 32; i++) {
    const h = hash[i]!
    const t = target[i]!
    if (h !== t) return h < t
  }
  return false
}

export async function solve(
  req: SolveRequest,
  onProgress?: (hashes: number) => void
): Promise<SolverResult> {
  const token = new TextEncoder().encode(req.challenge)
  const target = computeTarget(BigInt(req.workFactor), req.clickCount)
  const input = new Uint8Array(token.length + 4 + 8)
  input.set(token, 0)
  const view = new DataView(input.buffer)
  view.setUint32(token.length, req.clickCount) // DataView defaults to big-endian
  const hasher = await createSHA256()
  const started = performance.now()
  let hashes = 0
  for (let nonce = 0n; ; nonce++) {
    view.setBigUint64(token.length + 4, nonce)
    hasher.init()
    hasher.update(input)
    const digest = hasher.digest("binary")
    hashes++
    if (lessThan(digest, target)) {
      return {
        type: "result",
        jobId: req.jobId,
        nonce: nonce.toString(),
        hashes,
        elapsedMs: performance.now() - started,
      }
    }
    if (hashes % PROGRESS_EVERY === 0) onProgress?.(hashes)
  }
}

// Measures raw sustained SHA-256 throughput (calibration input for POW_W0).
export async function bench(durationMs: number): Promise<number> {
  const hasher = await createSHA256()
  const input = new Uint8Array(120) // ~ token + be32 + be64 sized payload
  const view = new DataView(input.buffer)
  const started = performance.now()
  let hashes = 0
  while (performance.now() - started < durationMs) {
    for (let i = 0; i < 10_000; i++, hashes++) {
      view.setBigUint64(112, BigInt(hashes))
      hasher.init()
      hasher.update(input)
      hasher.digest("binary")
    }
  }
  return Math.round((hashes / (performance.now() - started)) * 1000)
}

// ---- Worker message loop ----
const post = (msg: SolverResponse) =>
  (self as unknown as { postMessage(m: SolverResponse): void }).postMessage(msg)

self.onmessage = (e: MessageEvent<SolverRequest>) => {
  const req = e.data
  void (async () => {
    try {
      if (req.type === "solve") {
        post(await solve(req, hashes => post({ type: "progress", jobId: req.jobId, hashes })))
      } else {
        post({ type: "bench-result", jobId: req.jobId, hashesPerSecond: await bench(req.durationMs) })
      }
    } catch (err) {
      post({
        type: "error",
        jobId: req.jobId,
        message: err instanceof Error ? err.message : String(err),
      })
    }
  })()
}
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: PASS — the solve test finishes in well under a second (expected ~192 hashes at workFactor 64 × count 3).

- [ ] **Step 4: Main-thread solver interface**

`apps/the-button/src/lib/solverClient.ts`:

```ts
// The Web Worker behind a promise interface. The batcher depends only on
// `Solver`, so tests inject a fake and never touch a real worker.
import type {
  BenchResult,
  SolverRequest,
  SolverResponse,
  SolverResult,
} from "../worker/solver"

export interface SolveInput {
  challenge: string
  clickCount: number
  workFactor: string
}

export interface Solver {
  solve(input: SolveInput): Promise<SolverResult>
}

export interface WorkerSolver extends Solver {
  bench(durationMs: number): Promise<number>
  terminate(): void
}

export function createWorkerSolver(): WorkerSolver {
  const worker = new Worker(new URL("../worker/solver.ts", import.meta.url), { type: "module" })
  let nextJobId = 1
  const pending = new Map<
    number,
    { resolve: (msg: SolverResponse) => void; reject: (err: Error) => void }
  >()

  worker.onmessage = (e: MessageEvent<SolverResponse>) => {
    const msg = e.data
    if (msg.type === "progress") return
    const job = pending.get(msg.jobId)
    if (!job) return
    pending.delete(msg.jobId)
    if (msg.type === "error") job.reject(new Error(msg.message))
    else job.resolve(msg)
  }

  const request = (req: SolverRequest): Promise<SolverResponse> =>
    new Promise((resolve, reject) => {
      pending.set(req.jobId, { resolve, reject })
      worker.postMessage(req)
    })

  return {
    async solve(input: SolveInput): Promise<SolverResult> {
      return (await request({ type: "solve", jobId: nextJobId++, ...input })) as SolverResult
    },
    async bench(durationMs: number): Promise<number> {
      const msg = (await request({ type: "bench", jobId: nextJobId++, durationMs })) as BenchResult
      return msg.hashesPerSecond
    },
    terminate(): void {
      worker.terminate()
    },
  }
}
```

`apps/the-button/src/lib/bench.ts`:

```ts
import { createWorkerSolver } from "./solverClient"

// Calibration hook: open /the-button/?bench and read the console. T20 runs
// this on real devices to pick POW_W0 — never tune assuming WebCrypto speeds.
export async function runBench(durationMs = 4_000): Promise<number> {
  console.log(`[bench] measuring worker SHA-256 throughput for ${durationMs}ms…`)
  const solver = createWorkerSolver()
  try {
    const hps = await solver.bench(durationMs)
    console.log(`[bench] ${hps.toLocaleString("en-US")} H/s — calibration input for POW_W0`)
    return hps
  } finally {
    solver.terminate()
  }
}
```

- [ ] **Step 5: RED — batcher state-machine tests**

`apps/the-button/src/lib/__tests__/batcher.test.ts`:

```ts
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import { ApiError, type IssueChallengeResponse } from "../api"
import { Batcher } from "../batcher"
import type { Solver } from "../solverClient"

const challenge = (over: Partial<IssueChallengeResponse> = {}): IssueChallengeResponse => ({
  challenge: "chal-1",
  workFactor: "16384",
  minIntervalSeconds: 2,
  maxBatch: 10000,
  expiresAt: new Date(Date.now() + 300_000).toISOString(),
  ...over,
})

function makeDeps() {
  const solver: Solver = {
    solve: vi.fn(async (input: { clickCount: number }) => ({
      type: "result" as const,
      jobId: 1,
      nonce: "42",
      hashes: 10 * input.clickCount,
      elapsedMs: 5,
    })),
  }
  const api = {
    issueChallenge: vi.fn(async (_clicks: number, _token: string) => challenge()),
    submitClicks: vi.fn(async () => ({
      userTotalClicks: "10",
      unlocked: [],
      nextChallenge: challenge({ challenge: "chal-2" }),
    })),
  }
  return { solver, api }
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
})

it("issues, solves and submits a single click, then keeps the piggybacked challenge", async () => {
  const { solver, api } = makeDeps()
  const onUserTotal = vi.fn()
  const onPendingChange = vi.fn()
  const b = new Batcher({ api, solver, getToken: () => "tok", onUserTotal, onPendingChange })
  b.click()
  await vi.advanceTimersByTimeAsync(10)
  expect(api.issueChallenge).toHaveBeenCalledWith(1, "tok")
  expect(solver.solve).toHaveBeenCalledWith({ challenge: "chal-1", clickCount: 1, workFactor: "16384" })
  expect(api.submitClicks).toHaveBeenCalledWith(
    { challenge: "chal-1", nonce: "42", clickCount: 1 },
    "tok"
  )
  expect(onUserTotal).toHaveBeenCalledWith(10)
  expect(onPendingChange).toHaveBeenLastCalledWith(0)

  // second batch reuses next_challenge and waits out min_interval (2s);
  // margins avoid asserting exactly on the timer boundary
  b.click()
  b.click()
  await vi.advanceTimersByTimeAsync(1_900)
  expect(api.submitClicks).toHaveBeenCalledTimes(1)
  await vi.advanceTimersByTimeAsync(200)
  expect(api.issueChallenge).toHaveBeenCalledTimes(1) // no re-issue
  expect(api.submitClicks).toHaveBeenLastCalledWith(
    { challenge: "chal-2", nonce: "42", clickCount: 2 },
    "tok"
  )
})

it("honors Retry-After on 429 and resubmits the SAME solved batch", async () => {
  const { solver, api } = makeDeps()
  api.submitClicks
    .mockRejectedValueOnce(new ApiError(429, "ResourceExhausted", "slow down", 5))
    .mockResolvedValueOnce({
      userTotalClicks: "1",
      unlocked: [],
      nextChallenge: challenge({ challenge: "chal-2" }),
    })
  const b = new Batcher({ api, solver, getToken: () => "tok" })
  b.click()
  await vi.advanceTimersByTimeAsync(10)
  expect(api.submitClicks).toHaveBeenCalledTimes(1)
  await vi.advanceTimersByTimeAsync(5_000) // Retry-After: 5
  expect(api.submitClicks).toHaveBeenCalledTimes(2)
  expect(api.submitClicks.mock.calls[1]![0]).toEqual({
    challenge: "chal-1",
    nonce: "42",
    clickCount: 1,
  })
  expect(api.issueChallenge).toHaveBeenCalledTimes(1) // token still valid, no re-issue
  expect(solver.solve).toHaveBeenCalledTimes(1) // no re-solve
})

it("re-issues and re-solves after a 409 replay", async () => {
  const { solver, api } = makeDeps()
  api.issueChallenge
    .mockResolvedValueOnce(challenge())
    .mockResolvedValueOnce(challenge({ challenge: "chal-1b" }))
  api.submitClicks
    .mockRejectedValueOnce(new ApiError(409, "AlreadyExists", "replay"))
    .mockResolvedValueOnce({ userTotalClicks: "1", unlocked: [], nextChallenge: undefined })
  const b = new Batcher({ api, solver, getToken: () => "tok" })
  b.click()
  await vi.advanceTimersByTimeAsync(10)
  expect(api.issueChallenge).toHaveBeenCalledTimes(2)
  expect(solver.solve).toHaveBeenCalledTimes(2)
  expect(api.submitClicks).toHaveBeenLastCalledWith(
    { challenge: "chal-1b", nonce: "42", clickCount: 1 },
    "tok"
  )
})

it("re-issues after a 400 expired challenge", async () => {
  const { solver, api } = makeDeps()
  api.issueChallenge
    .mockResolvedValueOnce(challenge())
    .mockResolvedValueOnce(challenge({ challenge: "chal-1b" }))
  api.submitClicks
    .mockRejectedValueOnce(new ApiError(400, "FailedPrecondition", "challenge_expired"))
    .mockResolvedValueOnce({ userTotalClicks: "1", unlocked: [], nextChallenge: undefined })
  const b = new Batcher({ api, solver, getToken: () => "tok" })
  b.click()
  await vi.advanceTimersByTimeAsync(10)
  expect(api.issueChallenge).toHaveBeenCalledTimes(2)
  expect(api.submitClicks).toHaveBeenCalledTimes(2)
})

it("starts solving immediately at flushAt pending clicks, but still throttles the submit", async () => {
  const { solver, api } = makeDeps()
  const b = new Batcher({ api, solver, getToken: () => "tok", flushAt: 3 })
  b.click()
  await vi.advanceTimersByTimeAsync(10) // batch 1 done -> lastSubmitAt set
  expect(api.submitClicks).toHaveBeenCalledTimes(1)
  b.click()
  b.click()
  b.click() // hits flushAt -> solve starts now, without waiting the 2s
  await vi.advanceTimersByTimeAsync(10)
  expect(solver.solve).toHaveBeenCalledTimes(2)
  expect(api.submitClicks).toHaveBeenCalledTimes(1) // submit still gated by min_interval
  await vi.advanceTimersByTimeAsync(2_000)
  expect(api.submitClicks).toHaveBeenCalledTimes(2)
  expect(api.submitClicks.mock.calls[1]![0]).toEqual({
    challenge: "chal-2",
    nonce: "42",
    clickCount: 3,
  })
})

it("keeps clicks pending while signed out", async () => {
  const { solver, api } = makeDeps()
  const b = new Batcher({ api, solver, getToken: () => null })
  b.click()
  await vi.advanceTimersByTimeAsync(30_000)
  expect(api.issueChallenge).not.toHaveBeenCalled()
  expect(b.pendingCount).toBe(1)
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../batcher'`.

- [ ] **Step 6: GREEN — batcher.ts**

`apps/the-button/src/lib/batcher.ts`:

```ts
// Click batching state machine (spec §5): clicks accumulate; a batch freezes
// and starts solving once min_interval has elapsed since the last submit (or
// immediately when flushAt=300 clicks pile up); the submit itself never fires
// before min_interval (server-side SETNX throttle would 429 it anyway); the
// response's next_challenge keeps the pipeline full.
import {
  isExpiredChallenge,
  isRateLimited,
  isReplay,
  type Achievement,
  type IssueChallengeResponse,
  type SubmitClicksRequest,
  type SubmitClicksResponse,
} from "./api"
import type { Solver } from "./solverClient"

export interface BatcherApi {
  issueChallenge(intendedClicks: number, token: string): Promise<IssueChallengeResponse>
  submitClicks(req: SubmitClicksRequest, token: string): Promise<SubmitClicksResponse>
}

export interface BatcherOptions {
  api: BatcherApi
  solver: Solver
  getToken: () => string | null
  onUserTotal?: (total: number) => void
  onUnlocked?: (unlocked: Achievement[]) => void
  onPendingChange?: (pending: number) => void
  onError?: (err: unknown) => void
  flushAt?: number
}

const DEFAULT_FLUSH_AT = 300
const DEFAULT_MIN_INTERVAL_S = 2
const DEFAULT_MAX_BATCH = 10_000
const DEFAULT_WORK_FACTOR = "16384" // matches the POW_W0 default; server value always wins
const MAX_SUBMIT_ATTEMPTS = 3
const FAILURE_BACKOFF_MS = 2_000
const EXPIRY_MARGIN_MS = 10_000

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))

export class Batcher {
  private pending = 0
  private challenge: IssueChallengeResponse | null = null
  private inFlight = false
  private lastSubmitAt = 0
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private readonly flushAt: number

  constructor(private readonly opts: BatcherOptions) {
    this.flushAt = opts.flushAt ?? DEFAULT_FLUSH_AT
  }

  get pendingCount(): number {
    return this.pending
  }

  click(): void {
    this.pending++
    this.opts.onPendingChange?.(this.pending)
    this.schedule()
  }

  private minIntervalMs(): number {
    return (this.challenge?.minIntervalSeconds ?? DEFAULT_MIN_INTERVAL_S) * 1000
  }

  private schedule(): void {
    if (this.inFlight || this.flushTimer || this.pending === 0) return
    const earliest = this.lastSubmitAt + this.minIntervalMs()
    const delay = this.pending >= this.flushAt ? 0 : Math.max(0, earliest - Date.now())
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null
      void this.flush()
    }, delay)
  }

  private async flush(): Promise<void> {
    if (this.inFlight || this.pending === 0) return
    const token = this.opts.getToken()
    if (!token) return // signed out (or token expired): clicks stay pending
    this.inFlight = true
    try {
      if (this.challenge && expiringSoon(this.challenge)) this.challenge = null
      if (!this.challenge?.challenge) {
        this.challenge = await this.opts.api.issueChallenge(this.pending, token)
      }
      const active = this.challenge
      const count = Math.min(this.pending, active.maxBatch ?? DEFAULT_MAX_BATCH)
      const solved = await this.opts.solver.solve({
        challenge: active.challenge ?? "",
        clickCount: count,
        workFactor: active.workFactor ?? DEFAULT_WORK_FACTOR,
      })
      const wait = this.lastSubmitAt + this.minIntervalMs() - Date.now()
      if (wait > 0) await sleep(wait)
      await this.submit(
        { challenge: active.challenge ?? "", nonce: solved.nonce, clickCount: count },
        token,
        1
      )
    } catch (err) {
      this.opts.onError?.(err)
      await sleep(FAILURE_BACKOFF_MS)
    } finally {
      this.inFlight = false
      this.schedule() // anything still pending (new clicks, replay, expiry) retries here
    }
  }

  private async submit(req: SubmitClicksRequest, token: string, attempt: number): Promise<void> {
    try {
      const res = await this.opts.api.submitClicks(req, token)
      this.lastSubmitAt = Date.now()
      this.pending -= req.clickCount
      this.opts.onPendingChange?.(this.pending)
      this.challenge = res.nextChallenge ?? null
      if (res.userTotalClicks !== undefined) this.opts.onUserTotal?.(Number(res.userTotalClicks))
      if (res.unlocked?.length) this.opts.onUnlocked?.(res.unlocked)
    } catch (err) {
      if (isRateLimited(err) && attempt < MAX_SUBMIT_ATTEMPTS) {
        // 429: the server un-burned the challenge — same nonce stays valid.
        await sleep((err.retryAfterSeconds ?? 2) * 1000)
        return this.submit(req, token, attempt + 1)
      }
      if (isReplay(err) || isExpiredChallenge(err)) {
        // 409 burned or 400 expired: drop this solution; the next flush
        // re-issues a fresh challenge and re-solves for the pending clicks.
        this.challenge = null
        return
      }
      throw err
    }
  }
}

function expiringSoon(challenge: IssueChallengeResponse): boolean {
  if (!challenge.expiresAt) return false
  // solving + submitting takes time — leave a safety margin
  return Date.parse(challenge.expiresAt) - EXPIRY_MARGIN_MS < Date.now()
}
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: PASS (batcher suite green; all earlier suites still green).

- [ ] **Step 7: Mash button component and page wiring**

`apps/the-button/src/components/click-button.tsx`:

```tsx
import { Badge } from "@algovn/ui/badge"
import { cn } from "@algovn/ui/lib/utils"

export function ClickButton({
  onMash,
  myTotal,
  pending,
}: {
  onMash: () => void
  myTotal: number | null
  pending: number
}) {
  return (
    <div className="flex flex-col items-center gap-4">
      <button
        type="button"
        onClick={onMash}
        className={cn(
          "bg-primary text-primary-foreground size-40 rounded-full text-xl font-semibold shadow-lg sm:size-48",
          "transition-transform duration-75 select-none active:scale-90",
          "focus-visible:ring-ring/50 outline-none focus-visible:ring-[3px]"
        )}
      >
        the button
      </button>
      <div className="flex min-h-6 items-center gap-2 text-sm">
        {myTotal !== null && (
          <span className="text-muted-foreground">
            you:{" "}
            <span className="text-foreground font-mono tabular-nums">
              {(myTotal + pending).toLocaleString("en-US")}
            </span>
          </span>
        )}
        {pending > 0 && (
          <Badge variant="secondary">{pending.toLocaleString("en-US")} pending</Badge>
        )}
      </div>
    </div>
  )
}
```

Replace `apps/the-button/src/App.tsx` with:

```tsx
import { useEffect, useRef, useState } from "react"
import { Button } from "@algovn/ui/button"
import { Callback } from "./components/callback"
import { ClickButton } from "./components/click-button"
import { Counter } from "./components/counter"
import { Taglines } from "./components/taglines"
import { issueChallenge, submitClicks } from "./lib/api"
import { signIn } from "./lib/auth"
import { Batcher } from "./lib/batcher"
import { runBench } from "./lib/bench"
import { LiveCounter, type LiveMode } from "./lib/liveCounter"
import { createWorkerSolver } from "./lib/solverClient"
import { useAuth } from "./lib/use-auth"

// No router: the app has exactly two views — the page and the OIDC callback.
export default function App() {
  const [isCallback, setIsCallback] = useState(() =>
    window.location.pathname.endsWith("/callback")
  )
  if (isCallback) {
    return (
      <Callback
        onDone={() => {
          window.history.replaceState(null, "", "/the-button/")
          setIsCallback(false)
        }}
      />
    )
  }
  return <Home />
}

function Home() {
  const { user, token } = useAuth()
  const tokenRef = useRef<string | null>(null)
  tokenRef.current = token
  const [total, setTotal] = useState<number | null>(null)
  const [mode, setMode] = useState<LiveMode>("connecting")
  const [myTotal, setMyTotal] = useState<number | null>(null)
  const [pending, setPending] = useState(0)
  const batcherRef = useRef<Batcher | null>(null)

  useEffect(() => {
    const solver = createWorkerSolver()
    batcherRef.current = new Batcher({
      api: { issueChallenge, submitClicks },
      solver,
      getToken: () => tokenRef.current,
      onUserTotal: setMyTotal,
      onPendingChange: setPending,
      onError: err => console.error("submit failed", err),
    })
    return () => {
      batcherRef.current = null
      solver.terminate()
    }
  }, [])

  useEffect(() => {
    const live = new LiveCounter({
      onEvent: event => {
        if (event.type === "counter") setTotal(event.total)
      },
      onModeChange: setMode,
    })
    live.start()
    return () => live.stop()
  }, [])

  const benchRan = useRef(false)
  useEffect(() => {
    if (benchRan.current) return
    if (new URLSearchParams(window.location.search).has("bench")) {
      benchRan.current = true
      void runBench()
    }
  }, [])

  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-8 p-6 text-center">
      <header className="space-y-3">
        <h1 className="font-mono text-3xl font-semibold tracking-tight sm:text-4xl">the button</h1>
        <Taglines />
      </header>
      <Counter total={total} />
      <p className="text-muted-foreground text-xs">
        {mode === "live" ? "live" : mode === "polling" ? "live updates degraded — polling" : "connecting…"}
      </p>
      {user ? (
        <ClickButton
          onMash={() => batcherRef.current?.click()}
          myTotal={myTotal}
          pending={pending}
        />
      ) : (
        <Button size="lg" onClick={() => void signIn()}>
          sign in to contribute
        </Button>
      )}
    </main>
  )
}
```

- [ ] **Step 8: Verify and commit**

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button test
pnpm turbo lint typecheck build --filter=the-button
```

Expected: all suites PASS; build exits 0 and the output lists a separate worker chunk (`dist/assets/solver-*.js`).

Optional sanity check of the real worker + bench in a browser: `pnpm --filter the-button dev`, open `http://localhost:5173/the-button/?bench`, dev-tools console shows `[bench] … H/s — calibration input for POW_W0` after ~4s.

```bash
cd /Users/duclm27/the-algovn/web
git add apps/the-button pnpm-lock.yaml
git commit -m "feat(the-button): pow solver worker and click batcher"
```

---

### Task 16: Achievements — catalog grid, unlock toasts, milestone banner

**Files:**
- Create: `apps/the-button/src/lib/catalog.ts`, `apps/the-button/src/lib/unlocks.ts`, `apps/the-button/src/components/achievements-grid.tsx`, `apps/the-button/src/components/milestone-banner.tsx`
- Create: `apps/the-button/src/lib/__tests__/catalog.test.ts`, `apps/the-button/src/lib/__tests__/unlocks.test.ts`, `apps/the-button/src/components/__tests__/achievements.test.tsx`
- Edit: `apps/the-button/src/App.tsx`, `apps/the-button/src/main.tsx` (mount `Toaster`), `apps/the-button/package.json` (add `sonner`, `lucide-react`)

**Interfaces:**
- Consumes (frozen): `listAchievements(token?)` (personalized when a token is present — the anonymous rule still forwards the verified Authorization header), `SubmitClicksResponse.unlocked` via the batcher's `onUnlocked`, `MilestoneEvent` from T14's SSE wrapper, `Toaster` from `@algovn/ui/sonner`; spec §9 catalog ids/titles/triggers and milestone thresholds/titles.
- Produces: `ACHIEVEMENT_CATALOG` + `MILESTONE_CATALOG` (client-side fallback copy, full §9 set), `mergeCatalog`, `announceUnlocks`, `AchievementsGrid`, `MilestoneBanner`.

- [ ] **Step 1: Add sonner and lucide-react**

In `apps/the-button/package.json` add to `"dependencies"` (alphabetical; versions match showcase so pnpm dedupes):

```json
    "lucide-react": "^1.24.0",
    "sonner": "^2.0.7",
```

so dependencies read: `@algovn/ui`, `@fontsource-variable/geist`, `@fontsource-variable/geist-mono`, `hash-wasm`, `lucide-react`, `oidc-client-ts`, `react`, `react-dom`, `sonner`. Then:

```bash
cd /Users/duclm27/the-algovn/web && pnpm install
```

Expected: exit 0; both resolve to the versions already in the lockfile (no duplicates).

- [ ] **Step 2: RED — catalog fallback + merge tests**

`apps/the-button/src/lib/__tests__/catalog.test.ts`:

```ts
import { expect, it } from "vitest"
import { ACHIEVEMENT_CATALOG, MILESTONE_CATALOG, mergeCatalog } from "../catalog"

it("ships the full spec §9 catalog as client-side fallback", () => {
  expect(ACHIEVEMENT_CATALOG.map(entry => entry.id)).toEqual([
    "mvh",
    "ten",
    "century",
    "comma",
    "carpal",
    "stretch",
    "nice",
    "blaze",
    "bigbatch",
    "maxbatch",
    "night",
    "lunch",
  ])
  expect(MILESTONE_CATALOG.map(m => m.threshold)).toEqual([
    1_000, 100_000, 1_000_000, 10_000_000, 1_000_000_000,
  ])
})

it("merges server unlocks onto the fallback copy", () => {
  const merged = mergeCatalog([
    { id: "mvh", title: "Minimum Viable Human", unlockedAt: "2026-07-14T00:00:00Z" },
  ])
  expect(merged).toHaveLength(ACHIEVEMENT_CATALOG.length)
  const mvh = merged.find(entry => entry.id === "mvh")!
  expect(mvh.unlockedAt).toBe("2026-07-14T00:00:00Z")
  expect(mvh.description).not.toBe("") // fallback copy retained
  expect(merged.filter(entry => entry.unlockedAt)).toHaveLength(1)
})

it("returns the fully locked fallback catalog without server data", () => {
  const merged = mergeCatalog(undefined)
  expect(merged).toHaveLength(12)
  expect(merged.every(entry => !entry.unlockedAt)).toBe(true)
})
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: FAIL — `Cannot find module '../catalog'`.

- [ ] **Step 3: GREEN — catalog.ts**

`apps/the-button/src/lib/catalog.ts`:

```ts
// Client-side fallback copy for the spec §9 catalog. The server catalog wins
// when ListAchievements answers; this copy keeps the grid rendered (and the
// locked-entry mockery intact) when it can't (spec §13: Postgres down still
// serves the bare catalog).
import type { Achievement } from "./api"

export interface CatalogEntry {
  id: string
  title: string
  description: string
  unlockedAt?: string
}

export const ACHIEVEMENT_CATALOG: CatalogEntry[] = [
  {
    id: "mvh",
    title: "Minimum Viable Human",
    description: "You clicked the button. Once. Welcome to the revolution.",
  },
  {
    id: "ten",
    title: "Double Digits",
    description: "Ten clicks. The finger is warming up.",
  },
  {
    id: "century",
    title: "Century of Defiance",
    description: "100 clicks. Dedication, or boredom — we don't judge.",
  },
  {
    id: "comma",
    title: "The Comma Club",
    description: "1,000 clicks. You have earned punctuation.",
  },
  {
    id: "carpal",
    title: "Carpal Diem",
    description: "10,000 clicks. Seize the day. Stretch the wrist.",
  },
  {
    id: "stretch",
    title: "Please Stretch",
    description: "100,000 clicks. This is a health advisory.",
  },
  {
    id: "nice",
    title: "Nice.",
    description: "Your total crossed 69. Nice.",
  },
  {
    id: "blaze",
    title: "Botanical Enthusiast",
    description: "Your total crossed 420. Purely botanical interest, surely.",
  },
  {
    id: "bigbatch",
    title: "Mass Production",
    description: "500 clicks in a single batch. Industrial-grade mashing.",
  },
  {
    id: "maxbatch",
    title: "One Batch to Rule Them All",
    description: "A single 10,000-click batch. The server felt that.",
  },
  {
    id: "night",
    title: "3am Rebellion",
    description: "A batch between 03:00 and 03:59 in Ho Chi Minh City. Go to sleep. After one more.",
  },
  {
    id: "lunch",
    title: "Lunch Break Rebel",
    description: "A batch during the 12:00 lunch hour, Ho Chi Minh time. Productivity is a construct.",
  },
]

export const MILESTONE_CATALOG: { threshold: number; title: string }[] = [
  { threshold: 1_000, title: "A Thousand Tiny Rebellions" },
  { threshold: 100_000, title: "Six Figures of Defiance" },
  { threshold: 1_000_000, title: "One Million. Together We Did… This." },
  { threshold: 10_000_000, title: "Ten Million Clicks Nobody Asked For" },
  { threshold: 1_000_000_000, title: "The Billion" },
]

export function mergeCatalog(server: Achievement[] | undefined): CatalogEntry[] {
  const byId = new Map((server ?? []).map(a => [a.id, a]))
  return ACHIEVEMENT_CATALOG.map(entry => {
    const s = byId.get(entry.id)
    return {
      ...entry,
      title: s?.title ?? entry.title,
      description: s?.description ?? entry.description,
      unlockedAt: s?.unlockedAt,
    }
  })
}
```

```bash
cd /Users/duclm27/the-algovn/web && pnpm --filter the-button test
```

Expected: PASS.

- [ ] **Step 4: RED then GREEN — unlock toasts**

`apps/the-button/src/lib/__tests__/unlocks.test.ts`:

```ts
import { beforeEach, expect, it, vi } from "vitest"
import { toast } from "sonner"
import { announceUnlocks } from "../unlocks"

vi.mock("sonner", () => ({ toast: { success: vi.fn() } }))

beforeEach(() => {
  vi.mocked(toast.success).mockClear()
})

it("toasts each unlock with server copy when present", () => {
  announceUnlocks([
    { id: "mvh", title: "Minimum Viable Human", description: "server copy" },
    { id: "nice", title: "Nice." },
  ])
  expect(toast.success).toHaveBeenCalledTimes(2)
  expect(toast.success).toHaveBeenNthCalledWith(1, "Minimum Viable Human", {
    description: "server copy",
  })
  // falls back to the client catalog copy when the server omits fields
  expect(toast.success).toHaveBeenNthCalledWith(2, "Nice.", {
    description: "Your total crossed 69. Nice.",
  })
})

it("survives unknown achievement ids", () => {
  announceUnlocks([{ id: "mystery" }])
  expect(toast.success).toHaveBeenCalledWith("achievement unlocked", { description: undefined })
})
```

Run `pnpm --filter the-button test` — expected FAIL (`Cannot find module '../unlocks'`). Then create `apps/the-button/src/lib/unlocks.ts`:

```ts
import { toast } from "sonner"
import type { Achievement } from "./api"
import { ACHIEVEMENT_CATALOG } from "./catalog"

// Instant unlock toasts fed by SubmitClicksResponse.unlocked (spec §10).
export function announceUnlocks(unlocked: Achievement[]): void {
  for (const achievement of unlocked) {
    const fallback = ACHIEVEMENT_CATALOG.find(entry => entry.id === achievement.id)
    toast.success(achievement.title ?? fallback?.title ?? "achievement unlocked", {
      description: achievement.description ?? fallback?.description,
    })
  }
}
```

Run `pnpm --filter the-button test` — expected PASS.

- [ ] **Step 5: RED then GREEN — grid and banner components**

`apps/the-button/src/components/__tests__/achievements.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react"
import { expect, it } from "vitest"
import { mergeCatalog } from "../../lib/catalog"
import { AchievementsGrid } from "../achievements-grid"
import { MilestoneBanner } from "../milestone-banner"

it("renders the full catalog with locked entries greyed and mocked", () => {
  render(<AchievementsGrid entries={mergeCatalog([{ id: "mvh", unlockedAt: "2026-07-14T00:00:00Z" }])} />)
  const items = screen.getAllByRole("listitem")
  expect(items).toHaveLength(12)
  expect(screen.getByText("Minimum Viable Human").closest("[data-unlocked]")).toHaveAttribute(
    "data-unlocked",
    "true"
  )
  expect(screen.getByText("Carpal Diem").closest("[data-unlocked]")).toHaveAttribute(
    "data-unlocked",
    "false"
  )
  // locked entries keep their mocking copy visible
  expect(screen.getByText(/Seize the day\. Stretch the wrist\./)).toBeInTheDocument()
})

it("renders the milestone banner and hides it without a milestone", () => {
  const { rerender } = render(<MilestoneBanner milestone={null} />)
  expect(screen.queryByRole("status")).not.toBeInTheDocument()
  rerender(<MilestoneBanner milestone={{ threshold: 1000, title: "A Thousand Tiny Rebellions" }} />)
  expect(screen.getByRole("status")).toHaveTextContent("1,000 clicks — A Thousand Tiny Rebellions")
})
```

Run `pnpm --filter the-button test` — expected FAIL (`Cannot find module '../achievements-grid'`). Then:

`apps/the-button/src/components/achievements-grid.tsx`:

```tsx
import { LockIcon, TrophyIcon } from "lucide-react"
import { Card, CardContent } from "@algovn/ui/card"
import { cn } from "@algovn/ui/lib/utils"
import type { CatalogEntry } from "../lib/catalog"

export function AchievementsGrid({ entries }: { entries: CatalogEntry[] }) {
  return (
    <section aria-label="achievements" className="w-full max-w-3xl">
      <h2 className="text-muted-foreground mb-3 text-left text-sm font-medium">achievements</h2>
      <ul className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        {entries.map(entry => {
          const unlocked = Boolean(entry.unlockedAt)
          return (
            <li key={entry.id}>
              <Card data-unlocked={unlocked} className={cn("h-full", !unlocked && "opacity-60")}>
                <CardContent className="space-y-1.5 p-4 text-left">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    {unlocked ? (
                      <TrophyIcon className="text-primary size-4 shrink-0" />
                    ) : (
                      <LockIcon className="text-muted-foreground size-4 shrink-0" />
                    )}
                    <span>{entry.title}</span>
                  </div>
                  <p className="text-muted-foreground text-xs">{entry.description}</p>
                </CardContent>
              </Card>
            </li>
          )
        })}
      </ul>
    </section>
  )
}
```

`apps/the-button/src/components/milestone-banner.tsx`:

```tsx
import { PartyPopperIcon } from "lucide-react"

export function MilestoneBanner({
  milestone,
}: {
  milestone: { threshold: number; title: string } | null
}) {
  if (!milestone) return null
  return (
    <div
      role="status"
      className="bg-primary/10 text-primary border-primary/30 flex items-center gap-2 rounded-md border px-4 py-2 text-sm font-medium"
    >
      <PartyPopperIcon className="size-4 shrink-0" />
      <span>
        {milestone.threshold.toLocaleString("en-US")} clicks — {milestone.title}
      </span>
    </div>
  )
}
```

Run `pnpm --filter the-button test` — expected PASS.

- [ ] **Step 6: Wire into the page: load on mount, toasts on unlock, banner from SSE**

Add the `Toaster` in `apps/the-button/src/main.tsx` (full replacement):

```tsx
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { Toaster } from "@algovn/ui/sonner"
import { ThemeProvider } from "@algovn/ui/theme-provider"
import App from "./App"
import "@fontsource-variable/geist"
import "@fontsource-variable/geist-mono"
import "./index.css"

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider>
      <App />
      <Toaster />
    </ThemeProvider>
  </StrictMode>
)
```

Replace `apps/the-button/src/App.tsx` with the final assembly:

```tsx
import { useEffect, useRef, useState } from "react"
import { Button } from "@algovn/ui/button"
import { AchievementsGrid } from "./components/achievements-grid"
import { Callback } from "./components/callback"
import { ClickButton } from "./components/click-button"
import { Counter } from "./components/counter"
import { MilestoneBanner } from "./components/milestone-banner"
import { Taglines } from "./components/taglines"
import { issueChallenge, listAchievements, submitClicks } from "./lib/api"
import { signIn } from "./lib/auth"
import { Batcher } from "./lib/batcher"
import { runBench } from "./lib/bench"
import { mergeCatalog, type CatalogEntry } from "./lib/catalog"
import { LiveCounter, type LiveMode } from "./lib/liveCounter"
import { createWorkerSolver } from "./lib/solverClient"
import { announceUnlocks } from "./lib/unlocks"
import { useAuth } from "./lib/use-auth"

// No router: the app has exactly two views — the page and the OIDC callback.
export default function App() {
  const [isCallback, setIsCallback] = useState(() =>
    window.location.pathname.endsWith("/callback")
  )
  if (isCallback) {
    return (
      <Callback
        onDone={() => {
          window.history.replaceState(null, "", "/the-button/")
          setIsCallback(false)
        }}
      />
    )
  }
  return <Home />
}

type Milestone = { threshold: number; title: string }

function Home() {
  const { user, token } = useAuth()
  const tokenRef = useRef<string | null>(null)
  tokenRef.current = token
  const [total, setTotal] = useState<number | null>(null)
  const [mode, setMode] = useState<LiveMode>("connecting")
  const [myTotal, setMyTotal] = useState<number | null>(null)
  const [pending, setPending] = useState(0)
  const [catalog, setCatalog] = useState<CatalogEntry[]>(() => mergeCatalog(undefined))
  const [milestone, setMilestone] = useState<Milestone | null>(null)
  const batcherRef = useRef<Batcher | null>(null)

  useEffect(() => {
    const solver = createWorkerSolver()
    batcherRef.current = new Batcher({
      api: { issueChallenge, submitClicks },
      solver,
      getToken: () => tokenRef.current,
      onUserTotal: setMyTotal,
      onPendingChange: setPending,
      onUnlocked: unlocked => {
        announceUnlocks(unlocked)
        setCatalog(prev =>
          prev.map(entry => {
            const hit = unlocked.find(a => a.id === entry.id)
            return hit
              ? { ...entry, unlockedAt: hit.unlockedAt ?? new Date().toISOString() }
              : entry
          })
        )
      },
      onError: err => console.error("submit failed", err),
    })
    return () => {
      batcherRef.current = null
      solver.terminate()
    }
  }, [])

  useEffect(() => {
    const live = new LiveCounter({
      onEvent: event => {
        if (event.type === "counter") setTotal(event.total)
        else setMilestone({ threshold: event.threshold, title: event.title })
      },
      onModeChange: setMode,
    })
    live.start()
    return () => live.stop()
  }, [])

  // Catalog + reached milestones on load; personalized when a token exists
  // (the anonymous rule still forwards the verified Authorization header).
  useEffect(() => {
    let cancelled = false
    listAchievements(token ?? undefined)
      .then(res => {
        if (cancelled) return
        setCatalog(mergeCatalog(res.catalog))
        const latest = (res.milestones ?? [])
          .map(m => ({ threshold: Number(m.threshold ?? "0"), title: m.title ?? "" }))
          .filter(m => m.threshold > 0 && m.title !== "")
          .sort((a, b) => b.threshold - a.threshold)[0]
        if (latest) setMilestone(current => (current ? current : latest))
      })
      .catch(() => {
        // offline/unreachable: the fallback catalog is already rendered
      })
    return () => {
      cancelled = true
    }
  }, [token])

  const benchRan = useRef(false)
  useEffect(() => {
    if (benchRan.current) return
    if (new URLSearchParams(window.location.search).has("bench")) {
      benchRan.current = true
      void runBench()
    }
  }, [])

  return (
    <main className="mx-auto flex min-h-svh max-w-4xl flex-col items-center justify-center gap-8 p-6 text-center">
      <header className="space-y-3">
        <h1 className="font-mono text-3xl font-semibold tracking-tight sm:text-4xl">the button</h1>
        <Taglines />
      </header>
      <MilestoneBanner milestone={milestone} />
      <Counter total={total} />
      <p className="text-muted-foreground text-xs">
        {mode === "live" ? "live" : mode === "polling" ? "live updates degraded — polling" : "connecting…"}
      </p>
      {user ? (
        <ClickButton
          onMash={() => batcherRef.current?.click()}
          myTotal={myTotal}
          pending={pending}
        />
      ) : (
        <Button size="lg" onClick={() => void signIn()}>
          sign in to contribute
        </Button>
      )}
      <AchievementsGrid entries={catalog} />
    </main>
  )
}
```

- [ ] **Step 7: Verify and commit**

```bash
cd /Users/duclm27/the-algovn/web
pnpm --filter the-button test
pnpm turbo lint typecheck build --filter=the-button
```

Expected: all suites PASS — including the app smoke test (the setup file's rejecting `fetch` makes `listAchievements` fail, which correctly leaves the fallback catalog rendered); lint/typecheck/build exit 0.

```bash
cd /Users/duclm27/the-algovn/web
git add apps/the-button pnpm-lock.yaml
git commit -m "feat(the-button): achievements grid, unlock toasts, milestone banner"
```

---

### Task 17: Delivery — nginx static image + web CI job

**Files:**
- Create: `apps/the-button/Dockerfile`, `apps/the-button/nginx.conf`
- Edit: `.github/workflows/ci.yml` (web repo root — add `build-push-the-button` job)

**Interfaces:**
- Consumes: `pnpm --filter the-button build` → `dist/` (T12); repo Docker conventions (build context = repo root, corepack + frozen lockfile, amd64-only) from `apps/landing/Dockerfile` and `.github/workflows/ci.yml`.
- Produces (frozen): image `ghcr.io/the-algovn/web-the-button:main` (+ `sha-*` tag), nginx:1.27-alpine serving under `/the-button/` with `Cache-Control: public,max-age=31536000,immutable` on `/the-button/assets/` and `no-cache` on `index.html`, SPA fallback to `/the-button/index.html`; `VITE_OIDC_CLIENT_ID` build-arg wired from the repo Actions variable (set by the user after T19; `placeholder` until then). Consumed by T18 (Deployment in ns `the-button-web`) and the Cloudflare cache rule for `/the-button/assets/*`.

- [ ] **Step 1: nginx config**

`apps/the-button/nginx.conf`:

```nginx
server {
  listen 80;
  server_name _;
  root /usr/share/nginx/html;
  index index.html;

  # hashed bundles: cache forever (Cloudflare cache rule targets this path)
  location /the-button/assets/ {
    add_header Cache-Control "public, max-age=31536000, immutable";
  }

  # entrypoint must revalidate so deploys roll out immediately
  location = /the-button/index.html {
    add_header Cache-Control "no-cache";
  }

  # SPA fallback: client-side paths (e.g. /the-button/callback) serve the app
  location /the-button/ {
    try_files $uri $uri/ /the-button/index.html;
  }

  location = / {
    return 302 /the-button/;
  }
}
```

- [ ] **Step 2: Dockerfile**

`apps/the-button/Dockerfile` (build stage mirrors `apps/landing/Dockerfile`; runtime pinned to the frozen `nginx:1.27-alpine`):

```dockerfile
# Build context is the REPO ROOT: podman build -f apps/the-button/Dockerfile .
FROM node:24-slim AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json .npmrc ./
COPY packages ./packages
COPY apps/the-button ./apps/the-button
RUN pnpm install --frozen-lockfile
# Vite inlines import.meta.env at build time. "placeholder" until the Zitadel
# SPA app exists (T19) — then set the VITE_OIDC_CLIENT_ID Actions variable.
ARG VITE_OIDC_CLIENT_ID=placeholder
ENV VITE_OIDC_CLIENT_ID=$VITE_OIDC_CLIENT_ID
RUN pnpm --filter the-button build

FROM nginx:1.27-alpine AS runtime
COPY apps/the-button/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/apps/the-button/dist /usr/share/nginx/html/the-button
EXPOSE 80
```

- [ ] **Step 3: Local verification with podman**

```bash
cd /Users/duclm27/the-algovn/web
podman machine start 2>/dev/null || true
podman build -f apps/the-button/Dockerfile -t web-the-button:dev .
podman run -d --rm --name tb -p 8080:80 web-the-button:dev
sleep 1
curl -sI http://localhost:8080/the-button/ | grep -i '^cache-control'
```

Expected: `Cache-Control: no-cache` (the directory request internally serves `/the-button/index.html`).

```bash
ASSET=$(curl -s http://localhost:8080/the-button/ | grep -o '/the-button/assets/[^"]*' | head -1)
echo "$ASSET"
curl -sI "http://localhost:8080$ASSET" | grep -i '^cache-control'
```

Expected: an asset path like `/the-button/assets/index-D3adB33f.js`, then `Cache-Control: public, max-age=31536000, immutable`.

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/the-button/callback
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/
podman stop tb
```

Expected: `200` (SPA fallback serves index.html for the callback route), then `302` (root redirects to /the-button/).

- [ ] **Step 4: CI job**

In `/Users/duclm27/the-algovn/web/.github/workflows/ci.yml`, append this job at the end of the `jobs:` map (sibling of `build-push-landing`, identical shape — the shared `ci` job already lints/tests/builds the new app via turbo):

```yaml
  build-push-the-button:
    # amd64-only per iac convention: workloads run on algovn-w1, not the arm64 Pi.
    needs: ci
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    concurrency:
      group: the-button-build-push
      cancel-in-progress: false
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/metadata-action@v5
        id: meta
        with:
          images: ghcr.io/the-algovn/web-the-button
          tags: |
            type=raw,value=main
            type=sha,prefix=sha-
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: apps/the-button/Dockerfile
          platforms: linux/amd64
          push: true
          build-args: |
            VITE_OIDC_CLIENT_ID=${{ vars.VITE_OIDC_CLIENT_ID || 'placeholder' }}
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

Validate the YAML parses:

```bash
cd /Users/duclm27/the-algovn/web
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml OK')"
```

Expected: `yaml OK`.

**USER STEP (deferred — record it, do not do it now):** after T19 creates the Zitadel SPA app, the user sets the repo Actions variable and rebuilds:

```bash
gh variable set VITE_OIDC_CLIENT_ID --repo the-algovn/web --body "<zitadel-client-id>"
git commit --allow-empty -m "chore(the-button): rebuild with real oidc client id" && git push
```

Until then every push builds with `placeholder` — the site renders and the counter is live; only sign-in fails (Zitadel rejects the unknown client), which is expected pre-T19.

- [ ] **Step 5: Commit (two focused commits)**

```bash
cd /Users/duclm27/the-algovn/web
git add apps/the-button/Dockerfile apps/the-button/nginx.conf
git commit -m "feat(the-button): nginx static image with immutable asset caching"
git add .github/workflows/ci.yml
git commit -m "ci: build and push web-the-button image"
```

- [ ] **Step 6: Push and verify CI (after the T12–T16 commits are reviewed)**

```bash
cd /Users/duclm27/the-algovn/web
git push origin main
gh run watch --repo the-algovn/web --exit-status
```

Expected: `ci` job green, then `build-push`, `build-push-landing` and `build-push-the-button` all green; `gh api /orgs/the-algovn/packages/container/web-the-button/versions --jq '.[0].metadata.container.tags'` lists `["main","sha-…"]`. Note: GHCR packages in this org are private — T18's namespace needs the sealed `registry-creds` copy (cluster D handles it).
### Task 18: iac deploy — service + SPA + registration + secrets + Postgres DB

**Files:**
- Create: `iac/apps/the-button/namespace.yaml`
- Create: `iac/apps/the-button/registry-creds-sealed.yaml` (kubeseal output)
- Create: `iac/apps/the-button/amqp-creds-sealed.yaml` (kubeseal output)
- Create: `iac/apps/the-button/redis-creds-sealed.yaml` (kubeseal output)
- Create: `iac/apps/the-button/pow-secret-sealed.yaml` (kubeseal output)
- Create: `iac/apps/the-button/pg-the-button-sealed.yaml` (kubeseal output)
- Create: `iac/apps/the-button/deployment.yaml`
- Create: `iac/apps/the-button/service.yaml`
- Create: `iac/apps/the-button/vmservicescrape.yaml`
- Create: `iac/apps/the-button/kustomization.yaml`
- Create: `iac/apps/the-button-web/namespace.yaml`
- Create: `iac/apps/the-button-web/registry-creds-sealed.yaml` (kubeseal output)
- Create: `iac/apps/the-button-web/deployment.yaml`
- Create: `iac/apps/the-button-web/service.yaml`
- Create: `iac/apps/the-button-web/ingress.yaml`
- Create: `iac/apps/the-button-web/kustomization.yaml`
- Create: `iac/apps/api-control-plane/registrations/the-button.yaml`
- Create: `iac/platform/postgres/manifests/db-the-button.yaml`
- Create: `iac/platform/postgres/manifests/pg-role-the-button-sealed.yaml` (kubeseal output)
- Create: `iac/clusters/algovn/apps/the-button.yaml`
- Create: `iac/clusters/algovn/apps/the-button-web.yaml`
- Create: `iac/platform/image-updater/the-button-updater.yaml`
- Create: `iac/platform/image-updater/the-button-web-updater.yaml`
- Modify: `iac/apps/api-control-plane/kustomization.yaml` (add `registrations/the-button.yaml` to `configMapGenerator.files`)
- Modify: `iac/platform/postgres/manifests/cluster.yaml` (add managed role `the_button`)
- Modify: `iac/platform/postgres/manifests/kustomization.yaml` (add db + role sealed file)
- Modify: `iac/platform/image-updater/kustomization.yaml` (add both new updater files)

**Interfaces:**
- Consumes (frozen): image `ghcr.io/the-algovn/the-button-service:main` (T11), image `ghcr.io/the-algovn/web-the-button:main` (T17), env contract `PG_URL`/`REDIS_URL`/`AMQP_URL`/`POW_SECRET`/`POW_W0`/`LISTEN_ADDR`/`METRICS_ADDR` (skeleton), proto package `algovn.button.v1` (T6), registration YAML content (spec §4), redis password (sealed `redis-auth` key `password`, ns `redis`, from T2), amqp url (sealed `amqp-creds` key `url`, ns `demo-service`/`api-control-plane`, from api-control-plane repo Task 11), ghcr dockerconfig (sealed `ghcr-creds`, ns `argocd`).
- Produces: ns `the-button` running `the-button-service` (headless svc :9090 grpc / :9091 metrics, replicas 2); ns `the-button-web` running `web-the-button` behind Ingress `algovn.com` path `/the-button`; Postgres database `the_button` owned by role `the_button`; sealed secret `pg-the-button` key `uri` (the DSN the service reads as `PG_URL`) — **this resolves the skeleton's open item: the declarative CNPG onboarding (postgres.md) does NOT emit a connection-URI secret; it seals `username`/`password` only, so author D seals a second secret `pg-the-button` key `uri` in ns `the-button` carrying the full DSN.**

> **Sealing model (memory `algovn-local-kubeseal`):** all seals run locally —
> `kubectl --context algovn-remote create secret … --dry-run=client -o yaml | kubeseal --context algovn-remote --controller-name sealed-secrets --controller-namespace sealed-secrets --format yaml`.
> Sealing is namespace+name-scoped and fails SILENTLY on mismatch — after applying, always confirm the *unsealed* Secret appears in-cluster.
> Run every multi-line loop below in **bash** (the login shell is fish). Plaintext staging goes in `~/.secrets/` (`chmod 700`); scrub afterward.

- [ ] **Step 1: Namespaces**

`iac/apps/the-button/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: the-button
```

`iac/apps/the-button-web/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: the-button-web
```

- [ ] **Step 2: Seal registry-creds into BOTH app namespaces (private GHCR packages)**

```bash
cd /Users/duclm27/the-algovn/iac
mkdir -p apps/the-button apps/the-button-web
TMP=$(mktemp)
kubectl --context algovn-remote -n argocd get secret ghcr-creds \
  -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d > "$TMP"
for ns in the-button the-button-web; do
  kubectl --context algovn-remote create secret generic registry-creds -n "$ns" \
    --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson="$TMP" \
    --dry-run=client -o yaml \
  | kubeseal --context algovn-remote --controller-name sealed-secrets \
      --controller-namespace sealed-secrets --format yaml \
  > "apps/$ns/registry-creds-sealed.yaml"
done
rm -f "$TMP"
```

Expected: both files start with `apiVersion: bitnami.com/v1alpha1` / `kind: SealedSecret` and carry `metadata.namespace` matching their dir. Grep-check: `grep -l 'kind: SealedSecret' apps/the-button/registry-creds-sealed.yaml apps/the-button-web/registry-creds-sealed.yaml` prints both.

- [ ] **Step 3: Seal amqp-creds into ns the-button (reuse the live RabbitMQ url)**

The unsealed `amqp-creds` Secret already exists in `demo-service`; copy its `url` verbatim so the-button shares the same `events` user (no password re-derivation).

```bash
cd /Users/duclm27/the-algovn/iac
AMQP_URL=$(kubectl --context algovn-remote -n demo-service get secret amqp-creds \
  -o jsonpath='{.data.url}' | base64 -d)
kubectl --context algovn-remote create secret generic amqp-creds -n the-button \
  --from-literal=url="$AMQP_URL" --dry-run=client -o yaml \
| kubeseal --context algovn-remote --controller-name sealed-secrets \
    --controller-namespace sealed-secrets --format yaml \
> apps/the-button/amqp-creds-sealed.yaml
unset AMQP_URL
```

Expected: `apps/the-button/amqp-creds-sealed.yaml` is a SealedSecret named `amqp-creds`. The url should read `amqp://events:…@rabbitmq.rabbitmq.svc.cluster.local:5672/` (confirm the host substring: `grep -q rabbitmq.rabbitmq apps/the-button/amqp-creds-sealed.yaml || echo MISMATCH` — note the value is encrypted, so instead verify after apply in Step 14).

- [ ] **Step 4: Seal redis-creds into ns the-button (url embeds the platform Redis password)**

```bash
cd /Users/duclm27/the-algovn/iac
REDIS_PASS=$(kubectl --context algovn-remote -n redis get secret redis-auth \
  -o jsonpath='{.data.password}' | base64 -d)
REDIS_URL="redis://:${REDIS_PASS}@redis.redis.svc.cluster.local:6379/0"
kubectl --context algovn-remote create secret generic redis-creds -n the-button \
  --from-literal=url="$REDIS_URL" --dry-run=client -o yaml \
| kubeseal --context algovn-remote --controller-name sealed-secrets \
    --controller-namespace sealed-secrets --format yaml \
> apps/the-button/redis-creds-sealed.yaml
unset REDIS_PASS REDIS_URL
```

Expected: SealedSecret named `redis-creds`. (`redis-auth` is produced by T2; if `kubectl … get secret redis-auth` 404s, T2 has not landed — block here.)

- [ ] **Step 5: Generate + seal pow-secret (32 random bytes, base64, key `key`)**

```bash
cd /Users/duclm27/the-algovn/iac
mkdir -p ~/.secrets && chmod 700 ~/.secrets
openssl rand -base64 32 | tr -d '\n' > ~/.secrets/the-button-pow-key
echo "SAVE THIS to the password manager as 'the-button-pow-secret': $(cat ~/.secrets/the-button-pow-key)"
kubectl --context algovn-remote create secret generic pow-secret -n the-button \
  --from-file=key=$HOME/.secrets/the-button-pow-key --dry-run=client -o yaml \
| kubeseal --context algovn-remote --controller-name sealed-secrets \
    --controller-namespace sealed-secrets --format yaml \
> apps/the-button/pow-secret-sealed.yaml
# scrub plaintext (macOS has no shred)
dd if=/dev/urandom of=$HOME/.secrets/the-button-pow-key bs=1 \
  count=$(stat -f%z $HOME/.secrets/the-button-pow-key) conv=notrunc && rm -f $HOME/.secrets/the-button-pow-key
```

Expected: SealedSecret named `pow-secret` with encrypted key `key`. The plaintext is now also in the password manager (`the-button-pow-secret`) — T20 (calibration) and T21 (corpus generator) read it from there, since HMAC key rotation would otherwise invalidate every issued challenge.

- [ ] **Step 6: Postgres role + DB (declarative CNPG onboarding, postgres.md)**

Generate one password, seal it TWICE (same password, two namespaces): a basic-auth role secret in ns `postgres` (CNPG reads `password` to set the role) and a full-DSN secret in ns `the-button` (the service reads it as `PG_URL`). Role name `the_button` (underscore — a valid SQL identifier); k8s secret names use hyphens.

```bash
cd /Users/duclm27/the-algovn/iac
openssl rand -base64 24 | tr -d '[:space:]' > ~/.secrets/the-button-pg-pw
echo "SAVE THIS to the password manager as 'the-button-postgres': $(cat ~/.secrets/the-button-pg-pw)"

# (a) role secret in ns postgres — basic-auth username/password (postgres.md pattern)
kubectl --context algovn-remote create secret generic pg-role-the-button -n postgres \
  --type=kubernetes.io/basic-auth --from-literal=username=the_button \
  --from-file=password=$HOME/.secrets/the-button-pg-pw --dry-run=client -o yaml \
| kubeseal --context algovn-remote --controller-name sealed-secrets \
    --controller-namespace sealed-secrets --format yaml \
> platform/postgres/manifests/pg-role-the-button-sealed.yaml

# (b) DSN secret in ns the-button — full connection URI the service dials
PG_PW=$(cat ~/.secrets/the-button-pg-pw)
PG_URI="postgres://the_button:${PG_PW}@pg-rw.postgres.svc.cluster.local:5432/the_button"
kubectl --context algovn-remote create secret generic pg-the-button -n the-button \
  --from-literal=uri="$PG_URI" --dry-run=client -o yaml \
| kubeseal --context algovn-remote --controller-name sealed-secrets \
    --controller-namespace sealed-secrets --format yaml \
> apps/the-button/pg-the-button-sealed.yaml
unset PG_PW PG_URI

dd if=/dev/urandom of=$HOME/.secrets/the-button-pg-pw bs=1 \
  count=$(stat -f%z $HOME/.secrets/the-button-pg-pw) conv=notrunc && rm -f $HOME/.secrets/the-button-pg-pw
```

Expected: `pg-role-the-button-sealed.yaml` (ns postgres) and `pg-the-button-sealed.yaml` (ns the-button), both `kind: SealedSecret`.

- [ ] **Step 7: Managed role + Database CR in the postgres platform**

In `iac/platform/postgres/manifests/cluster.yaml`, append the role under `spec.managed.roles` (after the `openfga` entry, keeping the two-space list style):

```yaml
      - name: the_button
        ensure: present
        login: true
        passwordSecret: { name: pg-role-the-button }
```

`iac/platform/postgres/manifests/db-the-button.yaml` (model on `db-zitadel.yaml`):

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Database
metadata:
  name: the-button
  namespace: postgres
spec:
  name: the_button
  owner: the_button
  cluster: { name: pg }
```

Add both new files to `iac/platform/postgres/manifests/kustomization.yaml` `resources:` (append):

```yaml
  - db-the-button.yaml
  - pg-role-the-button-sealed.yaml
```

- [ ] **Step 8: Service Deployment (2 replicas, env per skeleton)**

`iac/apps/the-button/deployment.yaml` (model on `demo-service` — grpc probes, headless-ready; add rolling strategy like api-control-plane):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: the-button-service
  namespace: the-button
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  selector:
    matchLabels: { app: the-button-service }
  template:
    metadata:
      labels: { app: the-button-service }
    spec:
      containers:
        - name: the-button-service
          image: ghcr.io/the-algovn/the-button-service:main
          ports:
            - { containerPort: 9090, name: grpc }
            - { containerPort: 9091, name: metrics }
          env:
            - { name: LISTEN_ADDR, value: ":9090" }
            - { name: METRICS_ADDR, value: ":9091" }
            - { name: POW_W0, value: "16384" }
            - name: PG_URL
              valueFrom: { secretKeyRef: { name: pg-the-button, key: uri } }
            - name: REDIS_URL
              valueFrom: { secretKeyRef: { name: redis-creds, key: url } }
            - name: AMQP_URL
              valueFrom: { secretKeyRef: { name: amqp-creds, key: url } }
            - name: POW_SECRET
              valueFrom: { secretKeyRef: { name: pow-secret, key: key } }
          readinessProbe:
            grpc: { port: 9090 }
            initialDelaySeconds: 5
          livenessProbe:
            grpc: { port: 9090 }
            initialDelaySeconds: 10
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { memory: 256Mi }
      imagePullSecrets:
        - name: registry-creds
```

- [ ] **Step 9: Headless Service + VMServiceScrape**

`iac/apps/the-button/service.yaml` (headless like demo-service so grpc-go `dns:///…` round-robins the 2 pods; `labels.app` present for VMServiceScrape):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: the-button-service
  namespace: the-button
  labels: { app: the-button-service }   # VMServiceScrape selects Services by THEIR labels
spec:
  clusterIP: None          # headless: enables grpc-go dns:/// round_robin
  selector: { app: the-button-service }
  ports:
    - { port: 9090, targetPort: 9090, name: grpc }
    - { port: 9091, targetPort: 9091, name: metrics }
```

`iac/apps/the-button/vmservicescrape.yaml`:

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMServiceScrape
metadata:
  name: the-button-service
  namespace: monitoring
spec:
  namespaceSelector: { matchNames: [the-button] }
  selector:
    matchLabels: { app: the-button-service }
  endpoints:
    - port: metrics
```

- [ ] **Step 10: Service kustomization**

`iac/apps/the-button/kustomization.yaml` (mirror api-control-plane, including the `images` digest block image-updater writes back to):

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - registry-creds-sealed.yaml
  - amqp-creds-sealed.yaml
  - redis-creds-sealed.yaml
  - pow-secret-sealed.yaml
  - pg-the-button-sealed.yaml
  - deployment.yaml
  - service.yaml
  - vmservicescrape.yaml
images:
  - name: ghcr.io/the-algovn/the-button-service
    newName: ghcr.io/the-algovn/the-button-service
    newTag: main
```

> No `digest:` line yet — the ImageUpdater CR (Step 13) writes it on first reconcile. `kustomize build` is valid without it.

- [ ] **Step 11: SPA namespace, deployment, service, ingress, kustomization (model on `showcase`)**

`iac/apps/the-button-web/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-the-button
  namespace: the-button-web
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  selector:
    matchLabels: { app: web-the-button }
  template:
    metadata:
      labels: { app: web-the-button }
    spec:
      containers:
        - name: web-the-button
          image: ghcr.io/the-algovn/web-the-button:main
          ports: [{ containerPort: 80 }]
          readinessProbe:
            httpGet: { path: /the-button/, port: 80 }
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet: { path: /the-button/, port: 80 }
            initialDelaySeconds: 15
            periodSeconds: 20
          resources:
            requests: { cpu: 25m, memory: 32Mi }
            limits: { memory: 128Mi }
      imagePullSecrets:
        - name: registry-creds
```

`iac/apps/the-button-web/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: web-the-button
  namespace: the-button-web
spec:
  selector: { app: web-the-button }
  ports: [{ port: 80, targetPort: 80 }]
```

`iac/apps/the-button-web/ingress.yaml` (same host `algovn.com` as showcase/landing; `/the-button` is a longer prefix than landing's `/`, so Kong routes it first):

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web-the-button
  namespace: the-button-web
spec:
  ingressClassName: kong
  rules:
    - host: algovn.com
      http:
        paths:
          - path: /the-button
            pathType: Prefix
            backend:
              service:
                name: web-the-button
                port: { number: 80 }
  tls:
    - hosts: [algovn.com]
```

`iac/apps/the-button-web/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - registry-creds-sealed.yaml
  - deployment.yaml
  - service.yaml
  - ingress.yaml
images:
  - name: ghcr.io/the-algovn/web-the-button
    newName: ghcr.io/the-algovn/web-the-button
    newTag: main
```

- [ ] **Step 12: Registration file (spec §4 verbatim) + wire into acp configMapGenerator**

`iac/apps/api-control-plane/registrations/the-button.yaml`:

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

In `iac/apps/api-control-plane/kustomization.yaml`, add the file to the generator (keep `demo.yaml`):

```yaml
configMapGenerator:
  - name: api-registrations
    namespace: api-control-plane
    files:
      - registrations/demo.yaml
      - registrations/the-button.yaml
```

- [ ] **Step 13: Argo Applications (auto-discovered by root recurse) + ImageUpdater CRs**

`iac/clusters/algovn/apps/the-button.yaml`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: the-button
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/the-algovn/iac.git
    targetRevision: main
    path: apps/the-button
  destination:
    server: https://kubernetes.default.svc
    namespace: the-button
  syncPolicy:
    automated: { prune: true, selfHeal: true }
```

`iac/clusters/algovn/apps/the-button-web.yaml`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: the-button-web
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  project: default
  source:
    repoURL: https://github.com/the-algovn/iac.git
    targetRevision: main
    path: apps/the-button-web
  destination:
    server: https://kubernetes.default.svc
    namespace: the-button-web
  syncPolicy:
    automated: { prune: true, selfHeal: true }
```

`iac/platform/image-updater/the-button-updater.yaml` (model on `api-control-plane-updater.yaml`):

```yaml
apiVersion: argocd-image-updater.argoproj.io/v1alpha1
kind: ImageUpdater
metadata:
  name: the-button-updater
  namespace: argocd
spec:
  applicationRefs:
    - namePattern: the-button
      images:
        - alias: the-button-service
          imageName: ghcr.io/the-algovn/the-button-service:main
          commonUpdateSettings:
            updateStrategy: digest
            pullSecret: pullsecret:argocd/ghcr-creds
          manifestTargets:
            kustomize:
              name: ghcr.io/the-algovn/the-button-service
      writeBackConfig:
        method: git:secret:argocd/git-creds
        gitConfig:
          branch: main
          writeBackTarget: kustomization:.
```

`iac/platform/image-updater/the-button-web-updater.yaml`:

```yaml
apiVersion: argocd-image-updater.argoproj.io/v1alpha1
kind: ImageUpdater
metadata:
  name: the-button-web-updater
  namespace: argocd
spec:
  applicationRefs:
    - namePattern: the-button-web
      images:
        - alias: web-the-button
          imageName: ghcr.io/the-algovn/web-the-button:main
          commonUpdateSettings:
            updateStrategy: digest
            pullSecret: pullsecret:argocd/ghcr-creds
          manifestTargets:
            kustomize:
              name: ghcr.io/the-algovn/web-the-button
      writeBackConfig:
        method: git:secret:argocd/git-creds
        gitConfig:
          branch: main
          writeBackTarget: kustomization:.
```

Add both to `iac/platform/image-updater/kustomization.yaml` `resources:` (append):

```yaml
  - the-button-updater.yaml
  - the-button-web-updater.yaml
```

- [ ] **Step 14: Validate, push, verify rollout**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh
```

Expected: ends with `PASS` (every `kustomize build` prints `ok:`, kubeconform strict passes, gitleaks finds nothing — the sealed files match the `*-sealed.yaml` allowlist).

Commit (see Step 15), push, then let Argo sync (or force it) and verify:

```bash
argocd app sync postgres the-button the-button-web api-control-plane image-updater --core 2>/dev/null || true
kubectl --context algovn-remote -n postgres get database the-button \
  -o jsonpath='{.status.applied}{"\n"}'                    # expect: true
kubectl --context algovn-remote -n postgres get cluster pg \
  -o jsonpath='{.status.managedRolesStatus}{"\n"}'          # the_button under "reconciled"
kubectl --context algovn-remote -n the-button get secret pg-the-button redis-creds amqp-creds pow-secret registry-creds
kubectl --context algovn-remote -n the-button rollout status deploy/the-button-service --timeout=120s
kubectl --context algovn-remote -n the-button-web rollout status deploy/web-the-button --timeout=120s
kubectl --context algovn-remote -n the-button get pods -l app=the-button-service  # expect 2/2 Running
```

Expected: `database applied=true`; all 5 secrets listed (any missing = sealed for the wrong ns/name — reseal); both deployments `successfully rolled out`; two service pods Ready.

- [ ] **Step 15: End-to-end curl acceptance**

```bash
# SPA index served under the path (note the TRAILING SLASH — /the-button/ )
curl -s -o /dev/null -w '%{http_code}\n' https://algovn.com/the-button/     # expect: 200
curl -s https://algovn.com/the-button/ | grep -o '<title>[^<]*</title>'      # expect: the SPA <title>

# gRPC-transcoded anonymous read through the gateway
curl -s https://api.algovn.com/the-button/algovn.button.v1.ButtonService/GetCounter \
  -H 'content-type: application/json' -d '{}'
```

Expected for `GetCounter` on a **fresh** deploy: `{}` — **NOT** `{"total":"0"}`. The api-control-plane marshals with `protojson.MarshalOptions{}` (proto3 JSON defaults, `EmitUnpopulated:false`), which **omits zero-valued fields**, so `total = 0` disappears. Once the counter is non-zero the field appears and — because `total` is `uint64` — **protojson renders 64-bit integers as JSON strings**: `{"total":"1"}`, `{"total":"12345"}`. So the acceptance assertion is: fresh ⇒ `{}` (HTTP 200); after the first accepted batch (T19) ⇒ `{"total":"<n>"}` with `<n>` a quoted decimal. (If you want a literal `{"total":"0"}` the acp marshaler would need `EmitUnpopulated:true` — that is an api-control-plane change, out of scope here; the string encoding is inherent and correct.)

- [ ] **Step 16: Commit (stage explicit files only; no AI trailers)**

```bash
cd /Users/duclm27/the-algovn/iac
git add apps/the-button apps/the-button-web \
  apps/api-control-plane/registrations/the-button.yaml apps/api-control-plane/kustomization.yaml \
  platform/postgres/manifests/db-the-button.yaml platform/postgres/manifests/pg-role-the-button-sealed.yaml \
  platform/postgres/manifests/cluster.yaml platform/postgres/manifests/kustomization.yaml \
  clusters/algovn/apps/the-button.yaml clusters/algovn/apps/the-button-web.yaml \
  platform/image-updater/the-button-updater.yaml platform/image-updater/the-button-web-updater.yaml \
  platform/image-updater/kustomization.yaml
git diff --cached --stat   # confirm ONLY the above files
git commit -m "Deploy the-button service + SPA: namespaces, sealed secrets, Postgres db, registration, Argo apps, image updaters"
```

Verify: `git diff --cached --stat` (before commit) lists exactly the intended files and no stray scratch/superpowers artifacts.

---

### Task 19: Zitadel onboarding (USER console task) + end-to-end smoke

**Files:**
- Modify: `iac/docs/runbooks/api-control-plane.md` (append the-button acceptance transcript — optional, at controller discretion) — primary output is a completed checklist + the recorded `VITE_OIDC_CLIENT_ID`.

**Interfaces:**
- Consumes (frozen): Zitadel project/app contract (skeleton) — project `the-button`, SPA app PKCE, **Access Token Type: JWT**, redirects `https://algovn.com/the-button/callback` + `http://localhost:5173/the-button/callback`; SPA build var `VITE_OIDC_CLIENT_ID`; running service + SPA + registration from T18.
- Produces: a live Zitadel client id; a rebuilt SPA image whose `VITE_OIDC_CLIENT_ID` matches; a passing e2e smoke transcript. **Added interface:** GitHub Actions **repository variable** `VITE_OIDC_CLIENT_ID` on repo `the-algovn/web` (the web CI, T17, reads it at build via `${{ vars.VITE_OIDC_CLIENT_ID }}`).

> This task is **USER-GATED at the console step**: the controller performs the console clicks itself only if the user is driving; otherwise the controller presents the exact click-path and waits for the user to complete it and paste back the client id.

- [ ] **Step 1: (USER, console) Create the project + SPA application**

Exact click-path (authnz-conventions.md §"Registering a product" + zitadel.md), signed in as an AlgoVN-org admin at `https://id.algovn.com/ui/console`:

1. **Projects → Create** → name `the-button` → Create.
2. On the project, **check "Assert Roles on Authentication"** is fine to leave default; **do NOT** enable the two project-level "Check authorization/project on authentication" toggles (zitadel.md warns these produce an IdP-side `Errors.User.ProjectRequired` deny instead of the app-side flow). No roles needed (v1 uses only `anonymous`/`authenticated` — spec §11.6).
3. In the project → **Applications → New** → type **User Agent (SPA)** → Next.
4. **Authentication Method: PKCE** (no secret) → Next.
5. **Redirect URIs** — add BOTH:
   - `https://algovn.com/the-button/callback`
   - `http://localhost:5173/the-button/callback`
6. **Post Logout URIs** — add BOTH:
   - `https://algovn.com/the-button/`
   - `http://localhost:5173/the-button/`
7. Create. On the app **Configuration** / **Token Settings**: set **Auth Token Type: JWT** and check **Add user roles to the access token** (`accessTokenRoleAssertion`) — the authnz-conventions ⚠️ note: opaque Bearer is Zitadel's default and would 401 at the Kong-exception gate; the token must be JWT for `api-control-plane` to verify it against Zitadel's JWKS. Save.
8. Copy the **Client ID** shown on the app overview.

Record it: `CLIENT_ID=<paste>` (an 18-digit numeric string — keep it quoted everywhere; zitadel.md warns unquoted 18-digit ints get float64-mangled).

- [ ] **Step 2: Set the SPA build variable and rebuild the image**

```bash
gh variable set VITE_OIDC_CLIENT_ID --repo the-algovn/web --body "<CLIENT_ID>"
gh variable list --repo the-algovn/web | grep VITE_OIDC_CLIENT_ID   # confirm it is set
# trigger a fresh main build so web-the-button bakes the client id (empty commit is deterministic)
cd /Users/duclm27/the-algovn/web
git commit --allow-empty -m "the-button: rebuild SPA with Zitadel OIDC client id"
git push origin main
gh run watch --repo the-algovn/web --exit-status   # wait for CI + build-push-the-button to go green
```

Expected: `gh variable list` shows `VITE_OIDC_CLIENT_ID`; the `build-push-the-button` job pushes a new `ghcr.io/the-algovn/web-the-button:main` digest. The `the-button-web` ImageUpdater (T18 Step 13) then writes the new digest to `apps/the-button-web/kustomization.yaml` and Argo rolls the pod (verify: `kubectl --context algovn-remote -n the-button-web rollout status deploy/web-the-button --timeout=180s`).

- [ ] **Step 3: Smoke — login roundtrip (browser)**

In a **fresh private window**, open `https://algovn.com/the-button/`:
1. Page renders: rotating tagline, one big live counter, one big button, achievements grid (all locked with mocking copy). The counter is watchable while logged out.
2. Click the button while logged out → prompted to log in → redirected to `https://id.algovn.com/ui/v2/login` → complete Google login (per zitadel.md verification) → redirected back to `https://algovn.com/the-button/callback` → lands back on the button, now authenticated.

Pass criteria: no console CORS error (the apex `https://algovn.com` is in the acp `CORS_ORIGINS`, T3/T4); the network tab shows `POST https://api.algovn.com/the-button/algovn.button.v1.ButtonService/IssueChallenge` returning 200 with a `challenge`.

- [ ] **Step 4: Smoke — click batch succeeds (curl transcript + browser)**

Browser: mash the button; the SPA batches, the WASM worker solves the PoW, and a `SubmitClicks` fires. Confirm the counter increments and `user_total_clicks` climbs.

Curl transcript (paste a live access token from the browser devtools → Network → any authed request → `authorization` header; call it `$TOKEN`). Use the T21 generator (`load/gen`, Step from Task 21) to solve ONE genuine nonce for the token you were issued — or, simplest, let the browser do the batch and just capture the transcript below by replaying the exact request devtools shows:

```bash
TOKEN='<paste access token>'
# 1) issue a challenge for a small batch
curl -s https://api.algovn.com/the-button/algovn.button.v1.ButtonService/IssueChallenge \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"intended_clicks":5}'
# → {"challenge":"<b64url>","work_factor":"16384","min_interval_seconds":2,"max_batch":10000,"expires_at":"..."}

# 2) solve the nonce for click_count=5 with the tiny helper (Task 21 gen, --solve-one mode),
#    then submit (challenge + nonce + click_count):
curl -s https://api.algovn.com/the-button/algovn.button.v1.ButtonService/SubmitClicks \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"challenge":"<challenge>","nonce":<solved-nonce>,"click_count":5}'
# → {"user_total_clicks":"5","unlocked":[{"id":"mvh","title":"Minimum Viable Human",...}],"next_challenge":{...}}
```

Pass criteria: `SubmitClicks` returns 200 with `user_total_clicks` (string-encoded int64) and at least the `mvh` achievement on the first-ever click. Replaying the SAME challenge again returns HTTP 409 (`AlreadyExists` → replay). Submitting again within `min_interval_seconds` returns HTTP 429 with `Retry-After: 2` (`ResourceExhausted` → throttle).

- [ ] **Step 5: Smoke — achievement toast, SSE tick, milestone-once**

- **Achievement toast:** the browser shows an unlock toast sourced from `SubmitClicksResponse.unlocked` (e.g. "Minimum Viable Human" on click #1; "Nice." when total crosses 69).
- **SSE tick visible:** in a second terminal, `curl -N --max-time 8 https://api.algovn.com/events/the-button.counter` — expect, before the first data frame, a `retry: 3000` line (T3 acp change), then a `data: {"type":"counter","total":<N>}` frame roughly every 1s while clicks land. Confirm the browser counter animates in lockstep with these ticks.
- **Milestone at 1000 fires once:** drive the total across 1000 (batch or many clicks). Confirm exactly ONE `data: {"type":"milestone","threshold":1000,"title":"A Thousand Tiny Rebellions",...}` frame is emitted (the tick leader claims it via `SETNX milestone:1000`). Restart a service pod and re-cross-check: no duplicate milestone frame is emitted (the SETNX claim persists in Redis). Verify with:

```bash
kubectl --context algovn-remote -n redis exec redis-0 -- \
  redis-cli -a "$(kubectl --context algovn-remote -n redis get secret redis-auth -o jsonpath='{.data.password}' | base64 -d)" \
  GET milestone:1000    # expect: "1"  (claimed exactly once)
```

- [ ] **Step 6: Record the acceptance transcript**

Append the passing curl transcript (Steps 4-5, redacting the token) and the recorded `CLIENT_ID` to `iac/docs/runbooks/api-control-plane.md` under a new `## the-button acceptance` heading, then commit in the iac repo:

```bash
cd /Users/duclm27/the-algovn/iac
git add docs/runbooks/api-control-plane.md
git commit -m "Record the-button e2e acceptance transcript"
```

No code/manifest change here — this task's real deliverable is the green checklist. (The empty SPA rebuild commit lives in `the-algovn/web`, already pushed in Step 2.)

---

### Task 20: Calibration — pg_test_fsync, 1k-txn/s soak, solver H/s → POW_W0

**Files:**
- Create: `the-button-service/load/soak/main.go`
- Create: `the-button-service/load/go.mod` is NOT separate — `load/` lives in the service module; reuse the root `go.mod` (module `github.com/the-algovn/the-button-service`). Deps `github.com/jackc/pgx/v5/pgxpool` are already required (T7).

**Interfaces:**
- Consumes (frozen): PG LAN endpoint `192.168.102.201:5432` (postgres.md; never `.200`), the txn shape from spec §6/§7, the solver bench from the SPA (`?bench`, T15), `POW_W0` default `16384`.
- Produces: recorded `pg_test_fsync` numbers, a soak percentile table, a measured mid-phone H/s, and a **decision on `POW_W0`** (update the T18 deployment env only if the chosen value ≠ 16384). This task CREATES `docs/superpowers/load-results.md` with a top-level `## Calibration` section; Task 21 appends its own `## Load test results (k6)` section below it and must never overwrite this file.

- [ ] **Step 1: pg_test_fsync on w1 (via the CNPG pod — pg_test_fsync ships in the postgres image)**

The runnable path (postgres.md uses in-pod exec; the pod's data dir is the w1 local-path PV, so this measures the real fsync surface):

```bash
kubectl --context algovn-remote -n postgres exec pg-1 -c postgres -- \
  pg_test_fsync -f /var/lib/postgresql/data/pgdata/pg_test_fsync.tmp -s 5
```

Expected (representative NVMe): under "Compare file sync methods using one 8kB write", `fdatasync` reports thousands of ops/sec (e.g. `> 2000 ops/sec`, i.e. `< 0.5 ms/op`). This validates the spec's ~3ms/txn model (fsync is the dominant cost; a few fsyncs per commit stays well under 3ms). Record the `fdatasync` and `open_datasync` ops/sec lines. Clean up: the tmp file is removed by pg_test_fsync on exit; if it lingers, `kubectl … exec pg-1 -c postgres -- rm -f /var/lib/postgresql/data/pgdata/pg_test_fsync.tmp`.

- [ ] **Step 2: RED — write the soak bench with a failing sanity assertion first**

Create `the-button-service/load/soak/main.go`. TDD framing: run it FIRST against a DSN that points at a **non-existent** database and confirm it fails loudly (connection refused / database does not exist), proving the harness actually connects and does not silently no-op. Then create the scratch DB (Step 3) and run for real.

`the-button-service/load/soak/main.go`:

```go
// Command soak drives the-button's per-batch Postgres transaction shape at a
// fixed target rate against a THROWAWAY database, reporting commit-latency
// percentiles to validate the ~3ms/txn capacity model (spec §12).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := flag.String("dsn", "", "postgres DSN for the throwaway database")
	rate := flag.Int("rate", 1000, "target transactions per second")
	dur := flag.Duration("duration", 60*time.Second, "soak duration")
	users := flag.Int("users", 5000, "distinct user_sub values cycled (contention model)")
	flag.Parse()
	if *dsn == "" {
		log.Fatal("--dsn required")
	}

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		log.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = 10 // matches the service's per-replica pool (spec §7)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err) // fails loudly if the DB is absent — the RED check
	}

	// Idempotent schema (mirrors spec §7).
	mustExec(ctx, pool, `CREATE TABLE IF NOT EXISTS user_clicks (user_sub text PRIMARY KEY, clicks bigint NOT NULL)`)
	mustExec(ctx, pool, `CREATE TABLE IF NOT EXISTS user_achievements (
		user_sub text NOT NULL, achievement_id text NOT NULL,
		unlocked_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (user_sub, achievement_id))`)

	var (
		lat   []time.Duration
		latMu sync.Mutex
		ok    int64
		fail  int64
	)
	work := make(chan string, *rate)
	var wg sync.WaitGroup
	workers := *rate / 100
	if workers < 8 {
		workers = 8
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sub := range work {
				t0 := time.Now()
				if err := oneTxn(ctx, pool, sub); err != nil {
					atomic.AddInt64(&fail, 1)
					continue
				}
				d := time.Since(t0)
				atomic.AddInt64(&ok, 1)
				latMu.Lock()
				lat = append(lat, d)
				latMu.Unlock()
			}
		}()
	}

	// Rate pacing: emit one job every 1s/rate.
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	deadline := time.After(*dur)
	i := 0
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-tick.C:
			select {
			case work <- fmt.Sprintf("soak-%d", rand.Intn(*users)):
			default: // backpressure: pool saturated, drop the tick (records a shortfall)
			}
			i++
		}
	}
	close(work)
	wg.Wait()

	latMu.Lock()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	latMu.Unlock()
	fmt.Printf("target_rate=%d duration=%s ok=%d fail=%d achieved_rate=%.0f/s\n",
		*rate, *dur, ok, fail, float64(ok)/dur.Seconds())
	if len(lat) > 0 {
		fmt.Printf("commit_latency p50=%s p95=%s p99=%s max=%s\n",
			pct(lat, 50), pct(lat, 95), pct(lat, 99), lat[len(lat)-1])
	}
	if fail > 0 {
		os.Exit(1)
	}
}

func oneTxn(ctx context.Context, pool *pgxpool.Pool, sub string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var total int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO user_clicks AS u (user_sub, clicks) VALUES ($1,$2)
		 ON CONFLICT (user_sub) DO UPDATE SET clicks = u.clicks + $2 RETURNING clicks`,
		sub, 100).Scan(&total); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO user_achievements (user_sub, achievement_id) VALUES ($1,'mvh')
		 ON CONFLICT DO NOTHING`, sub); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func mustExec(ctx context.Context, pool *pgxpool.Pool, sql string) {
	if _, err := pool.Exec(ctx, sql); err != nil {
		log.Fatalf("exec %q: %v", sql, err)
	}
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := p * len(sorted) / 100
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
```

Verify RED:

```bash
cd /Users/duclm27/the-algovn/the-button-service
go run ./load/soak --dsn 'postgres://postgres:x@192.168.102.201:5432/does_not_exist' --duration 2s
```

Expected: exits non-zero with `ping: ... database "does_not_exist" does not exist` (or connection error) — proving the harness truly connects.

- [ ] **Step 3: Create the throwaway database, run the 1k-txn/s soak (GREEN)**

Create a scratch DB in-pod (postgres.md: in-pod psql, no password), soak against it via the LAN svclb, drop it after.

```bash
SUPERPW=$(kubectl --context algovn-remote -n postgres get secret pg-superuser -o jsonpath='{.data.password}' | base64 -d)
kubectl --context algovn-remote -n postgres exec pg-1 -c postgres -- psql -U postgres -c 'CREATE DATABASE the_button_soak;'
cd /Users/duclm27/the-algovn/the-button-service
go run ./load/soak \
  --dsn "postgres://postgres:${SUPERPW}@192.168.102.201:5432/the_button_soak" \
  --rate 1000 --duration 60s
```

Expected: `achieved_rate` ≈ 1000/s, `fail=0`, and `commit_latency p95` in the low single-digit ms (consistent with §12's ~3ms model, running same-LAN not same-node so allow a few ms of network). Record the full line.

Drop the scratch DB (postgres.md remove-order: DB first; this DB has no managed role so a plain DROP is enough):

```bash
kubectl --context algovn-remote -n postgres exec pg-1 -c postgres -- psql -U postgres -c 'DROP DATABASE the_button_soak;'
unset SUPERPW
```

- [ ] **Step 4: Measure solver H/s on real devices (user-assisted)**

The SPA exposes a calibration bench at `?bench` (T15). USER opens `https://algovn.com/the-button/?bench` on:
- a **mid-range phone** (the binding device — spec §5 targets "a mid phone with a WASM solver"),
- a **laptop** (upper bound).

Read the reported **hashes/sec** off the bench UI on each. Record e.g. `mid-phone: H_s = 1.4e6 H/s`, `laptop: H_s = 9e6 H/s`.

- [ ] **Step 5: Decide POW_W0 and apply only if ≠ 16384**

**Decision rule.** Expected hashes for a batch = `W0 × n × L` (spec §5: per-click expected = `W0×L`, times `n` clicks). Solve time on the mid phone = `W0 × n × L / H_s`. Pin the design point `n = 100, L = 1` and target **1.0s** (inside the required 0.5–1.5s band):

```
W0_ideal = H_s_midphone × target_seconds / n = H_s_midphone × 1.0 / 100
```

Round `W0_ideal` to the nearest power of two. Then sanity-check the band at `n=100, L=1`: solve time must fall in `[0.5s, 1.5s]`.

- If `H_s_midphone ≈ 1.4e6`: `W0_ideal ≈ 14000` → nearest power of two `16384` (`2^14`). Solve time at 16384 = `16384×100/1.4e6 ≈ 1.17s` → inside the band → **keep the default `16384`, no change**.
- If the measured phone is much faster/slower and 16384 lands outside `[0.5,1.5]s`, pick the power of two that lands inside and update the deployment env:

```bash
# only if the chosen W0 differs from 16384
# edit iac/apps/the-button/deployment.yaml → env POW_W0 value: "<new>"
cd /Users/duclm27/the-algovn/iac
git add apps/the-button/deployment.yaml
git commit -m "the-button: set POW_W0=<new> from solver calibration (mid-phone H/s)"
./scripts/validate.sh && git push
```

Document the chosen value, the measured H/s per device, and the arithmetic in `docs/superpowers/load-results.md` (Step 6).

- [ ] **Step 6: Commit the soak tool + record results**

```bash
cd /Users/duclm27/the-algovn/the-button-service
gofmt -w load/soak/main.go
go vet ./load/soak
mkdir -p docs/superpowers
# append the "Calibration" section (pg_test_fsync numbers, soak percentile line,
# per-device H/s, POW_W0 decision + arithmetic) to docs/superpowers/load-results.md
git add load/soak/main.go docs/superpowers/load-results.md
git commit -m "Add PG soak bench and record calibration results (fsync, 1k-txn/s soak, solver H/s, POW_W0)"
```

Verify: `go vet ./load/soak` is clean; the results doc contains the four recorded artifacts (fsync ops/sec, soak p95, H/s, POW_W0 decision).

---

### Task 21: k6 suite — SSE ramp, click soak, rollout drill

**Files:**
- Create: `the-button-service/load/gen/main.go` (offline token+nonce corpus generator)
- Create: `the-button-service/load/proto/button.proto` (copy from the protos repo, for k6 gRPC)
- Create: `the-button-service/load/sse-ramp.js`
- Create: `the-button-service/load/click-soak.js`
- Create: `the-button-service/load/rollout-drill.js`
- Create/Modify: `the-button-service/docs/superpowers/load-results.md` (results table)

**Interfaces:**
- Consumes (frozen): SSE channel `the-button.counter` at `https://api.algovn.com/events/the-button.counter`; the-button-service gRPC `algovn.button.v1.ButtonService/SubmitClicks` on headless `:9090`; PoW token format (skeleton line 20: `base64url(payloadJSON || HMAC-SHA256(payloadJSON, K))`, target `2^256/(w0·count·l)`, preimage `SHA-256(tokenBytes || be32(count) || be64(nonce))`); `POW_SECRET` (password-manager `the-button-pow-secret`, T18 Step 5); trust model §4 (service does a read-only segment-2 decode of the bearer and does NOT re-verify).
- Produces: three runnable k6 scripts, a Go corpus generator, and a results table in `docs/superpowers/load-results.md`.

**SSE tooling decision (justified, honest).** Stock k6 cannot hold an SSE stream idiomatically: `http.get` reads the response body to completion, but an SSE stream never completes, so the request just blocks until its timeout — you can *hold* a socket but you **cannot observe per-event tick timing**, which the spec's SSE-ramp assertion ("tick-latency assertion", §12) requires. Therefore the two SSE scripts use the **`xk6-sse` extension** (`github.com/phymbert/xk6-sse`), which surfaces `open`/`event`/`error` callbacks so we can measure inter-tick latency and count reconnects. The click-soak script needs no extension (it is gRPC, native `k6/net/grpc`).

**Auth-bypass decision for click-soak (justified).** `SubmitClicks` is gateway-`authenticated`: through `api.algovn.com` it needs a real Zitadel-signed JWT, and each authenticated `sub` is throttled to one batch per `min_interval` (spec §5/§6) — a single test user cannot generate ceiling load, and we cannot forge Zitadel signatures. But the capacity we must prove is the **service + Redis + Postgres** path (the 750-txn/s PG ceiling, §12), and per §4 the service trusts in-cluster callers: it does a read-only segment-2 decode of the bearer and never re-verifies. So click-soak talks **gRPC directly to the service** (via `kubectl port-forward`), forging bearers with distinct `sub`s (so no throttle collisions) and replaying genuinely-solved PoW tokens (real difficulty, we hold `POW_SECRET`). This is the honest worst-case: §12 bounds real external click load to `5000 clickers × (1/10s) = 500/s` by construction, so a service-direct run at ≥500/s IS the ceiling. External-origin repeat therefore applies to **sse-ramp and rollout-drill** (both anonymous, both go through the full Cloudflare→Kong→acp chain); click-soak is LAN/in-cluster only by design (documented in the results table).

- [ ] **Step 1: Install k6 + build the xk6-sse binary**

```bash
brew install k6                      # already present is fine; confirm:
k6 version
go install go.k6.io/xk6/cmd/xk6@latest
cd /Users/duclm27/the-algovn/the-button-service/load
"$(go env GOPATH)/bin/xk6" build --with github.com/phymbert/xk6-sse@latest --output ./k6-sse
./k6-sse version                     # the custom binary that has k6/x/sse
```

Expected: `k6 version` prints a version; `./k6-sse version` prints a version with the extension compiled in. Use `./k6-sse` (not stock `k6`) for the two SSE scripts; stock `k6` for the gRPC click-soak.

- [ ] **Step 2: RED — corpus generator, prove it produces service-acceptable tokens BEFORE bulk-generating**

Copy the proto for k6 gRPC, then write the generator. **Correctness gate:** the generator's PoW math must be **byte-for-byte identical** to the service's `internal/pow` verify (T8). To prove it (not assume it), the generator has a `--count 1` mode; you submit that single entry through the smoke path (Task 19 Step 4) and confirm the service accepts it. If the service rejects it with `INVALID_ARGUMENT`, the preimage/target definition diverges from T8 — fix the generator to match T8 before generating the full corpus. This RED-first check is the test.

```bash
cd /Users/duclm27/the-algovn/the-button-service
mkdir -p load/proto
# copy the button proto from the protos working copy (tag gen/go/v0.2.0, T6)
cp /Users/duclm27/the-algovn/protos/algovn/button/v1/button.proto load/proto/button.proto
```

`the-button-service/load/gen/main.go`:

```go
// Command gen produces an offline corpus of {bearer, challenge, nonce,
// click_count} entries for the click-soak k6 script. It forges bearers with
// distinct subs (the service does not re-verify — spec §4) and solves GENUINE
// proof-of-work (we hold POW_SECRET), never a lowered server-side difficulty.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// payload MUST match the service's token payload (skeleton line 20 / spec §5).
type payload struct {
	ID           string `json:"id"`
	Sub          string `json:"sub"`
	Iat          int64  `json:"iat"`
	Exp          int64  `json:"exp"`
	W0           uint64 `json:"w0"`
	L            uint64 `json:"l"`
	MinIntervalS uint32 `json:"min_interval_s"`
	MaxBatch     uint32 `json:"max_batch"`
}

type entry struct {
	Bearer     string `json:"bearer"`
	Challenge  string `json:"challenge"`
	Nonce      uint64 `json:"nonce"`
	ClickCount uint32 `json:"click_count"`
}

var two256 = new(big.Int).Lsh(big.NewInt(1), 256)

func main() {
	secretB64 := flag.String("secret", os.Getenv("POW_SECRET"), "base64 HMAC key K (defaults to $POW_SECRET)")
	w0 := flag.Uint64("w0", 16384, "W0 (must match deployed POW_W0)")
	l := flag.Uint64("l", 1, "difficulty level L (1 = genuine floor; keeps offline solve tractable)")
	clicks := flag.Uint("clicks", 100, "click_count per batch")
	count := flag.Int("count", 1, "corpus size (= target_rate * duration)")
	out := flag.String("out", "load/corpus.jsonl", "output JSONL path")
	subPrefix := flag.String("sub-prefix", "loadtest", "distinct sub prefix")
	flag.Parse()

	K, err := base64.StdEncoding.DecodeString(*secretB64)
	if err != nil || len(K) == 0 {
		log.Fatalf("bad --secret (base64 POW_SECRET): %v", err)
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// target = floor(2^256 / (w0 * count * l))  — spec §5 full-target form.
	target := new(big.Int).Div(two256, new(big.Int).SetUint64(*w0*uint64(*clicks)*(*l)))

	jobs := make(chan int, *count)
	results := make(chan entry, 256)
	var wg sync.WaitGroup
	var done int64
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results <- solveOne(K, *w0, *l, *clicks, *subPrefix, i, target)
				atomic.AddInt64(&done, 1)
			}
		}()
	}
	go func() {
		for i := 0; i < *count; i++ {
			jobs <- i
		}
		close(jobs)
	}()
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			n := atomic.LoadInt64(&done)
			fmt.Fprintf(os.Stderr, "solved %d/%d\n", n, *count)
			if int(n) >= *count {
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()

	enc := json.NewEncoder(f)
	n := 0
	for e := range results {
		if err := enc.Encode(&e); err != nil {
			log.Fatal(err)
		}
		n++
	}
	fmt.Fprintf(os.Stderr, "wrote %d entries to %s\n", n, *out)
}

func solveOne(K []byte, w0, l uint64, clicks uint, subPrefix string, i int, target *big.Int) entry {
	now := time.Now().Unix()
	p := payload{
		ID: uuid.Must(uuid.NewV7()).String(), Sub: fmt.Sprintf("%s-%d", subPrefix, i),
		Iat: now, Exp: now + 300, W0: w0, L: l, MinIntervalS: 2, MaxBatch: 10000,
	}
	pj, _ := json.Marshal(&p)
	mac := hmac.New(sha256.New, K)
	mac.Write(pj)
	tokenBytes := append(append([]byte{}, pj...), mac.Sum(nil)...) // payload || HMAC
	challenge := base64.RawURLEncoding.EncodeToString(tokenBytes)

	// preimage = tokenBytes || be32(click_count) || be64(nonce), where tokenBytes
	// are the ASCII bytes of the CHALLENGE STRING as issued (Global Constraints;
	// Task 8 pow.CheckWork and Task 15's solver hash exactly these bytes — never
	// the decoded payload||HMAC).
	tok := []byte(challenge)
	pre := make([]byte, len(tok)+4+8)
	copy(pre, tok)
	binary.BigEndian.PutUint32(pre[len(tok):], uint32(clicks))
	hashInt := new(big.Int)
	var nonce uint64
	for {
		binary.BigEndian.PutUint64(pre[len(tok)+4:], nonce)
		h := sha256.Sum256(pre)
		hashInt.SetBytes(h[:])
		if hashInt.Cmp(target) < 0 {
			break
		}
		nonce++
	}
	// forge a JWT-shaped bearer the service will segment-2 decode (unsigned; not verified in-cluster).
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString([]byte(
		fmt.Sprintf(`{"sub":%q,"iss":"https://id.algovn.com"}`, p.Sub)))
	return entry{Bearer: hdr + "." + body + ".", Challenge: challenge, Nonce: nonce, ClickCount: uint32(clicks)}
}
```

RED gate:

```bash
cd /Users/duclm27/the-algovn/the-button-service
gofmt -w load/gen/main.go && go vet ./load/gen
export POW_SECRET="$(<paste from password manager 'the-button-pow-secret'>)"
go run ./load/gen --count 1 --w0 16384 --l 1 --out load/corpus-smoke.jsonl
cat load/corpus-smoke.jsonl   # one JSON line: {bearer, challenge, nonce, click_count}
```

Then submit that single entry through the gateway with a REAL token whose `sub` you override is impossible — so instead verify it in-cluster (matches how click-soak runs): port-forward the service and grpcurl one call using the forged bearer + solved nonce (this is the real acceptance of the generator's math against T8):

```bash
kubectl --context algovn-remote -n the-button port-forward deploy/the-button-service 9090:9090 &
PF=$!; sleep 2
B=$(jq -r .bearer load/corpus-smoke.jsonl); C=$(jq -r .challenge load/corpus-smoke.jsonl)
N=$(jq -r .nonce load/corpus-smoke.jsonl);  K=$(jq -r .click_count load/corpus-smoke.jsonl)
grpcurl -plaintext -H "authorization: Bearer $B" \
  -d "{\"challenge\":\"$C\",\"nonce\":$N,\"click_count\":$K}" \
  -import-path load/proto -proto button.proto \
  localhost:9090 algovn.button.v1.ButtonService/SubmitClicks
kill $PF
```

Expected: a `SubmitClicksResponse` with `userTotalClicks` — proving the generator's PoW preimage/target match `internal/pow` (T8). If it returns `InvalidArgument`, the generator diverges from T8 — reconcile the `tokenBytes` definition (decoded `payload||HMAC` vs base64 string) and target rounding until it is accepted. Do NOT proceed to bulk generation until this passes.

- [ ] **Step 3: sse-ramp.js — ramp to 10k SSE with a tick-latency assertion**

`the-button-service/load/sse-ramp.js`:

```javascript
import sse from "k6/x/sse";
import { check } from "k6";
import { Trend, Counter } from "k6/metrics";

const tickGap = new Trend("sse_tick_gap_ms", true);
const events = new Counter("sse_events");

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "2m", target: 2000 },
        { duration: "3m", target: 10000 },
        { duration: "3m", target: 10000 },
        { duration: "1m", target: 0 },
      ],
      gracefulStop: "10s",
    },
  },
  thresholds: {
    // the leader ticks every 1s; allow slack for fan-out + network jitter.
    sse_tick_gap_ms: ["p(95)<2000"],
  },
};

const URL = __ENV.SSE_URL || "https://api.algovn.com/events/the-button.counter";

export default function () {
  let last = 0;
  const res = sse.open(URL, {}, function (client) {
    client.on("event", function (e) {
      events.add(1);
      const now = Date.now();
      if (last > 0) tickGap.add(now - last);
      last = now;
    });
    client.on("error", function (e) {
      check(e, { "no sse error": (err) => err === undefined || err === null });
    });
  });
  check(res, { "sse status 200": (r) => r && r.status === 200 });
}
```

Run (LAN, custom binary):

```bash
cd /Users/duclm27/the-algovn/the-button-service/load
./k6-sse run sse-ramp.js
```

Expected: at 10k VUs the run holds, `acp_sse_clients` on the acp side approaches 10k (verify: `kubectl … -n api-control-plane exec … curl :9091/metrics | grep acp_sse_clients`), and the `sse_tick_gap_ms p(95)<2000` threshold PASSES (real gaps cluster near 1000ms). Record the p95 gap and the max concurrent clients. Note: the global SSE cap is `SSE_MAX_CONNS=15000` (T3) — a 10k ramp stays under it (excess would 503, by design).

- [ ] **Step 4: click-soak.js — replay the corpus at ceiling (gRPC, service-direct)**

Bulk-generate the corpus (size = rate × duration; one-time burn — each token's `pow:<id>` and each `sub` is used exactly once):

```bash
cd /Users/duclm27/the-algovn/the-button-service
export POW_SECRET="$(<from password manager 'the-button-pow-secret'>)"
# 600/s * 120s = 72000 entries at L=1 (~1.6M hashes each; parallel across all cores)
go run ./load/gen --count 72000 --w0 16384 --l 1 --clicks 100 --out load/corpus.jsonl
wc -l load/corpus.jsonl   # 72000
```

`the-button-service/load/click-soak.js`:

```javascript
import grpc from "k6/net/grpc";
import { check } from "k6";
import { Rate, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";

const corpus = new SharedArray("corpus", () =>
  open(__ENV.CORPUS || "corpus.jsonl").split("\n").filter((l) => l.length > 0).map(JSON.parse)
);

const submitLatency = new Trend("submit_latency_ms", true);
const exhausted = new Rate("grpc_resource_exhausted"); // == 429 at the gateway

const client = new grpc.Client();
client.load(["proto"], "button.proto");

export const options = {
  scenarios: {
    ceiling: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 600),
      timeUnit: "1s",
      duration: __ENV.DURATION || "120s",
      preAllocatedVUs: 200,
      maxVUs: 800,
    },
  },
  thresholds: {
    "submit_latency_ms": ["p(95)<300"],
    "grpc_resource_exhausted": ["rate<0.01"],
  },
};

export default function () {
  const i = __ITER % corpus.length; // one-time burn: each entry used once
  const e = corpus[i];
  client.connect(__ENV.TARGET || "127.0.0.1:9090", { plaintext: true });
  const t0 = Date.now();
  const resp = client.invoke(
    "algovn.button.v1.ButtonService/SubmitClicks",
    { challenge: e.challenge, nonce: String(e.nonce), click_count: e.click_count },
    { metadata: { authorization: "Bearer " + e.bearer } }
  );
  submitLatency.add(Date.now() - t0);
  exhausted.add(resp.status === grpc.StatusResourceExhausted);
  check(resp, {
    "ok or benign": (r) =>
      r.status === grpc.StatusOK ||
      r.status === grpc.StatusResourceExhausted ||
      r.status === grpc.StatusAlreadyExists,
  });
  client.close();
}
```

Run (LAN, port-forward, stock k6):

```bash
cd /Users/duclm27/the-algovn/the-button-service/load
kubectl --context algovn-remote -n the-button port-forward deploy/the-button-service 9090:9090 &
PF=$!; sleep 2
CORPUS=corpus.jsonl RATE=600 DURATION=120s TARGET=127.0.0.1:9090 k6 run click-soak.js
kill $PF
```

Expected: thresholds PASS — `submit_latency_ms p(95) < 300ms` and `grpc_resource_exhausted rate < 1%` (near-zero, since subs are distinct). Sustained 600/s > the §12 worst-case 500/s and under the 750-txn/s PG ceiling. If p95 breaches, that is the finding to record (identify PG vs Redis via the acp/cnpg metrics). Record achieved rate + p95 + exhausted rate.

- [ ] **Step 5: rollout-drill.js — hold 5k SSE across a rolling restart**

`the-button-service/load/rollout-drill.js`:

```javascript
import sse from "k6/x/sse";
import { check } from "k6";
import { Counter, Rate } from "k6/metrics";

const reconnects = new Counter("sse_reconnects");
const errRate = new Rate("sse_error_rate");

export const options = {
  scenarios: {
    hold: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 5000),
      duration: __ENV.DURATION || "5m",
      gracefulStop: "15s",
    },
  },
  thresholds: {
    // deploy churn must NOT produce a 5xx spike; reconnects are expected & fine.
    sse_error_rate: ["rate<0.02"],
  },
};

const URL = __ENV.SSE_URL || "https://api.algovn.com/events/the-button.counter";

export default function () {
  while (true) {
    const res = sse.open(URL, {}, function (client) {
      client.on("error", function () {});
    });
    const ok = res && res.status === 200;
    errRate.add(!ok || res.status >= 500); // count only true failures / 5xx
    check(res, { "reconnect ok (200)": () => ok });
    reconnects.add(1);
    // sse.open blocks until the stream drops (deploy cycles the pod); loop reconnects.
  }
}
```

Run (LAN), and mid-run trigger the rollout from a second terminal:

```bash
# terminal 1
cd /Users/duclm27/the-algovn/the-button-service/load
VUS=5000 DURATION=5m ./k6-sse run rollout-drill.js

# terminal 2, ~60s in — restart BOTH the edge and the service
kubectl --context algovn-remote -n api-control-plane rollout restart deploy/api-control-plane
kubectl --context algovn-remote -n the-button        rollout restart deploy/the-button-service
kubectl --context algovn-remote -n api-control-plane rollout status  deploy/api-control-plane
kubectl --context algovn-remote -n the-button        rollout status  deploy/the-button-service
```

Expected: the reconnect wave drains (each VU reconnects via `sse.open`), `sse_error_rate rate < 2%` PASSES, and there is **no 5xx spike** — cross-check `sum(rate(acp_requests_total{code=~"5.."}[1m]))` stays flat in VictoriaMetrics during the restart window. Record reconnect count + error rate + whether any 5xx appeared.

- [ ] **Step 6: External-origin repeat + results table**

Repeat **sse-ramp.js** (a reduced target, e.g. 2000 VUs, is acceptable if running from a single external host) and **rollout-drill.js** from a machine OUTSIDE the LAN (e.g. a cloud VM / phone-tethered laptop) against the same public URLs — this exercises the full Cloudflare→cloudflared→Kong→acp chain and NAT-cohort rate limits (the `/events` KongPlugin `50/s,1000/min`, T4). click-soak is **not** repeated externally (service-direct by design — see the auth-bypass justification above; note this explicitly in the table).

**APPEND** the results table to `the-button-service/docs/superpowers/load-results.md`, below the `## Calibration` section Task 20 wrote (never overwrite the file — Task 20's fsync/soak/H-s numbers and the `POW_W0` decision are the launch evidence):

```markdown
## Load test results (k6)

| Scenario        | Origin    | Target        | Key assertion              | Result |
|-----------------|-----------|---------------|----------------------------|--------|
| sse-ramp        | LAN       | 10k SSE       | tick-gap p95 < 2000ms      | <fill> |
| sse-ramp        | External  | 2k SSE        | tick-gap p95 < 2000ms      | <fill> |
| click-soak      | LAN/svc   | 600 txn/s     | submit p95 < 300ms; 429<1% | <fill> |
| rollout-drill   | LAN       | 5k SSE + roll | error rate < 2%, no 5xx    | <fill> |
| rollout-drill   | External  | 5k SSE + roll | error rate < 2%, no 5xx    | <fill> |

click-soak is LAN/service-direct only: SubmitClicks is gateway-authenticated and per-sub throttled,
so ceiling load is generated in-cluster with forged bearers (service trusts in-cluster callers, §4)
and genuinely-solved PoW; §12 bounds real external click load to 500/s by construction.
```

- [ ] **Step 7: Commit the k6 suite + generator**

```bash
cd /Users/duclm27/the-algovn/the-button-service
gofmt -w load/gen/main.go
go vet ./load/gen
rm -f load/corpus.jsonl load/corpus-smoke.jsonl load/k6-sse   # do NOT commit corpora or the built binary
echo -e "load/corpus*.jsonl\nload/k6-sse" >> .gitignore
git add load/gen/main.go load/proto/button.proto \
  load/sse-ramp.js load/click-soak.js load/rollout-drill.js \
  docs/superpowers/load-results.md .gitignore
git diff --cached --stat
git commit -m "Add k6 load suite (SSE ramp, click soak, rollout drill) + PoW corpus generator and results"
```

Verify: `git diff --cached --stat` excludes `corpus*.jsonl` and the `k6-sse` binary; results doc holds the filled table.

---

### Task 22: Ops finish — alerts, runbook, catalog status flip

**Files:**
- Create: `iac/platform/monitoring/manifests/the-button-rules.yaml` (VMRule)
- Modify: `iac/platform/monitoring/manifests/kustomization.yaml` (add the rule file)
- Create: `iac/docs/runbooks/the-button.md`
- Modify: `specs/README.md` (portfolio row `building` → `live`)
- Modify: `the-button-service/docs/superpowers/specs/2026-07-14-the-button-design.md` (status line)

**Interfaces:**
- Consumes (frozen): metric names — `acp_sse_clients`, `process_resident_memory_bytes` (acp process collector, ns `api-control-plane`), `cnpg_pg_stat_database_*` (CNPG, T-postgres), node-exporter `node_network_transmit_bytes_total` on w1 (`192.168.102.201`), `kubelet_volume_stats_*`; degradation knobs `SSE_MAX_CONNS` (acp env, T3), acp/service replicas, Redis keys `pow:L`/`pow:min_interval`.
- Produces: VMRule `the-button-alerts`; runbook `the-button.md`; portfolio + spec status = live.

- [ ] **Step 1: VMRule (model on `vmrules.yaml` structure — groups/rules/expr/for/labels/annotations)**

`iac/platform/monitoring/manifests/the-button-rules.yaml`:

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMRule
metadata:
  name: the-button-alerts
  namespace: monitoring
spec:
  groups:
    - name: the-button-edge
      rules:
        # Uplink TX on w1 — the binding constraint (§12). NOTE: 100 Mbps assumed
        # symmetric home uplink; adjust the /1e6 threshold to the real cap.
        - alert: ButtonUplinkTxHigh
          expr: |
            sum(rate(node_network_transmit_bytes_total{instance=~"192.168.102.201:.*",device!~"lo|veth.*|cali.*|cni.*|flannel.*"}[5m])) * 8 / 1e6 > 70
          for: 5m
          labels: { severity: warning }
          annotations:
            summary: 'w1 uplink TX >70 Mbps (>70% of assumed 100Mbps cap) — widen tick / lower SSE cap'
        - alert: ButtonSSEClientsNearCap
          expr: sum(acp_sse_clients) > 13500
          for: 5m
          labels: { severity: warning }
          annotations:
            summary: 'SSE clients {{ $value }} > 90% of SSE_MAX_CONNS (15000) — new connections will 503'
        - alert: ButtonAcpMemoryHigh
          expr: max(process_resident_memory_bytes{namespace="api-control-plane"}) > 900 * 1024 * 1024
          for: 10m
          labels: { severity: warning }
          annotations:
            summary: 'api-control-plane RSS {{ $value | humanize1024 }}B > 900Mi (limit 1Gi)'
    - name: the-button-data
      rules:
        # Commit THROUGHPUT guard near the 750-txn/s engineered ceiling (§12).
        # (True commit p95 needs a service-side histogram — CNPG exposes no latency
        # histogram; see open question / runbook.)
        - alert: ButtonPGCommitRateNearCeiling
          expr: rate(cnpg_pg_stat_database_xact_commit{datname="the_button"}[1m]) > 700
          for: 5m
          labels: { severity: warning }
          annotations:
            summary: 'the_button commit rate {{ $value | humanize }}/s near 750/s ceiling'
        - alert: ButtonPGRollbackSpike
          expr: rate(cnpg_pg_stat_database_xact_rollback{datname="the_button"}[5m]) > 5
          for: 5m
          labels: { severity: warning }
          annotations:
            summary: 'the_button rollback rate elevated ({{ $value | humanize }}/s) — txn contention/failures'
        - alert: ButtonPVUsageWarning
          expr: |
            max by (namespace, persistentvolumeclaim) (
              kubelet_volume_stats_used_bytes{namespace=~"postgres|redis"}
              / kubelet_volume_stats_capacity_bytes{namespace=~"postgres|redis"}
            ) > 0.70
          for: 15m
          labels: { severity: warning }
          annotations:
            summary: 'PVC {{ $labels.namespace }}/{{ $labels.persistentvolumeclaim }} >70% full'
        - alert: ButtonPVUsageCritical
          expr: |
            max by (namespace, persistentvolumeclaim) (
              kubelet_volume_stats_used_bytes{namespace=~"postgres|redis"}
              / kubelet_volume_stats_capacity_bytes{namespace=~"postgres|redis"}
            ) > 0.85
          for: 5m
          labels: { severity: critical }
          annotations:
            summary: 'PVC {{ $labels.namespace }}/{{ $labels.persistentvolumeclaim }} >85% full — local-path cannot expand'
```

Add to `iac/platform/monitoring/manifests/kustomization.yaml` `resources:` (append):

```yaml
  - the-button-rules.yaml
```

- [ ] **Step 2: Validate + push the rules**

```bash
cd /Users/duclm27/the-algovn/iac
./scripts/validate.sh          # expect PASS (kustomize build + kubeconform accept the VMRule)
git add platform/monitoring/manifests/the-button-rules.yaml platform/monitoring/manifests/kustomization.yaml
git commit -m "Add the-button alerts: uplink TX, SSE cap, acp RSS, PG commit rate/rollback, PV usage"
git push
```

After Argo syncs, confirm the rules loaded:

```bash
kubectl --context algovn-remote -n monitoring get vmrule the-button-alerts
# spot-check an expr evaluates (vmsingle HTTP API, svc vmsingle-vm:8428 — postgres.md):
kubectl --context algovn-remote -n monitoring exec deploy/vmalert-vm -- \
  sh -c 'wget -qO- "http://vmsingle-vm:8428/api/v1/query?query=sum(acp_sse_clients)"' | head -c 200
```

Expected: VMRule present; the query returns a JSON result envelope (`"status":"success"`).

- [ ] **Step 3: Runbook — one-page degradation knobs with exact commands**

`iac/docs/runbooks/the-button.md`:

```markdown
# the-button (algovn.com/the-button)

Live global click counter; PoW-gated. Service ns `the-button` (2 replicas, gRPC :9090),
SPA ns `the-button-web`, routed via api-control-plane registration `/the-button` and SSE
channel `the-button.counter`. Spec: the-button-service repo
`docs/superpowers/specs/2026-07-14-the-button-design.md`. Data: Redis (hot control state),
Postgres db `the_button` (durable truth). Alerts: VMRule `the-button-alerts`.

## Degradation knobs (manual, no automation — spec §12)

Apply in order of blast radius (cheapest first). `REDIS_PASS` helper:
`REDIS_PASS=$(kubectl --context algovn-remote -n redis get secret redis-auth -o jsonpath='{.data.password}' | base64 -d)`

1. **Lower the SSE cap (edge relief, immediate).** Fewer held connections → less uplink.
   `kubectl --context algovn-remote -n api-control-plane set env deploy/api-control-plane SSE_MAX_CONNS=8000`
   (new connections above the cap get 503 + jittered client reconnect; existing ones keep serving.)

2. **Scale the edge / service (throughput relief).**
   `kubectl --context algovn-remote -n api-control-plane scale deploy/api-control-plane --replicas=3`
   `kubectl --context algovn-remote -n the-button scale deploy/the-button-service --replicas=3`
   (No HPA on any long-lived-connection path — scale by hand, then revert.)

3. **Raise the hard throttle (click relief, immediate but transient).** The min-interval
   ladder is the HARD valve; L is the cost valve. Force them up:
   `kubectl --context algovn-remote -n redis exec redis-0 -- redis-cli -a "$REDIS_PASS" SET pow:min_interval 10`
   `kubectl --context algovn-remote -n redis exec redis-0 -- redis-cli -a "$REDIS_PASS" SET pow:L 16`
   ⚠️ The tick leader recomputes `pow:L`/`pow:min_interval` every ~1s from measured load, so a
   manual SET is overwritten within a tick UNLESS real load already keeps them pinned high (under a
   genuine storm the controller will itself hold min_interval=10, L=16). Treat this as a nudge, not a
   latch; for a durable clamp use knobs 1/2/4.

4. **Widen the tick (uplink relief, durable, needs a redeploy).** Halving tick frequency
   halves SSE fan-out bytes. The tick interval is a service setting — bump it and roll:
   set the service `TICK_INTERVAL` to `2s` and redeploy. (Open item: `TICK_INTERVAL` is not in the
   frozen env set — if the service does not yet read it, this knob requires a one-line service change;
   until then, knobs 1-2 are the durable uplink levers.)

## Quick checks
- Counter live: `curl -s https://api.algovn.com/the-button/algovn.button.v1.ButtonService/GetCounter -d '{}'`
  → `{}` when 0 (protojson omits zero uint64), else `{"total":"<n>"}` (int64 → JSON string).
- SSE: `curl -N --max-time 5 https://api.algovn.com/events/the-button.counter` → `retry: 3000` then `data:` ticks.
- Redis counter vs PG truth: `redis-cli … GET counter:global` vs `SELECT SUM(clicks) FROM user_clicks;`
  (hourly reconcile heals drift via INCRBY — never SET).

## Failure modes (spec §13)
Redis down → clicks fail closed (UNAVAILABLE→502); counter/SSE serve from per-replica PG SUM cache.
RabbitMQ down → SSE 503 → SPA polls GetCounter. Postgres down → SubmitClicks fails; GetCounter/SSE
keep serving from Redis; bare catalog + milestones still served.
```

- [ ] **Step 4: Commit the runbook**

```bash
cd /Users/duclm27/the-algovn/iac
git add docs/runbooks/the-button.md
git commit -m "Add the-button runbook: degradation knobs, quick checks, failure modes"
git push
```

- [ ] **Step 5: Flip the portfolio status building → live (AFTER T21 passes)**

Only after Task 21's assertions all pass. In `specs/README.md`, change the-button's status cell:

```
| The Button | One global click counter; PoW-gated mashing, troll achievements | live | [spec](products/the-button.md) · [service](https://github.com/the-algovn/the-button-service) |
```

```bash
cd /Users/duclm27/the-algovn/specs
git add README.md
git commit -m "Portfolio: the-button building -> live"
git push
```

- [ ] **Step 6: Amend the spec status line**

In `the-button-service/docs/superpowers/specs/2026-07-14-the-button-design.md`, line 4, append the launch note so the design doc reflects reality:

```
**Status:** Approved (brainstorm dialogue + 4-lens design workflow + adversarial judge panel) — Live 2026-07 (10k-target load suite passed; see docs/superpowers/load-results.md)
```

```bash
cd /Users/duclm27/the-algovn/the-button-service
git add docs/superpowers/specs/2026-07-14-the-button-design.md
git commit -m "Spec: mark the-button live after load acceptance"
git push
```

Verify: `git log --oneline -1` in each of the three repos shows the respective commit; the portfolio table renders `live`.
