# Gatekeeper — Build Plan

Source: `docs/PROPOSAL_AND_PLAN.md`.

**Rule: one phase at a time. A phase is done only when its gate passes. Commit after every gate, then stop.**

Each gate has three parts:
- **Automated:** `tests/gates/test_phase<N>.py` must pass (`make gate N=<N>`).
- **Manual:** a short check you run and eyeball, with the result logged in `NOTES.md`.
- **Code review:** `make lint` and `make test` clean (100% line + branch coverage). Then read every line written in the phase and cut anything lengthy, duplicated, unused or speculative (see `CLAUDE.md`). List what was cut in the phase summary.

Safety (every phase): all traffic stays on the isolated `lab` network. Attack tools only target our own containers. No external IPs, ever.

Design rules from the proposal's scalability section (§6b) that shape every phase. They are cheap to build in from the start and expensive to retrofit:
- **No state in memory:** Kafka consumers keep all window state in Redis, so any number of replicas can run in one consumer group.
- **At-least-once:** commit offsets only after an event is fully processed. Downstream effects must tolerate duplicates.
- **Idempotent mitigation:** applying the same block twice is harmless.
- **Config via env vars only**, so the same image runs in Compose and Kubernetes.

---

## Phase 0: Setup ✅

- [x] Proposal saved as `docs/PROPOSAL_AND_PLAN.md`; repo layout; `pyproject.toml`; `shared/` (event schema, settings); `NOTES.md`; `.gitignore`; `Makefile`.

**Gate 0:** `make lint`, `make test`, `make gate N=0` clean. Passed, commit `6ae7dcc`.

---

## Phase 1: Foundations ✅

### Tasks
- [x] `infra/compose/docker-compose.yml`. Internal `lab` network; only Redpanda also joins `default` so the host can reach it on `127.0.0.1:19092`.
  - [x] Redpanda (single broker, dev mode). A one-shot `redpanda-init` creates `flow-events` with 6 partitions (room for up to 6 consumer replicas in Phase 5b) before the collector starts.
  - [x] Redis and PostgreSQL with healthchecks (tables come in Phase 3).
  - [x] `victim`: nginx.
  - [x] `loadgen`: headless Locust, 10 users, normal traffic to `victim`.
- [x] `collector`: runs in the victim's network namespace (`NET_RAW`), raw `AF_PACKET` socket, parses inbound IPv4 TCP/UDP frames into per-packet `FlowEvent`s, skips its own Kafka traffic, publishes keyed by `src_ip` with an idempotent producer.
- [x] `scripts/consume.py`: prints live events.
- [x] Unit tests: frame parsing (TCP, UDP, IP options, non-IPv4, other protocols, truncated), schema round-trip, partition key.
- [x] Manual check passes (see below).
- [x] End-of-phase code review.

### Gate 1: flow events visible in a simple consumer
- **Automated:** `tests/gates/test_phase1.py`, run with the stack up (`make up`). It consumes 20 events within 20 s, checks each one's schema, checks that the message key equals `src_ip`, and checks that every event is inbound HTTP (port 80), which proves outgoing and Kafka traffic are filtered out.
- **Manual:** `make up && .venv/bin/python scripts/consume.py` streams events while Locust runs; `make down` cleans up.

---

## Phase 2: Detect ✅

### Tasks
- [x] `feature_engine`: consumer-group member on `flow-events`; per-second buckets in Redis; once a second, emits a vector for every source seen that second:
  - [x] per source: packets/s, bytes/s, SYN-to-ACK ratio (`syn / (ack + 1)`)
  - [x] per destination (the one the source hit most): distinct source count, source entropy, destination-port entropy
- [x] All window state in Redis (hash buckets, `HINCRBY`, TTL = window + 5 s), so replicas add up instead of overwriting. Window 10 s, step 1 s (`WINDOW_S`).
- [x] Exactly-once counting: Kafka offsets are committed after the Redis write; the last counted offset per partition is stored in the same Redis transaction as the counters, and replays are skipped.
- [x] Clean shutdown: SIGTERM closes consumers (they leave the group immediately) and flushes producers. Session timeout 10 s for crashed members.
- [x] Feature vectors published to `features`, keyed by source IP.
- [x] `detector` (FastAPI + Kafka worker thread): `Detector` protocol (`name`, `score(features) -> float`), chosen by `DETECTOR`, baseline loaded at startup (fail fast), publishes to `scores`, logs `ATTACK` above `SCORE_THRESHOLD`, `/healthz`.
- [x] `ThresholdDetector` (the baseline): upper bound per source feature = 2 × p99 of normal traffic (floor 1.0); score 0.5 at the bound. Destination entropy can't say *which* source is attacking, so it isn't scored here; it's computed for the Phase 4 models. Fitted with `make baseline`, saved to `models/baseline.json`.
- [x] `attacker` (profile `attack`, fixed IP 172.30.0.200): `syn`, `udp` (hping3) and `http` (ab) floods against `victim` only, at a controlled rate (default 2000 pps).
- [x] Normal traffic from 3 Locust replicas (3 source IPs).
- [x] Collector sheds load: when the producer queue is full it drops and counts, never crashes.
- [x] Unit tests: batch aggregation, entropy, window bounds, concurrent writers, TTL, zero-ACK SYN ratio, detector fit/score/save/load, Kafka error handling.
- [x] End-of-phase code review.

### Gate 2: a simulated flood is flagged in the logs
- **Automated:** `tests/gates/test_phase2.py`. 30 s of normal traffic gives scores but no flags. A 20 s SYN flood gets the attacker, and only the attacker, flagged within 10 s. The test prints the detection delay. Needs `make up` and `make baseline` first.
- **Manual:** `make logs` (or `docker compose logs detector`) shows `ATTACK src=172.30.0.200` during an attack. Log the delay in `NOTES.md`.

---

## Phase 3: Mitigate

### Tasks
- [ ] PostgreSQL tables `alerts` and `actions` (id, ts, src_ip, action, reason, detector, score, expires_at, revoked_at).
- [ ] `mitigation` controller consumes `scores`:
  - [ ] Acts after K consecutive windows above the threshold (configurable).
  - [ ] Allowlist: listed sources are never blocked.
  - [ ] Rate-limit first, hard block if the attack persists.
  - [ ] Every rule has a TTL; expired rules are removed automatically.
  - [ ] Every action (apply, escalate, expire) is logged to PostgreSQL.
  - [ ] Idempotent: a duplicate or replayed score never creates a second rule or a duplicate action row.
- [ ] Enforcement agent in the victim's namespace (`NET_ADMIN`) applying nftables rules in a dedicated `inet gatekeeper` table. The controller calls it over a small HTTP API.
- [ ] Measure time to mitigation (attack start to rule applied).
- [ ] Unit tests: consecutive-window logic, allowlist, TTL expiry, idempotent apply/remove.

### Gate 3: flood blocked automatically, normal traffic untouched
- **Automated:** `tests/gates/test_phase3.py`.
  - Setup: Locust runs throughout, then the flood starts.
  - Asserts during the flood: the attacker's rule appears and attacker packets are dropped. Locust's success rate stays ≥ 99%. No rule targets Locust or an allowlisted IP. An `actions` row exists.
  - Asserts after the TTL: the rule is gone and the expiry is logged.
  - Idempotency: replaying the same scores leaves the rule count and action rows unchanged.
- **Manual:** `nft list table inet gatekeeper` during an attack; `SELECT * FROM actions` shows apply, then expire. Log the time to mitigation.

---

## Phase 4: Compare

### Tasks
- [ ] Dataset generator: scripted sessions with randomized attack rate, duration, packet size and source count; labels come from the schedule.
  - [ ] Hard cases: flash crowd, slow ramp, mixed floods.
  - [ ] Feature vectors, labels, `session_id` and `attack_type` saved to Parquet under `data/`.
- [ ] Split by session; hold out one whole attack type (e.g. train without UDP floods, test on them).
- [ ] XGBoost model exported to ONNX.
- [ ] Autoencoder (PyTorch, normal traffic only) exported to ONNX; threshold from validation reconstruction error.
- [ ] `OnnxDetector` behind the same `Detector` interface; models chosen by config only.
- [ ] Evaluation harness replays the same traffic through each detector and records: detection delay, false-positive rate (including flash crowd), time to mitigation, throughput (windows/s), CPU and memory, and results on the held-out type and slow ramp.
- [ ] Unit tests: no session leakage in the split; ONNX output matches the native model within tolerance.

### Gate 4: results table for all three detectors
- **Automated:** `tests/gates/test_phase4.py`. No `session_id` appears in both train and test. All three detectors load by config and score the same replay. `docs/results.md` has a row per detector with every metric filled.
- **Manual:** `docs/results.md` includes at least one hard case. If the baseline wins, record it: that is a result.

---

## Phase 5a: Observe

### Tasks
- [ ] Prometheus metrics in the feature engine, detector and mitigation controller: consumer lag, score histogram, detection latency, active/total blocks, expired rules, dropped events.
- [ ] Grafana provisioned from `infra/monitoring/` (datasource and dashboard JSON, no clicking).
- [ ] Drift detection (KS or PSI) between live features and the training reference; exposed as a metric plus an alert rule.
- [ ] Consumer-lag alert rule.
- [ ] Dead-letter topic: malformed events go to `flow-events-dlq` instead of crashing a consumer.

### Gate 5a
- **Automated:** `tests/gates/test_phase5a.py`. Every `/metrics` endpoint exposes the expected names; the Grafana dashboard loads via the API; a shifted feature distribution fires the drift alert; a malformed message lands in the DLQ while the consumer keeps running.
- **Manual:** Grafana shows an attack spike, the block, and recovery.

---

## Phase 5b: Scale and survive

### Tasks
- [ ] Kubernetes on kind: manifests for every service, health and readiness probes, multiple replicas of the feature engine and detector.
- [ ] Mitigation on Kubernetes: nftables agent (DaemonSet/sidecar) or NetworkPolicy. Decide and document.
- [ ] Autoscaling the feature engine and detector on consumer lag (KEDA or HPA with external metrics).
- [ ] Multi-broker Redpanda (3 brokers, replication factor 3).
- [ ] Load test at 1, 2 and 4 replicas: sustained events/s, p50/p95/p99 detection latency, and the first bottleneck (Redis, inference or Kafka). Report in `docs/scaling.md`.
- [ ] Chaos tests: kill a consumer (watch the rebalance), restart Redis, restart a broker. Record the detection impact and recovery time in `docs/scaling.md`.
- [ ] Written SLO in `docs/slo.md` (e.g. "99% of attacks blocked within N s", with N taken from measurements), plus the hot-partition and window-size trade-offs.

### Gate 5b
- **Automated:** `tests/gates/test_phase5b.py`. Against kind: an attack is detected and blocked with 2+ replicas. Killing one consumer pod mid-attack still leads to a block within the SLO.
- **Manual:** `docs/scaling.md` has the 1/2/4-replica table and the chaos results.

---

## Phase 5c: Ship

### Tasks
- [ ] Local CI: `make ci` (lint, unit tests, image build) run by a git pre-commit hook. The project is local-only, with no git remote.
- [ ] Helm chart or Kustomize overlays replacing the raw manifests.
- [ ] `make demo`: one command that brings up the stack, runs normal traffic, launches an attack and shows the mitigation.
- [ ] README: architecture diagram, results table, scaling numbers, SLO, limitations (simulated traffic, local cluster, generator-overfitting risk); short demo video.

### Gate 5c: one-command demo of an attack and its mitigation
- **Automated:** `make ci` passes; `tests/gates/test_phase5c.py` runs `make demo` and asserts that the attack is blocked and normal traffic is unaffected.
- **Manual:** from a clean clone, `make demo` works end to end. Record the video.

---

## Stretch (only after Gate 5c)
- [ ] Feature engine in Go or Rust; benchmark against Python.
- [ ] Harder attack variants.
- [ ] Drift-triggered retraining with versioned rollout and rollback.
- [ ] Autoscaling under attack.
