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

## Phase 3: Mitigate ✅

### Tasks
- [x] PostgreSQL schema (`infra/compose/postgres/init.sql`): `alerts` (one row per flagged source window) and `actions` (rate_limit/block with reason, score, window, `expires_at`, `expired_at`). Unique keys make replays no-ops.
- [x] `mitigation` controller consumes `scores`:
  - [x] Streak of consecutive flagged windows, computed from `alerts` (no in-process state).
  - [x] Rate-limit after `CONSECUTIVE_WINDOWS` (3); block after `ESCALATE_AFTER` (5) more.
  - [x] Allowlist (`ALLOWLIST`, CIDRs): flagged and recorded as alerts, never acted on.
  - [x] Every rule has a TTL (`RULE_TTL_S`; 15 s in Compose). The kernel removes it; the controller records `expired_at`.
  - [x] Idempotent: unique (source, action, window) rows, an idempotent enforcer add, no action while a rule is active, scores older than `MAX_SCORE_AGE_S` ignored. Enforce happens before the log write, so a crash in between re-applies on replay.
- [x] `enforcer` in the victim's namespace (`NET_ADMIN`): nftables table `inet gatekeeper` with timeout sets `blocked`/`ratelimited` and a per-source rate-limit meter. `POST /rules` requires a bearer token (`ENFORCER_TOKEN`), since the API is reachable from the lab network.
- [x] The collector ignores control-plane ports (Kafka, enforcer).
- [x] Time to mitigation measured by the gate.
- [x] Unit tests: streak, decide (thresholds, escalation, active rules, allowlist), nft commands, settings.
- [x] End-of-phase code review.

### Gate 3: flood blocked automatically, normal traffic untouched
- **Automated:** `tests/gates/test_phase3.py`.
  - Setup: floods from the attacker (172.30.0.200) and an allowlisted tester (172.30.0.201) while Locust runs.
  - Asserts during the attack: the attacker is rate-limited within 10 s and then blocked; drop counters rise; no Locust or allowlisted IP ever gets a rule; Locust failure rate stays below 1%.
  - Asserts afterwards: replayed scores create no actions; the audit tables are correct; the rules expire and the expiry is recorded.
  - A rule POST without the token, made from inside the lab, gets 401.
- **Manual:** `docker compose exec enforcer nft list table inet gatekeeper` during an attack; `SELECT * FROM actions` shows apply, then expire. Log the time to mitigation in `NOTES.md`.

---

## Phase 4: Compare ✅

### Tasks
- [x] Dataset generator (`make dataset`, ~20 min): 28 scripted sessions against the live stack, mitigation paused. SYN, HTTP and mixed floods; UDP floods; flash crowds (legitimate Locust bursts); slow-ramp SYN floods. Randomized rate, payload size, source count (1-3) and duration. Labels come from the schedule (attacker IPs 172.30.0.200-209). Saved to `data/dataset.parquet` with `session_id`, `kind`, `role`, `label`.
- [x] Split by session (30% of seen-kind sessions to test); UDP, flash and ramp are held out of training entirely.
- [x] XGBoost on all six features, exported to ONNX (onnxmltools).
- [x] Autoencoder (PyTorch) on normal traffic, per-source features; log1p and standardization inside the graph; threshold 2 × p99 of the training error; exported with the current torch.export-based ONNX exporter.
- [x] `OnnxDetector` behind the same `Detector` protocol; `DETECTOR=threshold|xgboost|autoencoder` picks the model from `MODELS_DIR`. The threshold baseline is refit on the same training split (`make train`).
- [x] Evaluation harness (`make evaluate`): every detector scores the same recorded test vectors, each in its own process; writes `docs/results.md` (false-positive rates incl. flash crowd, detection rate and delay for seen/UDP/ramp, time to mitigation, throughput, CPU per window, peak memory).
- [x] Unit tests: schedule, labels, leak-free split, Parquet round trip, metrics, ONNX output equals native output (XGBoost and autoencoder), served models separate attacks.
- [x] End-of-phase code review.

### Gate 4: results table for all three detectors
- **Automated:** `tests/gates/test_phase4.py`. No `session_id` in both train and test, and held-out kinds only in test. All three detectors load by name and score the same replay. `docs/results.md` has a column per detector, every cell filled, and rows for both hard cases.
- **Manual:** read `docs/results.md`; run the live pipeline with `DETECTOR=xgboost` (and `autoencoder`) and check that Gate 2 still flags the flood. If the baseline wins, record it: that is a result.

---

## Phase 5a: Observe ✅

### Tasks
- [x] Prometheus metrics (`prometheus_client`, port 9100) in the collector, feature engine, detector, mitigation and drift monitor: events published/dropped/counted/replayed, vectors emitted, windows scored/flagged, score histogram, detection latency (window end to score) and mitigation latency (window end to rule), rules applied/expired/active, dead letters, drift. Labelled series are created at 0 so the first increase is visible.
- [x] Consumer-group lag from the broker (Redpanda upgraded to v25.1.9, `enable_consumer_group_metrics`), so lag shows even when a consumer is down.
- [x] Prometheus (127.0.0.1:9090) with alert rules: FeatureDrift, ConsumerLag, ServiceDown, EventsDropped, DeadLetters.
- [x] Grafana (127.0.0.1:3000, no login, local only) provisioned from `infra/monitoring/`: an 8-panel dashboard.
- [x] Drift monitor (`services/drift`): one-sided PSI over log-scale bins, on per-source features, last 600 vectors vs normal training traffic (reference written by `make train`). Sources the detector flagged in the last 60 s are left out, so attacks don't count as drift; traffic moving where training never was does.
- [x] Dead-letter topics (`<topic>-dlq`): malformed messages are parked with the parse error in a header, and every consumer keeps running.
- [x] Unit tests: drift score (stable, narrower, shifted, short attack), dead-letter routing.
- [x] End-of-phase code review.

### Gate 5a
- **Automated:** `tests/gates/test_phase5a.py`. All 6 scrape targets up and every expected metric exposed; the Grafana dashboard is provisioned and all its queries are valid. A malformed message lands in `flow-events-dlq`, the DeadLetters alert fires, and the feature engine keeps running. Shifted traffic (1.5x, from 192.0.2.0/24) fires FeatureDrift. Stopping the feature engine fires ConsumerLag, which resolves after restart.
- **Manual:** open http://127.0.0.1:3000, run `make gate N=3`, and watch the attack spike, the rules, and the recovery.

---

## Phase 5b: Scale and survive

### Tasks
- [x] Kubernetes on kind: manifests for every service, health and readiness probes, multiple replicas of the feature engine and detector.
- [x] Mitigation on Kubernetes: nftables agent (DaemonSet/sidecar) or NetworkPolicy. Decide and document.
- [x] Autoscaling the feature engine and detector on consumer lag (KEDA or HPA with external metrics).
- [x] Multi-broker Redpanda (3 brokers, replication factor 3).
- [x] Load test at 1, 2 and 4 replicas: sustained events/s, p50/p95/p99 detection latency, and the first bottleneck (Redis, inference or Kafka). Report in `docs/scaling.md`.
- [x] Chaos tests: kill a consumer (watch the rebalance), restart Redis, restart a broker. Record the detection impact and recovery time in `docs/scaling.md`.
- [x] Written SLO in `docs/slo.md` (e.g. "99% of attacks blocked within N s", with N taken from measurements), plus the hot-partition and window-size trade-offs.

### Gate 5b
- **Automated:** `tests/gates/test_phase5b.py`. Against kind: an attack is detected and blocked with 2+ replicas. Killing one consumer pod mid-attack still leads to a block within the SLO.
- **Manual:** `docs/scaling.md` has the 1/2/4-replica table and the chaos results.

---

## Phase 5c: Ship

### Tasks
- [x] Local CI: `make ci` (lint, unit tests, image build) run by a git pre-commit hook.
- [x] Helm chart or Kustomize overlays replacing the raw manifests. (Kustomize builds every manifest, with generated ConfigMaps and Secret; autoscaling is a separate layer. No overlays: there is only one environment, so one would be speculative.)
- [x] `make demo`: one command that brings up the stack, runs normal traffic, launches an attack and shows the mitigation.
- [x] README: architecture diagram, results table, scaling numbers, SLO, limitations (simulated traffic, local cluster, generator-overfitting risk).
- [x] Short demo video: `docs/demo/demo.mp4`, with snapshots.

### Gate 5c: one-command demo of an attack and its mitigation
- **Automated:** `make ci` passes; `tests/gates/test_phase5c.py` runs `make demo` and asserts that the attack is blocked and normal traffic is unaffected.
- **Manual:** from a clean clone, `make demo` works end to end. Record the video.

---

## Stretch (only after Gate 5c)
- [ ] Feature engine in Go or Rust; benchmark against Python.
- [ ] Harder attack variants.
- [ ] Drift-triggered retraining with versioned rollout and rollback.
- [ ] Autoscaling under attack.
