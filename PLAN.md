# Gatekeeper — Build Plan

Source: `docs/PROPOSAL_AND_PLAN.md`.

**Rule: one phase at a time. A phase is done only when its gate test passes. Commit after every gate, then stop.**

Each gate has two parts:
- **Automated:** a pytest file under `tests/gates/` that must pass (`pytest tests/gates/test_phaseN.py`).
- **Manual:** a short check you run and eyeball, with the result logged in `NOTES.md`.
- **Code review (every phase):** `make lint` and `make test` clean, including 100% line coverage. Then read every line written in the phase and cut anything lengthy, duplicated, unused or speculative (see "Code quality" in `CLAUDE.md`). List what was cut in the phase summary.

Safety (applies to every phase): all traffic stays on the isolated Compose network. Attack tools only target our own containers. No external IPs, ever.

---

## Phase 0: Setup

### Tasks
- [ ] Confirm repo name on GitHub (`gatekeeper` is free under your account); create it and add remote — **you do this**.
- [x] Move `PROPOSAL_AND_PLAN.md` to `docs/PROPOSAL_AND_PLAN.md`.
- [x] Create layout:
  - `services/{collector,feature_engine,detector,mitigation,loadgen,victim}/`
  - `infra/{compose,k8s,monitoring}/`
  - `models/`, `notebooks/`, `tests/{unit,gates}/`, `docs/`
- [x] `pyproject.toml` (Python 3.11+), dev deps: `pytest`, `ruff`, `mypy`.
- [x] `shared/` package for common code: event schema (flow event dataclass/pydantic), config loading.
- [x] `NOTES.md` (what broke, what was measured, what surprised you).
- [x] `.gitignore` (venv, `__pycache__`, `data/`, `*.parquet`, `*.onnx` unless deliberately committed).
- [x] `Makefile` targets: `up`, `down`, `logs`, `test`, `lint`, `gate N=<n>`.

### Gate 0
- **Automated:** `make lint`, `make test` and `make gate N=0` (layout check) run clean.
- **Manual:** `tree -L 2` matches the layout above.

---

## Phase 1: Foundations

### Tasks
- [ ] `infra/compose/docker-compose.yml` with an isolated bridge network (`internal: true` where possible):
  - [ ] Redpanda (Kafka API) + topic `flow-events` (N partitions, keyed by source IP)
  - [ ] Redis
  - [ ] PostgreSQL (with init SQL for later tables)
  - [ ] `victim`: minimal FastAPI/nginx web service
  - [ ] `loadgen`: Locust in headless mode generating normal traffic against `victim`
- [ ] Healthchecks on every service; `depends_on` with `condition: service_healthy`.
- [ ] `collector`: shares the victim's network namespace (`network_mode: service:victim`, `NET_RAW` cap), sniffs packets (scapy or raw socket), aggregates into compact flow events, publishes to `flow-events` keyed by `src_ip`.
- [ ] Flow event schema in `shared/`: `ts, src_ip, dst_ip, src_port, dst_port, proto, tcp_flags, pkt_len` (or per-flow aggregates — decide and document in NOTES.md).
- [ ] `scripts/consume.py`: tiny consumer that prints events from `flow-events`.
- [ ] Unit tests: event serialization round-trip, partition key = `src_ip`.

### Gate 1 — flow events visible in a simple consumer
- **Automated:** `tests/gates/test_phase1.py`
  - brings stack up (or assumes `make up`), sends ~100 HTTP requests to `victim`,
  - consumes `flow-events` for ≤10 s,
  - asserts ≥1 event received, schema validates, message key equals event `src_ip`.
- **Manual:** `make up && python scripts/consume.py` shows a live stream while Locust runs. `docker compose down` cleans everything up.

---

## Phase 2: Detect

### Tasks
- [ ] `feature_engine`: consumes `flow-events`, computes sliding-window features per source and per destination:
  - [ ] packets/s, bytes/s
  - [ ] SYN-to-ACK ratio
  - [ ] distinct source count (per destination)
  - [ ] source and destination entropy
- [ ] Window state in Redis with TTL expiry; window size and step configurable (start: 1 s step, 10 s window).
- [ ] Publish feature vectors to topic `features` (one record per key per window).
- [ ] `detector` (FastAPI): common interface
  ```python
  class Detector(Protocol):
      name: str

      def score(self, features: FeatureVector) -> float: ...  # attack probability in [0, 1]
  ```
  - [ ] Detector selected by config/env var — no code changes elsewhere to swap.
  - [ ] Consumes `features`, scores, publishes to `scores` and logs scores above threshold.
- [ ] `EntropyThresholdDetector`: learns normal ranges (e.g. mean ± k·std on packet rate and source entropy) from a normal-traffic capture; flags out-of-range windows.
- [ ] `loadgen`: add attack container with hping3 (SYN flood, UDP flood) and an HTTP flood mode, all targeting `victim` only, triggered on a schedule or via CLI flag.
- [ ] Unit tests: entropy on known distributions, window rollover/expiry, SYN/ACK ratio edge cases (zero ACKs), detector interface contract.

### Gate 2 — a simulated flood is flagged in the logs
- **Automated:** `tests/gates/test_phase2.py`
  - 30 s normal traffic → asserts zero (or ≤ agreed tolerance) flagged windows,
  - start SYN flood for 20 s → asserts detector flags the attacker source within N windows,
  - records detection delay (attack start → first flag) and prints it.
- **Manual:** `docker compose logs -f detector` shows the flag line during an attack. Log the measured delay in NOTES.md.

---

## Phase 3: Mitigate

### Tasks
- [ ] PostgreSQL schema: `alerts` and `actions` (id, ts, src_ip, action, reason, detector, score, expires_at, revoked_at).
- [ ] `mitigation` controller: consumes `scores`.
  - [ ] Act only after K consecutive windows above threshold (configurable).
  - [ ] Allowlist (config file) — allowlisted sources never blocked.
  - [ ] Escalation: rate-limit first, hard block if attack persists.
  - [ ] Every rule has a TTL; expired rules are removed automatically.
  - [ ] Every action (apply, escalate, expire) written to PostgreSQL.
- [ ] Enforcement agent sharing the victim's network namespace (`NET_ADMIN`), applying nftables rules (dedicated `gatekeeper` table so cleanup is trivial). Controller talks to it over a small HTTP API.
- [ ] Metric: time to mitigation (attack start → rule applied).
- [ ] Unit tests: consecutive-window logic, allowlist, TTL expiry, idempotent rule apply/remove.

### Gate 3 — flood blocked automatically, normal traffic untouched
- **Automated:** `tests/gates/test_phase3.py`
  - normal Locust traffic running throughout,
  - start flood from attacker container,
  - asserts: nft rule for attacker IP appears; attacker packets dropped (counter rises / victim stops seeing them); Locust success rate stays ≥ 99%; no rule ever targets the Locust IP or an allowlisted IP; action row exists in `actions`,
  - after TTL, asserts rule is gone and expiry is logged.
- **Manual:** `nft list table inet gatekeeper` inside the victim namespace during an attack; `SELECT * FROM actions` shows apply → expire. Log time-to-mitigation in NOTES.md.

---

## Phase 4: Compare

### Tasks
- [ ] Dataset generator script: runs scripted sessions with randomized attack rate, duration, packet size, source count; exact labels from the schedule.
  - [ ] Include hard cases: flash crowd (legit spike), slow ramp, mixed floods.
  - [ ] Save feature vectors + labels + `session_id` + `attack_type` to Parquet under `data/`.
- [ ] Split by session; hold out at least one whole attack type (e.g. train without UDP floods, test on them).
- [ ] Train gradient-boosted tree (XGBoost) → export ONNX.
- [ ] Train autoencoder on normal traffic only (PyTorch) → export ONNX; threshold from validation reconstruction error.
- [ ] `OnnxDetector` implementing the same `Detector` interface; both models load via config only.
- [ ] Evaluation harness: replays the same recorded traffic through each detector and records:
  - [ ] detection delay, false-positive rate (incl. flash crowd), time to mitigation
  - [ ] throughput (windows scored/s), CPU and memory
  - [ ] performance on held-out attack type and slow ramp
- [ ] Unit tests: split has no session leakage; ONNX output matches native model output within tolerance.

### Gate 4 — results table for all three detectors
- **Automated:** `tests/gates/test_phase4.py`
  - asserts no `session_id` appears in both train and test,
  - asserts all three detectors load via config and score the same replay,
  - asserts `docs/results.md` exists with a row per detector and every metric column filled.
- **Manual:** read `docs/results.md`; it includes at least one hard case (flash crowd or slow ramp). If the baseline wins, write that down — it's a result.

---

## Phase 5: Harden

### Tasks
- [ ] Prometheus metrics in feature engine, detector, mitigation: consumer lag, score histogram, detection latency, blocks active/total, rules expired.
- [ ] Grafana provisioned from `infra/monitoring/` (datasource + dashboard JSON, no manual setup).
- [ ] Drift detection: compare live feature distributions to the training reference (KS test or PSI) on a schedule; expose as metric + Prometheus alert rule.
- [ ] Kubernetes manifests in `infra/k8s/` (kind); mitigation via nftables agent or NetworkPolicy. Stretch: HPA on detector.
- [ ] `make demo`: one command that brings up the stack, runs normal traffic, launches an attack, and shows it being mitigated.
- [ ] README: architecture diagram, results table, limitations (simulated traffic, generator overfitting risk), demo video link.

### Gate 5 — one-command demo of attack and mitigation
- **Automated:** `tests/gates/test_phase5.py`
  - every service's `/metrics` endpoint responds and exposes the expected metric names,
  - Grafana dashboard loads via API,
  - injecting a shifted feature distribution fires the drift alert.
- **Manual:** from a clean clone, `make demo` runs end to end; Grafana shows the attack spike, the block, and recovery. Record the demo video.

---

## Stretch (only after Gate 5)
- [ ] Feature engine in Go or Rust; benchmark vs. Python.
- [ ] Harder attack variants.
- [ ] Drift-triggered retraining with versioned rollout and rollback.
- [ ] Autoscaling under attack.
