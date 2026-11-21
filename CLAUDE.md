# Gatekeeper

Real-time DDoS detection and automated mitigation platform. Runs locally; public repo at github.com/adarshmohan29/Gatekeeper (push only when asked). Full proposal: `docs/PROPOSAL_AND_PLAN.md`. Build plan with gates: `PLAN.md`.

## Rule
**Work one phase at a time. Stop at each gate.** Only implement the phase you're asked for. When its gate test passes, report the result and stop — don't start the next phase, and don't commit unless asked.

## Stack
- Python 3.11+; ruff + mypy; pytest
- Redpanda (Kafka API) for streaming; plain Python consumers
- Redis (sliding-window state, TTL expiry); PostgreSQL (alerts + actions log)
- FastAPI + ONNX Runtime for the detector service
- XGBoost and PyTorch for training (host only, `[ml]` extra); models are served as ONNX with onnxruntime; KS/PSI for drift
- nftables in the victim's network namespace for mitigation
- Prometheus + Grafana (provisioned from `infra/monitoring/`); `prometheus_client` metrics on port 9100 in every service
- Docker Compose first; Kubernetes (kind, multi-broker Redpanda, lag-based autoscaling) in Phase 5b
- Local CI: `make ci` (lint, unit tests, image build), run by the pre-commit hook in `.githooks/` (`make venv` enables it); Kustomize for Kubernetes
- `make demo`: stack up, flood, narrated mitigation; exits non-zero on failure
- Locust (normal traffic), hping3 (attack traffic)

## Layout
`services/{collector,feature_engine,detector,mitigation,enforcer,drift,loadgen,bench,attacker}`, `shared/`, `scripts/` (host-side tools; `scripts/lab.py` drives the Compose lab for the demo and gates), `infra/{compose,k8s,monitoring}`, `models/`, `notebooks/`, `tests/{unit,gates}`, `docs/`.

## Conventions
- Detectors implement `score(features) -> float` and are swapped by config only.
- Kafka messages keyed by source IP. Topics are created explicitly by `redpanda-init` in Compose, never auto-created.
- Shared Kafka helpers live in `shared/kafka.py`; don't duplicate client config.
- Config via env vars; no hardcoded hosts/ports.
- Consumers hold no state in memory: all window state lives in Redis so services scale as consumer-group replicas.
- At-least-once: commit offsets after processing. Effects must be idempotent; blocking twice is harmless. Where an effect isn't naturally idempotent (counters), store the last applied offset per partition in the same transaction and skip replays (see `services/feature_engine/windows.py`).
- Under overload, drop and count; never crash or block on an unbounded queue.
- On SIGTERM: close consumers (leave the group immediately) and flush producers. Never leave a stale group member.
- Python services share one image (`services/Dockerfile`); compose picks the entrypoint.
- Control-plane APIs (e.g. the enforcer) are reachable from the lab network, attackers included: they require the shared token, and their inputs are validated models (no shell, no string-built commands from raw input).
- Pure logic lives in testable modules; `services/*/main.py` is thin I/O wiring, excluded from unit coverage and exercised by the gate tests.
- Gate tests live in `tests/gates/test_phase<N>.py` (N = 0–4, 5a, 5b, 5c); run with `make gate N=<N>`.
- Run the venv's Python (`.venv/bin/python`). Docker is OrbStack.
- Kubernetes: `make k8s-up` (kind cluster `gatekeeper`, Calico, KEDA, builds and loads images), `make k8s-down`, `make load-test`, `make gate N=5b`. After a code change, rebuild `gatekeeper:dev`, `kind load docker-image` it and `rollout restart` the deployments. Compose and kind don't fit in memory together: `make k8s-down` before `make up`.
- Debug a restarting pod with `kubectl -n gatekeeper logs <pod> --previous`.
- macOS training quirk: torch and XGBoost each bundle OpenMP. `ml/__init__.py` imports XGBoost first, and torch runs single-threaded; otherwise XGBoost segfaults or training deadlocks.
- Gates judge only what their own run caused (filter by start time), never accumulated history.
- Metrics: prefix `gatekeeper_`; create labelled counter series at 0 at startup (a series that first appears at 1 hides that increase from `increase()`/alerts).
- Every consumer parses through `parse_or_dead_letter`: malformed messages go to `<topic>-dlq`, never crash the loop.
- Log what broke and what was measured in `NOTES.md`.
- Core logic (feature windows, detector interface, mitigation rules, evaluation) — keep it readable and explain it when asked; the user must be able to defend it.

## Code quality — no slop
- Write only what the current phase needs. No speculative fields, options, abstractions or "for later" code.
- No wrappers that just call one function, no dead code, no commented-out code, no unused imports/params.
- Short functions, flat control flow. Comments only where the *why* isn't obvious; no restating the code.
- No defensive checks for impossible cases; no broad `except` that hides errors.
- Every line must earn its place: if deleting it changes nothing, delete it.

## End-of-phase review (part of every gate)
Before reporting a gate as passed:
1. `make lint` (ruff incl. complexity, unused args, commented-out code; strict mypy) and `make test` are clean.
2. Read every line written or changed in the phase. Cut anything lengthy, duplicated, unused or unneeded, then rerun the checks.
3. Every new module has unit tests; every branch of core logic is exercised.
4. Report what was cut in the phase summary.

## Safety
Attack traffic only on the isolated Compose/kind network, against our own containers. Never target external IPs.
