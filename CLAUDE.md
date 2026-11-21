# Gatekeeper

Real-time DDoS detection and automated mitigation platform. Full proposal: `docs/PROPOSAL_AND_PLAN.md`. Build plan with gates: `PLAN.md`.

## Rule
**Work one phase at a time. Stop at each gate.** Only implement the phase you're asked for. When its gate test passes, report the result and stop — don't start the next phase, and don't commit unless asked.

## Stack
- Python 3.11+; ruff + mypy; pytest
- Redpanda (Kafka API) for streaming; plain Python consumers
- Redis (sliding-window state, TTL expiry); PostgreSQL (alerts + actions log)
- FastAPI + ONNX Runtime for the detector service
- scikit-learn, XGBoost, PyTorch (autoencoder → ONNX); KS/PSI for drift
- nftables in the victim's network namespace for mitigation
- Prometheus + Grafana
- Docker Compose first; Kubernetes (kind) in Phase 5
- Locust (normal traffic), hping3 (attack traffic)

## Layout
`services/{collector,feature_engine,detector,mitigation,loadgen,victim}`, `shared/`, `infra/{compose,k8s,monitoring}`, `models/`, `notebooks/`, `tests/{unit,gates}`, `docs/`.

## Conventions
- Detectors implement `score(features) -> float` and are swapped by config only.
- Kafka messages keyed by source IP.
- Config via env vars; no hardcoded hosts/ports.
- Gate tests live in `tests/gates/test_phaseN.py`; run with `make gate N=<n>`.
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
