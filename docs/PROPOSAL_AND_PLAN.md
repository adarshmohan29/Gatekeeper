# Gatekeeper: Real-Time DDoS Detection and Automated Mitigation Platform

## 1. What this project is

A platform that watches network traffic in real time, flags volumetric attacks (SYN, UDP and HTTP floods), and automatically blocks or rate-limits the offending sources. The ML model is one swappable component. The engineering around it is the project: a streaming pipeline, low-latency feature computation, model serving, automated mitigation, monitoring and drift detection, all running in containers.

Effort split (rough estimate): about 40% systems and infrastructure, 35% backend, 25% ML.

## 2. Goals and non-goals

Goals
- Detect a simulated volumetric attack and trigger mitigation automatically, with measured end-to-end latency (attack start to block).
- Run the whole pipeline with one command (`docker compose up`), later on a local Kubernetes cluster.
- Compare three detectors in the same pipeline: an entropy/threshold baseline, a gradient-boosted tree model, and an unsupervised autoencoder.
- Report honest numbers: detection delay, false-positive rate, throughput, CPU and memory per detector.
- Monitor the system and detect data drift.

Non-goals
- Beating state-of-the-art accuracy on a benchmark.
- Protecting real production infrastructure. All attack traffic is generated against your own containers in an isolated Docker network.
- Detecting every attack type. Scope is volumetric floods only.

## 3. Architecture

Load generator -> Victim service -> Collector -> Kafka (flow events, keyed by source IP) -> Feature engine (windowed features, state in Redis) -> Detector service (FastAPI + ONNX, swappable models) -> Mitigation controller (block / rate-limit, allowlist, expiring rules, PostgreSQL audit log) -> block rules pushed back to the Victim service.
Prometheus and Grafana scrape the feature engine, detector and mitigation controller (pipeline lag, scores, detection latency, blocks).

## 4. Tech stack

| Layer | Choice |
| --- | --- |
| Language | Python (Go or Rust optional later for the hot path) |
| Streaming | Kafka or Redpanda; Python consumers first, Faust or Flink later |
| Serving | FastAPI + ONNX Runtime |
| State | Redis (sliding windows), PostgreSQL (alerts and actions log) |
| ML | scikit-learn, XGBoost, PyTorch or Keras (autoencoder), MLflow (optional), drift check (KS test or PSI) |
| Mitigation | nftables/iptables in a container, or Kubernetes NetworkPolicy |
| Observability | Prometheus + Grafana |
| Packaging | Docker Compose, then Kubernetes (kind or minikube) |
| Simulation and tests | Locust (normal traffic), hping3 (attack traffic, own containers only), pytest |

## 5. Components

1. Load generator and victim service: a small web service as the target; a generator produces normal and attack traffic on a schedule.
2. Collector: reads flow records from the victim's interface and publishes compact events to Kafka, partitioned by source IP.
3. Feature engine: sliding-window features per source and destination: packets/s, bytes/s, SYN-to-ACK ratio, distinct source count, source and destination entropy. Counters in Redis with expiry.
4. Detector service: common interface `score(features) -> attack_probability`; detectors are swappable without touching the rest of the system.
5. Mitigation controller: acts after several consecutive windows above threshold; allowlist for known-good sources; rules expire automatically; every action logged to PostgreSQL.
6. Monitoring and drift: Prometheus metrics, Grafana dashboards, drift alert when live feature distributions diverge from training.

## 6. Model comparison and metrics

| Detector | Idea |
| --- | --- |
| Entropy and threshold | Alert when packet rate or source entropy leaves a learned range. The bar everything else must beat. |
| Gradient-boosted trees | Supervised model on the engineered features. Strong tabular baseline. |
| Autoencoder | Trained on normal traffic only; high reconstruction error flags an anomaly. Exported to ONNX. |

Metrics per detector: detection delay, false-positive rate (including legitimate flash crowds), time to mitigation, throughput (windows scored per second), CPU and memory, and robustness on attack patterns unseen in training (slow ramps, mixed floods).

Data: all data comes from your own traffic generator, so no external dataset is needed. Because you start every attack yourself, labels are exact. To keep the models honest, randomize attack rates, durations, packet sizes and source counts, and hold out whole attack types (for example, train without UDP floods and test on them). Split by attack session, never randomly.

## 7. Risks

| Risk | Handling |
| --- | --- |
| Simulated traffic is not real traffic | State it in the write-up; use public datasets alongside your own generator |
| Models overfit to the generator's patterns | Randomize attack parameters, hold out attack types, split by attack session |
| Simple baseline may win | Treat it as a result; the engineering story stands |
| Scope creep | Docker Compose first; Kubernetes is a stretch goal |
| Blocking legitimate users | Allowlist, expiring rules, rate limit before hard block, log every action |
| Safety and legality | Isolated Docker network only, targets you own, no external IPs |

## 8. Step-by-step plan

Rule: finish each phase and pass its gate before starting the next. Commit after every gate. Each phase is roughly a weekend; adjust to your pace.

### Setup (before Phase 1)
- [ ] Create the repo named `gatekeeper` (search GitHub first; if the name is taken, use `gatekeeper-ddos`).
- [ ] Create the repo, run `git init`, open it in VS Code.
- [ ] Save this file as `docs/PROPOSAL_AND_PLAN.md`.
- [ ] Create `CLAUDE.md` with the stack, conventions, and the rule: "work one phase at a time, stop at each gate".
- [ ] Create a `NOTES.md` where you log what broke, what you measured, and what surprised you.
- [ ] Suggested layout: `services/{collector,feature_engine,detector,mitigation,loadgen,victim}`, `infra/{compose,k8s,monitoring}`, `models/`, `notebooks/`, `tests/`, `docs/`.

### Phase 1: Foundations
- [ ] Docker Compose with Kafka (or Redpanda), Redis, PostgreSQL, a victim web service, and a normal-traffic generator (Locust).
- [ ] Collector publishes flow events to a Kafka topic keyed by source IP.
- [ ] Gate: flow events visible in a simple Kafka consumer.

### Phase 2: Detect
- [ ] Feature engine consumes events and computes windowed features, with state in Redis.
- [ ] Detector interface plus the entropy/threshold baseline.
- [ ] Add a controlled attack to the generator (hping3, own containers only).
- [ ] Gate: a simulated flood is flagged in the logs.

### Phase 3: Mitigate
- [ ] Mitigation controller: consecutive-window rule, allowlist, expiring block or rate-limit rules, PostgreSQL action log.
- [ ] Apply rules to the victim container (nftables/iptables).
- [ ] Gate: flood blocked automatically, normal traffic untouched.

### Phase 4: Compare
- [ ] Generate a labeled dataset from your own generator (varied rates, timings, attack types), save it as Parquet, and split by attack session.
- [ ] Train the tree model and the autoencoder; export to ONNX.
- [ ] Plug both into the same detector interface.
- [ ] Run the same traffic through each detector and record detection delay, false positives, throughput and resource use.
- [ ] Gate: a results table for all three detectors, including at least one hard case (flash crowd or slow ramp).

### Phase 5: Harden
- [ ] Prometheus metrics and Grafana dashboards (lag, scores, detection latency, blocks).
- [ ] Drift detection with an alert.
- [ ] Kubernetes version (kind or minikube); stretch: autoscaling.
- [ ] README with the architecture diagram, results table and limitations; a short demo video.
- [ ] Gate: one-command demo of an attack and its mitigation.

## 9. Working with Claude Code

- Ask it to read this file and turn it into a `PLAN.md` with task checklists and a gate test per phase. Edit that plan yourself before building.
- Say "implement Phase N only". Run it, check the gate, commit.
- Let Claude scaffold boilerplate (Compose files, config, Prometheus setup, test skeletons). Write or closely review the core logic yourself: feature windows, detector interface, mitigation logic, evaluation code.
- After each module, ask Claude to explain it and quiz yourself. If you cannot explain a file, you cannot defend it in an interview.

## 10. Stretch goals and resume framing

Stretch: move the feature engine to Go or Rust and measure the gain; generate harder attack variants to stress the detectors; automated retraining triggered by the drift alert with versioned rollout and rollback; autoscaling under attack.

Resume line (fill in only what you measured): "Built a real-time DDoS detection and mitigation platform (Kafka, Redis, FastAPI, Kubernetes); automated blocking X seconds after attack onset; benchmarked three detectors on latency, false positives and resource use."
