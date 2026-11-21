# Scaling and failure behaviour

Measured on a local kind cluster: one node, 8 vCPUs and 4 GB RAM (OrbStack on an 8 GB Mac).
Everything (3 Redpanda brokers, Redis, PostgreSQL, the services, and the load generators)
shares that node, so these numbers show *how the system scales and fails*, not internet-scale
capacity.

## Load test (`make load-test`)

Synthetic flow events are produced straight into Kafka: 40,000 events/s from 1,600 simulated
clients at 25 events/s each (a normal client rate, so nothing gets flagged). The feature engine
and detector are pinned at N replicas, each run starts with no backlog, and results are read
from Prometheus over the last 45 s of a 90 s run.

| Replicas | Offered events/s | Processed events/s | Feature-engine lag (end) | Vectors scored/s (of 1,600) | Detection latency p50 / p95 / p99 (s) | Feature-engine CPU (cores) | Detector CPU (cores) |
|---|---|---|---|---|---|---|---|
| 1 | 40,000 | 31,500 | 598,372 | 437 | 0.18 / 0.39 / 0.87 | 0.9 | 0.0 |
| 2 | 40,000 | 39,273 | 222,239 | 1,572 | 0.45 / 0.93 / 0.99 | 1.9 | 0.1 |
| 4 | 40,000 | 40,074 | 7,033 | 1,600 | 0.43 / 0.92 / 0.98 | 2.0 | 0.1 |

Detection latency is from the end of a feature window to its score being published.

**Reading it:**
- **1 replica can't keep up.** It is CPU-bound (0.9 of a core for a single-threaded Python
  process) and lag grows without bound. Worse, its once-a-second tick is starved, so only 27%
  of sources get scored each second. Its low latency describes those few, not the system.
- **4 replicas keep up with headroom:** about 0.5 cores each, lag stays small, and every
  source is scored every second with p99 latency under 1 s.
- **2 replicas is the break-even point** for this load: each is about 1 core busy.
- **First bottleneck: the feature engine's per-event Python work** (parsing and aggregating).
  Kafka brokers stayed around 12% CPU and the detector needs 0.1 cores for 1,600 vectors/s.
  Next steps if it mattered: batch parsing, a compiled hot path (the Go/Rust stretch goal),
  or more partitions and replicas.
- **Scaling is not perfectly linear here:** more replicas compete with the generators and
  brokers for the same 8 cores.

**Fixed along the way (see `NOTES.md`):** the feature engine originally read Redis 3 times per
source, one after another, at every tick. A first probe at 20,000 events/s scored only 140 of
700 sources per second. All reads for a tick are now two pipelined round trips. Strict liveness
probes were killing busy pods under CPU saturation, which made the overload worse; they are now
lenient (5 s timeout, 6 failures).

## Autoscaling

KEDA scales the feature engine and detector on Kafka consumer-group lag (1 to 4 replicas;
topics have 6 partitions). Observed: a 2.1M-event backlog left by a test scaled both from 1 to
4 replicas without intervention.

## Chaos tests (`tests/gates/test_phase5b.py`)

Two replicas each of the feature engine and detector. Each scenario starts a 2,000 packets/s
SYN flood, injects its failure 2 s in, and times attack start to the attacker's IP appearing in
the enforcer's nftables sets. Pods are killed abruptly (`--force --grace-period=0`), so dead
consumers cannot leave their groups. Three runs, no pod restarts in any:

| Scenario | Run 1 (s) | Run 2 (s) | Run 3 (s) | Objective (`docs/slo.md`) |
|---|---|---|---|---|
| Baseline, no failure | 2.8 | 3.4 | 4.6 | 5 |
| Kill a feature-engine pod | 3.2 | 3.6 | 11.3 | 20 |
| Kill a detector pod | 3.6 | 18.2 | 10.0 | 20 |
| Restart Redis (window state lost) | 5.8 | 7.3 | 7.0 | 20 |
| Restart one of 3 Redpanda brokers | 15.3 | 16.1 | 15.5 | 20 |

**Reading it:**
- **Healthy baseline:** 3 consecutive flagged 1 s windows set a floor of about 3 s. Run 3's
  4.6 s ran while lint and unit tests shared the CPU; the 5 s objective has little slack on
  this saturated node.
- **Killed consumers:** recovery depends on whether the dead pod owned the attacker's partition.
  If it did, that partition idles until the 6 s session timeout evicts the pod and the group
  rebalances (the 18.2 s run); if not, the attack is caught at baseline speed.
- **Redis restart:** all window state is lost; the feature engine waits for Redis
  (`while_redis_down`) and the windows refill, costing a few seconds.
- **Broker restart:** the slowest case. Partition leadership and the group coordinator move to
  the surviving brokers, and clients notice within the 10 s socket timeout. Replication factor 3
  means no events are lost.

**Fixed along the way (see `NOTES.md`):** the first runs took up to 40 s and pods restarted
mid-test. Causes: the default 45 s session timeout and 60 s socket timeout (now 6 s and 10 s);
consumers crashing on transient commit errors after a coordinator moved; and the feature engine
crashing on a Redis connection reset. Each crash added a pod restart with backoff on top of the
failure being tested.
