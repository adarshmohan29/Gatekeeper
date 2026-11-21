# Service level objective

**An attacking source is rate-limited within 5 s of its attack starting while the system is
healthy, and within 20 s while one component is failing** (a feature-engine or detector pod
killed, Redis restarted, or one Redpanda broker restarted).

Measured from the attack's first packet to the attacker's IP appearing in the enforcer's
nftables sets, by `tests/gates/test_phase5b.py` on the kind cluster (2 replicas of the feature
engine and detector, a 2,000 packets/s SYN flood, the failure injected 2 s in). The table of
runs is in `docs/scaling.md`.

## Why these numbers

**Healthy, 5 s (measured 2.8-3.4 s).** Features are 10 s sliding windows, re-evaluated every
second, and mitigation acts after 3 consecutive flagged windows (`CONSECUTIVE_WINDOWS`). That
puts the floor at about 3 s. The remaining margin covers the pipeline (collector, two Kafka
hops, inference, the enforcer call), which takes well under a second.

**One failure, 20 s (worst measured 18.2 s).** The slow cases are not crashes; they are Kafka
handing work to the survivors:
- *A consumer pod dies abruptly:* it can't leave its group, so the broker waits for the session
  timeout (6 s) before reassigning its partitions, and then the group rebalances. Depending on
  when the pod died, this ranged from almost no delay to about 15 s.
- *A broker restarts:* partition leadership and the group coordinator move to the other two
  brokers, and clients find out within the 10 s socket timeout (measured 15-16 s).
- *Redis restarts:* window state is lost and the feature engine retries until Redis answers
  (`while_redis_down`), so the windows refill from scratch (measured 6-7 s).

**Why not tune it lower.** The 6 s session timeout is already the broker's minimum. A shorter
socket timeout would cut off slow but live brokers on this shared, often CPU-saturated node,
and every false failover is another rebalance that itself stalls detection. The 20 s objective
is set from what these settings deliver, with headroom.

## Trade-offs

**Window size (10 s).** A longer window smooths out legitimate bursts (fewer false positives)
but dilutes a new attack: the flood's rate is averaged with the quiet seconds before it, so it
takes longer to cross the threshold. A shorter window reacts faster and is noisier. Time to
detect is roughly *seconds for the averaged rate to cross the threshold* plus
*`CONSECUTIVE_WINDOWS` ticks*; raising `CONSECUTIVE_WINDOWS` trades latency for fewer
false blocks one second at a time.

**Hot partitions.** Flow events are keyed by source IP, so all of one source's events land on
one partition and one consumer, in order. The flip side: a single very loud source can saturate one feature-engine replica while others idle; adding
replicas does not help. Mitigations in place: the feature engine aggregates each batch (one Redis
round trip per batch, not per event), and the collector drops and counts events when its
producer queue is full rather than queueing without bound. A distributed attack (many sources) spreads evenly and is the case autoscaling handles.
