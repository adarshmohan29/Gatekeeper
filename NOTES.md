# Notes

What broke, what was measured, what surprised me. Newest first.

## Phase 3: Mitigate (2026-11-20)
- **Measured:** time to mitigation (attack launch to first rule) of 3.4 s, 2.3 s and 3.3 s over three runs; block (escalated) at 8.0 s, 7.6 s and 8.0 s. That's detection (~1.5 s) plus 3 consecutive windows, then 5 more for the block. Normal traffic during the attacks: 481, 488 and 515 Locust requests with 0 failures. The allowlisted tester (172.30.0.201) raised 21 alerts and got 0 actions.
- **Design:** the streak is computed from the `alerts` table (unique per source and window), so the controller holds no state and replays are naturally idempotent. Rules are nftables timeout-set members, so the kernel expires them; the controller only records `expired_at`.
- **Design:** the collector taps packets (AF_PACKET) *before* netfilter, so a blocked attacker still shows up in features and keeps being flagged. That's useful: when a rule expires mid-attack, the streak is already past the escalation point and it goes straight back to a block.
- **Security fix (review):** the enforcer API was reachable from the whole lab network, attacker included, so anyone could POST a rule and get legitimate users blocked: our mitigation turned into a DoS against them. Fix: bearer token (`ENFORCER_TOKEN`, constant-time compare; an empty token rejects everything). Gate 3 now probes it from inside the lab and gets 401. The pre-fix images let the same probe through.
- **Broke (measurement):** running Gate 3 right after Gate 2 reported time to mitigation 0.0 s, because Gate 2's block was still active. The gate now waits for any leftover rules to expire before attacking.
- **Known gaps:** if the victim container restarts, its netns (and nft rules) are lost while Postgres still lists them as active; that needs reconcile-on-startup (Phase 5b). Mitigation is per source IP only: a spoofed or distributed flood needs per-destination rate limiting.

## Phase 2: Detect (2026-11-20)
- **Measured:** detection delay (attack launch to first flagged score) of 1.3 s, 1.5 s and 1.3 s over three Gate 2 runs, two of them cold starts. This includes about 1 s of attacker container start-up. 0 false positives in 30 s of normal traffic; only the attacker was ever flagged.
- **Baseline** fitted on 177 vectors from 60 s of normal traffic: bounds pkt_rate 52.6/s, byte_rate 5216.8 B/s, syn_ack_ratio 2.0 (floor). Normal sources run at about 24 pkt/s each. The SYN flood (2000 pps) scores 1.0 immediately.
- **Design:** destination entropy drops for *every* source talking to an attacked destination, so it can't attribute an attack to a source. The baseline scores per-source features only; entropy is kept for the Phase 4 models and for distributed or flash-crowd cases.
- **Exactly-once counting (follow-up fix):** HINCRBY counters alone aren't idempotent, so a crash between the Redis write and the Kafka commit replayed the batch and double-counted it. Fix: the highest counted offset per partition (`fe:offset:{p}`) is written in the same MULTI/EXEC transaction as the counters, and replayed messages at or below it are skipped. This needs one MGET per batch, not per-event IDs. Remaining gap: a zombie replica that still believes it owns a partition after a rebalance could race the new owner. That would need WATCH on the offset key or a fencing token; noted for Phase 5b.
- **Measured (chaos smoke test):** feature engine SIGKILLed then restarted immediately; features resumed after 11 s and 12 s (the 10 s session timeout evicts the dead member, plus ~1 s). After a clean restart, ~1 s. Note: Docker doesn't apply restart policies to `docker kill`, so the test restarts by hand.
- **Gate 2 after the fix:** detection delay 2.0 s.
- **Broke:** after `docker compose restart detector`, no scores arrived for about 45 s. The killed process never left its consumer group, so the rebalance waited out the 45 s default session timeout. Fix: SIGTERM closes consumers (they leave immediately) and flushes producers; session timeout cut to 10 s for real crashes. Recovery after a restart went from about 45 s to about 1 s. This matters for the Phase 5b chaos tests.
- **Broke:** Docker's embedded DNS on `lab` returned SERVFAIL for every name after a failed `make up` (invalid compose file). Identical throwaway networks worked fine, and recreating the stack (`make down && make up`) fixed it. Couldn't reproduce afterwards; if it happens again, recreate first.
- **Broke:** compose `!reset` doesn't apply through a YAML merge (`<<:`). Fix: `networks` moved out of the shared anchor.
- **Review fixes:** the detector now loads the baseline at startup (a missing file fails startup instead of leaving a dead thread behind a 503); the training script exits clearly on zero vectors; renamed a confusing variable; added a named constant in the gate.

## Phase 1: Foundations (2026-11-20)
- Proposal updated (adds §6b on scalability and reliability). Phase 5 is split into 5a/5b/5c. Design rules (no state in memory, at-least-once, idempotent mitigation) now apply from Phase 2 on.
- Decision: per-packet events (not per-flow aggregates). Simplest to parse and lets the feature engine own all windowing. Revisit if Kafka throughput becomes the bottleneck under flood.
- Capture: raw AF_PACKET socket plus a hand-written header parser (no scapy). Fast, no dependency, unit-testable with crafted bytes.
- Broke: the host consumer logged IPv6 connection errors. `localhost` resolved to ::1, but the port is only published on 127.0.0.1. Fix: use 127.0.0.1 everywhere.
- Broke: `scripts/consume.py` printed nothing for 60 s. It used a fixed group id, and killed runs left stale members that the rebalance waited on until the session timeout. Fix: a fresh group id per run.
- Broke (intermittent): Gate 1 sometimes failed on a cold start with UNKNOWN_TOPIC_OR_PART. The topic was only auto-created on the collector's first publish, so a consumer that started earlier failed. It only showed up after a rebuild, when the collector started more slowly. `allow.auto.create.topics` on the consumer did not help (Redpanda doesn't auto-create for consumers); 8 passing runs had hidden this. Reproduced with a no-cache rebuild. Fix: a `redpanda-init` one-shot creates the topic (6 partitions) before the collector starts. Lesson: "passes N times" isn't a fix; reproduce first.

## Phase 0: Setup (2026-11-20)
- Local Python is 3.11 (no 3.12 installed), so the project targets `>=3.11`.
- Docker is not installed yet; it's needed before Phase 1.
- Flow event schema (`shared/events.py`) is per-packet for now. Revisit in Phase 1: per-packet vs per-flow aggregates.
