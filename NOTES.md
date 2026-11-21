# Notes

What broke, what was measured, what surprised me. Newest first.

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
