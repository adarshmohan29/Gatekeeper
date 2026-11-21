"""Sliding-window features kept in Redis.

State is per-second buckets (hashes with a TTL), so any number of feature-engine
replicas can share it, and per-destination aggregates see every partition:
  s:{src}:{sec}       p, b, syn, ack (packets, bytes, SYN, ACK) and to:{dst} (packets per dst)
  d:{dst}:{sec}:src   packets per source
  d:{dst}:{sec}:port  packets per destination port
  fe:offset:{part}    last flow-events offset counted, per partition (no TTL)

Exactly-once counting: a batch's counters and its offsets are written in one transaction,
and a replayed message (crash after the Redis write, before the Kafka commit) is skipped.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence

from redis import Redis

from shared.events import FeatureVector, FlowEvent, TcpFlags
from shared.kafka import Delivered

Increments = dict[str, Counter[str]]


def aggregate(events: Iterable[FlowEvent]) -> Increments:
    """Sum a batch into per-key counter increments, so a batch costs one Redis round trip."""
    inc: Increments = defaultdict(Counter)
    for e in events:
        sec = int(e.ts)
        s = inc[f"s:{e.src_ip}:{sec}"]
        s["p"] += 1
        s["b"] += e.pkt_len
        s["syn"] += e.has_flag(TcpFlags.SYN)
        s["ack"] += e.has_flag(TcpFlags.ACK)
        s[f"to:{e.dst_ip}"] += 1
        inc[f"d:{e.dst_ip}:{sec}:src"][str(e.src_ip)] += 1
        inc[f"d:{e.dst_ip}:{sec}:port"][str(e.dst_port)] += 1
    return inc


def _offset_key(partition: int) -> str:
    return f"fe:offset:{partition}"


Parsed = Sequence[tuple[Delivered, FlowEvent]]


def apply(r: Redis, batch: Parsed, ttl_s: int) -> list[FlowEvent]:
    """Count a batch of flow events exactly once. Returns the events that were new."""
    if not batch:
        return []
    fresh = _unapplied(r, batch)
    if fresh:
        _write(r, aggregate(e for _, e in fresh), ttl_s, [m for m, _ in fresh])
    return [e for _, e in fresh]


def _unapplied(r: Redis, batch: Parsed) -> list[tuple[Delivered, FlowEvent]]:
    """Drop messages whose offset was already counted (replays after a crash)."""
    parts = sorted({m.partition for m, _ in batch})
    stored = r.mget([_offset_key(p) for p in parts])
    done = {p: int(o) for p, o in zip(parts, stored, strict=True) if o is not None}
    return [(m, e) for m, e in batch if m.offset > done.get(m.partition, -1)]


def _write(r: Redis, inc: Increments, ttl_s: int, msgs: Sequence[Delivered]) -> None:
    """Apply counter increments and record `msgs`' offsets, atomically (MULTI/EXEC)."""
    pipe = r.pipeline(transaction=True)
    for key, fields in inc.items():
        for field, n in fields.items():
            pipe.hincrby(key, field, n)
        pipe.expire(key, ttl_s)
    # A partition's messages arrive in offset order, so the last one seen is its highest.
    for partition, offset in {m.partition: m.offset for m in msgs}.items():
        pipe.set(_offset_key(partition), offset)
    pipe.execute()


def entropy(counts: Iterable[int]) -> float:
    """Shannon entropy, in bits, of the distribution given by `counts`."""
    total = sum(counts := list(counts))
    return sum(c / total * math.log2(total / c) for c in counts if c) if total else 0.0


def _merged_groups(r: Redis, groups: list[list[str]]) -> list[Counter[str]]:
    """Sum the hashes in each group of keys, for all groups in a single round trip."""
    pipe = r.pipeline(transaction=False)
    for keys in groups:
        for key in keys:
            pipe.hgetall(key)
    results = iter(pipe.execute())
    merged = []
    for keys in groups:
        total: Counter[str] = Counter()
        for _ in keys:
            total.update({f: int(n) for f, n in next(results).items()})
        merged.append(total)
    return merged


def read_features(
    r: Redis, srcs: Sequence[str], window_end: int, window_s: int
) -> list[FeatureVector]:
    """Features of each source over [window_end - window_s, window_end); silent ones are skipped.

    Two Redis round trips per tick however many sources there are: one for every source's
    buckets, one for their destinations' (usually a single victim, read once, not per source).
    """
    secs = range(window_end - window_s, window_end)
    per_src = _merged_groups(r, [[f"s:{src}:{sec}" for sec in secs] for src in srcs])
    active = {src: c for src, c in zip(srcs, per_src, strict=True) if c["p"]}
    dst_of = {
        src: max((f for f in c if f.startswith("to:")), key=c.__getitem__).removeprefix("to:")
        for src, c in active.items()
    }
    dsts = sorted(set(dst_of.values()))
    per_dst = _merged_groups(
        r, [[f"d:{d}:{sec}:{kind}" for sec in secs] for d in dsts for kind in ("src", "port")]
    )
    by_dst = {d: (per_dst[2 * i], per_dst[2 * i + 1]) for i, d in enumerate(dsts)}
    vectors = []
    for src, c in active.items():
        senders, ports = by_dst[dst_of[src]]
        vectors.append(
            FeatureVector(
                src_ip=src,
                dst_ip=dst_of[src],
                window_end=window_end,
                pkt_rate=c["p"] / window_s,
                byte_rate=c["b"] / window_s,
                syn_ack_ratio=c["syn"] / (c["ack"] + 1),
                dst_distinct_srcs=len(senders),
                dst_src_entropy=entropy(senders.values()),
                dst_port_entropy=entropy(ports.values()),
            )
        )
    return vectors
