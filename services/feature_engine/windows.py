"""Sliding-window features kept in Redis.

State is per-second buckets (hashes with a TTL), so any number of feature-engine
replicas can share it, and per-destination aggregates see every partition:
  s:{src}:{sec}       p, b, syn, ack (packets, bytes, SYN, ACK) and to:{dst} (packets per dst)
  d:{dst}:{sec}:src   packets per source
  d:{dst}:{sec}:port  packets per destination port
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable

from redis import Redis

from shared.events import FeatureVector, FlowEvent, TcpFlags

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


def write(r: Redis, inc: Increments, ttl_s: int) -> None:
    pipe = r.pipeline(transaction=False)
    for key, fields in inc.items():
        for field, n in fields.items():
            pipe.hincrby(key, field, n)
        pipe.expire(key, ttl_s)
    pipe.execute()


def entropy(counts: Iterable[int]) -> float:
    """Shannon entropy, in bits, of the distribution given by `counts`."""
    total = sum(counts := list(counts))
    return sum(c / total * math.log2(total / c) for c in counts if c) if total else 0.0


def _merged(r: Redis, keys: list[str]) -> Counter[str]:
    pipe = r.pipeline(transaction=False)
    for key in keys:
        pipe.hgetall(key)
    total: Counter[str] = Counter()
    for fields in pipe.execute():
        total.update({f: int(n) for f, n in fields.items()})
    return total


def read_features(r: Redis, src: str, window_end: int, window_s: int) -> FeatureVector | None:
    """Features of `src` over [window_end - window_s, window_end), or None if it sent nothing."""
    secs = range(window_end - window_s, window_end)
    s = _merged(r, [f"s:{src}:{sec}" for sec in secs])
    if not s["p"]:
        return None
    dst = max((f for f in s if f.startswith("to:")), key=s.__getitem__).removeprefix("to:")
    srcs = _merged(r, [f"d:{dst}:{sec}:src" for sec in secs])
    ports = _merged(r, [f"d:{dst}:{sec}:port" for sec in secs])
    return FeatureVector(
        src_ip=src,
        dst_ip=dst,
        window_end=window_end,
        pkt_rate=s["p"] / window_s,
        byte_rate=s["b"] / window_s,
        syn_ack_ratio=s["syn"] / (s["ack"] + 1),
        dst_distinct_srcs=len(srcs),
        dst_src_entropy=entropy(srcs.values()),
        dst_port_entropy=entropy(ports.values()),
    )
