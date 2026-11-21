"""Detection metrics from scored test rows. Pure: the harness in scripts/evaluate.py does I/O.

Delays are in windows, which are 1 s apart, from an attacker's first feature vector. The live
pipeline adds about 1 s of capture and Kafka latency on top (measured in Gates 2-3).
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any

from services.mitigation.policy import streak

SEEN = ("syn", "http", "mixed")  # attack kinds that also appear in training


def _rate(flags: Iterable[bool]) -> float | None:
    flags = list(flags)
    return sum(flags) / len(flags) if flags else None


def _median(values: Sequence[int]) -> float | None:
    return statistics.median(values) if values else None


def metrics(
    scored: Sequence[tuple[dict[str, Any], float]], threshold: float, k: int
) -> dict[str, float | None]:
    """`scored` is (row, score) pairs; `k` is the mitigation controller's consecutive windows."""
    flagged = [(r, s >= threshold) for r, s in scored]

    # Per attacking source per session: did we flag it, how fast, and how fast K-in-a-row.
    windows: dict[tuple[int, str], list[tuple[int, bool]]] = defaultdict(list)
    kind = {}
    for r, f in flagged:
        if r["role"] == "attacker":
            windows[r["session_id"], r["src_ip"]].append((r["window_end"], f))
            kind[r["session_id"], r["src_ip"]] = r["kind"]
    detected, delay, ttm = defaultdict(list), defaultdict(list), []
    for src, ws in windows.items():
        first = min(w for w, _ in ws)
        hits = sorted(w for w, f in ws if f)
        group = "seen" if kind[src] in SEEN else kind[src]
        detected[group].append(bool(hits))
        if hits:
            delay[group].append(hits[0] - first)
        mitigated = [w for w in hits if streak(hits, w) >= k]
        if mitigated:
            ttm.append(mitigated[0] - first)

    return {
        "False-positive rate, normal traffic": _rate(
            f for r, f in flagged if r["role"] == "normal"
        ),
        "False-positive rate, flash crowd": _rate(f for r, f in flagged if r["role"] == "flash"),
        "Detection rate, seen attacks (SYN/HTTP/mixed)": _rate(detected["seen"]),
        "Detection rate, unseen attack type (UDP)": _rate(detected["udp"]),
        "Detection rate, slow ramp": _rate(detected["ramp"]),
        "Median detection delay, seen attacks (s)": _median(delay["seen"]),
        "Median detection delay, UDP (s)": _median(delay["udp"]),
        "Median detection delay, slow ramp (s)": _median(delay["ramp"]),
        f"Median time to mitigation, all attacks ({k} windows in a row, s)": _median(ttm),
    }
