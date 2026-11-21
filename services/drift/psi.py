"""Drift score: how much live traffic lands where the training data was rare.

A one-sided PSI (population stability index) over log-scale bins:
- Log bins, one per factor of sqrt(2): rates are heavy-tailed, and normal traffic is so steady
  that quantile bins end up a few percent wide, so harmless jitter read as a large shift.
- One-sided: only bins where live traffic has *more* mass than training count. Models go stale
  when traffic moves somewhere training never covered; live traffic that is narrower than
  training (quiet hours) is not drift, but standard PSI scores it as one.
- Probabilities below 1/n are treated as 1/n: n training values can't resolve rarer events.
Rule of thumb as for PSI: below 0.1 stable, above 0.25 a significant shift.
"""

from __future__ import annotations

import bisect
import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path

EDGES = [2.0 ** (k / 2) for k in range(-14, 42)]  # bins from ~0.008 to ~1e6


def _proportions(values: Sequence[float]) -> list[float]:
    counts = [0] * (len(EDGES) + 1)
    for v in values:
        counts[bisect.bisect_left(EDGES, v)] += 1
    return [c / len(values) for c in counts]


@dataclass(frozen=True)
class Reference:
    """A feature's training distribution over EDGES, and how many values it came from."""

    proportions: list[float]
    n: int

    @classmethod
    def fit(cls, values: Sequence[float]) -> Reference:
        return cls(_proportions(values), len(values))


def drift(ref: Reference, values: Sequence[float]) -> float:
    floor = 1 / ref.n
    return sum(
        (q - p) * math.log(q / max(p, floor))
        for q, p in zip(_proportions(values), ref.proportions, strict=True)
        if q > max(p, floor)
    )


def save(refs: dict[str, Reference], path: Path) -> None:
    path.write_text(json.dumps({f: asdict(r) for f, r in refs.items()}) + "\n")


def load(path: Path) -> dict[str, Reference]:
    return {f: Reference(**r) for f, r in json.loads(path.read_text()).items()}


@dataclass
class FlaggedSources:
    """Sources the detector flagged recently. Drift is about *normal* traffic changing (models
    going stale); attacks are the detector's job, so their vectors stay out of the window."""

    hold_s: float
    last: dict[str, float] = field(default_factory=dict)

    def flag(self, src: str, now: float) -> None:
        self.last[src] = now

    def recent(self, src: str, now: float) -> bool:
        return now - self.last.get(src, -math.inf) < self.hold_s

    def prune(self, now: float) -> None:
        self.last = {s: t for s, t in self.last.items() if now - t < self.hold_s}
