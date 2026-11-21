"""Detector interface and the threshold baseline."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from shared.events import FeatureVector

# Per-source features. Destination entropy drops for every source talking to an attacked
# destination, so it can't say *which* source to block; it is kept for the Phase 4 models.
SCORED_FEATURES = ("pkt_rate", "byte_rate", "syn_ack_ratio")


class Detector(Protocol):
    @property
    def name(self) -> str: ...

    def score(self, features: FeatureVector) -> float:
        """Attack probability in [0, 1]."""
        ...


@dataclass(frozen=True)
class ThresholdDetector:
    """Learns an upper bound per feature from normal traffic; 0.5 means "at the bound"."""

    bounds: dict[str, float]
    name: str = "threshold"

    @classmethod
    def fit(cls, normal: Sequence[FeatureVector], margin: float = 2.0) -> ThresholdDetector:
        def p99(values: list[float]) -> float:
            return sorted(values)[int(0.99 * (len(values) - 1))]

        # The 1.0 floor stops a feature that was always ~0 (e.g. SYN/ACK under keep-alive)
        # from getting a zero-width range that any packet would exceed.
        return cls(
            {f: margin * max(p99([getattr(fv, f) for fv in normal]), 1.0) for f in SCORED_FEATURES}
        )

    def score(self, features: FeatureVector) -> float:
        worst: float = max(getattr(features, f) / bound for f, bound in self.bounds.items())
        return min(1.0, 0.5 * worst)

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(self.bounds, indent=2) + "\n")

    @classmethod
    def load(cls, path: Path) -> ThresholdDetector:
        return cls(json.loads(path.read_text()))


def load_detector(name: str, baseline_path: Path) -> Detector:
    if name != "threshold":
        raise ValueError(f"unknown detector {name!r}")
    return ThresholdDetector.load(baseline_path)
