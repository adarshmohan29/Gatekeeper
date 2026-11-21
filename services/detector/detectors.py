"""Detector interface, the threshold baseline, and ONNX-served ML detectors."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import onnxruntime as ort

from shared.events import FeatureVector

# Per-source features. Destination entropy drops for every source talking to an attacked
# destination, so it can't say *which* source to block.
SCORED_FEATURES = ("pkt_rate", "byte_rate", "syn_ack_ratio")


class Detector(Protocol):
    @property
    def name(self) -> str: ...

    def score(self, features: FeatureVector) -> float:
        """Attack probability in [0, 1]."""
        ...


def p99(values: Sequence[float]) -> float:
    return sorted(values)[int(0.99 * (len(values) - 1))]


@dataclass(frozen=True)
class ThresholdDetector:
    """Learns an upper bound per feature from normal traffic; 0.5 means "at the bound"."""

    bounds: dict[str, float]
    name: str = "threshold"

    @classmethod
    def fit(cls, normal: Sequence[FeatureVector], margin: float = 2.0) -> ThresholdDetector:
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


@dataclass(frozen=True)
class OnnxDetector:
    """An exported model plus metadata saying which features it takes and how to read it.

    Classifiers output P(attack) in `column`. Autoencoders output a reconstruction error,
    scaled so that 0.5 means "at the threshold", like the baseline.
    """

    name: str
    session: ort.InferenceSession
    features: tuple[str, ...]
    output: str
    column: int | None = None
    threshold: float | None = None

    @classmethod
    def load(cls, name: str, models_dir: Path) -> OnnxDetector:
        meta: dict[str, Any] = json.loads((models_dir / f"{name}.json").read_text())
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 1  # one core per replica; scale out with replicas instead
        session = ort.InferenceSession(str(models_dir / f"{name}.onnx"), opts)
        return cls(
            name,
            session,
            tuple(meta["features"]),
            meta["output"],
            meta.get("column"),
            meta.get("threshold"),
        )

    def score(self, features: FeatureVector) -> float:
        x = np.array([[getattr(features, f) for f in self.features]], dtype=np.float32)
        (out,) = self.session.run([self.output], {"input": x})
        if self.threshold is None:
            return float(out[0][self.column])
        return min(1.0, 0.5 * float(out[0]) / self.threshold)


def load_detector(name: str, models_dir: Path) -> Detector:
    if name == "threshold":
        return ThresholdDetector.load(models_dir / "baseline.json")
    if (models_dir / f"{name}.onnx").exists():
        return OnnxDetector.load(name, models_dir)
    raise ValueError(f"unknown detector {name!r}: no {name}.onnx in {models_dir}")
