"""Settings from environment variables. Add fields only when a phase needs them."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    flow_topic: str
    features_topic: str
    scores_topic: str
    capture_iface: str
    redis_url: str
    window_s: int
    detector: str
    baseline_path: str
    score_threshold: float

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            kafka_bootstrap=env("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            flow_topic=env("FLOW_TOPIC", "flow-events"),
            features_topic=env("FEATURES_TOPIC", "features"),
            scores_topic=env("SCORES_TOPIC", "scores"),
            capture_iface=env("CAPTURE_IFACE", "eth0"),
            redis_url=env("REDIS_URL", "redis://localhost:6379/0"),
            window_s=int(env("WINDOW_S", "10")),
            detector=env("DETECTOR", "threshold"),
            baseline_path=env("BASELINE_PATH", "models/baseline.json"),
            score_threshold=float(env("SCORE_THRESHOLD", "0.5")),
        )
