"""Settings from environment variables. Add fields only when a phase needs them."""

from __future__ import annotations

import os
from dataclasses import dataclass
from ipaddress import IPv4Network


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
    postgres_dsn: str
    enforcer_url: str
    enforcer_token: str  # shared secret: only the controller may change rules
    allowlist: tuple[IPv4Network, ...]
    consecutive_windows: int  # flagged windows in a row before acting
    escalate_after: int  # further flagged windows before rate-limit becomes a block
    rule_ttl_s: int
    rate_limit_pps: int
    max_score_age_s: int  # older scores are replays of the past: never act on them

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
            postgres_dsn=env(
                "POSTGRES_DSN", "postgresql://gatekeeper:gatekeeper@localhost/gatekeeper"
            ),
            enforcer_url=env("ENFORCER_URL", "http://victim:9000"),
            enforcer_token=env("ENFORCER_TOKEN", ""),
            allowlist=tuple(IPv4Network(n) for n in env("ALLOWLIST", "").split(",") if n),
            consecutive_windows=int(env("CONSECUTIVE_WINDOWS", "3")),
            escalate_after=int(env("ESCALATE_AFTER", "5")),
            rule_ttl_s=int(env("RULE_TTL_S", "60")),
            rate_limit_pps=int(env("RATE_LIMIT_PPS", "100")),
            max_score_age_s=int(env("MAX_SCORE_AGE_S", "30")),
        )
