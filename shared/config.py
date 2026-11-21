"""Settings from environment variables. Add fields only when a phase needs them."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    flow_topic: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            flow_topic=os.environ.get("FLOW_TOPIC", "flow-events"),
        )
