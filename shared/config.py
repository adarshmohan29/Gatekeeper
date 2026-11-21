"""Settings from environment variables. Add fields only when a phase needs them."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    flow_topic: str
    capture_iface: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            flow_topic=os.environ.get("FLOW_TOPIC", "flow-events"),
            capture_iface=os.environ.get("CAPTURE_IFACE", "eth0"),
        )
