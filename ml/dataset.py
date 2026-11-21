"""Labelled dataset from our own generator: session schedule, labels, split, Parquet I/O.

Every attack is started by us, so labels are exact: a feature vector is an attack if and only
if its source was one of that session's attacker IPs.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from shared.events import FeatureVector

ATTACKER_IPS = [f"172.30.0.{n}" for n in range(200, 210)]
FLASH_IPS = [f"172.30.0.{n}" for n in range(210, 220)]

# Kinds never seen in training: an unseen attack type and the two hard cases.
HELD_OUT = frozenset({"udp", "flash", "ramp"})
SCHEDULE = {"syn": 8, "http": 6, "mixed": 4, "udp": 4, "flash": 3, "ramp": 3}


@dataclass(frozen=True)
class Session:
    id: int
    kind: str  # syn | http | mixed | udp | flash | ramp
    sources: tuple[str, ...]  # attacker IPs, or flash-crowd client IPs for "flash"
    duration_s: int
    pps: int  # packets/s per source (floods), or users per client (flash)
    size: int  # payload bytes per packet (hping3 -d)


def make_sessions(seed: int = 7) -> list[Session]:
    """Randomized schedule: rates, sizes, source counts and durations vary per session."""
    rng = random.Random(seed)
    kinds = [k for k, n in SCHEDULE.items() for _ in range(n)]
    rng.shuffle(kinds)
    sessions = []
    for i, kind in enumerate(kinds):
        pool = FLASH_IPS if kind == "flash" else ATTACKER_IPS
        count = 2 if kind == "mixed" else rng.randint(1, 3)
        sessions.append(
            Session(
                id=i,
                kind=kind,
                sources=tuple(rng.sample(pool, count)),
                duration_s=rng.randint(10, 20),
                pps=rng.randint(30, 60) if kind == "flash" else int(10 ** rng.uniform(2.3, 3.5)),
                size=rng.choice([0, 64, 512, 1200]),
            )
        )
    return sessions


def role(fv: FeatureVector, session: Session) -> str:
    if str(fv.src_ip) not in session.sources:
        return "normal"
    return "flash" if session.kind == "flash" else "attacker"


def row(fv: FeatureVector, session: Session) -> dict[str, Any]:
    r = role(fv, session)
    return fv.model_dump(mode="json") | {
        "session_id": session.id,
        "kind": session.kind,
        "role": r,
        "label": int(r == "attacker"),
    }


def split(
    rows: Sequence[dict[str, Any]], test_frac: float = 0.3, seed: int = 7
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split by session, never by row. Held-out kinds go to test only."""
    kinds = {r["session_id"]: r["kind"] for r in rows}
    seen = sorted(s for s, k in kinds.items() if k not in HELD_OUT)
    rng = random.Random(seed)
    test = set(rng.sample(seen, round(len(seen) * test_frac))) | {
        s for s, k in kinds.items() if k in HELD_OUT
    }
    return [r for r in rows if r["session_id"] not in test], [
        r for r in rows if r["session_id"] in test
    ]


def vector(r: dict[str, Any]) -> FeatureVector:
    return FeatureVector.model_validate({k: r[k] for k in FeatureVector.model_fields})


def save(rows: Sequence[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(list(rows)), path)


def load(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = pq.read_table(path).to_pylist()
    return rows
