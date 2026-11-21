"""Run the session schedule against the live stack and record labelled feature vectors.

Mitigation is paused meanwhile, so our own blocking can't distort the traffic we record.
Takes about 20 minutes. Output: data/dataset.parquet.
"""

import subprocess
from pathlib import Path

from ml.dataset import Session, make_sessions, row, save
from shared.config import Settings
from shared.events import FeatureVector
from shared.kafka import stream, tail_consumer

ROOT = Path(__file__).parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "infra/compose/docker-compose.yml")]
LEAD_IN_S, TAIL_S = 8, 4  # normal traffic before and after each event


def launch(s: Session, ip: str, kind: str) -> subprocess.Popen[bytes]:
    net = ["docker", "run", "--rm", "--network", "gatekeeper_lab", "--ip", ip]
    if kind == "flash":  # legitimate clients, just many of them at once
        locustfile = ROOT / "services/loadgen/locustfile.py"
        cmd = [
            *net,
            "-v",
            f"{locustfile}:/mnt/locustfile.py:ro",
            "locustio/locust:2.32.0",
            "-f",
            "/mnt/locustfile.py",
            "--headless",
            "-u",
            str(s.pps),
            "-r",
            str(s.pps),
            "-t",
            f"{s.duration_s}s",
            "--host",
            "http://victim",
        ]
    else:
        cmd = [
            *net,
            "--cap-add",
            "NET_RAW",
            "gatekeeper-attacker",
            kind,
            str(s.duration_s),
            str(s.pps),
            str(s.size),
        ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run(s: Session, settings: Settings) -> list[dict[str, object]]:
    consumer = tail_consumer(settings.kafka_bootstrap, settings.features_topic, f"dataset-{s.id}")
    try:
        rows = [row(FeatureVector.from_bytes(m.value), s) for m in stream(consumer, LEAD_IN_S)]
        kinds = ["syn", "http"] if s.kind == "mixed" else [s.kind] * len(s.sources)
        procs = [launch(s, ip, k) for ip, k in zip(s.sources, kinds, strict=True)]
        rows += [
            row(FeatureVector.from_bytes(m.value), s)
            for m in stream(consumer, s.duration_s + TAIL_S)
        ]
        for p in procs:
            p.wait(timeout=60)
    finally:
        consumer.close()
    return rows


def main() -> None:
    settings = Settings.from_env()
    subprocess.run([*COMPOSE, "stop", "mitigation"], check=True, capture_output=True)
    rows: list[dict[str, object]] = []
    try:
        for s in make_sessions():
            rows += run(s, settings)
            attack = sum(r["label"] == 1 for r in rows if r["session_id"] == s.id)
            print(
                f"session {s.id:2d} {s.kind:5s} sources={len(s.sources)} pps={s.pps:4d}"
                f" size={s.size:4d} -> {attack} attack vectors, {len(rows)} total",
                flush=True,
            )
    finally:
        subprocess.run([*COMPOSE, "start", "mitigation"], check=True, capture_output=True)
    save(rows, ROOT / "data/dataset.parquet")


if __name__ == "__main__":
    main()
