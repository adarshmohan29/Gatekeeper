"""Detector service: scores feature vectors from Kafka, publishes scores, logs likely attacks.

The Kafka loop runs in a background thread; FastAPI serves /healthz for readiness probes.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from prometheus_client import Counter, Histogram, start_http_server

from services.detector.detectors import Detector, load_detector
from shared.config import Settings
from shared.events import FeatureVector, Score
from shared.kafka import batch, group_consumer, parse_or_dead_letter, producer

log = logging.getLogger("detector")
settings = Settings.from_env()
stop = threading.Event()

SCORED = Counter("gatekeeper_windows_scored_total", "Feature vectors scored", ["detector"])
FLAGGED = Counter("gatekeeper_windows_flagged_total", "Scores above threshold", ["detector"])
SCORES = Histogram(
    "gatekeeper_score", "Attack scores", ["detector"], buckets=[0.1, 0.25, 0.5, 0.75, 0.9, 1.0]
)
LATENCY = Histogram(
    "gatekeeper_detection_latency_seconds",
    "From the end of a feature window to its score being published",
    buckets=[0.1, 0.25, 0.5, 1, 2, 5, 10],
)


def run(detector: Detector) -> None:
    consumer = group_consumer(settings.kafka_bootstrap, settings.features_topic, "detector")
    out = producer(settings.kafka_bootstrap)
    try:
        while not stop.is_set():
            msgs = batch(consumer.consume(500, timeout=0.5))
            for _, fv in parse_or_dead_letter(msgs, FeatureVector, out, settings.features_topic):
                s = Score(
                    src_ip=fv.src_ip,
                    dst_ip=fv.dst_ip,
                    window_end=fv.window_end,
                    detector=detector.name,
                    score=detector.score(fv),
                )
                SCORED.labels(detector.name).inc()
                SCORES.labels(detector.name).observe(s.score)
                if s.score >= settings.score_threshold:
                    FLAGGED.labels(detector.name).inc()
                    log.warning("ATTACK src=%s dst=%s score=%.2f", s.src_ip, s.dst_ip, s.score)
                out.produce(settings.scores_topic, key=s.partition_key, value=s.to_bytes())
                LATENCY.observe(time.time() - s.window_end)
            if msgs:
                out.flush()  # scores delivered before offsets commit: at-least-once
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()  # leave the group now, so a replacement gets the partitions at once


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=logging.INFO)
    # Load before serving: a missing or bad model fails startup instead of a silent thread.
    detector = load_detector(settings.detector, Path(settings.models_dir))
    SCORED.labels(detector.name)  # labelled series start at 0, so the first increase is visible
    FLAGGED.labels(detector.name)
    start_http_server(settings.metrics_port)
    app.state.worker = threading.Thread(target=run, args=(detector,))
    app.state.worker.start()
    yield
    stop.set()
    app.state.worker.join()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
def healthz(request: Request) -> dict[str, str]:
    if not request.app.state.worker.is_alive():
        raise HTTPException(status_code=503, detail="scoring loop stopped")
    return {"status": "ok"}
