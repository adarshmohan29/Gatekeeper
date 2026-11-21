"""Detector service: scores feature vectors from Kafka, publishes scores, logs likely attacks.

The Kafka loop runs in a background thread; FastAPI serves /healthz for readiness probes.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

from services.detector.detectors import Detector, load_detector
from shared.config import Settings
from shared.events import FeatureVector, Score
from shared.kafka import batch_values, group_consumer, producer

log = logging.getLogger("detector")
settings = Settings.from_env()
stop = threading.Event()


def run(detector: Detector) -> None:
    consumer = group_consumer(settings.kafka_bootstrap, settings.features_topic, "detector")
    out = producer(settings.kafka_bootstrap)
    try:
        while not stop.is_set():
            msgs = consumer.consume(500, timeout=0.5)
            for fv in map(FeatureVector.from_bytes, batch_values(msgs)):
                s = Score(
                    src_ip=fv.src_ip,
                    dst_ip=fv.dst_ip,
                    window_end=fv.window_end,
                    detector=detector.name,
                    score=detector.score(fv),
                )
                if s.score >= settings.score_threshold:
                    log.warning("ATTACK src=%s dst=%s score=%.2f", s.src_ip, s.dst_ip, s.score)
                out.produce(settings.scores_topic, key=s.partition_key, value=s.to_bytes())
            if msgs:
                out.flush()  # scores delivered before offsets commit: at-least-once
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()  # leave the group now, so a replacement gets the partitions at once


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=logging.INFO)
    # Load before serving: a missing or bad baseline fails startup instead of a silent thread.
    detector = load_detector(settings.detector, Path(settings.baseline_path))
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
