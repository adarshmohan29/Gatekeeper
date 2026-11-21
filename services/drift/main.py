"""Drift monitor: how far recent *normal* traffic moved from the training data.

A count-based window (DRIFT_WINDOW vectors, a few minutes of traffic) of vectors from sources
the detector hasn't flagged recently. Attacks are left to the detector; what's left is the
question drift answers: has normal traffic changed enough that the models may be stale?
"""

from __future__ import annotations

import signal
import sys
import time
from collections import deque
from pathlib import Path

from prometheus_client import Gauge, start_http_server

from services.drift.psi import FlaggedSources, Reference, drift, load
from shared.config import Settings
from shared.events import FeatureVector, Score
from shared.kafka import batch, group_consumer, parse_or_dead_letter, producer

EVAL_S = 5
FLAG_HOLD_S = 60  # keep a flagged source out of the window this long after its last flag
DRIFT = Gauge(
    "gatekeeper_feature_drift", "One-sided PSI of a feature vs training (see psi.py)", ["feature"]
)
WINDOW = Gauge("gatekeeper_drift_window_vectors", "Vectors in the drift window")


def report(window: deque[FeatureVector], refs: dict[str, Reference]) -> None:
    WINDOW.set(len(window))
    # Only a full window means anything: right after startup it holds a few vectors from
    # clients still opening connections, which read as huge drift.
    if len(window) == window.maxlen:
        for feature, ref in refs.items():
            DRIFT.labels(feature).set(drift(ref, [getattr(fv, feature) for fv in window]))


def main() -> None:
    settings = Settings.from_env()
    refs = load(Path(settings.models_dir) / "drift_reference.json")
    features = group_consumer(settings.kafka_bootstrap, settings.features_topic, "drift")
    scores = group_consumer(settings.kafka_bootstrap, settings.scores_topic, "drift-scores")
    out = producer(settings.kafka_bootstrap)  # dead letters only
    start_http_server(settings.metrics_port)
    window: deque[FeatureVector] = deque(maxlen=settings.drift_window)
    flagged = FlaggedSources(FLAG_HOLD_S)
    next_eval = time.time() + EVAL_S
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    try:
        while True:
            now = time.time()
            if msgs := batch(scores.consume(500, timeout=0)):
                for _, s in parse_or_dead_letter(msgs, Score, out, settings.scores_topic):
                    if s.score >= settings.score_threshold:
                        flagged.flag(str(s.src_ip), now)
                scores.commit(asynchronous=False)  # keeps this group's lag honest
            if msgs := batch(features.consume(500, timeout=0.5)):
                parsed = parse_or_dead_letter(msgs, FeatureVector, out, settings.features_topic)
                window.extend(fv for _, fv in parsed if not flagged.recent(str(fv.src_ip), now))
                features.commit(asynchronous=False)
            if now >= next_eval:
                report(window, refs)
                flagged.prune(now)
                next_eval += EVAL_S
    finally:
        out.flush()
        features.close()
        scores.close()


if __name__ == "__main__":
    main()
