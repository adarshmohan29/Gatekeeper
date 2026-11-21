"""Gate 5a: the system can be observed, and its alerts fire when they should.

Requires the stack (`make up`). Prometheus is on 127.0.0.1:9090, Grafana on 127.0.0.1:3000.
"""

import json
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from shared.config import Settings
from shared.events import FeatureVector
from shared.kafka import producer, stream, tail_consumer

COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(Path(__file__).parents[2] / "infra/compose/docker-compose.yml"),
]
PROMETHEUS, GRAFANA = "http://127.0.0.1:9090", "http://127.0.0.1:3000"
ALERT_WAIT_S = 90  # scrape (5 s) + evaluation (5 s) + `for:` (15 s), with room to spare
EXPECTED_METRICS = [
    "gatekeeper_events_published_total",
    "gatekeeper_events_counted_total",
    "gatekeeper_vectors_emitted_total",
    "gatekeeper_windows_scored_total",
    "gatekeeper_detection_latency_seconds_bucket",
    "gatekeeper_mitigation_latency_seconds_bucket",
    "gatekeeper_active_rules",
    "gatekeeper_drift_window_vectors",  # drift itself is only reported once the window is full
    "redpanda_kafka_consumer_group_lag_sum",
]


def get(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)


def query(expr: str) -> dict[str, Any]:
    result: dict[str, Any] = get(f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote(expr)}")
    return result


def firing() -> set[str]:
    alerts = get(f"{PROMETHEUS}/api/v1/alerts")["data"]["alerts"]
    return {a["labels"]["alertname"] for a in alerts if a["state"] == "firing"}


def wait_until(condition: Any, what: str, timeout: float = ALERT_WAIT_S) -> float:
    start = time.monotonic()
    while not condition():
        assert time.monotonic() - start < timeout, f"timed out waiting for: {what}"
        time.sleep(2)
    return time.monotonic() - start


def test_every_service_is_scraped_and_exposes_its_metrics() -> None:
    targets = get(f"{PROMETHEUS}/api/v1/targets")["data"]["activeTargets"]
    down = [t["labels"] for t in targets if t["health"] != "up"]
    assert len(targets) == 6 and not down, down
    missing = [m for m in EXPECTED_METRICS if not query(m)["data"]["result"]]
    assert not missing, f"metrics not exposed: {missing}"


def test_dashboard_is_provisioned_and_its_queries_are_valid() -> None:
    dashboard = get(f"{GRAFANA}/api/dashboards/uid/gatekeeper")["dashboard"]
    exprs = [t["expr"] for p in dashboard["panels"] for t in p["targets"]]
    assert len(dashboard["panels"]) == 8
    bad = [e for e in exprs if query(e)["status"] != "success"]
    assert not bad, bad


def test_malformed_message_is_dead_lettered_and_consumer_keeps_running() -> None:
    settings = Settings.from_env()
    dlq = tail_consumer(settings.kafka_bootstrap, f"{settings.flow_topic}-dlq", "gate5a")
    try:
        next(stream(dlq, 3), None)  # join the group before producing
        out = producer(settings.kafka_bootstrap)
        out.produce(settings.flow_topic, key=b"gate5a", value=b"not a flow event")
        out.flush()
        dead = [m for m in stream(dlq, 20) if m.key == b"gate5a"]
    finally:
        dlq.close()
    assert dead and dead[0].value == b"not a flow event"
    wait_until(lambda: "DeadLetters" in firing(), "DeadLetters alert")
    state = subprocess.run(
        [*COMPOSE, "ps", "feature-engine", "--format", "{{.State}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert state == "running"


def test_shifted_traffic_fires_the_drift_alert() -> None:
    settings = Settings.from_env()
    sample = tail_consumer(settings.kafka_bootstrap, settings.features_topic, "gate5a")
    try:
        normal = [FeatureVector.from_bytes(m.value) for m in stream(sample, 10)]
    finally:
        sample.close()
    assert normal
    assert "FeatureDrift" not in firing(), "drift alert already firing on normal traffic"

    # 1.5x busier clients, from a documentation-only range (192.0.2.0/24) so they can't be
    # confused with real sources; still under the detector's bounds, so nobody gets blocked.
    out = producer(settings.kafka_bootstrap)
    for i in range(settings.drift_window + 100):
        fv = normal[i % len(normal)]
        shifted = fv.model_copy(
            update={
                "src_ip": f"192.0.2.{i % 250 + 1}",
                "pkt_rate": fv.pkt_rate * 1.5,
                "byte_rate": fv.byte_rate * 1.5,
            }
        )
        out.produce(settings.features_topic, key=shifted.partition_key, value=shifted.to_bytes())
    out.flush()
    took = wait_until(lambda: "FeatureDrift" in firing(), "FeatureDrift alert")
    print(f"\ndrift alert fired {took:.0f}s after the shifted traffic arrived")


def test_a_stalled_consumer_fires_the_lag_alert_and_recovers() -> None:
    subprocess.run([*COMPOSE, "stop", "feature-engine"], check=True, capture_output=True)
    try:
        took = wait_until(lambda: "ConsumerLag" in firing(), "ConsumerLag alert")
        print(f"\nlag alert fired {took:.0f}s after the feature engine stopped")
    finally:
        subprocess.run([*COMPOSE, "start", "feature-engine"], check=True, capture_output=True)
    took = wait_until(lambda: "ConsumerLag" not in firing(), "ConsumerLag alert to resolve")
    print(f"lag alert resolved {took:.0f}s after restart")
