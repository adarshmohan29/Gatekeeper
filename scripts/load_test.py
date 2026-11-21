"""Load test on kind: throughput and detection latency at 1, 2 and 4 consumer replicas.

Requires the cluster (`make k8s-up`); reaches Prometheus through its own port-forward.
Pins the feature engine and detector at N replicas
(KEDA paused), offers a fixed load of synthetic flow events, and reads the results from
Prometheus. Prints a Markdown table for docs/scaling.md.
"""

import json
import subprocess
import time
import urllib.parse
import urllib.request

REPLICAS = (1, 2, 4)
GENERATORS, RATE_EACH, PER_SOURCE = 2, 20_000, 25  # 40,000 events/s from 1,600 clients
RUN_S, MEASURE_S, SETTLE_S = 90, 45, 20
SCALED = ("feature-engine", "detector")
GROUPS = {"feature-engine": "flow-events", "detector": "features"}
PROMETHEUS = "http://127.0.0.1:19090"
BOOTSTRAP = "redpanda-0.redpanda:9092,redpanda-1.redpanda:9092,redpanda-2.redpanda:9092"


def kubectl(*args: str) -> str:
    cmd = ["kubectl", "--context", "kind-gatekeeper", "-n", "gatekeeper", *args]
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def prom(expr: str) -> float | None:
    url = f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote(expr)}"
    with urllib.request.urlopen(url, timeout=10) as r:
        result = json.load(r)["data"]["result"]
    return float(result[0]["value"][1]) if result else None


def pin(n: int) -> None:
    """N replicas of each scaled service, starting from the end of their topics (no backlog)."""
    for name in SCALED:
        kubectl(
            "annotate", "scaledobject", name, "autoscaling.keda.sh/paused-replicas=0", "--overwrite"
        )
    for name in SCALED:
        kubectl("wait", "--for=delete", "pod", "-l", f"app={name}", "--timeout=120s")
    for group, topic in GROUPS.items():
        kubectl(
            "exec",
            "redpanda-0",
            "--",
            "rpk",
            "group",
            "seek",
            group,
            "--to",
            "end",
            "--topics",
            topic,
        )
    for name in SCALED:
        kubectl(
            "annotate",
            "scaledobject",
            name,
            f"autoscaling.keda.sh/paused-replicas={n}",
            "--overwrite",
        )
    for name in SCALED:
        kubectl("rollout", "status", f"deploy/{name}", "--timeout=180s")


def generate(i: int) -> subprocess.Popen[bytes]:
    sources = RATE_EACH // PER_SOURCE
    return subprocess.Popen(
        [
            "kubectl",
            "--context",
            "kind-gatekeeper",
            "-n",
            "gatekeeper",
            "run",
            f"bench-{i}",
            "--rm",
            "-i",
            "--restart=Never",
            "--image=gatekeeper:dev",
            "--image-pull-policy=Never",
            f"--env=KAFKA_BOOTSTRAP={BOOTSTRAP}",
            "--command",
            "--",
            "python",
            "-m",
            "services.bench.main",
            str(RATE_EACH),
            str(RUN_S),
            str(sources),
            f"--first-source={i * sources}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def measure(n: int) -> list[str]:
    pin(n)
    time.sleep(SETTLE_S)
    gens = [generate(i) for i in range(GENERATORS)]
    time.sleep(RUN_S - 5)  # measure while the load is still on
    w = f"[{MEASURE_S}s]"
    processed = prom(f"sum(rate(gatekeeper_events_counted_total{w}))") or 0
    lag = prom('max(redpanda_kafka_consumer_group_lag_sum{redpanda_group="feature-engine"})') or 0
    vectors = prom(f"sum(rate(gatekeeper_vectors_emitted_total{w}))") or 0
    buckets = f"sum by (le) (rate(gatekeeper_detection_latency_seconds_bucket{w}))"
    latency = [prom(f"histogram_quantile({q}, {buckets})") for q in (0.5, 0.95, 0.99)]
    cpu = {
        name: prom(f'sum(rate(process_cpu_seconds_total{{service="{name}"}}{w}))') or 0
        for name in SCALED
    }
    row = [
        str(n),
        f"{GENERATORS * RATE_EACH:,}",
        f"{processed:,.0f}",
        f"{lag:,.0f}",
        f"{vectors:,.0f}",
        " / ".join("n/a" if x is None else f"{x:.2f}" for x in latency),
        f"{cpu['feature-engine']:.1f}",
        f"{cpu['detector']:.1f}",
    ]
    for g in gens:
        g.wait(timeout=120)
    return row


def main() -> None:
    header = [
        "Replicas",
        "Offered events/s",
        "Processed events/s",
        "Feature-engine lag (end)",
        "Vectors scored/s",
        "Detection latency p50 / p95 / p99 (s)",
        "Feature-engine CPU (cores)",
        "Detector CPU (cores)",
    ]
    print("| " + " | ".join(header) + " |\n|" + "---|" * len(header))
    forward = subprocess.Popen(
        [
            "kubectl",
            "--context",
            "kind-gatekeeper",
            "-n",
            "gatekeeper",
            "port-forward",
            "svc/prometheus",
            "19090:9090",
        ],
        stdout=subprocess.DEVNULL,
    )
    time.sleep(3)
    kubectl("scale", "deploy/loadgen", "--replicas=0")  # not needed here; frees CPU and memory
    try:
        for n in REPLICAS:
            print("| " + " | ".join(measure(n)) + " |", flush=True)
    finally:
        for name in SCALED:
            kubectl("annotate", "scaledobject", name, "autoscaling.keda.sh/paused-replicas-")
        kubectl("scale", "deploy/loadgen", "--replicas=3")
        forward.terminate()


if __name__ == "__main__":
    main()
