"""Gate 5b: on Kubernetes, with 2 replicas, attacks are blocked within the SLO, even while
parts of the system fail. Also the chaos tests behind docs/scaling.md.

Requires the kind cluster (`make k8s-up`). Each scenario starts a real SYN flood, injects
its failure 2 s in, and times attack start -> the attacker's first rule in the enforcer.
"""

import json
import subprocess
import time
from collections.abc import Callable

import pytest

SLO_S = 15  # docs/slo.md: rate-limited within 15 s, including one consumer failing abruptly
FAULT_AFTER_S = 2
ATTACK_S = 40
REPLICAS = 2
SCALED = ("feature-engine", "detector")
RULE_TTL_S = 15


def k(*args: str) -> str:
    cmd = ["kubectl", "--context", "kind-gatekeeper", "-n", "gatekeeper", *args]
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def ruled_ips() -> set[str]:
    listing = json.loads(
        k(
            "exec",
            "deploy/victim",
            "-c",
            "enforcer",
            "--",
            "nft",
            "-j",
            "list",
            "table",
            "inet",
            "gatekeeper",
        )
    )
    return {
        e["elem"]["val"]
        for o in listing["nftables"]
        if "set" in o and o["set"]["name"] in ("blocked", "ratelimited")
        for e in o["set"].get("elem", [])
    }


def kill_one(app: str) -> None:
    """Abrupt: no SIGTERM, so the consumer can't leave its group; Kafka must time it out."""
    pod = k("get", "pod", "-l", f"app={app}", "-o", "jsonpath={.items[0].metadata.name}")
    k("delete", "pod", pod, "--grace-period=0", "--force")


def restart_broker() -> None:
    """Abrupt; topics are replicated 3 ways, so the other two brokers carry on."""
    k("delete", "pod", "redpanda-1", "--grace-period=0", "--force")


SCENARIOS: dict[str, Callable[[], None]] = {
    "baseline": lambda: None,
    "kill a feature-engine pod": lambda: kill_one("feature-engine"),
    "kill a detector pod": lambda: kill_one("detector"),
    "restart Redis (window state lost)": lambda: kill_one("redis"),
    "restart a Redpanda broker": restart_broker,
}


@pytest.fixture(scope="module", autouse=True)
def two_replicas() -> object:
    for name in SCALED:
        k(
            "annotate",
            "scaledobject",
            name,
            f"autoscaling.keda.sh/paused-replicas={REPLICAS}",
            "--overwrite",
        )
    for name in SCALED:
        k("rollout", "status", f"deploy/{name}", "--timeout=180s")
    yield
    for name in SCALED:
        k("annotate", "scaledobject", name, "autoscaling.keda.sh/paused-replicas-")


def wait_for(condition: Callable[[], bool], what: str, timeout: float) -> float:
    start = time.monotonic()
    while not condition():
        assert time.monotonic() - start < timeout, f"timed out: {what}"
        time.sleep(0.5)
    return time.monotonic() - start


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_attack_blocked_within_slo(scenario: str) -> None:
    wait_for(lambda: not ruled_ips(), "rules from the previous attack to expire", RULE_TTL_S + 20)
    for name in ("redis", "redpanda", *SCALED):  # previous scenario's failure fully healed
        k("wait", "--for=condition=ready", "pod", "-l", f"app={name}", "--timeout=180s")

    name = f"attacker-{int(time.time())}"
    k(
        "run",
        name,
        "--restart=Never",
        "--labels=role=attacker",
        "--image=gatekeeper-attacker:dev",
        "--image-pull-policy=Never",
        "--",
        "syn",
        str(ATTACK_S),
        "2000",
    )
    try:
        k("wait", "--for=condition=ready", f"pod/{name}", "--timeout=60s")
        attacker = k("get", "pod", name, "-o", "jsonpath={.status.podIP}")
        start = time.monotonic()
        time.sleep(FAULT_AFTER_S)
        SCENARIOS[scenario]()
        wait_for(lambda: attacker in ruled_ips(), "a rule on the attacker", 60)
        took = time.monotonic() - start
    finally:
        k("delete", "pod", name, "--grace-period=0", "--force", "--ignore-not-found")
    print(f"\n{scenario}: attacker rate-limited {took:.1f}s after the attack started")
    assert took <= SLO_S
