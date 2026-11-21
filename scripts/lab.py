"""Drive the Compose lab from the host: floods, nftables state, normal-traffic counts.

Shared by the Gate 3 test and the demo. Attacks only ever run on the isolated lab network.
"""

import json
import re
import subprocess
from pathlib import Path

COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(Path(__file__).parents[1] / "infra/compose/docker-compose.yml"),
]
ATTACKER = "172.30.0.200"


def sh(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def nft_state() -> tuple[dict[str, set[str]], int]:
    """(set name -> member IPs, packets dropped so far) from the enforcer's nftables table."""
    listing = sh(
        *COMPOSE, "exec", "-T", "enforcer", "nft", "-j", "list", "table", "inet", "gatekeeper"
    )
    objs = json.loads(listing)["nftables"]
    sets = {
        o["set"]["name"]: {e["elem"]["val"] for e in o["set"].get("elem", [])}
        for o in objs
        if "set" in o
    }
    dropped = sum(
        x["counter"]["packets"]
        for o in objs
        if "rule" in o
        for x in o["rule"]["expr"]
        if "counter" in x
    )
    return sets, dropped


def loadgen() -> list[str]:
    return sh(*COMPOSE, "ps", "-q", "loadgen").split()


def locust_totals() -> tuple[int, int]:
    """(requests, failures) summed over the Locust replicas, from their periodic stats output.

    A replica that hasn't printed stats yet counts as zero.
    """
    reqs = fails = 0
    for cid in loadgen():
        logs = subprocess.run(
            ["docker", "logs", "--tail", "40", cid], capture_output=True, text=True, check=True
        )
        stats = re.findall(r"Aggregated\s+(\d+)\s+(\d+)\(", logs.stdout + logs.stderr)
        if stats:
            reqs, fails = reqs + int(stats[-1][0]), fails + int(stats[-1][1])
    return reqs, fails


def flood(ip: str, seconds: int) -> subprocess.Popen[bytes]:
    """A SYN flood from `ip` on the lab network against the victim."""
    return subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "gatekeeper_lab",
            "--ip",
            ip,
            "--cap-add",
            "NET_RAW",
            "gatekeeper-attacker",
            "syn",
            str(seconds),
        ],
        stdout=subprocess.DEVNULL,
    )
