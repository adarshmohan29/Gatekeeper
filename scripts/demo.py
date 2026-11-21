"""The demo: with the stack up and normal traffic flowing, launch a SYN flood and narrate how
Gatekeeper detects it, rate-limits it, then blocks it, while legitimate clients keep working.

Run with `make demo` (it starts the stack first). Exits non-zero unless the attacker is blocked
and normal traffic stays healthy, so Gate 5c can run it as is.
"""

import time

from scripts.lab import ATTACKER, flood, locust_totals, nft_state
from shared.config import Settings
from shared.kafka import stream, tail_consumer

ATTACK_S = 30
READY_TIMEOUT_S = 180
MAX_FAILURE_RATE = 0.01


def say(start: float, text: str) -> None:
    print(f"[{time.monotonic() - start:5.1f}s] {text}", flush=True)


def wait_until_ready(start: float) -> None:
    """Normal traffic is being served and scored, and no rule is left from a previous run."""
    settings = Settings.from_env()
    scores = tail_consumer(settings.kafka_bootstrap, settings.scores_topic, "demo")
    try:
        while not any(stream(scores, 1.0)) or locust_totals()[0] == 0:
            assert time.monotonic() - start < READY_TIMEOUT_S, "pipeline not ready"
    finally:
        scores.close()
    while ATTACKER in (sets := nft_state()[0])["blocked"] | sets["ratelimited"]:
        time.sleep(1)  # a rerun within the rule TTL: wait for the old rule to expire
    say(start, "pipeline ready: normal clients are browsing the victim and being scored")


def watch(start: float) -> tuple[float | None, float | None]:
    """Run the flood, report each mitigation step as it happens. Returns their times."""
    limited_s = blocked_s = None
    dropped_before = nft_state()[1]  # nftables counters are cumulative
    attack = flood(ATTACKER, ATTACK_S)
    launched = time.monotonic()
    say(start, f"attacker {ATTACKER} starts a SYN flood for {ATTACK_S} s")
    while attack.poll() is None:
        sets, _ = nft_state()
        elapsed = time.monotonic() - launched
        if limited_s is None and ATTACKER in sets["ratelimited"] | sets["blocked"]:
            limited_s = elapsed
            say(start, f"detected: attacker rate-limited {elapsed:.1f} s into the attack")
        if blocked_s is None and ATTACKER in sets["blocked"]:
            blocked_s = elapsed
            say(start, f"still attacking: blocked {elapsed:.1f} s into the attack")
        time.sleep(0.5)
    dropped = nft_state()[1] - dropped_before
    say(start, f"flood over: nftables dropped {dropped:,} attack packets")
    return limited_s, blocked_s


def main() -> None:
    start = time.monotonic()
    wait_until_ready(start)
    reqs_before, fails_before = locust_totals()
    limited_s, blocked_s = watch(start)
    reqs, fails = (a - b for a, b in zip(locust_totals(), (reqs_before, fails_before), strict=True))
    say(start, f"normal traffic during the attack: {reqs} requests, {fails} failed")
    print("Dashboard: http://localhost:3000 (Gatekeeper)")
    healthy = reqs > 0 and fails / reqs < MAX_FAILURE_RATE
    if limited_s is None or blocked_s is None or not healthy:
        raise SystemExit("FAIL: attacker not blocked, or normal traffic was hurt")
    print(f"PASS: attacker blocked after {blocked_s:.1f} s; normal clients unaffected")


if __name__ == "__main__":
    main()
