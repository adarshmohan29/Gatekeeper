"""Gate 3: a flood is blocked automatically; normal and allowlisted traffic is untouched.

Requires the stack (`make up`) and a fitted baseline. Launches two floods itself: the attacker
(172.30.0.200) and an allowlisted load tester (172.30.0.201, ALLOWLIST in docker-compose.yml).
"""

import time

from scripts.lab import ATTACKER, COMPOSE, flood, loadgen, locust_totals, nft_state, sh
from shared.config import Settings
from shared.events import Score
from shared.kafka import Delivered, producer, stream, tail_consumer

ALLOWLISTED = "172.30.0.201"
ATTACK_S = 20
RULE_TTL_S = 15  # RULE_TTL_S in docker-compose.yml
MAX_TIME_TO_MITIGATION_S = 10
MAX_FAILURE_RATE = 0.01


def sql(query: str) -> list[str]:
    out = sh(*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "gatekeeper", "-tA", "-c", query)
    return out.splitlines()


def watch_attack(
    settings: Settings, protected: set[str]
) -> tuple[float | None, float | None, list[Delivered]]:
    """Run both floods. Returns (first rule at, blocked at, attacker's flagged score messages).

    Fails at once if any protected source ever gets a rule.
    """
    scores = tail_consumer(settings.kafka_bootstrap, settings.scores_topic, "gate3")
    flagged: list[Delivered] = []
    first_rule_s = blocked_s = None
    floods = [flood(ATTACKER, ATTACK_S), flood(ALLOWLISTED, ATTACK_S)]
    start = time.monotonic()
    try:
        while (elapsed := time.monotonic() - start) < ATTACK_S:
            for m in stream(scores, 0.5):
                s = Score.from_bytes(m.value)
                if str(s.src_ip) == ATTACKER and s.score >= settings.score_threshold:
                    flagged.append(m)
            sets, _ = nft_state()
            ruled = sets["blocked"] | sets["ratelimited"]
            assert not ruled & protected, f"rule on a protected source: {ruled & protected}"
            if ATTACKER in ruled and first_rule_s is None:
                first_rule_s = elapsed
            if ATTACKER in sets["blocked"] and blocked_s is None:
                blocked_s = elapsed
    finally:
        for f in floods:
            f.wait(timeout=ATTACK_S + 30)
        scores.close()
    return first_rule_s, blocked_s, flagged


def rules_expired_and_recorded() -> bool:
    sets, _ = nft_state()
    unrecorded = sql("SELECT count(*) FROM actions WHERE expired_at IS NULL")
    return not sets["blocked"] | sets["ratelimited"] and unrecorded == ["0"]


def wait_for_no_rules() -> None:
    deadline = time.monotonic() + RULE_TTL_S + 15
    while not rules_expired_and_recorded():
        assert time.monotonic() < deadline, "rules did not expire, or expiry was not recorded"
        time.sleep(1)


def test_flood_blocked_normal_untouched() -> None:
    settings = Settings.from_env()
    wait_for_no_rules()  # rules left by an earlier attack (e.g. Gate 2) would fake the timing
    protected = {ALLOWLISTED} | set(
        sh(
            "docker",
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            *loadgen(),
        ).split()
    )
    reqs_before, fails_before = locust_totals()
    (since,) = sql("SELECT now()")  # judge only what this run caused, not earlier history

    first_rule_s, blocked_s, attacker_flags = watch_attack(settings, protected)

    assert first_rule_s is not None and first_rule_s <= MAX_TIME_TO_MITIGATION_S, first_rule_s
    assert blocked_s is not None, "rate-limit never escalated to a block"
    print(
        f"\ntime to mitigation: first rule {first_rule_s:.1f}s, block {blocked_s:.1f}s after launch"
    )
    assert nft_state()[1] > 0, "rules matched no packets"

    reqs, fails = (a - b for a, b in zip(locust_totals(), (reqs_before, fails_before), strict=True))
    print(f"normal traffic during the attack: {reqs} requests, {fails} failures")
    assert reqs > 0 and fails / reqs < MAX_FAILURE_RATE

    # Idempotency: replaying the attacker's flagged scores must not create actions.
    actions = sql("SELECT count(*) FROM actions")
    out = producer(settings.kafka_bootstrap)
    for m in attacker_flags[-5:]:
        out.produce(settings.scores_topic, key=m.key, value=m.value)
    out.flush()
    time.sleep(3)
    assert sql("SELECT count(*) FROM actions") == actions, "replayed scores created actions"

    by_source = sql(
        f"SELECT host(src_ip) || ' ' || action FROM actions WHERE ts > '{since}' ORDER BY id"
    )
    assert f"{ATTACKER} rate_limit" in by_source and f"{ATTACKER} block" in by_source
    assert all(a.startswith(f"{ATTACKER} ") for a in by_source), by_source
    allowlisted_alerts = sql(
        f"SELECT count(*) FROM alerts WHERE src_ip = '{ALLOWLISTED}' AND ts > '{since}'"
    )
    assert int(allowlisted_alerts[0]) > 0, "the allowlisted flood should still be detected"

    # Expiry: the kernel drops the rules and the controller records it.
    wait_for_no_rules()


def test_enforcer_rejects_requests_without_the_token() -> None:
    # From inside the lab network, like an attacker would: a rule without the token is refused.
    probe = (
        "import urllib.request as u, urllib.error as e\n"
        "r = u.Request('http://victim:9000/rules', method='POST', "
        'data=b\'{"ip":"172.30.0.5","action":"block","ttl_s":60}\', '
        "headers={'Content-Type': 'application/json'})\n"
        "try: u.urlopen(r)\n"
        "except e.HTTPError as err: print(err.code)\n"
    )
    out = sh(*COMPOSE, "run", "--rm", "--no-deps", "-T", "mitigation", "python", "-c", probe)
    assert out.strip() == "401"
