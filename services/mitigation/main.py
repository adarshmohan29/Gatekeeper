"""Mitigation controller: turns flagged scores into expiring enforcer rules, logged to PostgreSQL.

Idempotent under replay: alerts and actions are unique per (source, window), the enforcer's add
is idempotent, a source with an active rule is not acted on again, and stale scores are ignored.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
import urllib.request

import psycopg

from services.mitigation.policy import decide, streak
from shared.config import Settings
from shared.events import Action, Rule, Score
from shared.kafka import batch, group_consumer

log = logging.getLogger("mitigation")


def enforce(url: str, token: str, rule: Rule) -> None:
    req = urllib.request.Request(
        f"{url}/rules",
        data=rule.model_dump_json().encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=2).close()  # raises on failure: crash, replay, retry


def handle(conn: psycopg.Connection, s: Score, settings: Settings) -> None:
    span = settings.consecutive_windows + settings.escalate_after
    with conn.transaction():
        conn.execute(
            "INSERT INTO alerts (src_ip, window_end, score, detector) VALUES (%s, %s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            (s.src_ip, s.window_end, s.score, s.detector),
        )
        flagged = conn.execute(
            "SELECT window_end FROM alerts"
            " WHERE src_ip = %s AND window_end > %s AND window_end <= %s",
            (s.src_ip, s.window_end - span, s.window_end),
        ).fetchall()
        active = conn.execute(
            "SELECT action FROM actions WHERE src_ip = %s AND expires_at > now()", (s.src_ip,)
        ).fetchall()

    n = streak((w for (w,) in flagged), s.window_end)
    allowlisted = any(s.src_ip in net for net in settings.allowlist)
    action = decide(
        n,
        {Action(a) for (a,) in active},
        allowlisted,
        settings.consecutive_windows,
        settings.escalate_after,
    )
    if action is None:
        return

    # Enforce before logging: a crash in between re-decides on replay and re-applies (idempotent).
    enforce(
        settings.enforcer_url,
        settings.enforcer_token,
        Rule(ip=s.src_ip, action=action, ttl_s=settings.rule_ttl_s),
    )
    conn.execute(
        "INSERT INTO actions (src_ip, action, reason, detector, score, window_end, expires_at)"
        " VALUES (%s, %s, %s, %s, %s, %s, now() + make_interval(secs => %s))"
        " ON CONFLICT DO NOTHING",
        (
            s.src_ip,
            action,
            f"{n} consecutive flagged windows",
            s.detector,
            s.score,
            s.window_end,
            settings.rule_ttl_s,
        ),
    )
    log.warning("%s src=%s after %d flagged windows", action.upper(), s.src_ip, n)


def record_expiries(conn: psycopg.Connection) -> None:
    """The kernel removes expired rules; this records when, in the audit log."""
    for ip, action in conn.execute(
        "UPDATE actions SET expired_at = expires_at"
        " WHERE expired_at IS NULL AND expires_at <= now() RETURNING src_ip, action"
    ):
        log.info("EXPIRED %s src=%s", action, ip)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    conn = psycopg.connect(settings.postgres_dsn, autocommit=True)
    consumer = group_consumer(settings.kafka_bootstrap, settings.scores_topic, "mitigation")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        while True:
            msgs = batch(consumer.consume(500, timeout=0.5))
            now = time.time()
            for s in (Score.from_bytes(m.value) for m in msgs):
                if (
                    s.score >= settings.score_threshold
                    and now - s.window_end <= settings.max_score_age_s
                ):
                    handle(conn, s, settings)
            if msgs:
                consumer.commit(asynchronous=False)
            record_expiries(conn)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
