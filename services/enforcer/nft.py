"""nftables for the enforcer: everything lives in one table, so cleanup is one delete.

Rules are elements of sets with a timeout, so the kernel removes them when they expire.
Rate limiting is per source: a dynamic meter keeps one token bucket per address.
"""

from __future__ import annotations

from shared.events import Action, Rule

TABLE = ("inet", "gatekeeper")
SETS = {Action.BLOCK: "blocked", Action.RATE_LIMIT: "ratelimited"}


def ruleset(rate_limit_pps: int) -> str:
    return f"""
table inet gatekeeper {{
    set blocked {{ type ipv4_addr; flags timeout; }}
    set ratelimited {{ type ipv4_addr; flags timeout; }}
    set rl_meter {{ type ipv4_addr; flags dynamic, timeout; timeout 10s; }}
    chain input {{
        type filter hook input priority -10; policy accept;
        ip saddr @blocked counter drop
        ip saddr @ratelimited update @rl_meter {{ ip saddr limit rate over \
{rate_limit_pps}/second burst {rate_limit_pps} packets }} counter drop
    }}
}}
"""


def add_rule_cmd(rule: Rule) -> list[str]:
    """`nft add` is idempotent: re-adding a member is a no-op, so replays are harmless."""
    return [
        "nft",
        "add",
        "element",
        *TABLE,
        SETS[rule.action],
        f"{{ {rule.ip} timeout {rule.ttl_s}s }}",
    ]
