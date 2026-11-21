from ipaddress import IPv4Address

from services.enforcer.nft import add_rule_cmd, ruleset
from shared.events import Action, Rule


def test_add_rule_cmd_targets_the_actions_set() -> None:
    block = Rule(ip=IPv4Address("172.30.0.200"), action=Action.BLOCK, ttl_s=15)
    limit = Rule(ip=IPv4Address("172.30.0.200"), action=Action.RATE_LIMIT, ttl_s=60)
    assert add_rule_cmd(block) == [
        "nft",
        "add",
        "element",
        "inet",
        "gatekeeper",
        "blocked",
        "{ 172.30.0.200 timeout 15s }",
    ]
    assert add_rule_cmd(limit)[5:] == ["ratelimited", "{ 172.30.0.200 timeout 60s }"]


def test_ruleset_drops_blocked_and_rate_limits_per_source() -> None:
    rules = ruleset(rate_limit_pps=100)
    assert "ip saddr @blocked counter drop" in rules
    assert "limit rate over 100/second burst 100 packets" in rules
