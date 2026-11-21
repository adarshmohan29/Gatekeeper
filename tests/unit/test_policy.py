import pytest

from services.mitigation.policy import decide, streak
from shared.events import Action

K, E = 3, 5  # rate-limit after 3 flagged windows, block after 3 + 5


@pytest.mark.parametrize(
    ("flagged", "end", "expected"),
    [
        ([], 100, 0),
        ([100], 100, 1),
        ([98, 99, 100], 100, 3),
        ([97, 99, 100], 100, 2),  # 98 missing: the streak restarts at 99
        ([98, 99], 100, 0),  # this window wasn't flagged
        ([99, 100, 101], 100, 2),  # later windows don't count
    ],
)
def test_streak(flagged: list[int], end: int, expected: int) -> None:
    assert streak(flagged, end) == expected


@pytest.mark.parametrize(
    ("n", "active", "allowlisted", "expected"),
    [
        (K - 1, set(), False, None),  # not yet
        (K, set(), False, Action.RATE_LIMIT),  # rate-limit first
        (K, {Action.RATE_LIMIT}, False, None),  # already rate-limited: replay is a no-op
        (K + E - 1, {Action.RATE_LIMIT}, False, None),
        (K + E, {Action.RATE_LIMIT}, False, Action.BLOCK),  # persists: escalate
        (K + E, set(), False, Action.BLOCK),  # rate-limit expired mid-attack: straight to block
        (K + E, {Action.BLOCK}, False, None),  # already blocked
        (K + E, set(), True, None),  # allowlisted: never
    ],
)
def test_decide(n: int, active: set[Action], allowlisted: bool, expected: Action | None) -> None:
    assert decide(n, active, allowlisted, K, E) == expected
