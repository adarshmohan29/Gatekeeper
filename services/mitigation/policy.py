"""When to rate-limit or block a source. Pure functions; I/O lives in main.py."""

from __future__ import annotations

from collections.abc import Iterable

from shared.events import Action


def streak(flagged_windows: Iterable[int], window_end: int) -> int:
    """How many consecutive windows, ending at `window_end`, were flagged."""
    flagged = set(flagged_windows)
    n = 0
    while window_end - n in flagged:
        n += 1
    return n


def decide(
    streak: int, active: set[Action], allowlisted: bool, consecutive: int, escalate_after: int
) -> Action | None:
    """Rate-limit after `consecutive` flagged windows; block after `escalate_after` more.

    Returns None when nothing should change, which is what makes replays harmless.
    """
    if allowlisted or streak < consecutive or Action.BLOCK in active:
        return None
    wanted = Action.BLOCK if streak >= consecutive + escalate_after else Action.RATE_LIMIT
    return None if wanted in active else wanted
