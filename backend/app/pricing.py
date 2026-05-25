"""Default rikishi prices derived from banzuke rank.

Prices come from the league rulebook; see `docs/spec.md` for the table.
The function is pure so it can be reused both from the API (to seed
defaults when an admin first opens the rikishi list) and from a future
re-seed admin action.
"""
from __future__ import annotations

import re

_HEAD_RANK_PRICE_PENCE = {
    "yokozuna": 3000,
    "ozeki": 2500,
    "sekiwake": 2200,
    "komusubi": 2000,
}

_MAEGASHIRA_RE = re.compile(r"^maegashira\s*(\d+)", re.IGNORECASE)


def default_price_pence(rank: str | None) -> int | None:
    """Return the default price (in pence) for `rank`, or None if unknown.

    Recognised forms (case-insensitive, trailing "East"/"West" optional):
      - "Yokozuna", "Yokozuna 1 East", "Yokozuna 1 West"  -> 3000
      - "Ozeki ..."                                       -> 2500
      - "Sekiwake ..."                                    -> 2200
      - "Komusubi ..."                                    -> 2000
      - "Maegashira N", "Maegashira N East/West"          -> (18 - N) * 100
    """
    if not rank:
        return None
    head = rank.strip().split()[0].lower()
    if head in _HEAD_RANK_PRICE_PENCE:
        return _HEAD_RANK_PRICE_PENCE[head]
    m = _MAEGASHIRA_RE.match(rank.strip())
    if m:
        n = int(m.group(1))
        if 1 <= n <= 17:
            return (18 - n) * 100
    return None
