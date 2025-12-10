"""
Hyperliquid rounding helpers aligned with official docs and example.
"""

from __future__ import annotations

import math
from decimal import Decimal, ROUND_HALF_UP, getcontext

# Increase precision to avoid intermediate rounding drift
getcontext().prec = 18

def round_price(px: float, sz_decimals: int, is_perp: bool = True) -> float:
    """
    Prices can have up to 5 significant figures, and at most
    (6 - szDecimals) decimals for perps, (8 - szDecimals) for spot.
    If px > 100_000, round to int.
    """
    if px > 100_000:
        return round(px)
    max_decimals = (6 - sz_decimals) if is_perp else (8 - sz_decimals)
    max_decimals = max(0, max_decimals)
    sig5 = float(f"{px:.5g}")
    return round(sig5, max_decimals)


def snap_to_tick(px: float, tick: float, side: str) -> float:
    if tick <= 0:
        return px
    ticks = math.floor(px / tick) if side == "sell" else math.ceil(px / tick)
    return ticks * tick
