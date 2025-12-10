"""
Utility helpers.
"""

from __future__ import annotations

import math
import time
from typing import Deque, Dict, Set
from collections import deque


def now_ms() -> int:
    return int(time.time() * 1000)


def tick_to_decimals(tick: float) -> int:
    if tick <= 0:
        return 2
    s = f"{tick:.10f}".rstrip("0")
    if "." in s:
        return max(0, len(s.split(".")[1]))
    return 0


def quantize(px: float, tick: float, side: str, px_decimals: int) -> float:
    if tick <= 0:
        return round(px, px_decimals)
    ticks = math.floor(px / tick) if side == "sell" else math.ceil(px / tick)
    return round(ticks * tick, px_decimals)


def is_divisible(px: float, tick: float, tol: float = 1e-9) -> bool:
    """
    Check if price is divisible by tick within tolerance.
    """
    if tick <= 0:
        return True
    ratio = px / tick
    nearest = round(ratio)
    return abs(ratio - nearest) <= tol


def hl_round_price(px: float, sz_decimals: int, is_perp: bool = True) -> float:
    """
    Hyperliquid price rounding per docs:
    - Perps: up to 5 significant figures, and at most (6 - szDecimals) decimals.
    - Spot:   up to 5 significant figures, and at most (8 - szDecimals) decimals.
    - If px > 100_000, round to int.
    """
    if px > 100_000:
        return round(px)
    max_decimals = (6 - sz_decimals) if is_perp else (8 - sz_decimals)
    max_decimals = max(0, max_decimals)
    # 5 significant figures
    sig_5 = float(f"{px:.5g}")
    return round(sig_5, max_decimals)


class BoundedSet:
    """Dedup with bounded memory."""

    def __init__(self, maxlen: int = 5000) -> None:
        self.maxlen = maxlen
        self.deque: Deque[str] = deque(maxlen=maxlen)
        self.set: Set[str] = set()

    def add(self, key: str) -> bool:
        if key in self.set:
            return False
        if len(self.deque) == self.maxlen:
            old = self.deque.popleft()
            self.set.discard(old)
        self.deque.append(key)
        self.set.add(key)
        return True
