"""
Hyperliquid rounding helpers aligned with official docs and example.

NOTE: Core implementations are in utils.py to avoid duplication.
This module re-exports them with more descriptive names and provides
thin wrappers for API compatibility.
"""

from __future__ import annotations

import math

# Re-export from core.utils to avoid circular imports
from src.core.utils import hl_round_price as round_price

__all__ = ["round_price", "snap_to_tick"]


def snap_to_tick(px: float, tick: float, side: str) -> float:
    """
    Snap price to nearest tick boundary.
    
    - sell: floor to tick (lower price)
    - buy: ceil to tick (higher price)
    
    This is a thin wrapper for backward compatibility.
    """
    if tick <= 0:
        return px
    ticks = math.floor(px / tick) if side == "sell" else math.ceil(px / tick)
    return ticks * tick
