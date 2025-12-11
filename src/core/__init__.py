"""
Core utilities package.

This package contains core utilities including bot context, event bus,
rounding functions, and common utilities.
"""

from src.core.bot_context import BotContext
from src.core.event_bus import EventBus, EventType, Event, Subscription
from src.core.rounding import round_price, snap_to_tick
from src.core.utils import now_ms, quantize, tick_to_decimals, BoundedSet, safe_call, to_int_safe

__all__ = [
    "BotContext",
    "EventBus",
    "EventType",
    "Event",
    "Subscription",
    "round_price",
    "snap_to_tick",
    "now_ms",
    "quantize",
    "tick_to_decimals",
    "BoundedSet",
    "safe_call",
    "to_int_safe",
]
