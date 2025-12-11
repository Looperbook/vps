"""StrategyFactory to create pluggable strategy instances.

Start with a simple registration for the existing GridStrategy.
"""

from __future__ import annotations

from typing import Any

from src.strategy.strategy import GridStrategy


class StrategyFactory:
    _registry: dict[str, Any] = {
        "grid": GridStrategy,
    }

    @classmethod
    def create(cls, name: str, cfg, tick_sz: float, px_decimals: int, sz_decimals: int, **kwargs):
        ctor = cls._registry.get(name)
        if ctor is None:
            raise ValueError(f"unknown strategy: {name}")
        return ctor(cfg, tick_sz, px_decimals, sz_decimals, **kwargs)
